#!/usr/bin/env bash
#
# Everything that has to be done once, by hand, after a `kind delete` +
# `kind create`. Four components that ArgoCD cannot install for you, plus the
# order they have to run in.
#
# Per ADR 0004 (docs/adr/0004-gitops-from-phase-0.md), every component from
# Phase 1 onward is an ArgoCD Application tracked in Git. What is in here are
# the deliberate exceptions, and each is an exception for its own reason:
#
#   phase0        ArgoCD has to be installed by something before it can manage
#                 anything else, and cert-manager comes with it.
#   minio-secret  A root password in Git would be a plaintext password in
#                 history. Created out of band, never written to a tracked file.
#   flink-secret  Same credentials, copied where the Flink jobs can read them.
#   karpenter     Upstream publishes no chart and no image for the kwok
#                 provider, so there is nothing for ArgoCD to sync (spec 9.3).
#
# A `helm install` or `kubectl create secret` typed into a terminal leaves no
# trace in this repo. Nothing records that it happened, or with which version,
# once the command scrolls off your history. This file is that record.
#
# Every stage is idempotent and safe to re-run.
#
# Usage:
#   ./scripts/bootstrap.sh phase0 [cert-manager|argocd|all]
#   ./scripts/bootstrap.sh minio-secret
#   ./scripts/bootstrap.sh flink-secret
#   ./scripts/bootstrap.sh karpenter [source|kwok|image|chart|all]
#   ./scripts/bootstrap.sh all
#
# THE ORDER IS NOT INTERCHANGEABLE, which is the main reason these four live in
# one file rather than four:
#
#   phase0 ──> minio-secret ──> flink-secret
#                                   ^
#              needs personalization-blue/green to exist, and those namespaces
#              arrive via ArgoCD, so this stage waits for them rather than
#              failing the moment it is run too early.
#
#   karpenter is independent of all three and can run at any point.
#
# Piping this script to `tail` or `head` reports the pipeline's exit code, not
# the script's. Redirect instead if you care about the status.
#
set -euo pipefail

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require() { command -v "$1" >/dev/null || die "$1 not found${2:+, $2}"; }

# ===========================================================================
# phase0: cert-manager and ArgoCD
# ===========================================================================

# ---------------------------------------------------------------------------
# cert-manager
#
# Version confirmed live via Context7 against cert-manager's own install
# docs (https://cert-manager.io/docs/installation/helm), then pinned to
# whatever was actually current at install time, v1.21.1.
#
# crds.enabled=true: cert-manager's CRDs are not installed by the chart by
# default (CRDs are cluster-scoped, and Helm has historically handled
# upgrading/deleting them badly). This opts back into Helm managing them,
# fine for a fresh install like this one.
# ---------------------------------------------------------------------------
CERT_MANAGER_VERSION="v1.21.1"

install_cert_manager() {
  require helm
  info "Installing cert-manager ${CERT_MANAGER_VERSION}"
  helm upgrade --install \
    cert-manager oci://quay.io/jetstack/charts/cert-manager \
    --version "${CERT_MANAGER_VERSION}" \
    --namespace cert-manager \
    --create-namespace \
    --set crds.enabled=true
  ok "cert-manager ${CERT_MANAGER_VERSION} applied"
}

# ---------------------------------------------------------------------------
# ArgoCD
#
# ARGOCD_VERSION confirmed via `helm search repo argo/argo-cd --versions`,
# current chart is 10.3.2 (app version v3.5.0) as of this writing.
#
# server.service.type: defaults to ClusterIP. Your kind config reserves
# nodePort 30010 for ArgoCD's UI (clusters/kind/kind-cluster.yaml, the
# extraPortMappings block), which only works if the Service is actually
# type NodePort.
#
# server.service.nodePortHttp: the chart defaults this to 30080, which does
# NOT match this project's reservation, so it has to be set explicitly here
# rather than left alone. (An earlier version of this comment claimed the
# default already matched 30010 by coincidence; it doesn't, that was a stale
# port number carried over from an early draft, not the real kind config.)
#
# configs.params."server\.insecure": argocd-server terminates TLS itself by
# default, with a self-signed cert. Hitting the "http" NodePort without this
# would mean a TLS handshake where a browser expects plain HTTP, a reset or a
# cert warning, not a clean login page. This tells the server to skip TLS
# entirely. Standard practice for a local/lab cluster, not something to do
# on a cluster anyone else can reach.
#
# server.insecure contains a literal dot as part of the key itself, it's not
# nested under a `server:` map, `configs.params` is flat and one of its keys
# happens to be the string "server.insecure". The dot has to be escaped or
# Helm reads it as nesting that doesn't exist.
# ---------------------------------------------------------------------------
ARGOCD_VERSION="10.3.2"

install_argocd() {
  require helm
  [[ -n "$ARGOCD_VERSION" ]] || die "ARGOCD_VERSION is not set yet. See the comment block above install_argocd()."
  info "Installing ArgoCD ${ARGOCD_VERSION}"
  helm repo add argo https://argoproj.github.io/argo-helm >/dev/null
  helm repo update argo >/dev/null
  helm upgrade --install \
    argocd argo/argo-cd \
    --version "${ARGOCD_VERSION}" \
    --namespace argocd \
    --create-namespace \
    --set server.service.type=NodePort \
    --set server.service.nodePortHttp=30010 \
    --set configs.params."server\.insecure"=true
  ok "ArgoCD ${ARGOCD_VERSION} applied"
}

stage_phase0() {
  case "${1:-all}" in
    cert-manager) install_cert_manager ;;
    argocd)       install_argocd ;;
    all)          install_cert_manager; install_argocd ;;
    *)            die "Usage: $0 phase0 {cert-manager|argocd|all}" ;;
  esac
}

# ===========================================================================
# minio-secret
#
# Creates the storage-configuration Secret that manifests/minio/tenant.yaml
# references but deliberately does not define itself (see that file's
# header comment). Committing MINIO_ROOT_USER/MINIO_ROOT_PASSWORD to git
# would put a plaintext root password in history, same reasoning CLAUDE.md
# already applies to ArgoCD's own admin password: created out of band,
# never written into a tracked file.
#
# Safe to re-run: if the Secret already exists, this does nothing rather
# than silently generating a new password out from under an already-running
# Tenant. To rotate the password on purpose, delete the Secret yourself
# first, then re-run this, and restart the Tenant's pod so it picks up the
# change (it reads config.env at startup, not continuously).
# ===========================================================================
MINIO_NS="minio-tenant"
MINIO_SECRET="storage-configuration"
MINIO_ROOT_USER="minioadmin"

stage_minio_secret() {
  require kubectl

  if kubectl get secret "${MINIO_SECRET}" -n "${MINIO_NS}" >/dev/null 2>&1; then
    ok "${MINIO_SECRET} already exists in ${MINIO_NS}, nothing to do"
    return 0
  fi

  # Checked here, not at the top: the no-op path above needs no password, and
  # the original script refused to run at all on a host without openssl even
  # when there was nothing to generate.
  require openssl "needed to generate a password"

  info "Creating namespace ${MINIO_NS} if it doesn't exist yet"
  kubectl create namespace "${MINIO_NS}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  local password
  password="$(openssl rand -base64 24)"

  info "Creating ${MINIO_SECRET}"
  kubectl create secret generic "${MINIO_SECRET}" \
    -n "${MINIO_NS}" \
    --from-literal=config.env="export MINIO_ROOT_USER=\"${MINIO_ROOT_USER}\"
export MINIO_ROOT_PASSWORD=\"${password}\"
export MINIO_STORAGE_CLASS_STANDARD=\"EC:2\""

  ok "${MINIO_SECRET} created in ${MINIO_NS}"
  info "Root user:     ${MINIO_ROOT_USER}"
  info "Root password: ${password}"
  info "Shown here for convenience. Always retrievable again with:"
  info "  kubectl get secret ${MINIO_SECRET} -n ${MINIO_NS} -o jsonpath='{.data.config\\.env}' | base64 -d"
}

# ===========================================================================
# flink-secret
#
# Copies the MinIO root credentials into the two Flink job namespaces as a
# Secret named minio-credentials, with keys access-key and secret-key.
# The FlinkDeployment references it by name only, so no credential enters git.
#
# Copies, never generates: a fresh password here would rotate the credentials
# under a running MinIO Tenant. Re-running is a no-op per namespace.
#
# This is a one-time copy, not a mirror. A rotation of the MinIO root password
# does NOT propagate. After rotating, delete minio-credentials in both
# namespaces and re-run this. An External Secrets Operator or a reflector
# controller would keep them in step; that was rejected as another operator to
# install for one Secret.
#
# The namespaces come from ArgoCD, not from here, so on a fresh cluster this
# stage can legitimately be run before they exist. It waits rather than
# failing, which is what lets `all` run straight through.
# ===========================================================================
FLINK_SECRET="minio-credentials"
FLINK_NAMESPACES=(personalization-blue personalization-green)
FLINK_NS_WAIT_SECONDS="${FLINK_NS_WAIT_SECONDS:-180}"

wait_for_flink_namespaces() {
  local waited=0
  while :; do
    local missing=0
    for ns in "${FLINK_NAMESPACES[@]}"; do
      kubectl get namespace "${ns}" >/dev/null 2>&1 || missing=1
    done
    [[ "${missing}" -eq 0 ]] && return 0
    [[ "${waited}" -ge "${FLINK_NS_WAIT_SECONDS}" ]] && return 1
    [[ "${waited}" -eq 0 ]] && info "Waiting up to ${FLINK_NS_WAIT_SECONDS}s for ArgoCD to create ${FLINK_NAMESPACES[*]}"
    sleep 5
    waited=$((waited + 5))
  done
}

stage_flink_secret() {
  require kubectl

  wait_for_flink_namespaces \
    || die "namespaces ${FLINK_NAMESPACES[*]} still missing after ${FLINK_NS_WAIT_SECONDS}s. They come from manifests/flink/namespaces.yaml via ArgoCD; check the Application is Synced, then re-run '$0 flink-secret'."

  info "Reading ${MINIO_SECRET} from ${MINIO_NS}"
  local config_env
  config_env="$(kubectl get secret "${MINIO_SECRET}" -n "${MINIO_NS}" \
      -o jsonpath='{.data.config\.env}' 2>/dev/null | base64 -d)"

  [[ -n "${config_env}" ]] \
    || die "could not read Secret ${MINIO_SECRET} in namespace ${MINIO_NS}. Is the cluster up, and has '$0 minio-secret' been run?"

  eval "${config_env}"

  if [[ -z "${MINIO_ROOT_USER:-}" || -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
    die "${MINIO_SECRET} did not yield MINIO_ROOT_USER and MINIO_ROOT_PASSWORD"
  fi

  for ns in "${FLINK_NAMESPACES[@]}"; do
    if kubectl get secret "${FLINK_SECRET}" -n "${ns}" >/dev/null 2>&1; then
      ok "${FLINK_SECRET} already exists in ${ns}, nothing to do"
      continue
    fi
    info "Creating ${FLINK_SECRET} in ${ns}"
    kubectl create secret generic "${FLINK_SECRET}" \
      -n "${ns}" \
      --from-literal=access-key="${MINIO_ROOT_USER}" \
      --from-literal=secret-key="${MINIO_ROOT_PASSWORD}" >/dev/null
    ok "${FLINK_SECRET} created in ${ns}"
  done

  unset config_env MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_STORAGE_CLASS_STANDARD
  ok "done. The password was not printed; read it with: source scripts/minio-env.sh"
}

# ===========================================================================
# karpenter: Phase 6's Karpenter with the kwok provider
#
# charts/ is gitignored ("Third-party Helm chart, unpacked for reading only"),
# so nothing in the repo would otherwise record which upstream commit was
# installed, which kwok release, or which helm values.
#
# The four sub-stages are in dependency order and are not interchangeable.
# Each installs the thing the next one talks to: types before objects,
# controller before the resources it watches, simulator before the nodes it
# has to bring to life. See the Karpenter section of
# docs/knowledge/phase-6-autoscaling.md for why an image must be built at all.
# ===========================================================================

# ---------------------------------------------------------------------------
# Pins
#
# KARPENTER_COMMIT is the only version identifier available. sigs.k8s.io/
# karpenter cuts no releases for the kwok provider, so a tag does not exist to
# pin to. Installed 2026-09-08 from this commit.
#
# kwok itself is pinned inside hack/install-kwok.sh as KWOK_RELEASE=v0.8.0.
# This script does not set it; verify_kwok_pin() fails loudly if a future
# checkout moves it, so the number here can never quietly go stale.
#
# KARPENTER_IMAGE_TAG carries the upstream commit for the same reason
# build-image.sh tags the Flink image 0.1-<sha>: with charts/ ignored, the tag
# is the only link between a running container and the source that produced it.
# ---------------------------------------------------------------------------
KARPENTER_REPO="https://github.com/kubernetes-sigs/karpenter"
KARPENTER_COMMIT="02caf5aec2e80d44b07e1c948597b520517e385f"
KWOK_RELEASE="v0.8.0"

KARPENTER_SRC="${ROOT}/charts/karpenter"
KARPENTER_IMAGE_NAME="lab/karpenter-kwok"
KARPENTER_IMAGE_TAG="0.1-${KARPENTER_COMMIT:0:7}"
KARPENTER_IMAGE="${KARPENTER_IMAGE_NAME}:${KARPENTER_IMAGE_TAG}"
KARPENTER_DOCKERFILE="${ROOT}/manifests/karpenter/Dockerfile.controller"

KIND_CLUSTER="personalization-lab"

# kube-system, not a namespace of our choosing. Upstream's Makefile exports
# KARPENTER_NAMESPACE=kube-system and the chart's RBAC is written against it.
KARPENTER_NS="kube-system"

verify_kwok_pin() {
  local actual
  actual="$(grep -oP 'KWOK_RELEASE=\K\S+' "${KARPENTER_SRC}/hack/install-kwok.sh" || true)"
  [[ "${actual}" == "${KWOK_RELEASE}" ]] \
    || die "upstream now pins kwok ${actual}, this script says ${KWOK_RELEASE}. Update KWOK_RELEASE and re-read the install script before continuing."
}

# Shallow clones cannot check out an arbitrary commit, so this fetches the one
# commit by name. Re-running against an existing checkout resets it to the pin
# rather than leaving whatever happens to be there.
karpenter_source() {
  require git
  if [[ -d "${KARPENTER_SRC}/.git" ]]; then
    info "Updating ${KARPENTER_SRC} to ${KARPENTER_COMMIT:0:7}"
  else
    info "Cloning ${KARPENTER_REPO} at ${KARPENTER_COMMIT:0:7}"
    mkdir -p "${KARPENTER_SRC}"
    git -C "${KARPENTER_SRC}" init -q
    git -C "${KARPENTER_SRC}" remote add origin "${KARPENTER_REPO}"
  fi
  git -C "${KARPENTER_SRC}" fetch -q --depth 1 origin "${KARPENTER_COMMIT}"
  git -C "${KARPENTER_SRC}" checkout -q --force FETCH_HEAD
  verify_kwok_pin
  ok "source at $(git -C "${KARPENTER_SRC}" rev-parse --short HEAD), kwok pin ${KWOK_RELEASE} confirmed"
}

# The node simulator, and the reason Karpenter's fake nodes ever go Ready. It
# installs six Stages, which are rules that write the status a real kubelet
# would have written. Nothing executes on a kwok node; the status is asserted.
#
# That is exactly why no Flink pod may tolerate the kwok taint. A TaskManager
# scheduled there reports 1/1 Running with no JVM behind it, and the job hangs
# waiting for slots that never register.
#
# Pure curl and kubectl, no toolchain. It resolves remote kustomize refs at
# apply time, so it needs network then, not only at clone time.
karpenter_kwok() {
  require kubectl; require curl
  [[ -x "${KARPENTER_SRC}/hack/install-kwok.sh" ]] || die "run '$0 karpenter source' first"
  verify_kwok_pin
  info "Installing kwok ${KWOK_RELEASE}"
  ( cd "${KARPENTER_SRC}" && ./hack/install-kwok.sh )
  kubectl wait --for=condition=Ready pod -l app=kwok-controller \
    -n "${KARPENTER_NS}" --timeout=180s
  ok "kwok ${KWOK_RELEASE} running"
}

# Upstream builds this with ko, which needs a Go toolchain on the host. The
# Dockerfile does the same work in a container instead, so nothing is installed
# on the machine and deleting the image undoes it.
#
# The build context is the ignored checkout, so the Dockerfile lives under
# manifests/karpenter/ and is passed with -f.
#
# kind load, not a registry push: the image exists only on this machine, which
# is also why the chart's imagePullPolicy default of IfNotPresent matters. On
# Always the kubelet would try Docker Hub and fail.
karpenter_image() {
  require docker; require kind
  [[ -f "${KARPENTER_SRC}/go.mod" ]] || die "run '$0 karpenter source' first"
  kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER}" \
    || die "kind cluster ${KIND_CLUSTER} not found"
  info "Building ${KARPENTER_IMAGE}"
  docker build -f "${KARPENTER_DOCKERFILE}" -t "${KARPENTER_IMAGE}" "${KARPENTER_SRC}"
  info "Loading ${KARPENTER_IMAGE} into kind cluster ${KIND_CLUSTER}"
  kind load docker-image "${KARPENTER_IMAGE}" --name "${KIND_CLUSTER}"
  ok "${KARPENTER_IMAGE} present on every node"
}

# CRDs first and separately: the chart is installed with --skip-crds, matching
# upstream's `make apply`, so if this does not apply them nobody does. Note
# that crds/karpenter.kwok.sh_kwoknodeclasses.yaml is a symlink into
# kwok/apis/crds/, so the directory is not self-contained and cannot be copied
# elsewhere on its own.
#
# The three featureGates are not optional. deployment.yaml interpolates
# staticCapacity, nodeRepair and capacityBuffer unconditionally into the
# container env, so leaving them unset renders empty values.
#
# Resources are half what upstream's HELM_OPTS asks for (1 CPU / 1Gi request,
# 2 / 2 limit). Task 0 measured host headroom down to 2.5 GiB, and Drill F
# showed this host saturating with three TaskManagers: the autoscaler's linear
# capacity projection came in 2.4x optimistic, most likely CPU contention.
# Drill H runs Karpenter beside a loaded cluster, so the controller is sized to
# leave that margin alone. Raise it if the controller is ever OOMKilled.
#
# logLevel=debug is upstream's own default in HELM_OPTS and is kept, because
# Drill H reads provisioning decisions out of these logs.
karpenter_chart() {
  require kubectl; require helm; require docker
  [[ -d "${KARPENTER_SRC}/kwok/charts" ]] || die "run '$0 karpenter source' first"
  docker image inspect "${KARPENTER_IMAGE}" >/dev/null 2>&1 \
    || die "image ${KARPENTER_IMAGE} missing, run '$0 karpenter image' first"

  info "Applying Karpenter CRDs"
  kubectl apply -f "${KARPENTER_SRC}/kwok/charts/crds"

  info "Installing Karpenter into ${KARPENTER_NS} from ${KARPENTER_SRC}/kwok/charts"
  helm upgrade --install karpenter "${KARPENTER_SRC}/kwok/charts" \
    --namespace "${KARPENTER_NS}" \
    --skip-crds \
    --set controller.image.repository="${KARPENTER_IMAGE_NAME}" \
    --set controller.image.tag="${KARPENTER_IMAGE_TAG}" \
    --set logLevel=debug \
    --set settings.featureGates.nodeRepair=true \
    --set settings.featureGates.capacityBuffer=true \
    --set settings.featureGates.staticCapacity=true \
    --set controller.resources.requests.cpu=250m \
    --set controller.resources.requests.memory=512Mi \
    --set controller.resources.limits.cpu=1 \
    --set controller.resources.limits.memory=1Gi

  kubectl wait --for=condition=Available deploy/karpenter \
    -n "${KARPENTER_NS}" --timeout=180s
  ok "Karpenter running, built from ${KARPENTER_COMMIT:0:7}"
}

stage_karpenter() {
  case "${1:-all}" in
    source) karpenter_source ;;
    kwok)   karpenter_kwok ;;
    image)  karpenter_image ;;
    chart)  karpenter_chart ;;
    all)    karpenter_source; karpenter_kwok; karpenter_image; karpenter_chart ;;
    *)      die "Usage: $0 karpenter {source|kwok|image|chart|all}" ;;
  esac
}

# ---------------------------------------------------------------------------
case "${1:-}" in
  phase0)       stage_phase0 "${2:-all}" ;;
  minio-secret) stage_minio_secret ;;
  flink-secret) stage_flink_secret ;;
  karpenter)    stage_karpenter "${2:-all}" ;;
  all)          stage_phase0 all
                stage_minio_secret
                stage_flink_secret
                stage_karpenter all ;;
  *)            die "Usage: $0 {phase0|minio-secret|flink-secret|karpenter|all}" ;;
esac
