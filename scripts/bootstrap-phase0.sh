#!/usr/bin/env bash
#
# Phase 0's two GitOps-exception installs: cert-manager and ArgoCD.
#
# Per ADR 0004 (docs/adr/0004-gitops-from-phase-0.md), every component in
# this project from Phase 1 onward is installed as an ArgoCD Application,
# tracked in Git. These two are the deliberate exceptions: ArgoCD has to be
# installed by something before it can manage anything else, and cert-manager
# is bootstrapped alongside it via plain Helm, before GitOps exists to take
# over.
#
# A `helm install` run from a terminal leaves no trace in this repo, nothing
# records that it happened, or with which version, once the command scrolls
# off your history. This script exists so that record exists somewhere
# durable. It uses `helm upgrade --install`, which is safe to re-run: it
# does nothing if the release already matches, and applies the same
# versions/values again if you ever need to rebuild the cluster from scratch.
#
# Usage:
#   ./scripts/bootstrap-phase0.sh cert-manager
#   ./scripts/bootstrap-phase0.sh argocd      # not ready yet, see below
#   ./scripts/bootstrap-phase0.sh all
#
set -euo pipefail

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

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

# ---------------------------------------------------------------------------
case "${1:-}" in
  cert-manager) install_cert_manager ;;
  argocd)       install_argocd ;;
  all)          install_cert_manager; install_argocd ;;
  *)            die "Usage: $0 {cert-manager|argocd|all}" ;;
esac
