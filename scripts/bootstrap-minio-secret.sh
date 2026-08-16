#!/usr/bin/env bash
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
#
# Usage:
#   ./scripts/bootstrap-minio-secret.sh
#
set -euo pipefail

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

NAMESPACE="minio-tenant"
SECRET_NAME="storage-configuration"
ROOT_USER="minioadmin"

command -v kubectl >/dev/null || die "kubectl not found"
command -v openssl >/dev/null || die "openssl not found, needed to generate a password"

if kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  ok "${SECRET_NAME} already exists in ${NAMESPACE}, nothing to do"
  exit 0
fi

info "Creating namespace ${NAMESPACE} if it doesn't exist yet"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

ROOT_PASSWORD="$(openssl rand -base64 24)"

info "Creating ${SECRET_NAME}"
kubectl create secret generic "${SECRET_NAME}" \
  -n "${NAMESPACE}" \
  --from-literal=config.env="export MINIO_ROOT_USER=\"${ROOT_USER}\"
export MINIO_ROOT_PASSWORD=\"${ROOT_PASSWORD}\"
export MINIO_STORAGE_CLASS_STANDARD=\"EC:2\""

ok "${SECRET_NAME} created in ${NAMESPACE}"
info "Root user:     ${ROOT_USER}"
info "Root password: ${ROOT_PASSWORD}"
info "Shown here for convenience. Always retrievable again with:"
info "  kubectl get secret ${SECRET_NAME} -n ${NAMESPACE} -o jsonpath='{.data.config\\.env}' | base64 -d"
