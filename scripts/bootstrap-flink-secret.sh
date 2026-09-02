#!/usr/bin/env bash
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
# Usage:
#   ./scripts/bootstrap-flink-secret.sh
set -euo pipefail

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

SOURCE_NS="minio-tenant"
SOURCE_SECRET="storage-configuration"
TARGET_SECRET="minio-credentials"
NAMESPACES=(personalization-blue personalization-green)

command -v kubectl >/dev/null || die "kubectl not found"

info "Reading ${SOURCE_SECRET} from ${SOURCE_NS}"
config_env="$(kubectl get secret "${SOURCE_SECRET}" -n "${SOURCE_NS}" \
    -o jsonpath='{.data.config\.env}' 2>/dev/null | base64 -d)"

if [ -z "${config_env}" ]; then
    die "could not read Secret ${SOURCE_SECRET} in namespace ${SOURCE_NS}. Is the cluster up, and has bootstrap-minio-secret.sh been run?"
fi

eval "${config_env}"

if [ -z "${MINIO_ROOT_USER:-}" ] || [ -z "${MINIO_ROOT_PASSWORD:-}" ]; then
    die "${SOURCE_SECRET} did not yield MINIO_ROOT_USER and MINIO_ROOT_PASSWORD"
fi

for ns in "${NAMESPACES[@]}"; do
    kubectl get namespace "${ns}" >/dev/null 2>&1 \
        || die "namespace ${ns} does not exist. Apply manifests/flink/namespaces.yaml first."

    if kubectl get secret "${TARGET_SECRET}" -n "${ns}" >/dev/null 2>&1; then
        ok "${TARGET_SECRET} already exists in ${ns}, nothing to do"
        continue
    fi

    info "Creating ${TARGET_SECRET} in ${ns}"
    kubectl create secret generic "${TARGET_SECRET}" \
        -n "${ns}" \
        --from-literal=access-key="${MINIO_ROOT_USER}" \
        --from-literal=secret-key="${MINIO_ROOT_PASSWORD}" >/dev/null
    ok "${TARGET_SECRET} created in ${ns}"
done

unset config_env MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_STORAGE_CLASS_STANDARD

ok "done. The password was not printed; read it with: source scripts/minio-env.sh"
