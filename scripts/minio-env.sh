#!/usr/bin/env bash
#
# Exports MINIO_ACCESS_KEY and MINIO_SECRET_KEY for the Phase 3 pipeline,
# read from the storage-configuration Secret that bootstrap-minio-secret.sh
# created. Nothing is written to disk, same no-durable-secrets reasoning
# README.md already applies to ArgoCD's admin password.
#
# MinIO calls them root user and root password. The S3 protocol calls them
# access key and secret key. Same two strings, two names.
#
# MUST be sourced, not executed. A child process cannot set its parent's
# environment, so running this does nothing useful:
#
#   source scripts/minio-env.sh
#
# Deliberately no `set -euo pipefail`. Those options would leak into the
# interactive shell that sources this and stay there for the rest of the day.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    printf 'error: source this script, do not execute it:\n\n    source %s\n\n' "$0" >&2
    exit 1
fi

_minio_config_env=$(kubectl get secret storage-configuration -n minio-tenant \
    -o jsonpath='{.data.config\.env}' 2>/dev/null | base64 -d)

if [ -z "${_minio_config_env}" ]; then
    printf 'error: could not read Secret storage-configuration in namespace minio-tenant.\n' >&2
    printf '       is the cluster up, and has bootstrap-minio-secret.sh been run?\n' >&2
    unset _minio_config_env
    return 1
fi

eval "${_minio_config_env}"

export MINIO_ACCESS_KEY="${MINIO_ROOT_USER}"
export MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD}"

unset _minio_config_env MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_STORAGE_CLASS_STANDARD

printf 'MINIO_ACCESS_KEY and MINIO_SECRET_KEY exported for this shell\n'
