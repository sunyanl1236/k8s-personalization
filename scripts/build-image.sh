#!/usr/bin/env bash
#
# Builds the pipeline image and loads it into every kind node.
#
# Usage:
#   ./scripts/build-image.sh
#
# Prints the full image tag on the last line. Paste that into
# manifests/flink/blue/flinkdeployment.yaml as spec.image.
set -euo pipefail

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME="lab/personalization-pipeline"
KIND_CLUSTER="personalization-lab"
JAR="apps/pipeline/build/libs/pipeline-all.jar"

cd "${REPO_ROOT}"

command -v docker >/dev/null || die "docker not found"
command -v kind   >/dev/null || die "kind not found"
command -v git    >/dev/null || die "git not found"

kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER}" \
  || die "kind cluster ${KIND_CLUSTER} not found. Run ./scripts/session-start.sh first."

SHA="$(git rev-parse --short HEAD)"
SUFFIX=""
if [[ -n "$(git status --porcelain)" ]]; then
  SUFFIX="-dirty"
  warn "working tree is dirty; tagging ${SHA}${SUFFIX}"
  warn "the image will not correspond to any commit until you commit and rebuild"
fi
TAG="0.1-${SHA}${SUFFIX}"
IMAGE="${IMAGE_NAME}:${TAG}"

info "Building the shadow jar"
apps/gradlew -p apps :pipeline:shadowJar

[[ -f "${JAR}" ]] || die "${JAR} was not produced"

info "Building ${IMAGE}"
docker build -t "${IMAGE}" apps/pipeline

info "Verifying both files landed in the image"
docker run --rm --entrypoint sh "${IMAGE}" -c \
  'test -f /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-2.2.0.jar' \
  || die "the s3 plugin is not in /opt/flink/plugins/s3-fs-hadoop/"
docker run --rm --entrypoint sh "${IMAGE}" -c \
  'test -f /opt/flink/usrlib/pipeline.jar' \
  || die "the job jar is not in /opt/flink/usrlib/"
ok "plugin and job jar both present"

info "Loading ${IMAGE} into kind cluster ${KIND_CLUSTER}"
kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}"

info "Confirming the image reached every node"
for node in $(kind get nodes --name "${KIND_CLUSTER}" | grep -- '-worker'); do
  for attempt in $(seq 1 15); do
    if docker exec "${node}" crictl images 2>/dev/null | grep -q "${TAG}"; then
      ok "${node}"
      continue 2
    fi
    sleep 2
  done
  die "${IMAGE} is missing on ${node} after 30s"
done

ok "done"
printf '%s\n' "${IMAGE}"
