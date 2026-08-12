#!/usr/bin/env bash
#
# Start (or stop) a work session on the Flink on Kubernetes Personalization Lab.
#
# Docker's boot-time autostart is deliberately disabled on this machine. The
# daemon is only needed while working on this project, so it is started on
# demand here rather than on every WSL boot.
#
# One-time setup, already done:
#   sudo systemctl disable docker.service
#
# Only docker.service needed disabling on this machine. docker.socket and
# containerd.service were never enabled to begin with; they are pulled in as
# dependencies at runtime, not started at boot.
#
# This script also pins each node container to its first-seen IP address
# (recorded in clusters/kind/.node-ips, host-specific, gitignored) and
# restores it on later runs if Docker hands the container a different one.
# See the "IP pinning" comment block below for why that matters.
#
# Usage:
#   ./scripts/session-start.sh           start Docker, wake the cluster, report status
#   ./scripts/session-start.sh --stop    stop Docker when finished for the day
#
set -euo pipefail

CLUSTER_NAME="personalization-lab"
DAEMON_TIMEOUT=60
KIND_NETWORK="kind"
NODE_IPS_FILE="$(dirname "${BASH_SOURCE[0]}")/../clusters/kind/.node-ips"

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# --stop: shut the daemon down for the day.
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--stop" ]]; then
  info "Stopping the Docker daemon"
  # docker.socket must go too. On its own it would socket-activate the daemon
  # again the moment anything touched /var/run/docker.sock.
  sudo systemctl stop docker.service docker.socket
  ok "Docker stopped."
  echo "    The kind node containers are stopped, not deleted. Your cluster"
  echo "    and its state survive. Re-run this script to bring it all back."
  exit 0
fi

# ---------------------------------------------------------------------------
# 1. Daemon
# ---------------------------------------------------------------------------
if systemctl is-active --quiet docker; then
  ok "Docker daemon already running"
else
  info "Starting the Docker daemon"
  sudo systemctl start docker
fi

info "Waiting for the daemon to accept connections"
deadline=$(( SECONDS + DAEMON_TIMEOUT ))
until sudo docker info >/dev/null 2>&1; do
  (( SECONDS < deadline )) || die "Daemon did not become ready in ${DAEMON_TIMEOUT}s. Try: sudo journalctl -u docker -n 50"
  sleep 1
done
ok "Daemon is up ($(sudo docker version --format '{{.Server.Version}}'))"

# ---------------------------------------------------------------------------
# 2. Can THIS shell talk to the daemon without sudo?
#
# Two different questions, and they have different answers:
#   - is the user in the docker group?          (permanent, /etc/group)
#   - has THIS shell activated that membership? (only at login)
# A shell opened before `usermod -aG` was run will fail the second check while
# passing the first. That produces a `docker info` with a populated ClientInfo
# and a completely blank server block, which is confusing if you don't know why.
# ---------------------------------------------------------------------------
NEEDS_SG=0
if docker info >/dev/null 2>&1; then
  ok "This shell can reach the daemon directly"
elif getent group docker | grep -qw "$USER"; then
  NEEDS_SG=1
  warn "You are in the 'docker' group, but THIS shell predates that change."
  echo "    Group membership is only picked up at login. Either open a new"
  echo "    terminal, or prefix commands for the rest of this session:"
  echo
  echo "        sg docker -c \"kind create cluster --config clusters/kind/kind-cluster.yaml\""
  echo
else
  NEEDS_SG=1
  warn "Your user is not in the 'docker' group."
  echo "    Either prefix every docker/kind command with:  sg docker -c \"...\""
  echo "    or grant permanent access with:  sudo usermod -aG docker \$USER"
  echo "    (the second is machine-wide and outlives this project)"
  echo
fi

# Run a docker command whichever way works in this shell.
d() {
  if (( NEEDS_SG )); then sg docker -c "docker $*"; else docker "$@"; fi
}

# ---------------------------------------------------------------------------
# 2b. IP pinning
#
# kubeadm bakes each control-plane node's container IP into etcd's peer
# certificates exactly once, at `kind create cluster` time. Docker does not
# guarantee a container gets the same IP back after the daemon restarts, and
# a full host/WSL restart in particular can reshuffle every node's address at
# once. When that happens, every etcd member's TLS handshake with every other
# member fails ("certificate is valid for X, not Y"), etcd can never re-form
# quorum, and kube-apiserver crash-loops forever waiting on a storage backend
# that will never become ready.
#
# kind has no config field for pinning node IPs (checked its docs directly;
# none exists). This works around that at the Docker layer instead: the first
# time a node is ever seen, its IP is recorded. On every later run, before a
# stopped node is started, its network attachment is forced back onto its
# recorded IP if Docker would otherwise hand it something different.
#
# Not a kind-endorsed fix. kind's own documented answer to "the host
# environment changed" is "delete and recreate the cluster." This is a
# best-effort workaround sitting entirely in our own script, between Docker
# and kind.
# ---------------------------------------------------------------------------
ensure_node_ip_pinned() {
  local name="$1"
  local recorded current

  recorded=""
  if [[ -f "$NODE_IPS_FILE" ]]; then
    recorded=$(grep -m1 "^${name}=" "$NODE_IPS_FILE" 2>/dev/null | cut -d= -f2 || true)
  fi

  current=$(d inspect -f "{{.NetworkSettings.Networks.${KIND_NETWORK}.IPAddress}}" "$name" 2>/dev/null || true)

  if [[ -z "$recorded" ]]; then
    if [[ -n "$current" ]]; then
      echo "${name}=${current}" >> "$NODE_IPS_FILE"
      ok "Pinned ${name} to ${current} (first time seen)"
    fi
    return
  fi

  if [[ "$current" != "$recorded" ]]; then
    warn "${name} came back as '${current:-<none>}', pinned IP is ${recorded}. Forcing it back."
    d network disconnect --force "$KIND_NETWORK" "$name" >/dev/null 2>&1 || true
    d network connect --ip "$recorded" "$KIND_NETWORK" "$name"
    ok "${name} reconnected at ${recorded}"
  fi
}

# ---------------------------------------------------------------------------
# 3. Cluster
#
# kind node containers do not reliably come back on their own after the daemon
# restarts, so stopped ones are started explicitly here.
# ---------------------------------------------------------------------------
mapfile -t nodes < <(d ps -a --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}" --format '{{.Names}}' 2>/dev/null || true)

if (( ${#nodes[@]} == 0 )); then
  info "No '${CLUSTER_NAME}' cluster exists yet. Creating it."
  if (( NEEDS_SG )); then
    sg docker -c "kind create cluster --config clusters/kind/kind-cluster.yaml --wait 180s"
  else
    kind create cluster --config clusters/kind/kind-cluster.yaml --wait 180s
  fi
  ok "Cluster created."
  mapfile -t nodes < <(d ps -a --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}" --format '{{.Names}}' 2>/dev/null || true)
fi

info "Checking node IPs against pinned addresses"
for n in "${nodes[@]}"; do
  # Skip the external load balancer: nothing else's certs reference its IP.
  [[ "$n" == *external-load-balancer ]] && continue
  ensure_node_ip_pinned "$n"
done

mapfile -t stopped < <(d ps -a --filter "label=io.x-k8s.kind.cluster=${CLUSTER_NAME}" --filter "status=exited" --format '{{.Names}}' 2>/dev/null || true)

if (( ${#stopped[@]} > 0 )); then
  info "Starting ${#stopped[@]} stopped node container(s)"
  d start "${stopped[@]}" >/dev/null
  ok "Node containers started. The control plane needs ~30s to settle."
else
  ok "All ${#nodes[@]} node containers already running"
fi

# ---------------------------------------------------------------------------
# 4. Report
# ---------------------------------------------------------------------------
if command -v kubectl >/dev/null 2>&1; then
  info "Cluster status"
  if kubectl get nodes -L topology.kubernetes.io/zone 2>/dev/null; then
    :
  else
    warn "kubectl cannot reach the API server yet."
    echo "    If the containers just started, wait ~30s and try:"
    echo "        kubectl get nodes -L topology.kubernetes.io/zone"
  fi
fi
