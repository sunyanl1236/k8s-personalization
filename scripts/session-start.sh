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
#
# The sg branch has to flatten the arguments back into a single string for
# `sg -c`, so each one is requoted with %q first. Plain "$*" would join them
# on spaces and destroy the quoting, which silently mangles any argument that
# itself contains spaces, such as the `sh -c '...'` reconciliation commands
# further down.
d() {
  if (( NEEDS_SG )); then
    sg docker -c "docker $(printf '%q ' "$@")"
  else
    docker "$@"
  fi
}

# Block until a started container can actually accept `docker exec`, and its
# systemd is far enough along to service `systemctl`. `docker start` returns
# as soon as the container's PID 1 exists, which is well before the node's
# init has brought kubelet's unit into being.
wait_for_node_exec() {
  local name="$1" i=0
  until d exec "$name" systemctl is-enabled kubelet >/dev/null 2>&1; do
    (( i++ < 60 )) || { warn "${name} did not become exec-ready in 60s; skipping its reconciliation."; return 1; }
    sleep 1
  done
  return 0
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
#
# A full host/WSL restart can reshuffle more than one node's IP at once, and
# the reshuffle can form a cycle: node A's pinned address is currently held
# by node B, B's pinned address is currently held by node C, and so on back
# to A. Fixing nodes one at a time (disconnect, immediately reconnect) fails
# on a cycle, because the address a node wants is still occupied by a node
# further along the chain that has not been freed yet ("Address already in
# use"). So this runs in two passes over all mismatched nodes: first every
# one of them is disconnected, freeing every contested address at once, then
# every one of them is reconnected to its pinned address. No reconnect in
# the second pass can collide with a stale holder from this run, because all
# of them were vacated in the first pass.
# ---------------------------------------------------------------------------
recorded_ip_for() {
  local name="$1"
  [[ -f "$NODE_IPS_FILE" ]] || return 0
  grep -m1 "^${name}=" "$NODE_IPS_FILE" 2>/dev/null | cut -d= -f2 || true
}

current_ip_for() {
  local name="$1"
  d inspect -f "{{.NetworkSettings.Networks.${KIND_NETWORK}.IPAddress}}" "$name" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# 2c. Kubelet / static-pod-manifest reconciliation
#
# The IP-pinning above fixes the container's Docker-level network address,
# but kind's own node bootstrap has usually already run by that point. It
# bakes whatever IP Docker handed the container at ITS boot into
# /var/lib/kubelet/kubeadm-flags.env (--node-ip), and on control-plane nodes
# also into the etcd and kube-apiserver static pod manifests
# (--listen-peer-urls, --advertise-address, etc). If that boot-time IP gets
# corrected afterward by the pinning step above, kubelet and its static pods
# are left pointing at an address the node's interface no longer has: etcd
# fails to bind ("cannot assign requested address") and kube-apiserver never
# comes up. Only nodes that actually got reconnected above can have this
# problem, so this only runs for those.
#
# A plain edit of a file already in /etc/kubernetes/manifests is not
# reliably picked up by kubelet's own file watcher, so a kubelet restart is
# required to make the fix take effect, not just the file rewrite.
# ---------------------------------------------------------------------------
reconcile_kubelet_ip() {
  local name="$1" correct="$2"
  local flag_wrong manifest_wrong changed=0

  flag_wrong=$(d exec "$name" grep -oE -- '--node-ip=[0-9.]+' /var/lib/kubelet/kubeadm-flags.env 2>/dev/null | cut -d= -f2 || true)
  if [[ -n "$flag_wrong" && "$flag_wrong" != "$correct" ]]; then
    d exec "$name" sed -i -E "s/node-ip=[0-9.]+/node-ip=${correct}/" /var/lib/kubelet/kubeadm-flags.env
    changed=1
  fi

  if [[ "$name" == *control-plane* ]]; then
    manifest_wrong=$(d exec "$name" grep -oE -- '--advertise-address=[0-9.]+' /etc/kubernetes/manifests/kube-apiserver.yaml 2>/dev/null | cut -d= -f2 || true)
    if [[ -n "$manifest_wrong" && "$manifest_wrong" != "$correct" ]]; then
      d exec "$name" sed -i "s#${manifest_wrong}#${correct}#g" /etc/kubernetes/manifests/etcd.yaml /etc/kubernetes/manifests/kube-apiserver.yaml
      changed=1
    fi
  fi

  if (( changed )); then
    warn "${name} had a stale IP baked into kubelet/static-pod config. Restarting kubelet to apply the fix."
    d exec "$name" systemctl restart kubelet
    ok "${name} kubelet restarted"
  fi
}

# ---------------------------------------------------------------------------
# 2d. Load balancer routing
#
# The external load balancer's own entrypoint resets its Envoy config to
# zero clusters and zero listeners on every container start: its startup
# script overwrites /home/envoy/cds.yaml and /home/envoy/lds.yaml with empty
# resource lists before launching Envoy, and nothing else in this project
# repopulates them. Left alone, the load balancer comes up after every
# restart with no route to any control-plane node, and kubectl fails with a
# bare EOF talking to it. This regenerates both files from the current
# pinned control-plane IPs and forces Envoy to reload, skipping the reload
# if the routes are already correct.
# ---------------------------------------------------------------------------
LB_NAME="${CLUSTER_NAME}-external-load-balancer"

reconcile_load_balancer() {
  d ps -a --filter "name=^${LB_NAME}\$" --format '{{.Names}}' 2>/dev/null | grep -q . || return 0

  # The load balancer runs Envoy directly, with no init system, so it needs a
  # plain exec probe rather than the systemd-based wait the nodes use.
  local w=0
  until d exec "$LB_NAME" true >/dev/null 2>&1; do
    (( w++ < 60 )) || { warn "Load balancer did not become exec-ready in 60s; skipping its routing."; return 0; }
    sleep 1
  done

  local cp_ips=() n ip missing=0
  for n in "${nodes[@]}"; do
    [[ "$n" == *control-plane* ]] || continue
    ip=$(recorded_ip_for "$n")
    [[ -n "$ip" ]] && cp_ips+=("$ip")
  done
  (( ${#cp_ips[@]} > 0 )) || { warn "No control-plane IPs recorded yet; skipping load balancer routing."; return 0; }

  local current_cds
  current_cds=$(d exec "$LB_NAME" cat /home/envoy/cds.yaml 2>/dev/null || true)
  for ip in "${cp_ips[@]}"; do
    grep -q "$ip" <<<"$current_cds" || { missing=1; break; }
  done
  if (( ! missing )); then
    ok "Load balancer already routes to all ${#cp_ips[@]} control-plane node(s)"
    return 0
  fi

  local tmp_cds tmp_lds
  tmp_cds=$(mktemp)
  tmp_lds=$(mktemp)
  trap 'rm -f "$tmp_cds" "$tmp_lds"' RETURN

  {
    echo 'resources:'
    echo '- "@type": type.googleapis.com/envoy.config.cluster.v3.Cluster'
    echo '  name: control_plane_backends'
    echo '  connect_timeout: 5s'
    echo '  type: STATIC'
    echo '  lb_policy: ROUND_ROBIN'
    echo '  load_assignment:'
    echo '    cluster_name: control_plane_backends'
    echo '    endpoints:'
    echo '    - lb_endpoints:'
    for ip in "${cp_ips[@]}"; do
      echo '      - endpoint:'
      echo '          address:'
      echo '            socket_address:'
      echo "              address: ${ip}"
      echo '              port_value: 6443'
    done
  } > "$tmp_cds"

  cat > "$tmp_lds" <<'YAML'
resources:
- "@type": type.googleapis.com/envoy.config.listener.v3.Listener
  name: listener_apiserver
  address:
    socket_address:
      address: 0.0.0.0
      port_value: 6443
  filter_chains:
  - filters:
    - name: envoy.filters.network.tcp_proxy
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
        stat_prefix: apiserver_tcp
        cluster: control_plane_backends
YAML

  info "Repopulating load balancer routes to the control plane"
  d cp "$tmp_cds" "${LB_NAME}:/home/envoy/cds.yaml"
  d cp "$tmp_lds" "${LB_NAME}:/home/envoy/lds.yaml"
  d exec "$LB_NAME" sh -c 'kill -9 $(pgrep -f "envoy -c") 2>/dev/null || true'
  ok "Load balancer routes updated (${#cp_ips[@]} control-plane backend(s): ${cp_ips[*]})"
}

# ---------------------------------------------------------------------------
# 2e. Control-plane kubeconfig endpoints
#
# On control-plane nodes, kubeadm writes the node's own IP as the apiserver
# endpoint into three kubeconfigs: scheduler.conf, controller-manager.conf
# and kubelet.conf. (admin.conf and the worker nodes' kubelet.conf already
# use the load balancer's hostname instead, which is why workers are never
# affected by this.) After an IP reshuffle those three point at an address
# that now belongs to some other node, so kube-scheduler and
# kube-controller-manager run but can never reach an apiserver. They stop
# renewing their leader leases, and the visible symptom is not an error
# anywhere: pods simply stay Pending forever with no events, and deleted
# pods stay Terminating forever, because nothing is scheduling or garbage
# collecting.
#
# Rewriting these to the load balancer's DNS name rather than the corrected
# IP fixes it permanently instead of per-reshuffle: every apiserver serving
# certificate already carries that name as a SAN, so TLS still verifies, and
# a name cannot go stale the way an address does. This is also what kubeadm
# itself does whenever a control-plane endpoint is configured.
#
# The components hold these files open, and editing a file that a static pod
# merely mounts does not change the pod manifest, so kubelet will not restart
# them on its own. Both have to be bounced explicitly for the change to take.
# ---------------------------------------------------------------------------
normalize_control_plane_kubeconfigs() {
  local n changed_any=0

  for n in "${nodes[@]}"; do
    [[ "$n" == *control-plane* ]] || continue
    wait_for_node_exec "$n" || continue

    # Only rewrite entries that point at a bare IP; a hostname is already correct.
    local stale
    stale=$(d exec "$n" sh -c 'grep -lE "server: https://[0-9.]+:6443" /etc/kubernetes/controller-manager.conf /etc/kubernetes/kubelet.conf /etc/kubernetes/scheduler.conf 2>/dev/null' || true)
    [[ -n "$stale" ]] || continue

    warn "${n} has kubeconfigs pointing at a raw IP. Repointing them at ${LB_NAME}."
    d exec "$n" sed -i -E "s#server: https://[0-9.]+:6443#server: https://${LB_NAME}:6443#" \
      /etc/kubernetes/controller-manager.conf \
      /etc/kubernetes/kubelet.conf \
      /etc/kubernetes/scheduler.conf
    d exec "$n" systemctl restart kubelet
    d exec "$n" sh -c 'for c in kube-scheduler kube-controller-manager; do id=$(crictl ps --name "$c" -q 2>/dev/null | head -1); [ -n "$id" ] && crictl stop "$id" >/dev/null 2>&1; done' || true
    ok "${n} kubeconfigs repointed; kubelet, scheduler and controller-manager bounced"
    changed_any=1
  done

  (( changed_any )) && echo "    Leader election needs ~30s to settle after this."
  return 0
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
to_reconnect=()
for n in "${nodes[@]}"; do
  # Skip the external load balancer: nothing else's certs reference its IP.
  [[ "$n" == *external-load-balancer ]] && continue

  recorded=$(recorded_ip_for "$n")
  if [[ -z "$recorded" ]]; then
    current=$(current_ip_for "$n")
    if [[ -n "$current" ]]; then
      echo "${n}=${current}" >> "$NODE_IPS_FILE"
      ok "Pinned ${n} to ${current} (first time seen)"
    fi
    continue
  fi

  current=$(current_ip_for "$n")
  if [[ "$current" != "$recorded" ]]; then
    warn "${n} came back as '${current:-<none>}', pinned IP is ${recorded}. Freeing it for reconnect."
    d network disconnect --force "$KIND_NETWORK" "$n" >/dev/null 2>&1 || true
    to_reconnect+=("$n")
  fi
done

for n in "${to_reconnect[@]}"; do
  recorded=$(recorded_ip_for "$n")
  d network connect --ip "$recorded" "$KIND_NETWORK" "$n"
  ok "${n} reconnected at ${recorded}"
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
# 3b. Post-start reconciliation
#
# Everything from here on reads or writes files INSIDE the node containers,
# so it can only run once they are actually running: `docker exec` against a
# stopped container fails outright. The IP pinning above deliberately runs
# before the start, because `docker network connect` is what has to happen
# while the container is still down. These steps are the opposite way round.
#
# Both orderings matter for a second reason. When the nodes are started from
# stopped, kind's own entrypoint re-derives the node's addresses at boot, so
# it sees the already-corrected IP and writes it correctly. When the daemon
# instead auto-started the nodes before this script ran, they booted on the
# wrong address and baked it in. Running these checks after the start covers
# both cases with one code path.
# ---------------------------------------------------------------------------
if (( ${#to_reconnect[@]} > 0 )); then
  info "Reconciling node config against corrected IPs"
  for n in "${to_reconnect[@]}"; do
    wait_for_node_exec "$n" || continue
    reconcile_kubelet_ip "$n" "$(recorded_ip_for "$n")"
  done
fi

reconcile_load_balancer
normalize_control_plane_kubeconfigs

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
