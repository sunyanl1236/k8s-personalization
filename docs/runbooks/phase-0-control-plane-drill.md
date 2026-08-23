# Phase 0 Drill: control-plane node failure

Date: 2026-08-10
Cluster: `personalization-lab`, kind v0.32.0, Kubernetes v1.34.8, 3 control-plane
nodes + 3 workers

This is the Drill that closes out Phase 0. Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

The thing being tested is the one requirement that survived every revision of
the design spec: a self-managed, multi-AZ, highly available control plane. This
Drill is the only way to actually prove that, as opposed to just believing it
because three pods say `Ready`.

## What's actually being exercised

Three mechanisms, stacked:

1. **etcd quorum.** 3 members, quorum is 2. Stopping 1 should leave the
   remaining 2 able to keep serving reads and accepting writes.
2. **HAProxy failover.** kind's multi-control-plane mode runs an HAProxy
   container in front of all 3 API servers. Every node's kubeconfig points at
   HAProxy, not at any one API server directly, so killing one shouldn't be
   visible to a client at all.
3. **kubelet's independence from the control plane.** Pods that are already
   scheduled keep running whether or not the control plane is reachable.
   kubelet doesn't ask permission to keep a container alive.

The Drill is designed to make all three observable in one pass.

## Procedure
**0. `kubectl get nodes`**

You should see all nodes in READY state

**1. `docker stop personalization-lab-control-plane`**

Expected behavior: returns immediately. The container stops; its etcd member,
API server, scheduler, and controller-manager all go down with it, since they
run as static pods inside that one container.

Rationale: this is the failure being injected. `docker stop`, not `kubectl
delete pod`, because kubelet's static pod mechanism reads
`/etc/kubernetes/manifests` on disk and would silently recreate a deleted API
server pod as long as the node itself is alive. Only stopping the node
container removes the process.

**2. `kubectl get nodes`**

Expected behavior: still returns, within normal latency. The stopped node
shows `NotReady` after ~40s (`node-monitor-grace-period` default). The other 5
nodes show `Ready`.

Rationale: proves HAProxy routed this request to one of the 2 surviving API
servers instead of failing. If this command hangs or errors, HAProxy failover
isn't working as designed, that would be a real Phase 0 finding, not an
expected outcome.

**3. `kubectl create deployment drill-check --image=nginx`**

Expected behavior: returns `deployment.apps/drill-check created`.

Rationale: read-only success (step 2) isn't sufficient proof. The API server
can sometimes serve cached/stale reads even under stress. A **write** forces a
`kube-apiserver` → `etcd` round trip and must reach 2-of-3 quorum to succeed.
This is the step that actually proves quorum survived, not just that a socket
accepted a connection.

**4. `kubectl get deployment drill-check --image=nginx`**

Expected behavior: shows `drill-check`, `READY 1/1` within a few seconds.

Rationale: confirms the write in step 3 wasn't just accepted, it was durably
persisted and reconciled: the deployment controller saw it, created a
ReplicaSet, the scheduler placed a pod, kubelet on some worker started it.

**5. `docker start personalization-lab-control-plane`**

Expected behavior: returns immediately. Container restarts; kubelet inside it
re-reads the static pod manifests and brings etcd, apiserver, scheduler, and
controller-manager back up.

Rationale: recovery half of the Drill. A Drill without a recovery step is just
downtime, not a Drill (`CONTEXT.md`: "observe recovery").

**6. `kubectl get nodes`**

Expected behavior: the previously-stopped node returns to `Ready` within
20-30s.

Rationale: confirms the etcd member rejoined and caught up via Raft log
replication, and the kubelet re-registered. If this node is stuck `NotReady`
indefinitely, that's a real finding worth investigating, not expected.

**7. `kubectl delete deployment drill-check`**

Expected behavior: returns `deployment.apps "drill-check" deleted`.

Rationale: cleanup. `drill-check` was created imperatively, with no YAML and no
ArgoCD `Application` tracking it (see
[ADR 0004](../adr/0004-gitops-from-phase-0.md)). Nothing in the GitOps pipeline
knows it exists, so nothing will ever clean it up automatically. Left alone,
it's a permanent, silent orphan in the cluster.

## What would have falsified Phase 0

Worth stating explicitly, since a Drill that can't fail isn't proving anything:

- Step 2 hanging or erroring instead of returning `NotReady` for one node.
- Step 3 timing out or returning a quorum/leader-election error.
- Step 6 never recovering, requiring manual etcd member removal and re-add.

None of these happened, per the "finished the drill" report, but Phase 0's
completion doesn't rest on "the commands ran." It rests on "the failure and the
recovery matched what the design was supposed to guarantee."

## Observed result

_Not yet recorded. Paste the actual terminal output from the run here (or send
it over and it'll be added) to turn this from a predicted-behavior runbook into
a record of what this specific cluster actually did._
