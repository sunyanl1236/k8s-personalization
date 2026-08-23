# Implementation phases

Date: 2026-08-10
Status: ready to implement
Derives from: [design spec](../specs/2026-07-25-flink-k8s-personalization-design.md)

Supersedes the spec's six-step "Build order" section. The spec's Phase 3 carried
roughly a third of the total hours and bundled seven unrelated concerns. It is
split here. Decisions that reshaped the order are recorded as
[ADRs 0001 to 0006](../../adr/).

Budget: 74 hours against the spec's 55 to 80 hour envelope.

| # | Phase | Hours |
|---|---|---|
| 0 | Cluster floor | 7 |
| 1 | Data platform | 5 |
| 2 | Domain model and generator | 4 |
| 3 | Core pipeline | 11 |
| 4 | Advanced Flink | 13 |
| 5 | Operator and HA | 9 |
| 6 | Autoscaling (6a native, 6b standalone) | 11 |
| 7 | Blue/green and OTel | 9 |
| 8 | Observability and docs | 5 |

---

## Phase 0: Cluster floor (7h)

Build a `kind` cluster with 3 control-plane nodes and zone-labelled workers.
Install cert-manager and ArgoCD. ArgoCD is bootstrapped once with Helm, then
manages itself and everything after via an app-of-apps root Application
([ADR 0004](../../adr/0004-gitops-from-phase-0.md)).

Set `automated.selfHeal: false` on Applications now. The Phase 5 drift drill
depends on it.

Assign `argocd.argoproj.io/sync-wave` annotations from the first Application.
CRD-providing controllers go in early waves, custom resources in later ones.
Retrofitting waves after a failed sync is worse than planning them.

**Done when**: `docker stop` one control-plane container, `kubectl apply`
still succeeds, and `etcdctl endpoint health` reports 2 of 3 healthy. Restart
it and confirm the member rejoins.

**Risk**: the spec flags that killing control-plane nodes in `kind` is not
well documented. Resolve it here, in the cheapest phase, not later.

**Drill** (full step-by-step with rationale for each command:
[docs/runbooks/phase-0-control-plane-drill.md](../../runbooks/phase-0-control-plane-drill.md)):

```bash
docker stop personalization-lab-control-plane
kubectl get nodes                                   # one NotReady, cluster still answers
kubectl create deployment drill-check --image=nginx  # proves etcd write quorum survived
kubectl get deployment drill-check                   # READY 1/1
docker start personalization-lab-control-plane
kubectl get nodes                                   # rejoins within ~20-30s
kubectl delete deployment drill-check                # cleanup, not GitOps-tracked
```

Expected: step 2 keeps responding through HAProxy failover, step 3 succeeds
because 2-of-3 etcd members still hold write quorum, step 5 shows the member
rejoin. Reported run once on this cluster's current (IP-pinned) instance;
actual command output wasn't reviewed here, worth pasting in if it's ever
needed as a real reference rather than a predicted one.

---

## Phase 1: Data platform (5h)

Strimzi and MinIO, both as ArgoCD Applications.

The Kafka CR declares a `nodeport` external listener with per-broker
`advertisedHost` overrides, and `kind` publishes those ports via
`extraPortMappings` ([ADR 0002](../../adr/0002-strimzi-external-listener.md)).
Without this, Phase 3 cannot start.

Create the clickstream, price-change, promo-rule, and recommendation topics.
Create the MinIO checkpoint bucket.

**Done when**: a producer running on the host machine, not in the cluster,
round-trips a message through the external listener. A pod being `Ready` is
not done.

**Drill** (external-access smoke test, `kcat`, no JVM/full Kafka distribution
needed):

```bash
sudo dnf install kcat
echo "hello from outside the cluster" | kcat -P -b localhost:30016 -t smoke-test
kcat -C -b localhost:30016 -t smoke-test -e
```

Actual output, run on this project's cluster:

```
hello from outside the cluster
% Reached end of topic smoke-test [0] at offset 1: exiting
```

Confirms the full chain end to end: bootstrap on `30016`, redirect via the
per-broker `advertisedHost`/`advertisedPort` overrides, direct reconnect to
the owning broker, all from a real external client, not inferred from `Ready`
pods. `auto.create.topics.enable` is on by default, since `smoke-test` was
never declared as a `KafkaTopic` and still worked; the four real topics still
get explicit `KafkaTopic` resources rather than relying on that.
`smoke-test` is throwaway, same disposable pattern as `drill-check` in
Phase 0, not one of the tracked topics.

---

## Phase 2: Domain model and generator (4h)

Fix the schemas for Click, Price Change, Promo Rule, and Recommendation against
the vocabulary in [CONTEXT.md](../../../CONTEXT.md). Build the synthetic
generator.

The generator must emit controlled out-of-orderness, not just random jitter.
Phase 3 needs to assert watermark behaviour, which requires knowing the exact
skew being injected.

**Done when**: the generator sustains a configurable event rate with a
configurable maximum skew, and a consumer confirms both.

---

## Phase 3: Core pipeline (11h)

Runs on `MiniCluster` against Phase 1's Kafka and MinIO
([ADR 0001](../../adr/0001-minicluster-first-dev-loop.md)).

Bounded out-of-orderness watermarks assigned once on the raw stream, before any
fork. Session windows on `keyBy(shopperId)`. Keyed state in a
`KeyedProcessFunction`. RocksDB state backend. Checkpoints to MinIO. Late Clicks
to a side output.

**Done when**: kill the job mid-stream and restart from the last checkpoint.
Output is identical to an uninterrupted run. Separately, inject a Click beyond
the watermark bound and confirm it lands in the Late Click side output rather
than being silently dropped.

---

## Phase 4: Advanced Flink (13h)

Still on `MiniCluster`. The heaviest code phase.

Broadcast state for Promo Rules. Interval join on its own `keyBy(productId)`
branch forked from the raw stream, not below `keyBy(shopperId)`
([ADR 0003](../../adr/0003-interval-join-key-and-semantics.md)). Unmatched
Clicks route to the side output with a discriminator distinguishing them from
Late Clicks. Re-key the join output to `shopperId` and merge the branches with
`connect` plus a `KeyedCoProcessFunction`, not `union`. CEP for the multi-step
behaviour pattern. Async I/O to a mocked recommendation service.

**Done when**: each of the five concepts has one assertion test. Promo Rules
change mid-run and the output changes without a restart.

**Risks**: the spec names CEP as the concept most likely to overrun. It also
pre-agrees the fallback: trim CEP to a single pattern before cutting anything
from the Kubernetes HA side. Apply that here if this phase passes 16 hours.

---

## Phase 5: Operator and HA (9h)

First real deployment. Flink Kubernetes Operator via ArgoCD, depending on
cert-manager's webhook certificate.

`mode: native`, `jobmanager.scheduler: adaptive`. JobManager standbys, pod
topology spread across zones for both JobManager and TaskManager, a
PodDisruptionBudget with `minAvailable: 1` on the JobManager.

Budget the first day of this phase for the S3 filesystem plugin. `MiniCluster`
takes `flink-s3-fs-hadoop` as a compile dependency. The container needs it in
`/opt/flink/plugins/s3-fs-hadoop/`, because Flink loads filesystems through a
separate plugin classloader.

Drills: kill a TaskManager, kill a JobManager, drain a zone's worth of nodes.
Then the ArgoCD Lua drift drill: patch `spec.job.state` on a live
FlinkDeployment, observe the patched state, trigger a manual sync, watch it
revert.

**Done when**: every drill recovers from checkpoint with no gap in the
recommendation topic, and the drift drill's intermediate state was actually
observable.

---

## Phase 6: Autoscaling (11h)

Two variants, never running together
([ADR 0005](../../adr/0005-autoscaling-two-deployment-modes.md)).

**6a, native (5h)**: Job Autoscaler under a Load Ramp. Backpressure drives a
parallelism change. Requires Flink 1.18+ for in-place rescaling.

**6b, standalone (4h)**: swap to `mode: standalone`, `scheduler-mode: reactive`.
KEDA `ScaledObject` on Kafka consumer-group lag drives
`spec.taskManager.replicas`. Confirm the reactive scheduler absorbs the change
with no savepoint restart. Zone spread and the PodDisruptionBudget are not
re-verified under this mode. That is a deliberate scope limit.

**Karpenter (2h)**: real controller, kwok provider, NodePool with a
`workload=flink:NoSchedule` taint. Flink pods must **not** tolerate it. A Decoy
Workload of pause-image pods tolerates the taint and generates the unschedulable
pods that drive provisioning. Routing real TaskManagers onto kwok nodes would
mark them Running with no process behind them, and the job would hang waiting
for slots that never register.

**Done when**: 6a shows a parallelism change from backpressure. 6b shows a
replica change from lag. Karpenter provisions under the Decoy Workload and
consolidates when it is scaled down.

---

## Phase 7: Blue/green and OTel (9h)

Both namespaces run the Native Variant
([ADR 0006](../../adr/0006-blue-green-native-mode.md)).

**Reconsideration flagged, design deferred to this phase**: the original spec
scoped Blue/Green for zero-downtime deployment only, explicitly not as a
recovery mechanism (recovery was assigned to the ArgoCD drift-correction
drill and manual pod/node kill drills instead). That exclusion is being
revisited, once this phase starts, work out concretely how the Active/Standby
split could also serve as a recovery path, not just a deployment one, and
whether that changes anything about the promotion runbook or the namespace
setup above.

Before the first promotion, confirm blue and green carry distinct
`high-availability.cluster-id` values. They share one MinIO bucket, and
colliding HA metadata would let the Standby Side's JobManager contend for
leadership against the Active Side's.

Build `scripts/promote-green.sh` per the spec's runbook. It must abort loudly if
the Active Side is not RUNNING, to guard against overlapping runs.

Drills: promote under a live Load Ramp and confirm a bounded pause with no
duplicate Recommendations. Promote back. Deliberately break the Standby Side to
exercise the step-5 timeout, the one failure mode with no automatic safe path.
Install the OTel Collector scoped to Flink metrics, then kill it and confirm
node-exporter and kube-state-metrics keep flowing while Flink metrics recover on
their own.

**Done when**: promotion works in both directions, the timeout fallback is
documented from having actually run it, and Grafana can group by namespace.

---

## Phase 8: Observability and docs (5h)

Grafana dashboards. Runbooks. A dashboard panel separating Late Clicks from
Unmatched Clicks, which is why Phase 4 added the discriminator.

Stretch, only if hours remain: a second OTel Collector pipeline forwarding Flink
pod logs, reusing the Collector from Phase 7. Or reimplement the interval join
as a hand-rolled left-outer `KeyedCoProcessFunction` and compare.

---

## Ordering constraints

```
0 ──> 1 ──> 2 ──> 3 ──> 4 ──> 5 ──> 6a ──> 7 ──> 8
      │                       │      │
      └── external listener ──┘      └── 6b (side branch, never promoted)
          blocks 3

0 must fully precede 1: ArgoCD manages every later install.
5 must precede 6 and 7: both build on the Native Variant.
6b is a detour off 5, not off 6a.
```
