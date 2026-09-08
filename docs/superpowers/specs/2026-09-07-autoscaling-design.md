# Phase 6 design: Autoscaling

Date: 2026-09-07
Status: draft, awaiting review

Parallelism stops being a constant. The Phase 5 Native Variant runs at a fixed
`parallelism: 6`. This phase hands that number to the Flink Job Autoscaler and
lets a Load Ramp move it. Separately and independently, Karpenter provisions
nodes for a Decoy Workload.

This document settles **every** design decision. The implementation plan that
follows it contains implementation tasks only, with one exception stated in
section 11, which is a verification gate rather than an open question.

---

## 1. What this phase closes

Three earlier documents deferred a cost to this phase, each for its own reason.

1. [The core pipeline design](2026-08-23-core-pipeline-design.md) records that
   `parallelism.default` is left unpinned because "Phase 6 varies it on
   purpose". Phase 3 Task 4 fixed a stalled watermark with `withIdleness`
   rather than by pinning parallelism, specifically to keep that door open.
2. [The Phase 5 design](2026-08-30-operator-and-ha-design.md) hands over
   parallelism 6 with 2 slots, and names the `256mb` network buffer reservation
   as the headroom a changing parallelism must stay inside.
3. [ADR 0005](../../adr/0005-autoscaling-two-deployment-modes.md) split
   autoscaling into two deployment modes. This phase implements one of them and
   drops the other, for the reason in section 4.

A fourth cost is closed here and was recorded nowhere: **the demonstration this
phase exists for cannot work on the artifacts as they stand.** Five separate
things would each make it silently prove nothing. Section 5 is that list.

## 2. Facts verified before this design was written

Every value below was read from the running cluster or the repository on
2026-09-07, not recalled. The command is given so each can be repeated.

| Fact | Evidence |
|---|---|
| Every vertex reports `maxParallelism` **128** | `curl -s localhost:30011/jobs/<id>` |
| The job graph has **9** vertices, all at parallelism 6 | same |
| Every vertex name contains `Sink: Print to Std. Out` | same |
| `debugPrints` defaults to **`true`** | `apps/pipeline/src/main/java/lab/personalization/pipeline/PipelineConfig.java:51` |
| `spec.job.args` passes only `--bootstrap-servers` | `manifests/flink/blue/flinkdeployment.yaml` |
| There is **no `.uid()`** anywhere in the pipeline | `grep -rn '\.uid(' apps/pipeline/src/main/java/` returns nothing |
| The generator's period floors at 1 ms, capping it at **1000 events/sec** | `apps/generator/.../SkewedEventStream.java:44` |
| That ticker is a **single-threaded** `scheduleAtFixedRate` | same file, line 23 and line 46 |
| `Catalog` hardcodes **10** Shoppers and **10** Products | `apps/generator/.../factory/Catalog.java` |
| Every Kafka topic has **3** partitions | `manifests/strimzi/kafka-topics.yaml` |
| A TaskManager's working set is **~0.9 GiB** against a 2 GiB limit | `kubectl get --raw /api/v1/nodes/<n>/proxy/stats/summary` |
| Host memory is 23 GiB total, **~6.7 GiB available** with the cluster up | `free -h` |
| **Every** kind node advertises **~23.5 GiB** allocatable, six times over | `kubectl get nodes -o custom-columns=...allocatable.memory` |
| `metrics-server` is **not** installed | `kubectl top nodes` returns `Metrics API not available` |
| `jobmanager.scheduler: Adaptive` is **already set** | `manifests/flink/blue/flinkdeployment.yaml:31` |
| `taskmanager.numberOfTaskSlots: "2"` | same file, line 32 |
| `FlinkDeployment` **has** a `scale` subresource, mapping to `spec.taskManager.replicas` | `kubectl get crd flinkdeployments.flink.apache.org -o jsonpath='{.spec.versions[*].subresources}'` |
| `selfHeal: false` on **every** Application | `manifests/argocd-apps/*.yaml` |
| The async lookup uses `orderedWait`, capacity **100**, latency **40 ms** | `PersonalizationJob.java:110`, `PipelineConfig.java:48-50` |
| The interval join window is `[-2s, +2s]` | `PipelineConfig.java:42-43` |
| The session gap is **6 s** | `PipelineConfig.java:41` |

Four of these changed the design and are called out where they land: the max
parallelism in 5.1, the debug prints in 5.2, the uniform node allocatable in
7.3, and the scale subresource in 4.2.

## 3. The mechanism this phase is really about

State the failure first. Under a Load Ramp the job falls behind. Nothing in
Kubernetes notices. No pod is `Pending`, no container is OOMKilled, no probe
fails. Every dashboard reads healthy while the Recommendation topic falls
further behind the clickstream.

The reason is that **Kubernetes has no idea what a backlog is.** It schedules
pods against requests. It cannot see that a keyed operator is saturated.

The Flink Job Autoscaler closes that gap by measuring inside the job. Its
central quantity is `busyTimeMsPerSecond`. A task thread is in exactly one of
three states, and they sum to about 1000 ms per second: idle waiting for input,
backpressured waiting for output, or busy running user code.

From busy time it derives capacity:

```
TRUE_PROCESSING_RATE = numRecordsInPerSecond ÷ (busyTimeMsPerSecond ÷ 1000)
```

which reads as "records per second this vertex could manage at 100% busy". It
then derives demand, and the second term is the one that distinguishes a real
autoscaler from a thermostat:

```
TARGET_DATA_RATE   = incoming rate + CATCH_UP_DATA_RATE
CATCH_UP_DATA_RATE = pending records ÷ job.autoscaler.catch-up.duration
```

Without the catch-up term a job sized for its input rate keeps pace forever and
never recovers the backlog it already has.

The result is applied through **Declarative Resource Management**. The
AdaptiveScheduler, which lives in the JobMaster, does not command slots. It
declares a range per vertex into a running JobManager:

```
PUT /jobs/:jobid/resource-requirements
{"jobVertexResourceRequirements": {
   "<jobVertexID>": {"parallelism": {"lowerBound": 1, "upperBound": 6}}}}
```

The ResourceManager, a separate component in the same pod, compares declared
against registered slots and creates TaskManager pods to cover the shortfall.

**Pod count is never set. It is derived, every time:**

```
slots needed = the highest parallelism among the vertices, not the sum
             = 6, because all operators share one slot sharing group
pods needed  = ceil(6 ÷ taskmanager.numberOfTaskSlots) = ceil(6 ÷ 2) = 3
```

That single line is why `spec.taskManager.replicas` is inert in the Native
Variant, and it is the same fact ADR 0005 rests on.

Docs:
[autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/),
[elastic scaling](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/elastic_scaling/).

## 4. Scope

### 4.1 What this phase builds

| Piece | Layer | Mechanism |
|---|---|---|
| **6a** | job parallelism | Flink Job Autoscaler, `mode: native`, busy time and backlog |
| **Karpenter** | nodes | kwok provider, unschedulable Decoy Workload pods |

The two are orthogonal. Karpenter reacts to unschedulable pods and never sees a
Flink object. 6a changes parallelism and never creates a node. They are in one
phase because both are called autoscaling, not because they interact.

### 4.2 The Standalone Variant is dropped, deliberately

ADR 0005 decided to maintain two `FlinkDeployment` manifests, the second
existing only to demonstrate KEDA with reactive scaling. **Phase 6 does not
build it.**

This is a deliberate exclusion, not an oversight. Recording the cost plainly:
the lab loses external-metric-driven autoscaling, which was ADR 0005's stated
reason for keeping KEDA and is the more transferable Kubernetes skill. A later
phase may pick it up. Nothing in Phase 7 depends on it, because
[ADR 0006](../../adr/0006-blue-green-native-mode.md) runs blue/green on the
Native Variant.

**ADR 0005 needs amending, and only in its Decision.** Its analysis stays
correct and load-bearing, and this design leans on it in section 3. Its
conclusion that two manifests will be maintained is now false.

The amendment should also record a fact ADR 0005 did not have. The CRD **does**
declare a `scale` subresource:

```json
{"scale":{"labelSelectorPath":".status.taskManager.labelSelector",
          "specReplicasPath":".spec.taskManager.replicas",
          "statusReplicasPath":".status.taskManager.replicas"}}
```

So KEDA and a HorizontalPodAutoscaler can both target a `FlinkDeployment`, and
both will report success. The plumbing is complete. It terminates in
`spec.taskManager.replicas`, a field the native code path never reads. That is a
sharper and more dangerous statement than "it is a no-op", because every layer
involved says it worked.

### 4.3 Direction: scale up, 2 to 6

6a starts at `parallelism: 2` and lets the Load Ramp drive it to 6.

The ceiling is chosen from measurement, not preference. Peak footprint at
parallelism 6 is three TaskManagers, which is exactly what Phase 5 already ran
and proved. The demonstration therefore adds **no new memory risk**.

| | Parallelism | TaskManagers | Requested | Real, at ~0.9 GiB each |
|---|---|---|---|---|
| Start | 2 | 1 | 2 GiB | ~0.9 GiB |
| End | 6 | 3 | 6 GiB | ~2.7 GiB |

Scaling to parallelism 8 would need four TaskManagers and was rejected. Host
headroom is ~6.7 GiB, and a TaskManager's working set climbs toward its 2 GiB
limit under load, so four is inside the arithmetic but outside the margin.

Shrinking the TaskManager to 1 GiB to widen the range was also rejected. The
measured **idle** cost at 2 slots is 0.9 GiB, so a 1 GiB limit leaves a 10%
margin and the failure would be an OOMKill that reads as an autoscaler fault.

## 5. Five blockers, and why each proves nothing silently

Each of these lets the phase run to completion and report success while
measuring something other than what it claims to measure. That shared property
is why they are grouped.

### 5.1 The divisor trap

The autoscaler computes scaled parallelism as **a divisor of max parallelism**,
to avoid key skew. Measured on the running job, every vertex reports
`maxParallelism` 128.

| Max parallelism | Divisors available to the autoscaler |
|---|---|
| **128**, today's default | 1, 2, 4, 8, 16, 32, 64, 128 |
| **120**, the fix | 1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 24, 30, 40, 60, 120 |

At 128 the autoscaler can never choose 3, 5 or 6. The whole ladder inside the
memory budget collapses to `2 → 4`, then a jump to 8, which is outside it.

**Fix**: `pipeline.max-parallelism: "120"`. The operator's own documentation
recommends numbers with many divisors and its example uses 720, so this is
ordinary configuration and not a workaround.

**Consequence**: max parallelism sets the key group count. It cannot change
across a state restore. This forces section 6.

### 5.2 Debug prints inflate busy time

`debugPrints` defaults to `true`, and `spec.job.args` never overrides it. So
every vertex in the running job carries a chained print sink:

```
EventTimeSessionWindows -> (Map, Sink: Print to Std. Out, ...)     PAR 6
```

A chain runs inline on **one** task thread and reports **one** busy time.
`System.out.println` from inside an operator is a synchronized write to a shared
file descriptor, through the container runtime's log pipe, onto disk. Busy time
would be measuring that, not the session window.

Worked example, at 1000 records/sec, with the operator costing 0.05 ms per
record and the print sink 0.45 ms:

| | With prints | Without prints |
|---|---|---|
| Busy time | 500 ms/s | 50 ms/s |
| `LOAD` | 0.50 | 0.05 |
| `TRUE_PROCESSING_RATE` | 2,000 rec/s | 20,000 rec/s |

The autoscaler would believe the vertex is ten times slower than it is, and at a
0.6 utilization target would ask for roughly ten times the parallelism.

The failure hides itself. Scaling up genuinely reduces busy time per subtask,
because each subtask prints less often, so the ineffective-scaling detector sees
improvement and keeps going. The number it converges on is a property of stdout
write throughput on a WSL2 disk. Nothing reports that.

There is a second-order effect too. Every subtask on one TaskManager writes to
the same file descriptor and contends for the same lock, so packing subtasks
onto a TaskManager makes each record slower. That is real anti-scaling, and it
is invisible in every metric.

**Fix**: add `--debug-prints=false` to `spec.job.args`. `PipelineConfig.java:85`
already parses it, so this is an argument change and not a code change.

**Consequence**: removing operators changes the chains, so the JobGraph changes,
so every auto-generated `JobVertexID` changes. With no `.uid()` anywhere,
savepoint state has nowhere to map. This forces section 6 independently of 5.1.

### 5.3 The generator's silent ceiling

```java
// SkewedEventStream.java:44
long periodMillis = Math.max(1, Math.round(1000.0 / eventsPerSecond));
```

One tick per Click, and the period cannot go below 1 ms. `--click-rate=5000`
yields 1000 per second and reports nothing. It is worse in practice: the ticker
is single-threaded and each tick does a JSON encode plus a Kafka send, so once a
tick exceeds 1 ms `scheduleAtFixedRate` simply falls behind, still silently.

**Fix**: fix the period, vary the batch.

```java
public void start(double eventsPerSecond) {
    final long periodMillis = 10;
    final int perTick = (int) Math.max(1, Math.round(eventsPerSecond * periodMillis / 1000.0));
    ticker.scheduleAtFixedRate(() -> { for (int i = 0; i < perTick; i++) tick(); },
                               0, periodMillis, TimeUnit.MILLISECONDS);
}
```

Three constraints on that change:

1. The ticker stays **single-threaded**. The comment at `SkewedEventStream.java:50`
   records that event time ordering depends on ticks firing sequentially.
   Batching preserves that. A thread pool would not.
2. `Instant.now()` is called **inside** the loop, once per Click. Otherwise 50
   Clicks share a timestamp.
3. The 1 ms floor is gone but the fall-behind mode is not. If a batch exceeds
   10 ms it silently lags. **The achieved rate must be measured, never read off
   the argument.**

`SkewedPublisher` schedules each send up to `--click-max-skew-seconds` into the
future. At 5000/sec with the 2 s default that is 10,000 pending scheduled tasks
at all times. Drop the skew to 1 s during a Load Ramp, or accept the memory.

### 5.4 Key cardinality

`Catalog` hardcodes ten Shoppers and ten Products, with no CLI knob. Ten keys
hash into at most ten of the 120 key groups.

| Parallelism | Expected non-empty subtasks | Gain |
|---|---|---|
| 6 | 6 × (1 − (5/6)¹⁰) ≈ **5.0** | |
| 8 | 8 × (1 − (7/8)¹⁰) ≈ **5.9** | +0.9 |

Scaling 6 to 8 buys about one more working subtask. The autoscaler would scale
up, throughput would not improve, and its ineffective-scaling detection would
then block further scale-ups. That failure is real and instructive, but it is
not the demonstration this phase is for.

**Fix**: make the Catalog sizes CLI options in `:generator`, defaulting to
**2,000 Shoppers and 200 Products**.

This requires lifting a Phase 5 constraint, and the lift is stated here rather
than left to a task. The Phase 5 plan's global constraints read:

> **No file under `apps/` changes except `apps/pipeline/build.gradle`.** The job
> graph is frozen and the Java is correct as it stands.

That was scoped to Phase 5, where the point was to deploy the Phase 4 job
unchanged. **Phase 6 lifts it for `:generator` only.** `:pipeline` stays frozen:
no change may alter the job graph, the operators, or their semantics. Blocker
5.3 and this one both land in `apps/generator/`, which no Flink operator reads.

**Why 2,000 Shoppers.** A Browsing Session only builds up if a Shopper's Clicks
arrive closer together than the 6 s session gap. Arrivals are effectively
Poisson, so:

```
E[Clicks per Session] = e^(sessionGap × clickRate ÷ shopperCount)
```

At 800 Clicks/sec:

| Shoppers | Per-Shopper rate | E[Clicks per Session] | What happens |
|---|---|---|---|
| 10 | 80/s | e⁴⁸⁰ | Sessions **never close**. State grows without bound, nothing is emitted |
| 1,000 | 0.8/s | ~121 | Sessions stay open for minutes |
| **2,000** | **0.4/s** | **~11** | Healthy Sessions, closing every few seconds |
| 10,000 | 0.08/s | ~1.6 | Nearly every Session is one Click, the aggregator does nothing |

Both extremes break the window branch, in opposite directions. `SessionAggregator`
is a `ProcessWindowFunction`, so it buffers every Click in window state until the
window fires. 2,000 keeps Sessions short enough that state turns over.

Note what today's defaults rely on. At `--click-rate=5` with 10 Shoppers,
`e^(6×5/10)` is about 20 Clicks per Session, which works. **The failure arrives
with the Load Ramp, not with the configuration.**

**Why 200 Products.** Product count is the denominator of the pressure lever in
section 8. Raising it further would remove that lever.

The pipeline's own `recommendationCatalogue` stays at P1..P10 and needs no
change. `DeterministicMockClient.answerFor` hashes any `candidateProductId` into
that list, so it is an answer set and not a key set.

### 5.5 Chaining, which is a stated limit and not a fix

`Co-Process-Broadcast -> (async wait operator -> ...)` is **one** JobVertex, with
one `JobVertexID` and one busy-time counter. `job.autoscaler.vertex.exclude.ids`
takes JobVertexIDs, so there is no identifier that names the async operator
alone.

Two consequences, both accepted:

- The async lookup **cannot be scaled independently** of the broadcast process.
- It **cannot be excluded** without also freezing the broadcast process, which is
  a genuinely scalable stateful operator.

Splitting them needs `disableChaining()`, which alters the JobGraph, changes
every downstream ID, and costs throughput, since chained operators pass records
by direct method call. **Do not chase it.** Section 8 shows the async operator is
not the bottleneck at any load this cluster can reach.

## 6. The clean start

Blockers 5.1 and 5.2 each break savepoint restore, for independent reasons.
`pipeline.max-parallelism` changes the key group count. Removing the print sinks
changes the operator IDs. Either alone is fatal to a restore.

`upgradeMode` governs **spec changes only**. It is never involved in a
TaskManager failure or a JobManager failover, which are Flink's and Kubernetes
HA's business respectively. Editing `spec.flinkConfiguration` and `spec.job.args`
**is** a spec change, and with `upgradeMode: savepoint` the operator will
savepoint, cancel, redeploy, and attempt a restore that cannot succeed. The error
names state mapping, which reads like a Flink fault rather than a decision.

**Phase 6 therefore starts the job from clean state.**

| Change | Value |
|---|---|
| `spec.job.upgradeMode` | `stateless`, for this one transition, then back to `savepoint` |
| `execution.checkpointing.dir` | `s3://checkpoints/phase-6` |
| `high-availability.storageDir` | `s3://checkpoints/phase-6-ha` |
| `execution.checkpointing.savepoint-dir` | `s3://checkpoints/phase-6-savepoints` |

Three procedural facts, each of which has already cost time on this project:

1. **`upgradeMode` must travel with the other changes.** It is annotated as an
   `IGNORE`-level diff, so on its own `diffType` is `IGNORE`, `specChanged` is
   `false`, and no reconciliation runs. Changing it in a separate commit does
   nothing.
2. **Push to `master`, not `phase-2`.** `root.yaml` uses `targetRevision: HEAD`,
   which resolves to the default branch. Work committed elsewhere deploys nothing
   and reports no error anywhere.
3. **New state prefixes keep Phase 5's evidence intact.** Its Drill runbooks cite
   checkpoints under `phase-5`, and those must remain readable.

The single fact that proves a clean start rather than a silent restore:

```bash
kubectl -n personalization-blue get flinkdeployment personalization \
  -o jsonpath='{.status.jobStatus.upgradeSavepointPath}{"\n"}{.status.jobStatus.state}{"\n"}'
```

An empty savepoint path beside `RUNNING` is the clean start. A populated path
means it restored from something, and the `maxParallelism` check in section 12
is then the thing that catches it.

## 7. `spec.flinkConfiguration` for 6a

### 7.1 The keys

| Key | Value | Reason |
|---|---|---|
| [`jobmanager.scheduler`](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/elastic_scaling/) | `Adaptive` | **Already set.** Phase 5 set it so the scheduler could run at reduced parallelism when slots are short. It is also the autoscaler's hard prerequisite. The enum is `Default`, `Adaptive`, `AdaptiveBatch`, so the capital is correct |
| [`job.autoscaler.enabled`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `"true"` | Collect metrics and compute recommendations |
| [`job.autoscaler.scaling.enabled`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `"false"`, then `"true"` | See 7.2. Dry run before acting |
| [`job.autoscaler.metrics.window`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `3m` | Averaging window. The shipped default is far larger and unwatchable in a session |
| [`job.autoscaler.stabilization.interval`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `1m` | Suspends metric collection after a recovery, because a just-restarted job replays a backlog and reports a throughput spike far above steady state |
| [`job.autoscaler.target.utilization`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `"0.6"` | Headroom for load fluctuation. Near 100% degrades before it scales |
| [`job.autoscaler.catch-up.duration`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `5m` | How fast a backlog is burned down. Drives `CATCH_UP_DATA_RATE` |
| [`job.autoscaler.vertex.max-parallelism`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/autoscaler/) | `"6"` | **The memory ceiling.** The default is 200. See 7.3 |
| [`pipeline.max-parallelism`](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/) | `"120"` | Blocker 5.1 |
| [`web.adaptive-scheduler.rescale-history.size`](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/elastic_scaling/) | `"10"` | The observation instrument. See 7.4 |

`spec.flinkConfiguration` is `map[string]string`, so every number and boolean is
quoted. Durations are accepted unquoted as strings.

`spec.job.parallelism` moves from `6` to `2`, per 4.3.

### 7.2 Dry run first, and why it is not optional

`job.autoscaler.enabled` and `job.autoscaler.scaling.enabled` are two switches.
The first collects metrics and publishes `RECOMMENDED_PARALLELISM` per vertex.
The second applies them.

Running with the first alone is a dry run. Given that four of the five blockers
in section 5 are ways to get a **plausible but wrong** recommendation, a dry run
is the only cheap way to tell a working autoscaler from a broken one.

The gate is specific: under a Load Ramp, `RECOMMENDED_PARALLELISM` must rise on
the three Click-consuming vertices, which are the session window, the CEP
operator, and the interval join. If it rises on the sink or on
`Co-Process-Broadcast` instead, something in section 5 was not actually fixed.

### 7.3 The ceiling must be configured, not discovered

Every kind node advertises ~23.5 GiB allocatable, which is the whole host, six
times over. Roughly 141 GiB is claimed and 23 GiB exists.

The consequence is direct. **The scheduler will never report `Pending` for
memory.** It will place TaskManager after TaskManager, because from its point of
view each worker has 19 GiB free. The wall is WSL2 beginning to swap, or the
kernel OOMKilling a container that crosses its 2 GiB limit. Neither looks like a
scaling problem in any Flink, Kubernetes or ArgoCD output.

So `job.autoscaler.vertex.max-parallelism: "6"` is a **safety mechanism**, not a
tuning preference. It is the only thing standing between a Load Ramp and the
host.

Sources are additionally capped at the Kafka partition count, which is 3. That
is automatic and needs no configuration.

### 7.4 How a rescale is observed

Pod counts are the worst available instrument. They cannot distinguish a scaling
event from a failover.

`web.adaptive-scheduler.rescale-history.size: "10"` makes Flink retain rescale
records. Each carries, per vertex, `preRescaleParallelism` and
`postRescaleParallelism`, plus `desiredSlots`, `minimalRequiredSlots`, a
`triggerCause` of `INITIAL_SCHEDULE`, `UPDATE_REQUIREMENT`,
`NEW_RESOURCE_AVAILABLE` or `RECOVERABLE_FAILOVER`, a `terminalState` of
`COMPLETED`, `FAILED` or `IGNORED`, and a `schedulerStates` array timing each
state transition.

`triggerCause: UPDATE_REQUIREMENT` is the autoscaler acting.
`RECOVERABLE_FAILOVER` is a crash. A pod count change looks identical for both.

### 7.5 Three timers, and which one is blocking

They belong to different components and are frequently conflated.

| Setting | Component | Purpose |
|---|---|---|
| `job.autoscaler.stabilization.interval` | the operator's autoscaler | Do not trust metrics from a just-recovered job |
| `jobmanager.adaptive-scheduler.executing.cooldown-after-rescaling` | the JobMaster's scheduler | Minimum gap between any two rescales, whoever asks |
| `jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout` | the JobMaster's scheduler | Wait in case more slots are still arriving, rather than rescaling once per pod |

The first two are pacing controls and either can block a scale on its own. The
third is not pacing. It sits inside a single rescale.

The trap: if `stabilization.interval` is shorter than the scheduler's cooldown,
the autoscaler will issue a `PUT` that the JobManager quietly defers, and the
autoscaler's own logs will report a scaling decision while nothing moves. The
rescale history in 7.4 is what distinguishes those.

### 7.6 There is only one config list, and that changes the Phase 5 rule

The Phase 5 design records that `apps/pipeline/conf/config.yaml` and
`spec.flinkConfiguration` are two lists that must not drift, and that a third
unexplained difference between them is a defect. **That rule no longer has two
sides.**

`apps/pipeline/conf/config.yaml` was deleted in commit `b0705e0`, "added flink
k8s operator", 15 lines removed. It is absent from `HEAD` and from the working
tree, and it is not gitignored. The deletion was deliberate and the Phase 5
knowledge doc explains it: the configuration **moved** into
`spec.flinkConfiguration`, and the operator renders it into
`/opt/flink/conf/config.yaml` inside every pod. Since the image working
directory is `/opt/flink` and `PipelineConfig` defaults `flinkConfDir` to the
relative path `conf`, `GlobalConfiguration.loadConfiguration("conf")` resolves
to the operator's rendered file.

So Phase 6 adds every key to exactly one place:

| Key | Where | Reason |
|---|---|---|
| `pipeline.max-parallelism` | `spec.flinkConfiguration` | Blocker 5.1. There is no second list to keep it in step with |
| `job.autoscaler.*` | `spec.flinkConfiguration` | The autoscaler is a component of the operator |
| `web.adaptive-scheduler.rescale-history.size` | `spec.flinkConfiguration` | Cluster only. There is no rescale to record locally |

**Two Phase 5 constraints are therefore stale rather than lifted.** The plan's
global constraints still read:

> **`apps/pipeline/conf/config.yaml` is not edited.** `:pipeline:run` against
> `MiniCluster` must keep working unchanged through Phase 6 and Phase 7.

Phase 6 does not edit that file, because there is nothing to edit. The
constraint is satisfied vacuously, and it should be corrected in the Phase 5
plan rather than carried forward as though it still bites.

**A consequence that predates Phase 6 and is worth stating plainly.**
`PersonalizationJob.flinkConfiguration` throws
`IllegalStateException("no config.yaml loaded from " + flinkConfDir)` when
`state.backend.type` is absent. With the file gone, a bare `:pipeline:run`
against `MiniCluster` fails at that line unless the developer supplies their own
`apps/pipeline/conf/config.yaml`. That is the state Phase 5 left behind, not
something this phase introduces, and Phase 6 changes nothing about it.
`:pipeline:test` does not touch that path and stays green.

## 8. The pressure lever

A demonstration needs a bottleneck the autoscaler can both see and fix.

### 8.1 Where volume actually is

```
Source: click-stream ──┬─ keyBy(shopperId) ─ SessionWindow ─┐   FULL Click rate
   (capped at 3)       ├─ keyBy(shopperId) ─ CepOperator ───┤   FULL Click rate
                       └─ keyBy(productId) ─ IntervalJoin ──┤   FULL Click rate
                                                            │
                          union ─ Co-Keyed-Process ─────────┘   signal rate, far lower
                                  Co-Process-Broadcast
                                    └ async wait (chained)
                                        └ KafkaSink
```

Only the three Click-consuming branches carry real volume, and all three sit
**after** a `keyBy`, so none is partition-bound. They are the only vertices where
a scalable bottleneck can live.

### 8.2 The primary knob is `--product-change-rate`

The interval join's cost per Click is proportional to how many Product Changes
sit in the `[-2s, +2s]` window for that same `productId`:

```
comparisons per Click ≈ productChangeRate × 4 s ÷ productCount
```

| Product Change rate | Products | Comparisons per Click |
|---|---|---|
| 1/s, today's default | 10 | 0.4 |
| 400/s | 200 | **8** |
| 2000/s | 200 | **40** |

That is pure CPU inside the operator's own task thread, which is exactly what
`busyTimeMsPerSecond` measures, and it lands on a vertex the autoscaler is free
to scale. No job graph change, no code change beyond 5.4, one generator
argument.

Note the tension with 5.4: raising Product cardinality **reduces** this cost.
Shoppers want to be numerous, Products want to stay moderate. 200 is chosen to
satisfy both.

### 8.3 Escalation, if that is not enough

Drop `spec.taskManager.resource.cpu` to `0.5`. Configuration only, it raises busy
time honestly, and scaling out genuinely adds CPU quota so the autoscaler's fix
actually works.

The risk is real and must be watched: CFS throttling at 0.5 CPU can start failing
checkpoints, and that reads as a different fault entirely. Use this only after
8.2 has been tried and measured.

### 8.4 The async operator is not the bottleneck, and here is the arithmetic

Little's law gives its ceiling:

```
capacity ÷ latency = 100 ÷ 0.040 s = 2,500 records/sec per subtask
at parallelism 2:                     5,000 records/sec
```

Its input is Recommendation candidates, not Clicks. The session windows collapse
roughly 11 Clicks into one signal at the section 5.4 settings, and the generator
caps near 1000 Clicks/sec. Its actual load is therefore in the tens to low
hundreds per second, more than an order of magnitude below its ceiling.

**Therefore it is not the bottleneck 6a measures**, and 5.5's limitation costs
nothing. Should it ever saturate, the fix is `--recommendation-capacity`, not
parallelism: raising capacity from 100 to 1000 multiplies the ceiling by ten for
the price of more in-flight records held in memory and in the checkpoint, where
one more subtask would buy 2.5 times for the price of a whole TaskManager.

Do **not** switch to `unorderedWait`. `PersonalizationJob.java:108` records that
`orderedWait` was chosen because unordered reorders records between watermarks
and Phase 3's restart Drill asserts identical output.

**One claim here is reasoning and not a verified fact**, and the design does not
depend on it: a saturated `orderedWait` operator is expected to be invisible to
busy time, because async waiting happens on a callback thread rather than the
task thread. If that ever needs to be relied upon, it must be measured first. The
detection signal would be high `backPressuredTimeMsPerSecond` on the upstream
vertex combined with **low** busy time on the async vertex. Either number alone
means nothing.

## 9. Karpenter

### 9.1 The Decoy Workload mechanism, corrected

The phase plan gives the Decoy Workload only a toleration for the NodePool's
`workload=flink:NoSchedule` taint. **That does not work.**

A toleration is permission, not attraction. Decoy pods that merely tolerate the
taint will schedule onto the real, untainted workers immediately. Karpenter would
see nothing pending and never provision. The Drill would appear to run correctly
and prove nothing.

Resource pressure cannot rescue it either, for the reason in 7.3.

**The fix uses two mechanisms for two jobs.**

| Object | Field | Job |
|---|---|---|
| NodePool `spec.template.metadata.labels` | `node-role: decoy` | Labels the fake nodes |
| NodePool `spec.template.spec.taints` | `workload=flink:NoSchedule` | Keeps everything else off them |
| Decoy `nodeSelector` | `node-role: decoy` | **Makes the pod unschedulable.** No real worker carries this label |
| Decoy `tolerations` | for that taint | Lets it land once Karpenter creates a node |
| Flink pods | neither | Can never reach a kwok node, even by accident |

The `nodeSelector` is what generates the unschedulable pods. The taint is what
protects the TaskManagers. The plan conflated them into one.

Docs:
[NodePools](https://karpenter.sh/docs/concepts/nodepools/),
[taints and tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/),
[assigning pods to nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/).

**[CONTEXT.md](../../../CONTEXT.md) must be corrected too.** Its Decoy Workload
entry currently bakes in the wrong mechanism:

> A pause-image Deployment that **tolerates the Karpenter NodePool taint** purely
> to generate unschedulable pods.

The toleration is not what generates them.

### 9.2 Why TaskManagers must never reach a kwok node

kwok is Kubernetes WithOut Kubelet. It creates `Node` objects and a controller
marks pods assigned to them as `Running`. There is no machine, no kubelet, no
container runtime, no process.

Route a TaskManager there and Kubernetes reports `1/1 Running`. No JVM starts. It
never registers its slots. The job sits waiting for slots that will never arrive,
while every dashboard says the pod is healthy. That failure is silent and it
looks like a Flink bug.

The Decoy Workload runs `registry.k8s.io/pause`, chosen precisely because it does
nothing. A pod that does nothing does not care that it is never really executed.

### 9.3 The install does not fit the app-of-apps pattern

The kwok provider lives in `kubernetes-sigs/karpenter` under `kwok/`, and its
README installs it with:

```bash
make install-kwok
make apply
```

That is a source build requiring a Go toolchain. **There is no published Helm
chart.** Every other component in this project arrives as an ArgoCD Application,
per [ADR 0004](../../adr/0004-gitops-from-phase-0.md).

This is a real tension and it needs a decision rather than a silent exception.
Two honest options:

| Option | Cost |
|---|---|
| **Install outside ArgoCD**, recorded in a script beside `bootstrap-phase0.sh` | One component sits outside the GitOps tree. Precedent exists: cert-manager and ArgoCD itself were installed directly in Phase 0 |
| **Vendor rendered manifests** into `manifests/` and wrap them in an Application | Keeps the pattern. Adds a generated artifact nobody will regenerate, and a Go build step to reproduce it |

**Recommendation: install outside ArgoCD**, and record the reason. Karpenter here
is a demonstration of a control loop, not a piece of the running platform, and
Phase 0 already set the precedent for bootstrap components. The alternative buys
pattern consistency by vendoring a build output, which is a worse thing to own.

Either way, this belongs in the ADR record, because ADR 0004 states the pattern
this breaks.

### 9.4 What this proves, and what it does not

Be explicit, because the honest scope is narrower than "autoscaling nodes".

It **does not** prove "we ran out of room and got more room". That cannot be
demonstrated on one laptop, with fake nodes or real ones. A kwok node adds no
memory. Neither would a seventh kind node, which would advertise another 23.5 GiB
that does not exist while sharing the same 16 cores.

It **does** prove the control loop:

```
pod cannot be placed  →  Karpenter notices  →  Karpenter creates a node
pods go away          →  node sits empty    →  Karpenter deletes it
```

That loop is identical on EKS with real EC2 instances. Only the provider beneath
it changes. It is the transferable half, and it is the half worth two hours.

### 9.5 Settings that make the Drill observable

| Setting | Value | Reason |
|---|---|---|
| `disruption.consolidationPolicy` | `WhenEmptyOrUnderutilized` | Removes nodes once the Decoy Workload scales down |
| `disruption.consolidateAfter` | `10s` | Makes the scale-down half observable in seconds rather than minutes |
| `limits.cpu` | `"20"` | A hard ceiling, in the spirit of 7.3. Nothing else bounds provisioning, and a runaway Decoy Workload would otherwise create kwok nodes without limit. Twenty is roughly four fake nodes, enough to watch and small enough to notice |

## 10. Drills

Each Drill is a deliberate, repeatable act, per
[CONTEXT.md](../../../CONTEXT.md), and each gets a runbook in `docs/runbooks/`
carrying the real transcript, following the pattern Phase 5 set.

| Drill | Action | What must be observed |
|---|---|---|
| E | Load Ramp against the dry-run autoscaler | `RECOMMENDED_PARALLELISM` rises on the session window, CEP and interval join vertices. **Not** on the sink |
| F | Load Ramp with `scaling.enabled: "true"` | A rescale record with `triggerCause: UPDATE_REQUIREMENT`, `terminalState: COMPLETED`, and `postRescaleParallelism` above `preRescaleParallelism`. Pod count reaches 3 |
| G | Load Ramp stopped, job left idle | Parallelism falls and TaskManager pods are deleted. Scale-down is the half most demonstrations skip |
| H | Decoy Workload scaled up, then to zero | Pods go `Pending` with `reason=Unschedulable`. Karpenter provisions kwok nodes. Scaling to zero consolidates them away |

**A timing note for Drill G.** Parallelism falls first, then the pods follow. The
scheduler releases the slots, and the ResourceManager deletes a TaskManager only
after it has been idle for its own timeout. Lingering pods immediately after a
scale-down are expected and are not a failure. Read the idle timeout from the
live configuration before deciding how long to wait, rather than assuming a
default:

```bash
curl -s localhost:30011/jobmanager/config | grep -i 'taskmanager-timeout\|taskmanager-release'
```

Drills F and G reuse Phase 5's `scripts/recommendation-snapshot.sh` unchanged. A
rescale restores from a checkpoint, so the gap and duplicate checks that Drills A
to C used apply here for the same reason and with the same trap: suppression is
not a gap.

## 11. Task 0: the one thing this design could not settle

Everything else here is decided. This is a verification gate, and it is placed
first because its answer changes a manifest.

**The question.** Does the autoscaler's computed parallelism reach the live CR's
`spec.flinkConfiguration` as `pipeline.jobvertex-parallelism-overrides`?

**Why it matters.** `selfHeal: false` everywhere means ArgoCD will never fight the
autoscaler. But this project uses **OutOfSync as the signal**, and Phase 5's
Drill D is built on it. If those overrides land in the live spec, `flink-job-blue`
goes permanently OutOfSync the moment the autoscaler first acts, and that signal
is dead. This is the same shape as the `priority: 0` defect diagnosed in Phase 5
Task 4.

**Why it is genuinely open.** `applyAutoscaler(ctx)` runs inside the reconcile
loop before the spec diff is built, which suggests the spec is touched. But the
in-place path is `PUT /jobs/:jobid/resource-requirements` against a running
JobManager, which would not touch the CR at all. The operator may also persist
overrides so they survive a JobManager restart. The documentation does not settle
it and this design will not assert it.

**The check**, run after the first scaling event in Drill F:

```bash
kubectl -n personalization-blue get flinkdeployment personalization \
  -o jsonpath='{.spec.flinkConfiguration.pipeline\.jobvertex-parallelism-overrides}{"\n"}'
```

**The branch:**

- **Empty**: nothing to do. Record it as verified.
- **Populated**: add `ignoreDifferences` for that field to
  `manifests/argocd-apps/flink-job-blue.yaml` **before** Drill F is repeated, so
  Drill D's signal stays meaningful for Phase 7 and Phase 8.

## 12. Done when

- Drill E shows `RECOMMENDED_PARALLELISM` rising on the three Click-consuming
  vertices under a Load Ramp, with `scaling.enabled` still `"false"`.
- Drill F shows a rescale record with `triggerCause: UPDATE_REQUIREMENT` and
  `terminalState: COMPLETED`, taking parallelism from 2 to 6 and the pod count
  from 1 to 3.
- Drill G shows parallelism and pod count falling again.
- Drill H shows kwok nodes appearing under the Decoy Workload and consolidating
  when it is scaled to zero.
- `curl -s localhost:30011/jobs/<id>` reports `maxParallelism` **120** on every
  vertex and **no** vertex name containing `Sink: Print to Std. Out`.
- The generator's achieved rate was **measured** from topic offsets, not read off
  `--click-rate`.
- Section 11's probe was run and its answer recorded, whichever way it went.

## 13. Out of scope, and why

| Left out | Reason |
|---|---|
| The Standalone Variant and KEDA | Dropped deliberately. Section 4.2. ADR 0005's Decision needs amending |
| HorizontalPodAutoscaler on CPU | The signal inverts. A backpressured job blocks on network buffers and its CPU **falls** exactly when it should scale up. It would also be a no-op against the Native Variant, per 4.2 |
| `metrics-server` | Only an HPA on resource metrics would need it. The Job Autoscaler reads Flink's own REST API |
| Splitting the async operator from its chain | Section 5.5. It would change every downstream `JobVertexID` and it buys nothing, per 8.4 |
| Zone spread and PodDisruptionBudget re-verification under scaling | Phase 5 proved them at a fixed parallelism. `whenUnsatisfiable: ScheduleAnyway` on TaskManagers means a scale-up may land unevenly across Zones. That is accepted, not re-tested |
| Real node capacity | Impossible on one host. Section 9.4 |
| Prometheus, Grafana, OTel | Phase 7 and Phase 8. Autoscaler metrics are read from the Flink REST API here |
| Sync wave annotations | Still nothing needs ordering. Deferred since Phase 0 |

## 14. Consequences worth recording now

- **`job.autoscaler.vertex.max-parallelism` is a safety mechanism, not a tuning
  knob.** Nothing else stops a Load Ramp from filling the host, because every
  node lies about its memory. Anyone raising it must first redo the arithmetic in
  4.3.
- **`--click-rate` and `--shopper-count` are now coupled.** Session length is
  `e^(6 × clickRate ÷ shopperCount)`. Changing the ramp changes Session
  semantics. Hold `shopperCount ≈ 2.5 × clickRate` to keep Session length roughly
  constant, or accept the amplification deliberately and say so.
- **Phase 6 leaves `parallelism` non-deterministic.** Phase 7's Promotion runbook
  cannot assume 6, or 3 TaskManagers, or any fixed pod count. It must read the
  live value.
- **`upgradeMode` returns to `savepoint` after the clean start.** Phase 7's
  Promotion depends on it, per the Phase 5 handover.
- **Karpenter sits outside the GitOps tree**, per 9.3. That is one more component
  a cluster rebuild must install by hand, alongside cert-manager and ArgoCD.
- **The network buffer reservation of `256mb` is the headroom parallelism must
  stay inside**, carried forward from the Phase 5 design. Parallelism 6 was
  validated against it. Nothing here exceeds 6.
- **The two-config-lists rule is dead, because one list is gone.**
  `apps/pipeline/conf/config.yaml` was deleted in `b0705e0`. Every Phase 6 key
  goes into `spec.flinkConfiguration` alone, per 7.6. Phase 7 inherits a single
  list and should not re-derive a drift rule for it.
- **Two Phase 5 constraints are lifted, narrowly, and both lifts are recorded.**
  `:generator` may now change, per 5.4. The `apps/pipeline/conf/config.yaml`
  constraint is satisfied vacuously, since the file no longer exists, per 7.6.
  `:pipeline`'s job graph stays frozen, and that constraint is not lifted.
- **Three documents need editing when this design is approved**, none of which is
  implementation work: ADR 0005's Decision, per 4.2. The Decoy Workload entry in
  `CONTEXT.md`, per 9.1. The Phase 6 section of the phase plan, whose Karpenter
  paragraph carries the mechanism 9.1 corrects and whose 6b hours are no longer
  spent.
