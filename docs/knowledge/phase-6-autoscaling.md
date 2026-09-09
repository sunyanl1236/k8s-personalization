# Phase 6 knowledge: Autoscaling

Written during Phase 6, not before it. Companion to
[phase-5-operator-and-ha.md](phase-5-operator-and-ha.md), which covers the
operator, the `FlinkDeployment`, and HA. That document explains how one fixed
job runs. This one explains how it stops being fixed.

Design decisions and their rejected alternatives live in
[the autoscaling design](../superpowers/specs/2026-09-07-autoscaling-design.md).
This file is only for how things actually work.

Every number below was measured on this cluster unless it is labelled otherwise.

---

## Three layers, and why they are not competitors

### The problem, before the mechanism

"Autoscaling" names three different jobs. Conflating them is the most common
source of confusion here, and it is what [ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md)
exists to untangle.

```
Layer 3   NODES        "no machine has room"
          Karpenter, Cluster Autoscaler
                    ▲   trigger: a pod the scheduler could not place
                    │
Layer 2   PODS        "how many TaskManager pods"
          KEDA, HPA
                    ▲   trigger: an external metric, or CPU
                    │
Layer 1   PARALLELISM "how many subtasks per operator"
          Flink Job Autoscaler
                        trigger: busy time and backlog, from Flink itself
```

Layer 3 sits underneath and does not compete with the other two. It
reacts to whatever the layers above ask for.

Layers 1 and 2 genuinely compete, and they cannot coexist, because they need
different values of `spec.mode`. That is ADR 0005's finding.

**Phase 6 implements Layer 1 and Layer 3.** Layer 2 is deliberately excluded.

### Why Layer 2 would be silent, not wrong

Pods **do** scale in `mode: native`, so "why can a pod-scaler not drive that?"
is a fair question. The answer is not about pods.

- **An autoscaler never creates pods. It writes an integer** into a field. A
  controller watches that field and makes reality match. That field is the
  interface, so Layer 2 needs a number something is watching.
- **`mode: standalone` has one.** The operator creates a TaskManager
  `Deployment`, and `.spec.replicas` is that number.
- **`mode: native` has none.** The one Deployment in the namespace is
  `replicas: 2` selecting `component: jobmanager`. TaskManager pods carry
  `component: taskmanager` and do not match it. No Deployment, ReplicaSet, or
  replica field counts them.
- **The count is a calculation, not a setting:**
  `ceil(slots needed / taskmanager.numberOfTaskSlots)`, re-run inside the
  JobManager and never stored anywhere writable. The ResourceManager works out 
  how many it is short and calls the Kubernetes API to create exactly that many 
  bare pods, one at a time.

**In standalone a number exists and a controller obeys it. In native the count
is a result, so there is nothing to push.**

---

## Three ways to scale this job, and the one this phase uses

Layers 1 and 2 offer three real candidates. Karpenter is not among them: it adds
machines and does not change the job.

### Option 1: Flink Job Autoscaler with the adaptive scheduler

`mode: native`. The operator reads Flink's own per-vertex metrics, decides a
parallelism for each vertex, and Flink's ResourceManager creates whatever pods
that needs.

**For**

- **Per vertex.** Only the bottleneck grows. This job has 9 vertices with very
  different loads: three carry the full Click rate, everything below the merge
  sees roughly a twentieth of it.
- **Nothing new to install.** It ships inside the operator already running.
- **It separates backlog from steady state**, so it sizes to catch up and then
  gives the capacity back.
- **It rescales without a savepoint or a redeploy.** The JobManager survives and
  the JobID is unchanged.
- **It runs on the Native Variant**, which Phase 5 already validated and Phase 7
  already depends on.

**Against**

- **The skill is Flink-only.** It transfers to no other workload.
- **Busy time is its only input, and busy time lies** in three places: chained
  operators share one counter, async waiting is not counted, and a chained print
  sink is counted as the operator's own work.
- **Parallelism must divide `pipeline.max-parallelism`.** Fixing that value is
  not savepoint compatible, so it has to be decided before the job first starts.
- **Slow by default.** The metrics window plus the stabilization interval put a
  full cycle near twenty minutes unless both are set explicitly.
- **It requires the adaptive scheduler**, so the default scheduler's behaviour no
  longer applies anywhere.

### Option 2: KEDA with reactive mode

`mode: standalone`. KEDA watches Kafka consumer-group lag and writes a replica
count. The operator adds TaskManager pods, and the reactive scheduler stretches
the job across whatever slots appear.

**For**

- **KEDA is workload-agnostic.** Around seventy scalers, and it drives anything
  with a `/scale` subresource. **The largest transfer of the three.**
- **Lag is unambiguous.** "We are 40,000 records behind" means something to a
  person. "Vertex 4 is 82% busy" does not.
- **It teaches the external-metrics extension pattern**, where an `APIService`
  makes your own code answer as part of the Kubernetes API.

**Against**

- **Whole job only.** Reactive mode has one instruction, "use everything
  available", so fixing one bottleneck over-provisions the other eight vertices.
- **It forfeits Phase 5's validation.** Zone spread, the PodDisruptionBudget and
  HA behaviour were all proven on the Native Variant and do not carry across.
- **It blocks Phase 7.** [ADR 0006](../adr/0006-blue-green-native-mode.md) runs
  blue/green on the Native Variant, so this would have to be undone first.
- **Reactive mode is a narrowing branch.** It is standalone-only and explicitly
  incompatible with active resource managers, including Kubernetes in native
  mode.
- **Two more components** to install and keep in step with the first manifest.

### Option 3: HPA on CPU

The built-in Kubernetes autoscaler, reading pod CPU from `metrics-server`.

**For**

- **The simplest.** Built into Kubernetes, no new CRDs, universally understood.

**Against**

- **The signal points the wrong way, and this alone disqualifies it.**
  Backpressure blocks threads on output buffers, a blocked thread burns no CPU,
  so utilisation **falls** exactly when you need to scale up. It would scale down
  into a growing backlog.
- **A silent no-op in native mode**, for the reasons in "Why Layer 2 would be
  silent, not wrong".
- **Whole job only**, without lag's clear meaning to compensate.
- **`metrics-server` is not installed** on this cluster.

### Side by side

| | Job Autoscaler | KEDA + reactive | HPA on CPU |
|---|---|---|---|
| `spec.mode` | `native` | `standalone` | `standalone` |
| Signal | busy time and backlog | Kafka consumer lag | pod CPU |
| Signal quality | earlier, but needs interpreting | later, but exact | **inverts under backpressure** |
| Granularity | **per vertex** | whole job | whole job |
| Causation | parallelism changes, pods follow | pods change, parallelism follows | same as KEDA |
| Rescale cost | no savepoint, no redeploy | identical | identical |
| Extra components | none | KEDA | `metrics-server` |
| Inherits Phase 5's validation | **yes** | no | no |
| Compatible with Phase 7 | **yes** | no | no |
| Skill transfers to | Flink | **any Kubernetes workload** | any Kubernetes workload |

### The decision: the Job Autoscaler with the adaptive scheduler

Four reasons, in the order they actually decided it.

1. **Phase 7 settles it before anything else does.** ADR 0006 runs blue/green on
   the Native Variant. Options 2 and 3 both need `mode: standalone`, so either
   would have to be torn down and rebuilt one phase later.
2. **Per-vertex granularity matches this job's shape.** Three vertices carry the
   full Click rate and six see a fraction of it. Giving all nine the same
   parallelism wastes most of what it allocates.
3. **It inherits Phase 5 intact.** Zone spread, the PodDisruptionBudget, HA
   failover and checkpoints to MinIO were all proven on this exact deployment.
   Neither other option keeps any of that.
4. **Nothing new to install**, so the phase has fewer moving parts to debug when
   a scaling event does not happen.

Option 3 is not a close third. It fails on the signal itself, and would still
fail on a cluster where everything else about it worked.

**What this costs, stated plainly.** No external-metric autoscaling is
demonstrated anywhere in this project. That was
[ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md)'s reason for keeping
KEDA as a second variant, and dropping it is a deliberate exclusion rather than
an oversight. The decision itself belongs in an amendment to that ADR; this
section records the comparison behind it.

## Why it is called the Job Autoscaler

Because it scales the **job's parallelism**. Not pods, not nodes.

Pods are a consequence in this model, never a target. Nothing anywhere sets a
TaskManager count. See "Slot sharing" below.

### Two switches, not one

| Key | Turns on |
|---|---|
| `job.autoscaler.enabled` | Collect metrics, compute recommendations, publish them. **Observe only** |
| `job.autoscaler.scaling.enabled` | Actually apply them |

The first without the second is a **dry run**. The full algorithm runs and
exports `RECOMMENDED_PARALLELISM` per vertex, and the job never changes.

---

## Where each piece lives inside the JobManager pod

### The thing that confuses

"JobManager" names both the pod and one component inside it. The scheduler and
the thing that creates pods are **different components**, and knowing which is
which makes the rest of this document readable.

```
┌─ JobManager pod ─────────────────────────────────────────┐
│                                                          │
│  Dispatcher            accepts submissions, spawns a     │
│                        JobMaster per job                 │
│                                                          │
│  ResourceManager       ONE per cluster                   │
│    └ SlotManager       tracks registered TaskManagers    │
│    └ Kubernetes driver ← creates TaskManager pods        │
│                                                          │
│  JobMaster             ONE per job                       │
│    └ AdaptiveScheduler ← decides parallelism             │
│    └ SlotPool          asks the ResourceManager for slots│
│    └ CheckpointCoordinator                               │
└──────────────────────────────────────────────────────────┘
```

**The scheduler never touches Kubernetes. The ResourceManager never decides
parallelism.**

### The one substitution that defines the two modes

```
mode: native      ResourceManager is a Kubernetes client. It creates pods.
mode: standalone  ResourceManager waits for TaskManagers to arrive on their own.
```

Everything else about the two modes follows from that single difference.

---

## Three graphs, and which one a rescale rebuilds

### Plain version first

- **StreamGraph.** You write code, and each call is a step. `.keyBy(...)` is a
  step. `.process(new SessionAggregator())` is a step. Flink writes down the list
  of steps and which one feeds which.
  - It says **what work happens**.
  - It says nothing about how many copies run, or where.

- **JobGraph.** Flink then looks for steps that can be glued together. If B only
  ever reads from A, and both can share a thread, Flink merges them.
  - Merged, they pass records by a direct method call.
  - Unmerged, records are serialized and sent over the network.
  - A merged run of steps is a **chain**. The list of chains is the JobGraph,
    and that is what gets submitted.

- **ExecutionGraph.** The JobManager then makes copies. Parallelism 6 means six
  copies of every chain, each working on a different slice of the data.
  - It tracks every copy, where it runs, and whether it is alive.

### The diagram

```
    your DataStream code
            │  built in the CLIENT
            ▼
    StreamGraph        one box per step you wrote
            │  glue together steps that can share a thread
            ▼
    JobGraph           one box per chain ("JobVertex"). Submitted.
            │  built in the JOBMANAGER: make `parallelism` copies of each box
            ▼
    ExecutionGraph     one box per running copy ("ExecutionVertex")
```

### This job, with numbers

| Graph | One box is | Count |
|---|---|---|
| StreamGraph | a step you wrote | ~17, counting the debug print sinks |
| **JobGraph** | a chain | **9**, which is what the REST API lists |
| **ExecutionGraph** at parallelism 6 | one running copy of one chain | **9 × 6 = 54** |

Flink's internals documentation describes the runtime layer precisely: each
ExecutionVertex is tracked by an **Execution** object, and keeps a history of
prior Executions. That per-subtask attempt history is how one failed subtask
restarts without a full job restart.

### Why this matters twice

**A rescale rebuilds only the ExecutionGraph.** The StreamGraph and the JobGraph
are untouched, so the steps and the chains stay exactly as they were. Only the
number of copies of each chain changes. That is why parallelism can change
without breaking state.

**Removing the debug print sinks changes the JobGraph.** Different steps get
glued together, so the chains change, so `JobVertexID`s change, so state saved
under the old IDs has nowhere to map. There is no `.uid()` anywhere in
`apps/pipeline/src/main/java/`, so every ID is derived by hashing graph
position.

Same mechanism, opposite consequences. It is worth holding both.

---

## Slot sharing, and why pod count is never set

### The thing that confuses

Nine vertices at parallelism 6 is 54 subtasks. So 54 slots?

No. **Six.**

### Why

By default every operator belongs to **one slot sharing group**. A slot does not
hold one subtask. A slot holds one subtask of *every* vertex, a complete
vertical slice of the pipeline.

```
slot 1: [source#1][session#1][cep#1][join#1][merge#1][promo#1][async#1][sink#1]
slot 2: [source#2][session#2][cep#2][join#2][merge#2][promo#2][async#2][sink#2]
  ...
slot 6: [source#6][session#6][cep#6][join#6][merge#6][promo#6][async#6][sink#6]
```

```
slots needed = the HIGHEST parallelism among the vertices, NOT the sum
```

### The arithmetic, live

```
parallelism 6
taskmanager.numberOfTaskSlots = 2

slots needed = 6
pods needed  = ceil(6 ÷ 2) = 3 TaskManagers
```

Which is what runs: 3 TaskManagers, one per Zone.

---

## Busy time, the one number everything rests on

### The problem

The autoscaler never looks at CPU. The operator's own documentation is explicit
that container CPU and memory are not used, because high resource use shows up
in processing rate and busy time instead.

So it needs a measure of "how hard is this vertex working" that survives being
blocked, throttled, or starved.

### The mechanism

A Flink task thread is always in exactly one of three states, and they sum to
about 1000 ms per second of wall clock:

| State | Metric | Meaning |
|---|---|---|
| Idle | `idleTimeMsPerSecond` | waiting for input |
| Backpressured | `backPressuredTimeMsPerSecond` | blocked on output buffers |
| Busy | `busyTimeMsPerSecond` | running user code |

From busy time the autoscaler derives the number that drives everything:

```
TRUE_PROCESSING_RATE = numRecordsInPerSecond ÷ (busyTimeMsPerSecond ÷ 1000)
```

Read as: **records per second this vertex could manage if it were busy 100% of
the time.**

### Worked example

A vertex takes 1,000 records per second and reports 500 ms/s busy.

```
busy fraction        = 0.5
TRUE_PROCESSING_RATE = 1000 ÷ 0.5 = 2,000 records/sec
LOAD                 = 0.5
```

At a 0.6 utilization target the vertex is under-loaded and nothing happens. Push
input to 1,400/s and busy time to 700 ms/s, and it is over target, so
parallelism rises.

Everything the autoscaler concludes rests on this one division. Inflate busy
time and it will size for a bottleneck that is not there. See "Three places busy
time lies".

---

## The algorithm, step by step

Five actors, in two pods. The label on each step is the one that performs it.

| Actor | Runs in |
|---|---|
| **Autoscaler** | the `flink-kubernetes-operator` pod |
| **JobMaster** | the JobManager pod, one per job |
| **AdaptiveScheduler** | inside the JobMaster |
| **ResourceManager** | the JobManager pod, one per cluster |
| **TaskManager** | its own pod |

1. **Autoscaler · Collect.** Polls the Flink REST API for per-vertex
   `busyTimeMsPerSecond`, `numRecordsInPerSecond`, and source backlog.
   Accumulates `job.autoscaler.metrics.window` of history.
2. **Autoscaler · Work out capacity.** `TRUE_PROCESSING_RATE` **per subtask**,
   as above. What one subtask could manage if it were busy 100% of the time.

   ```
   Example. The session window vertex, parallelism 2, under a Load Ramp.
     measured   numRecordsInPerSecond  800/s      the whole Click rate
     measured   busyTimeMsPerSecond    640 ms/s   busy fraction 0.64

     per subtask                800 / 2    = 400/s   while 64% busy
     TRUE_PROCESSING_RATE       400 / 0.64 = 625/s   if it were 100% busy
   ```

   It is doing 400/s and could do 625/s. That headroom is what step 4 spends.

3. **Autoscaler · Work out demand.**
   `TARGET_DATA_RATE = incoming rate + CATCH_UP_DATA_RATE`. See the next section.

   ```
   Example, continued. Lag 120,000 records, catch-up.duration 5m.
     CATCH_UP_DATA_RATE   120,000 / 300 s = 400/s   extra, to burn the backlog
     TARGET_DATA_RATE     800 + 400       = 1,200/s
   ```

   Sizing for 800/s would keep pace forever and never catch up. The 400/s is
   temporary: once the lag is gone the target falls back to 800/s.

4. **Autoscaler · Size.**
   `newParallelism = TARGET_DATA_RATE / (TRUE_PROCESSING_RATE * job.autoscaler.target.utilization)`.

   ```
   Example, continued. target.utilization 0.6.
     usable per subtask   625 * 0.6   = 375/s    deliberately not 625
     subtasks needed      1,200 / 375 = 3.2
   ```

   The utilization target is why it is 375 and not 625. Sizing every subtask to
   run flat out leaves no headroom for the next fluctuation.

5. **Autoscaler · Constrain.** Three rules turn the raw number into one the job
   can actually run:
   - It must divide `pipeline.max-parallelism` exactly.
   - It cannot go below `job.autoscaler.vertex.min-parallelism` or above
     `job.autoscaler.vertex.max-parallelism`.
   - A source vertex cannot go above the number of Kafka partitions, which is
     **3** on every topic here. More readers than partitions would leave the
     extra readers with nothing to read.

   ```
   Example, continued.
     3.2 is not a parallelism.
     round up to a divisor of 120:  1 2 3 4 5 6 8 10 12 ...  ->  4
     is 4 above the ceiling of 6?   no                        ->  4
     RECOMMENDED_PARALLELISM = 4
   ```

   With the default `pipeline.max-parallelism` of 128 the divisors are
   1, 2, 4, 8, so 3.2 would also have landed on 4 here. A result of 2.4 or 5.1
   would not have.

6. **Autoscaler · Publish.** Exports the metrics below. **Stops here if
   `job.autoscaler.scaling.enabled` is false.**
7. **Autoscaler · Apply.** `PUT /jobs/:jobid/resource-requirements` with a new
   `lowerBound` and `upperBound` per vertex. See "From a decision to pods".
8. **JobMaster · Declare.** Re-declares its slot requirement to the
   ResourceManager: "for this slot sharing group I now want 6 slots, minimum 2".
9. **ResourceManager · Allocate.** Compares declared against registered,
   computes `ceil(shortfall / taskmanager.numberOfTaskSlots)`, and in
   `mode: native` calls the Kubernetes API itself to create that many pods.
   **The job is still running at the old parallelism throughout this step.**
10. **TaskManager · Register.** New pods start, register with the JobMaster, and
    offer their slots.
11. **AdaptiveScheduler · Gate.** `...executing.cooldown-after-rescaling` must
    have elapsed. See "Four timers".
12. **AdaptiveScheduler · Cancel.** Stops every Execution, on every TaskManager,
    including the ones that are staying. **The pause begins here.**
13. **AdaptiveScheduler · Rebuild and restore.** `WaitingForResources`, then a
    new ExecutionGraph built from the same JobGraph, key groups redistributed,
    state read back from the checkpoint, Executions deployed. **The pause ends.**
    JobID unchanged. See "The rescale itself, state by state".
14. **Autoscaler and AdaptiveScheduler · Cool down.**
    `job.autoscaler.stabilization.interval` suspends the autoscaler's collection.
    `...cooldown-after-rescaling` blocks the scheduler's next action. Both run,
    independently.

Steps 1 to 6 are the operator's autoscaler. Step 7 is the handover. Steps 8 to
14 are the JobManager, and the autoscaler takes no further part in them.

### The metrics it exports

Named `[prefix].Autoscaler.[jobVertexID].[ScalingMetric].Current` or `.Average`,
where `jobVertexID` is the hex ID from the JobManager REST API.

| Metric | Reads as |
|---|---|
| `LOAD` | subtask utilization |
| `TRUE_PROCESSING_RATE` | capacity at 100% busy |
| `TARGET_DATA_RATE` | the rate it must sustain |
| `CATCH_UP_DATA_RATE` | extra rate to burn down backlog |
| `LAG` | pending records at the source |
| `PARALLELISM` / `RECOMMENDED_PARALLELISM` | now, and what it wants |
| `SCALE_UP_RATE_THRESHOLD` / `SCALE_DOWN_RATE_THRESHOLD` | the band it tolerates |
| `NUM_SOURCE_PARTITIONS` | source vertices only |
| `MAX_PARALLELISM` | the configured ceiling |

`RECOMMENDED_PARALLELISM` against `PARALLELISM` is the whole dry run.

---

## Backlog is not the same as throughput

### The problem

The job is 500,000 records behind and Clicks arrive at 800/sec. Size it for
800/sec and it processes exactly 800/sec forever. It keeps pace and **never
catches up**. Lag sits at 500,000 permanently.

### The mechanism

A second term:

```
TARGET_DATA_RATE   = incoming rate + CATCH_UP_DATA_RATE
CATCH_UP_DATA_RATE = pending records ÷ job.autoscaler.catch-up.duration
```

### Worked example, with `catch-up.duration: 5m`

| | |
|---|---|
| Incoming | 800 rec/s |
| Lag | 500,000 records |
| Catch-up term | 500,000 ÷ 300 s = **1,667 rec/s** |
| Target | 800 + 1,667 = **2,467 rec/s** |

It sizes for 2,467, burns the backlog down over five minutes, and then the lag
term falls to zero, the target returns to 800, and it **scales back down**.

So it over-provisions on purpose and unwinds it. Seeing parallelism rise then
fall with no change in input rate is correct behaviour, not oscillation.

`job.autoscaler.backlog-processing.lag-threshold` stops it doing this for
trivial lag.

---

## Parallelism must divide max parallelism

### The measurement

```
curl -s localhost:30011/jobs/<id>  →  every vertex: PAR 6, MAXPAR 128
```

### Why it constrains

The autoscaler computes scaled parallelism as a **divisor of max parallelism**,
to avoid key skew. Max parallelism is the key group count, and if the group
count does not divide evenly across subtasks, some subtasks own more groups than
others permanently.

Divisors of 128 are 1, 2, 4, 8, 16, 32, 64, 128. **It can never choose 3, 5 or
6.** Inside this cluster's memory budget the entire ladder is 2, then 4, then a
jump to 8.

### The fix, and its cost

```yaml
pipeline.max-parallelism: "120"
```

Divisors include 2, 3, 4, 5, 6. Flink's own documentation recommends values with
many divisors, naming 120, 180, 240, 360 and 720, rather than relying on the
default.

**The cost is not small.** Max parallelism sets the key group count, so this is
not savepoint compatible. It must be set before the job first starts and never
changed again.

### Why the default is 128

Flink derives it from the initial parallelism, with a floor of 128. Nothing
chose it for this job. That is exactly why it needs choosing.

---

## From a decision to pods: Declarative Resource Management

### The old way, for contrast

The default scheduler is imperative. It computes exactly which task goes in which
slot and demands precisely those slots. If they do not arrive, the job fails to
start. Parallelism is fixed at submission.

### The declarative way

The JobMaster does not demand slots. It publishes a **range** per vertex and
adapts to what it receives. There is a REST endpoint holding exactly that
document:

```
GET  /jobs/:jobid/resource-requirements
PUT  /jobs/:jobid/resource-requirements
```

```json
{ "jobVertexResourceRequirements": {
    "<jobVertexID>": { "parallelism": { "lowerBound": 1, "upperBound": 6 } }
}}
```

**That `PUT` is how a parallelism change is applied in place.** The autoscaler is
not editing a file and hoping. It writes a new desired state into a running
JobManager.

### The chain, scaling 2 to 6

```
BEFORE:  parallelism 2  →  2 slots  →  1 TaskManager pod

1. Autoscaler PUTs new bounds per vertex
                 │
2. JobMaster re-declares to the ResourceManager:
     "slot sharing group default: I want 6 slots, minimum 2"
                 │
3. ResourceManager compares declared against registered.
     declared 6, registered 2, shortfall 4
                 │
4. mode: native, so the ResourceManager IS a Kubernetes client.
     ceil(4 ÷ 2) = 2 new pods. It calls the Kubernetes API itself.
     ^^^ nobody set a replica count anywhere
                 │
5. Pods start, TaskManagers register, offer 4 slots. Total 6.
                 │
6. The rescale, below.

AFTER:   parallelism 6  →  6 slots  →  3 TaskManager pods
```

---

## The rescale itself, state by state

The adaptive scheduler is a state machine. A rescale is a round trip out of
`Executing` and back:

```
Executing ─► Restarting ─► WaitingForResources ─► CreatingExecutionGraph ─► Executing
```

**1. `Executing` notices a mismatch.** Either new requirements arrived, or new
slots registered.

**2. Cooldown check.** `jobmanager.adaptive-scheduler.executing.cooldown-after-rescaling`
must have elapsed, or the intent is held.

**3. Cancel every Execution.** All subtasks stop, on every TaskManager,
including ones that are staying. Slots are released. **This is the pause.**
Records stop flowing here.

### Step 3 does not take a savepoint

There are two different ways a Flink job can be stopped, and they are not
variants:

| | `stop-with-savepoint` | Cancel, for a rescale |
|---|---|---|
| Who does it | operator upgrade cycle, or a human | AdaptiveScheduler, step 3 |
| Takes a new snapshot? | **Yes.** Flushes, snapshots, then terminates | **No.** Nothing new is written |
| Restores from | that fresh savepoint | the **last checkpoint that already happened** |
| Waits for the snapshot | yes, and it can take a while on large state | nothing to wait for |

A savepoint would defeat the purpose. Writing the full state to S3 before the
rescale could begin would make every scaling decision expensive. Checkpoints are
already being written on a schedule, so the rescale simply uses the most recent
one.

**The cost is replay.** With `execution.checkpointing.interval: 10s`, the last
checkpoint was up to 10 seconds ago, and everything since is processed again
when the sources rewind to its offsets. No duplicate Recommendations reach the
topic, because `execution.checkpointing.mode: EXACTLY_ONCE` and the sink's
`DeliveryGuarantee.EXACTLY_ONCE` abort the uncommitted transaction and reopen it.
`scripts/recommendation-snapshot.sh` is what proves that.

**4. `WaitingForResources`.** Two numbers, and they differ:

| | Meaning |
|---|---|
| `minimalRequiredSlots` | below this the job cannot run at all |
| `desiredSlots` | what the new parallelism wants |

With `desired` available it proceeds at once. With only *sufficient* it waits
out `jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout` in
case the rest arrive, then proceeds with what it has.

**5. `CreatingExecutionGraph`.** A new ExecutionGraph, from the same unchanged
JobGraph, at the new parallelism.

**6. Redistribute key groups.** The expensive part.

```
pipeline.max-parallelism 120, so 120 key groups, fixed for the life of the state

parallelism 2:  subtask0 → groups 0..59    subtask1 → groups 60..119
parallelism 6:  subtask0 → groups 0..19    subtask1 → groups 20..39   ...
```

Each subtask reads **its** key-group range from the checkpoint in S3. Rescale
duration scales with state size, not with pod count. The session windows at
2,000 Shoppers are that state.

This is also why max parallelism cannot change across a restore: the boundaries
would move under the data.

**7. Deploy.** Executions are assigned to slots and started.

**8. `Executing`.** Records flow. **JobID unchanged.**

`scripts/recommendation-snapshot.sh`, built for Phase 5's Drills, measures the
gap between steps 3 and 8.

### Read the states rather than trusting this list

```yaml
web.adaptive-scheduler.rescale-history.size: "10"
```

Each record carries a `schedulerStates` array with `state`,
`enterTimestampInMillis`, `leaveTimestampInMillis` and `durationInMillis`, plus
per-vertex `preRescaleParallelism` and `postRescaleParallelism`, a
`triggerCause` of `INITIAL_SCHEDULE`, `UPDATE_REQUIREMENT`,
`NEW_RESOURCE_AVAILABLE` or `RECOVERABLE_FAILOVER`, and a `terminalState` of
`COMPLETED`, `FAILED` or `IGNORED`.

**That `triggerCause` separates a scaling event from a failover**, which pod
counts alone cannot.

---

## What survives a rescale and what does not

### The thing that confuses

"Pods are created and deleted as parallelism changes" sounds like the existing
pods are replaced. They are not.

**Two lifecycles move independently:**

| | On a rescale |
|---|---|
| **Subtasks** (Executions) | **All cancelled**, including on TaskManagers that stay |
| **TaskManager pods and JVMs** | **Survive.** They release slots and are handed new work |

Cancelling a subtask does not kill the JVM it ran in.

### Scale up, 2 to 6

```
BEFORE   TM-1 [ subtask0 | subtask1 ]                    1 pod

  cancel both. TM-1 stays up, slots free.
  ResourceManager creates 2 pods.

AFTER    TM-1 [ subtask0 | subtask1 ]  ← same pod, same JVM, new subtasks
         TM-2 [ subtask2 | subtask3 ]  ← new
         TM-3 [ subtask4 | subtask5 ]  ← new
```

`kubectl get pods` shows TM-1 with an unchanged name, unchanged age, and **no
increase in restart count**. Its subtasks changed underneath it.

**Keyed state is still re-read from the checkpoint, even on TM-1.** Key group
ranges moved, so `subtask0` now owns groups 0 to 19 instead of 0 to 59. Local
RocksDB files are not reusable across a range change.

---

## Scaling down does not always delete a pod

**A pod is deleted only when it holds zero subtasks.** A half-used TaskManager
is kept, wasting a slot.

```
PACKED
   TM-1 [ subtask0 | subtask1 ]
   TM-2 [  empty   |  empty   ]  → idle → released → POD DELETED
   TM-3 [  empty   |  empty   ]  → idle → released → POD DELETED

SPREAD
   TM-1 [ subtask0 |  empty   ]
   TM-2 [ subtask1 |  empty   ]
   TM-3 [  empty   |  empty   ]  → only this one is deletable
```

`jobmanager.adaptive-scheduler.prefer-minimal-taskmanagers` is the switch.

**This is a trap for any scale-down Drill.** Reduce parallelism, watch the pod
count barely move, conclude nothing happened. Read that setting's live value
before writing a Drill's expected result.

### And there is a delay on top

Even a fully idle TaskManager is not deleted at once. The ResourceManager holds
it for an idle timeout so a freed pod can be reused rather than recreated
seconds later. Read the live value rather than assuming a default:

```bash
kubectl -n personalization-blue exec deploy/personalization -- \
  cat /opt/flink/conf/config.yaml | grep -i taskmanager-timeout
```

---

## Three ways parallelism can change, compared

| | Old manual way | Operator upgrade cycle | **In-place rescale** |
|---|---|---|---|
| Trigger | a human | a spec change (`specChanged`) | autoscaler, or `PATCH /rescaling` |
| Snapshot | `flink stop --savepointPath` | operator calls the savepoint API | none taken; restores from last checkpoint |
| Where the path is kept | **your shell history** | `status.jobStatus.upgradeSavepointPath` | n/a |
| JobManager pod | destroyed | **destroyed** | **survives** |
| Redeploy | `flink run -p N -s ...` | operator redeploys the new spec | none |
| JobID | changes | changes | **unchanged** |
| Recovery if interrupted | you hold the pieces | `ReconciliationState.UPGRADING` | scheduler state machine |

The first two are nearly the same mechanism with a different actor and better
bookkeeping. **In-place rescale is the different one**, and its distinguishing
property is that the JobManager is never destroyed.

Note that both of the first two take everything down here, because
`spec.job.jarURI` makes this **application mode**: the cluster *is* the job. A
session cluster would survive `flink stop`. This project does not run one.

The one thing a human genuinely cannot do easily is `upgradeMode: last-state`,
where the operator skips the savepoint and recovers from the last checkpoint via
HA metadata. That works on a job too broken to take a savepoint.

---

## Four timers, and which one blocks what

They live in different components and answer different worries. They are not
variants of each other.

| Setting | Lives in | Worry |
|---|---|---|
| `job.autoscaler.metrics.window` | operator's autoscaler | "one spike is not a trend" |
| `job.autoscaler.stabilization.interval` | operator's autoscaler | "metrics from a just-restarted job are garbage" |
| `...executing.cooldown-after-rescaling` | JobManager's scheduler | "do not thrash the job" |
| `...executing.resource-stabilization-timeout` | JobManager's scheduler | "more slots may still be arriving" |

The first two throttle **decisions**. The third throttles **actions**, including
a manual `PATCH /rescaling`. The fourth is not pacing at all; it sits inside
step 4 of the rescale.

### Why stabilization exists

Right after a restart the job replays a backlog, so throughput briefly spikes far
above steady state. Feed that into `TRUE_PROCESSING_RATE` and every vertex looks
far faster than it is. The autoscaler suspends metric collection and scaling
actions until the job settles.

### The sequence

```
T+0     Load Ramp starts
T+0..3  collecting. Nothing happens. NOT a fault.
T+3m    metrics.window filled. Decision: parallelism 6.
        ├─ PUT /jobs/<id>/resource-requirements
        ▼
        SCHEDULER: cooldown elapsed?  no → hold   yes → rescale
        ▼
T+3m30  Executing again
        ├─ AUTOSCALER: stabilization.interval (1m) starts
        └─ SCHEDULER:  cooldown-after-rescaling starts
T+4m30  autoscaler resumes collecting
T+~7m   window refilled, next decision possible
```

**Either guard can block a scale alone.** Set `stabilization.interval` shorter
than the scheduler's cooldown and the autoscaler will log a scaling decision that
the JobManager quietly defers.

With the shipped defaults this cycle runs closer to twenty minutes, which is
unwatchable in a lab. **Set the window and the interval explicitly.** The most
common "the autoscaler is broken" report is someone watching for ninety seconds.

---

## Layer 2 in detail: the two pod-scalers this phase does not build

Neither is built here. Both are written down because the *shape* of how they
fail in `mode: native` is the shape this project keeps meeting, and because
`/scale` is the interface every Kubernetes autoscaler uses.

### `/scale`, the interface both of them use

Every object has a URL. `GET` it and you get the whole thing back, hundreds of
lines:

```
/apis/apps/v1/namespaces/default/deployments/nginx
```

A **subresource** is an extra path underneath, showing a **slice** of the same
object in its own small shape:

```
/apis/apps/v1/namespaces/default/deployments/nginx/scale
```

Nothing extra is stored. The API server reads the object, pulls out three
fields, and hands them back in a standard wrapper. Write to it and it puts them
back.

```yaml
apiVersion: autoscaling/v1
kind: Scale
spec:
  replicas: 2
status:
  replicas: 2
  selector: k8s-app=kube-dns
```

| Line | Meaning |
|---|---|
| `apiVersion: autoscaling/v1` | Which group and version defines the type. Still `v1` because it is tiny and has never needed to change |
| `kind: Scale` | **You never create one of these.** It is a view built on demand out of another object |
| `spec.replicas` | **What you want.** The only writable field in the whole object |
| `status.replicas` | **What there actually is.** Read-only |
| `status.selector` | The label query that finds this object's pods. Read-only |

#### Three reasons an autoscaler reads it

1. **It must work on types it has never heard of.** The HPA controller shipped
   years before your CRD existed. It cannot know that a `Deployment` keeps its
   count at `.spec.replicas` while a `FlinkDeployment` keeps it at
   `.spec.taskManager.replicas`. `/scale` is one shape that fits both. Each CRD
   tells the API server where its own fields live, and the autoscaler never
   sees that.
2. **It needs the selector to find the pods.** To average CPU across pods you
   must know which pods. `status.selector` answers that without the autoscaler
   understanding any target's labelling scheme.
3. **Permissions.** `deployments/scale: update` can be granted without
   `deployments: update`. An autoscaler that can change a number but not a
   container image is a much smaller thing to trust.

#### Why the target is the CR and never a pod

A pod has no count. There is exactly one of it, and no field saying five should
exist. It also does not last: `personalization-taskmanager-3-1` will not survive
the next rescale.

So the question is never "which pod" but **"which object holds the number".**
Objects that manage other objects form a chain, and each level overwrites the
one below:

```
FlinkDeployment "personalization"      ← the OPERATOR reads this
        │  operator writes down
   TaskManager Deployment              ← the operator writes this
        │  Deployment controller writes down
      ReplicaSet
        │  ReplicaSet writes down
       Pods                            ← no number lives here
```

**Write at the top or be overwritten.** Point KEDA at the TaskManager Deployment
and it sets 5; the operator's next reconcile reads 2 from the CR and puts it
back, forever. Pointing at the CR writes to what the operator *reads*, and
everything below follows.

Same rule as never editing a ReplicaSet's count while a Deployment owns it.

There is one other Deployment in the namespace, and it is a trap:

```
deployment.apps/personalization   2/2   selector: {component: jobmanager}
```

Its count is **JobManagers**. Scaling it gives you five JobManagers, of which one
is leader and four sit as standbys, burning 2 GiB each for no extra processing.

---

### HPA on CPU, step by step

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
spec:
  scaleTargetRef:
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkDeployment
    name: personalization
  minReplicas: 1
  maxReplicas: 3
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

| Line | Meaning |
|---|---|
| `autoscaling/v2` | **Not v1.** v1 could only do CPU. v2 added the `metrics:` list and custom and external metrics |
| `HorizontalPodAutoscaler` | Horizontal means more copies. Vertical would mean bigger copies, a different controller |
| `scaleTargetRef` | Three fields naming one object: group and version, kind, name. No namespace, because an HPA can only target its own |
| `minReplicas` / `maxReplicas` | Floor and ceiling. **The ceiling is the only thing between a bad metric and a full cluster** |
| `metrics:` | A list. With several entries the HPA computes a count for each and takes the **largest** |
| `type: Resource` | The metric is CPU or memory of the pods themselves. The others are `Pods`, `Object`, and `External`, which is KEDA's |
| `target.type: Utilization` | A **percentage of the pod's request**, not absolute cores. The alternative, `AverageValue`, is absolute |
| `averageUtilization: 70` | Hold the average at 70% of requests |

#### The steps

1. **metrics-server · Scrape.** Reads each kubelet's `/metrics/resource` every
   15 seconds.
2. **metrics-server · Serve.** Answers on `metrics.k8s.io`, registered as an
   `APIService` the same way KEDA registers its own.
3. **HPA · Read the target.** `GET /scale` gives it the current replica count
   **and the label selector**.
4. **HPA · Find and measure the pods.** Uses that selector, then asks
   `metrics.k8s.io` for their CPU.
5. **HPA · Compute.**
   `desired = ceil(current * currentUtilization / targetUtilization)`.
6. **HPA · Write.** `PUT /scale` with the new number.

#### Worked example

```
TaskManager request  cpu: 1        so 100% means 1000 millicores
current replicas     3
measured average     910m per pod  =  91%

desired = ceil(3 * 91 / 70) = ceil(3.90) = 4
maxReplicas is 3, so it stays at 3
```

#### Why the signal points the wrong way

This is the disqualifying problem, not a matter of taste.

```
load rises  →  operators saturate
            →  output buffers fill
            →  BACKPRESSURE: upstream tasks block waiting for a buffer
            →  a blocked thread burns no CPU
            →  CPU utilisation FALLS
```

Continue the example. Backpressure sets in and average CPU drops to 30%:

```
desired = ceil(3 * 30 / 70) = ceil(1.29) = 2
```

**It scales down, into a growing backlog.** CPU falls exactly when you most need
to scale up. Busy time does not have this defect, which is precisely why the Job
Autoscaler uses it.

Two further problems, in order: this would be a silent no-op in `mode: native`
for the reasons in "Why Layer 2 would be silent, not wrong", and it can only
scale the whole job, never one vertex.

---

### KEDA on Kafka lag, step by step

#### KEDA is two programs

| Pod | In plain terms |
|---|---|
| `keda-operator` | **The worker.** Reads your `ScaledObject`, connects to Kafka, asks how far behind the consumer group is, writes the answer on a notepad, repeats. Also creates the HPA |
| `keda-operator-metrics-apiserver` | **The receptionist.** A small web server. When asked "what is metric `s0-kafka-clickstream` right now?", it reads the notepad and says the number. It never touches Kafka |

They are separate because Kubernetes has a rule: to let an HPA use your metric,
you must **serve it as though it were part of the Kubernetes API**. That means
being a web server at a registered address, which is a different job from polling
Kafka.

The registration is an `APIService`, and after it the API server forwards every
request for that whole API group to KEDA:

```yaml
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1beta1.external.metrics.k8s.io
spec:
  group: external.metrics.k8s.io
  version: v1beta1
  service:
    name: keda-operator-metrics-apiserver
    namespace: keda
```

To the HPA, `external.metrics.k8s.io` now looks like a native part of Kubernetes.
It has no idea KEDA exists.

There are three such metric API groups, and Kafka lag belongs to the third,
because it is not a property of any Kubernetes object:

| Group | Carries | Served by |
|---|---|---|
| `metrics.k8s.io` | pod and node CPU and memory | `metrics-server` |
| `custom.metrics.k8s.io` | metrics *about* Kubernetes objects | e.g. prometheus-adapter |
| `external.metrics.k8s.io` | metrics from **outside** the cluster | KEDA |

#### The manifests

```yaml
# FlinkDeployment
spec:
  mode: standalone
  flinkConfiguration:
    scheduler-mode: reactive          # standalone only
    jobmanager.scheduler: Adaptive    # reactive is a mode OF the adaptive scheduler
  taskManager:
    replicas: 1                       # now a real, honoured number
```

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
spec:
  scaleTargetRef:
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkDeployment
    name: personalization
  pollingInterval: 15
  cooldownPeriod: 60
  minReplicaCount: 1
  maxReplicaCount: 3
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: personalization-kafka-bootstrap.kafka.svc:9092
        consumerGroup: personalization-pipeline
        topic: clickstream
        lagThreshold: "1000"
```

#### The steps

1. **Operator · Build.** Creates a JobManager Deployment and a **TaskManager
   Deployment** with `replicas: 1`. Flink knows nothing about Kubernetes here;
   TaskManagers find the JobManager by Service name and phone in.
2. **Reactive scheduler · Start wide.** Runs the job at the highest parallelism
   the available slots allow. One TaskManager with 2 slots gives parallelism 2.
3. **keda-operator · Poll.** Reads consumer-group lag every `pollingInterval`
   and caches it.
4. **HPA · Ask.** On its own 15 second cycle it requests
   `s0-kafka-clickstream` from `external.metrics.k8s.io`.
5. **API server · Forward.** The `APIService` sends that to
   `keda-operator-metrics-apiserver`, which answers from the cache.
6. **HPA · Compute.** `desired = ceil(totalLag / lagThreshold)`, held inside
   `minReplicaCount` and `maxReplicaCount`.
7. **HPA · Write.** `PUT /scale`, which lands in `spec.taskManager.replicas`.
8. **Operator · Reconcile.** Classifies this as a **scale** change, not an
   upgrade. No savepoint, no redeploy, no JobManager restart. It just sets the
   TaskManager Deployment's replica count.
9. **TaskManager · Register.** New pods start and offer their slots. **Existing
   pods are untouched**, because KEDA only ever writes `spec.replicas` and never
   the pod template.
10. **Reactive scheduler · Rescale.** Sees more slots than it is using, cancels
    the job's tasks and redeploys them at a higher parallelism, restoring from
    the last checkpoint.
11. **cooldownPeriod · Hold.** Blocks scale-down for 60 seconds after the last
    trigger fires.

**KEDA supplies the number. The HPA makes the decision.** Useful when debugging:
a wrong replica count is HPA arithmetic, a stale or missing metric is KEDA. The
one exception is 0 and 1, which the HPA cannot do, so KEDA writes those
transitions itself.

#### Worked example

```
lagThreshold  1000      maxReplicaCount 3      current replicas 1

Load Ramp starts, lag climbs
  total lag 2,400   →  desired = ceil(2400 / 1000) = 3   →  3 replicas
                       parallelism rises from 2 to 6

Ramp stops, the job catches up
  total lag   800   →  desired = ceil( 800 / 1000) = 1   →  1 replica
                       held for cooldownPeriod 60s first
```

#### What restarts, and what does not

**No pod restarts. The job restarts.**

| | Restarted by a KEDA scale event? |
|---|---|
| JobManager pod | **No** |
| Existing TaskManager pods | **No.** Same names, same age |
| New TaskManager pods | they are created, not restarted |
| The job's tasks | **Yes.** Cancelled and redeployed from the last checkpoint |

**Both layers are in place, in the same sense.** Reactive mode is a mode *of*
the adaptive scheduler, so a KEDA-driven scale runs the same state machine as an
autoscaler-driven one: cancel the Executions, rebuild the ExecutionGraph, restore
from the last checkpoint. No savepoint, no redeploy, no JobManager restart,
either way.

The real difference is which way the causation runs:

```
Layer 1   parallelism changes  ->  pods follow
Layer 2   pods change          ->  parallelism follows
```

The Job Autoscaler picks a parallelism per vertex, and Flink's ResourceManager
works out how many pods that needs. KEDA picks how many pods exist, and the
reactive scheduler stretches the job across whatever slots turn up.

That is also why Layer 2 can only move the whole job. Reactive mode has exactly
one instruction, "use everything available", so it has no way to give one vertex
more subtasks than another.

---

## Questions this document answers

### The mechanism

**Why is it called the *Job* Autoscaler?**
Because it changes the job's parallelism. Pods are a consequence of that, never
the target.

**Where does the scheduler live?**
In the **JobMaster**. The **ResourceManager** is what creates pods. Both sit
inside the JobManager pod, and they are different components.

**What is the difference between the JobGraph and the ExecutionGraph?**
The JobGraph is the plan. The ExecutionGraph is that plan expanded into N
running copies. A rescale rebuilds only the second, which is why parallelism can
change without breaking state.

**Why do 9 vertices at parallelism 6 need 6 slots and not 54?**
Slot sharing. A slot does not hold one subtask, it holds one subtask of every
vertex, a full vertical slice of the pipeline.

**Who sets the TaskManager pod count?**
Nobody. It is `ceil(slots needed / slots per TaskManager)`, recalculated every
time the slot requirement changes, and never stored in a field.

### The rescale

**Does the rescale take a savepoint?**
No. It restores from the last checkpoint that already happened, so up to
`execution.checkpointing.interval` of records is replayed.

**Are existing TaskManagers deleted?**
No. Their **subtasks** are cancelled and redeployed. The pods and the JVMs
survive, with unchanged names and no increase in restart count.

**Why does scaling down not always remove a pod?**
A pod is removed only once it holds zero subtasks, and only after an idle
timeout. Spread out rather than packed, several pods can each keep one subtask
and none becomes removable.

### Layer 2, the pod-scalers this phase does not build

**What is `/scale`?**
A three-field view of another object: desired count, actual count, and the
selector that finds its pods. It lets one autoscaler drive types it has never
heard of.

**Why target the `FlinkDeployment` and not a pod?**
A pod has no count and does not survive the next rescale. Write at the top of
the ownership chain, or the operator overwrites you on its next reconcile.

**What does "Flink is oblivious to Kubernetes" mean?**
In `mode: standalone` Flink does not know Kubernetes exists. It holds no
credentials and simply waits for TaskManagers to phone in over the network.

**Why can KEDA not drive `mode: native`?**
An autoscaler works by writing a number into a field. Native mode has no such
field, because the pod count is a result rather than a setting.

**Is KEDA plus reactive mode still an in-place rescale?**
Yes, in the same sense. Same scheduler, same state machine, no savepoint and no
JobManager restart. What differs is the direction: Layer 1 changes parallelism
and pods follow, Layer 2 changes pods and parallelism follows.

**Why not autoscale on CPU?**
Backpressure blocks threads, and a blocked thread burns no CPU. Utilisation
falls exactly when you need to scale up, so the signal points the wrong way.

**Why does Karpenter with kwok need a Docker build when AWS Karpenter does not?**
A Helm chart points at an image by name; it does not contain the program. AWS's
image is published, kwok's is not, because kwok is a test harness rather than a
product. `kwok/charts/values.yaml` ships `repository: ""` to say so.

**Why not reuse `apps/pipeline/Dockerfile`?**
It starts `FROM flink:2.2.0-java21` and copies a Java jar that Gradle already
built. Karpenter is Go, needs no JVM, and has nothing pre-built, so its
Dockerfile must compile as well as package. One Dockerfile packages one program.

## Karpenter

### The problem, before the mechanism

Installing Karpenter for a real cloud is two commands. Add the Helm repo, install
the chart. Installing it with the kwok provider needs a compile step first, and
nothing in the install instructions says why. The reason is worth understanding,
because it is the same reason for every "there is no chart for this" tool.

### Kubernetes cannot run source code

Three facts stacked on each other:

1. **Kubernetes runs containers.** That is the only unit it starts.
2. **A container comes from an image**, which is the program already compiled and
   packaged.
3. **A Helm chart does not contain the program.** It is instructions: run *this
   image*, with *these settings*, in *this namespace*. It refers to the image by
   name.

For the AWS or Azure providers, that image already exists in a public registry.
Helm reads the name, Kubernetes pulls it, and nobody thinks about step 2.

For the kwok provider, **the image does not exist anywhere.** kwok is a test
harness rather than a product, so upstream never publishes one. Only the source
does exist, in `kubernetes-sigs/karpenter` under `kwok/`.

The chart states this outright. `kwok/charts/values.yaml`:

```yaml
controller:
  image:
    repository: ""
```

An empty string is not an oversight. It is the chart saying it needs an image and
expects the operator to supply one.

**So the missing step is turning source on disk into something Kubernetes can
start.** Compile once, name the result, hand the name to Helm.

### Why `apps/pipeline/Dockerfile` cannot be reused

The obvious question is whether the project's existing Dockerfile can do it. It
cannot, and the reasons name the difference between the two builds:

```dockerfile
FROM flink:2.2.0-java21                       # a Flink runtime, with a JVM
COPY build/libs/pipeline-all.jar ...          # a jar Gradle already built
```

1. **Wrong base.** It starts from a Flink image, which supplies a JVM and the
   Flink runtime. Karpenter is not a Flink job and needs neither.
2. **Wrong language.** It copies a `.jar`, compiled Java. Karpenter is Go. There
   is no jar, and a JVM cannot run Go.
3. **It compiles nothing.** Gradle built the jar before Docker ran, so `COPY`
   only moves a finished file in. Karpenter has nothing built yet, so its
   Dockerfile must compile as well as package. That is why it needs two stages:
   one to build, one to hold the result.

A Dockerfile describes how to package **one** program. Two programs, two files.

### The same shape, different ingredients

| | Flink job | Karpenter with kwok |
|---|---|---|
| Compiled by | Gradle, on the host, before Docker | Go, inside the Docker build |
| Packaged onto | `flink:2.2.0-java21` | `distroless/static`, an almost empty image |
| Delivered by | `kind load docker-image` | `kind load docker-image` |
| Referenced from | `spec.image` in the FlinkDeployment | `--set controller.image.repository` in Helm |

The last two rows are identical, and that is the point. Once an image exists,
Karpenter is installed like anything else. Everything unusual happens before
that.

### Why the compiler runs inside Docker

`go.mod` requires **Go 1.26.6**, and this host has neither `go` nor `make`. A
multi-stage build borrows a compiler from the `golang:1.26` image, uses it, and
throws the container away. Nothing is installed on the machine and deleting the
image undoes the whole thing.

This matters beyond convenience. Everything else in this lab is installed in a
scoped, reversible way, and a Go toolchain on the host would be the one piece
that is not.

### What a NodePool is

- A file you write and apply, the same way you apply a Deployment.
- It is **not a node**. Applying it does not create a node.
- It is a set of rules. Karpenter reads them only when some pod cannot start.
- The rules answer four questions:
  - How much may I add in total? → `limits`
  - What labels and taints should a new node carry? → `labels`, `taints`
  - What kind of node may I pick? → `requirements`
  - When should I delete the node again? → `disruption`
- It never says **how to build the machine**. No image. No network. No login.

### What a NodeClass is

- The settings one cloud needs in order to build a real machine.
- On AWS that means: which image to boot, which subnets and security groups to
  join, which IAM role to use, how big the disk is.
- It **decides nothing**. By the time anything reads it, Karpenter has already
  decided a node is needed.
- Each cloud has its own kind: `EC2NodeClass`, `AKSNodeClass`, `KWOKNodeClass`.
- The kwok one is empty. A fake node has no image and no network, so there is
  nothing to set.

### How they differ

Follow one round of provisioning:

1. A pod cannot start. It sits `Pending`.
2. Karpenter reads the **NodePool**. May I add a node? What should it look like?
   Would the pod fit on it?
3. Karpenter writes a `NodeClaim`. It means "I want one node like this".
4. The cloud reads the **NodeClass** and builds the machine.
5. The node joins, carrying the labels and taints from the NodePool. The pod
   starts.
6. Later the node is empty. The **NodePool** says when to delete it.

- The NodePool is read at step 2 and step 6. **It decides.**
- The NodeClass is read at step 4 only. **It builds.**
- Short version: NodePool is *what and whether*. NodeClass is *how*.

### Which file does a setting go in

Ask one question: does this setting mean the same thing on another cloud?

- **Yes → NodePool.** A label. A taint. `amd64`. A CPU ceiling.
- **No → NodeClass.** An AMI id. A subnet id. An IAM role name. A disk type.

### Why not one object

1. **Portability.** Labels and taints mean the same thing on every cloud. AMI ids
   do not. Keeping them apart lets one NodePool move to another cloud by editing
   three lines.
2. **Reuse.** Several NodePools can share one NodeClass. A `gpu` pool and a
   `general` pool can differ in taints and requirements while using the same
   image, subnets and IAM role.
3. **Schema.** `EC2NodeClass` and `KWOKNodeClass` have no fields in common.
   Merging them into one type would make that type a pile of every cloud's
   settings.

### The seam

- **Seam**: a place where one implementation can be swapped for another without
  changing anything around it.
- Toy version: a lamp's socket. Swap the bulb; the lamp, wiring and switch are
  untouched.
- In a NodePool the seam is exactly three lines:

```yaml
      nodeClassRef:
        group: karpenter.kwok.sh
        kind: KWOKNodeClass
        name: default
```

- Point them at an `EC2NodeClass` and the **byte-identical** NodePool provisions
  real EC2 instances.
- This is what lets the lab exercise Karpenter's real provisioning loop with no
  cloud account.

### This project's NodePool, field by field

| Field | What it does | Why it matters here |
|---|---|---|
| `template.metadata.labels.node-role: decoy` | every created node gets this label | the Decoy selects on it; no real worker carries it, which is what makes the Decoy unschedulable and is the **only** thing that triggers Karpenter |
| `template.spec.taints` `workload=flink:NoSchedule` | every created node gets this taint | keeps TaskManagers off fake nodes |
| `template.spec.requirements` | the space of nodes Karpenter may pick from | on a real cloud this narrows instance types and zones; on kwok it only pins arch and OS |
| `nodeClassRef` | the provider seam | see above |
| `limits.cpu: "20"` | ceiling on the pool | nothing else bounds provisioning |
| `disruption.consolidationPolicy` / `consolidateAfter: 10s` | the scale-down half | 10s makes Drill H's consolidation observable in seconds |

- **A toleration is permission, not attraction.** The label attracts the Decoy;
  the taint repels everything else. A Decoy that only tolerated the taint would
  schedule onto a real worker, Karpenter would see nothing pending, and the Drill
  would prove nothing.

### `limits` is the whole pool, not one node

- Karpenter's docs: *"Resource limits constrain the total size of the pool.
  Limits prevent Karpenter from creating new instances once the limit is
  exceeded."*
- `cpu: "20"` = the **sum across every node this NodePool has created**, not a
  per-node size.
- At roughly 5 CPU per fake node that is about 4 nodes.
- **`limits.nodes` also exists** and states the intent directly:

```yaml
  limits:
    nodes: 4
```

- Prefer it when the intent is a node count. The CPU form carries a hidden
  dependency on the per-node size.

### KWOKNodeClass against EC2NodeClass

```yaml
# kwok: no spec at all
apiVersion: karpenter.kwok.sh/v1alpha1
kind: KWOKNodeClass
metadata:
  name: default
```

```yaml
# AWS: infrastructure detail, abridged from the v1 API docs
apiVersion: karpenter.k8s.aws/v1
kind: EC2NodeClass
metadata:
  name: default
spec:
  role: "KarpenterNodeRole-my-cluster"
  amiFamily: AL2023
  amiSelectorTerms:
    - alias: al2023@v20240625
  subnetSelectorTerms:
    - tags:
        karpenter.sh/discovery: "my-cluster"
  securityGroupSelectorTerms:
    - tags:
        karpenter.sh/discovery: "my-cluster"
  blockDeviceMappings:
    - deviceName: /dev/xvda
      ebs:
        volumeSize: 100Gi
        volumeType: gp3
        encrypted: true
  metadataOptions:
    httpTokens: required
```

- The `KWOKNodeClass` is empty because kwok invents nodes out of nothing: no
  image to pick, no network to join, no role to assume.
- It exists only so `nodeClassRef` has something to point at.
- **The emptiness is the clearest demonstration of what the seam separates.**

### Why an EC2NodeClass carries an IAM role

- **The problem**: software on the node must call AWS APIs (ECR image pulls, CNI
  attaching network interfaces, EKS registration). Baking an access key into the
  AMI would put a long-lived secret on every machine.
- **An IAM role is assumed by a principal, and an EC2 instance is one.** It is not
  limited to people or services.
- Toy version: a hotel gives the cleaner a badge that opens today's rooms and
  expires at shift end, not a copy of every key.

The chain:

```
IAM role ──wrapped in──> instance profile ──attached at launch──> EC2 instance
                                                                       │
                              instance metadata service (169.254.169.254)
                              hands out temporary, auto-rotating credentials
                                                                       │
                                        kubelet, CNI and AWS SDKs pick them up
```

- An **instance profile** is a thin container whose only job is to carry one role
  onto an instance. It exists because EC2 predates the modern IAM model.
- The role's **trust policy** names `ec2.amazonaws.com` as the service allowed to
  assume it. That service assumes the role on the instance's behalf.

### `spec.role` against `spec.instanceProfile`

- Exactly one must be set. Neither means the node has no identity; both is
  ambiguous.
- Karpenter's docs: *"The `role` field allows Karpenter to manage the instance
  profile, while the `instanceProfile` field requires you to pre-provision and
  manage the IAM instance profile yourself."*

| | `role` | `instanceProfile` |
|---|---|---|
| Creates the instance profile | Karpenter, on the fly | you, beforehand |
| Deletes it | Karpenter | you |
| Needs `iam:CreateInstanceProfile` | yes | no |
| Use when | the normal case | the org forbids controllers from creating IAM objects |

- `status.instanceProfile` reports back what Karpenter built from the role named
  in `spec`.
- Upstream's stated reasoning for preferring `role`: instance profiles are an
  EC2-specific oddity with weak tooling, roles are what people already understand,
  so Karpenter absorbs the awkward step.

### Three roles, and the question that separates them

- **The question is not which machine. It is which process is making the AWS API
  call.**

One scenario, all three:

1. Pod is `Pending`, nothing fits. Karpenter calls `ec2:RunInstances`.
   → **controller role**
2. The instance boots. kubelet calls `ecr:GetAuthorizationToken`, CNI calls
   `ec2:CreateNetworkInterface`. → **node role** (`spec.role`)
3. The pod starts and calls `s3:GetObject`. → **pod role** (IRSA / Pod Identity)

**Two mechanisms, three purposes.** The controller role is mechanically a pod
role, because Karpenter is itself a pod:

| Mechanism | How credentials arrive | Who gets them |
|---|---|---|
| Instance profile | the metadata service at `169.254.169.254` | **everything** on that machine |
| IRSA / Pod Identity | a projected token on the pod's ServiceAccount | **one** pod |

| Role | Assumed by | Typical permissions |
|---|---|---|
| **Node** | the EC2 instance | `ecr:GetAuthorizationToken`, `ec2:CreateNetworkInterface`, `eks:DescribeCluster` |
| **Controller** | the Karpenter pod | `ec2:RunInstances`, `ec2:TerminateInstances`, `iam:PassRole`, `iam:CreateInstanceProfile` |
| **Pod** | one application pod | only what that app needs, e.g. `s3:GetObject` |

- **Why not one role for everything**: anything on a node can reach
  `169.254.169.254` and use the node role. A node role with S3 write hands S3
  write to every pod on that node, including ones you did not write. That is why
  IRSA exists and why the node role stays minimal.
- **Where two of them meet**: the controller needs `iam:PassRole` for the node
  role. Handing a role to a new instance is, in IAM's view, handing out
  permissions, so it must be explicitly allowed. Without it Karpenter launches
  nothing.

### What this lab has instead

| Role | Present here? | Why |
|---|---|---|
| Node | no | a kwok node runs no kubelet and pulls no images |
| Pod | no | the Decoy is `pause`, which does nothing |
| Controller | **yes, as Kubernetes RBAC** | the chart created a ServiceAccount, ClusterRole and ClusterRoleBinding in `kube-system`, granting the controller `NodeClaim` and `Node` permissions and nothing else |

- Same shape, one cloud, one cluster.

### The Decoy Workload is a Deployment, and a Deployment does not create pods

- A Deployment never makes a pod itself. There is a middle object:

```
Deployment  ──creates──>  ReplicaSet  ──creates──>  Pod
```

- Three things follow from that, all visible with `kubectl get pod -o json`:
  - **The pod name has two suffixes**, `decoy-<replicaset-hash>-<random>`. The
    middle part names the ReplicaSet.
  - **`ownerReferences` points at the ReplicaSet**, not at the Deployment.
  - **The pod gains a `pod-template-hash` label you never wrote.** The ReplicaSet
    adds it so that during a rollout it can tell its own pods apart from the
    older ReplicaSet's pods.

- A Deployment is used here rather than a bare Pod for one reason: **`kubectl
  scale` works on it**. Drill H starts by scaling the Decoy up, and a bare Pod
  cannot be scaled.

### A Deployment has two `metadata` blocks, and only one reaches the pod

- The top `metadata` describes the **Deployment**.
- `spec.template.metadata` describes **each pod** it stamps out.
- Measured on the `karpenter` Deployment in this cluster: 5 labels on the
  Deployment, 2 on the template, and the pod ended up with **the template's 2
  plus `pod-template-hash`**. The Deployment's own Helm labels never reached it.
- From the top `metadata`, only two things affect the pod:
  - `name`, used as a **prefix** for the pod name.
  - `namespace`, which the pod inherits.
- **Labels and annotations on the top `metadata` are not copied.** Putting a label
  there and expecting it on the pods is a common mistake. It goes in
  `spec.template.metadata.labels`.

### Why `app: decoy` is written twice

- `template.metadata.labels` is **what gets stamped onto** each pod.
- `spec.selector` is **what the Deployment searches for**.
- A Deployment keeps no list of the pods it made. On every pass it runs the
  selector as a search and counts the results. Below `replicas`, it makes more.
- So the stamp and the search must use the same label, or the search finds
  nothing and the Deployment creates pods forever.
- **Kubernetes does not copy it automatically on purpose.** `selector` is
  immutable; the template is not. If the selector were derived from the template,
  changing a label during a rollout would silently change which pods the
  Deployment owns and orphan the running ones. The API server rejects a mismatch
  at apply time instead.

- Three label-shaped fields sit close together in `decoy.yaml`, and they do
  different jobs:
  - `spec.selector` searches for **pods**.
  - `template.metadata.labels` is stamped onto **pods**.
  - `template.spec.nodeSelector` searches for **nodes**. Only this one has
    anything to do with Karpenter.
