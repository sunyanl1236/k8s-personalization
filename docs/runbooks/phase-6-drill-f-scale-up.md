# Phase 6 Drill F: turn scaling on and watch the job scale out

Date: 2026-09-08
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Deployment: `FlinkDeployment personalization` in `personalization-blue`,
`mode: native`, image `lab/personalization-pipeline:0.1-0bd7f52`
Start state: 2 JobManagers, 1 TaskManager, running parallelism 1 to 2
End state: 2 JobManagers, **3 TaskManagers**, CepOperator at parallelism 6
Autoscaler: `enabled: true`, **`scaling.enabled: true`**,
`metrics.window: 3m`, `stabilization.interval: 1m`,
`target.utilization: 0.6`, `catch-up.duration: 5m`,
`vertex.max-parallelism: 6`
Job upgrade mode: `savepoint`, restored in this Drill's Step 1

Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

Nothing breaks here either. Drill E proved the autoscaler's arithmetic. Drill F
lets it act, and proves the change was scaling and not a crash.

## What this Drill checks

Drill E left `job.autoscaler.scaling.enabled` at `"false"`. The autoscaler
computed a parallelism and did not apply it. This Drill turns that switch on.

Three things then have to be true, and each needs its own evidence:

1. **The parallelism went up.** Read it from the job graph.
2. **The pod count followed.** Nothing sets it. It is derived.
3. **It was the autoscaler and not a failover.** Pod counts change either way.
   This is the check that is easy to skip and the only one that proves anything.

### The two edits have to travel together

`scaling.enabled` and `upgradeMode` change in one commit.

`upgradeMode` on its own is an `IGNORE`-level diff. The operator sees it,
reconciles nothing, and the change sits in Git looking applied. `scaling.enabled`
is a real diff, so it forces a reconcile and carries the other change with it.
This is the same trick Task 3 used to land `upgradeMode: stateless`.

Phase 7's promotion runbook needs `savepoint` in place, so this is the commit
that puts the deployment back on a state-preserving footing.

### What in-place rescaling does not do

The Adaptive scheduler changes parallelism through
`PUT /jobs/:jobid/resource-requirements`. The JobMaster re-declares how many
slots it wants. In `mode: native` the ResourceManager then creates TaskManager
pods to match.

**The job does not restart. The JobManager does not restart. Existing
TaskManagers are not replaced.** Their subtasks are cancelled and redeployed
underneath them, from a checkpoint. Expect the original TaskManager to keep its
name, its age and its restart count.

Mechanism is in [the Phase 6 knowledge doc](../knowledge/phase-6-autoscaling.md).

## Procedure

**1. Flip scaling on and restore `upgradeMode`, in one commit.**

In `manifests/flink/blue/flinkdeployment.yaml`:

```yaml
    job.autoscaler.scaling.enabled: "true"
```
```yaml
  job:
    upgradeMode: savepoint
```

Push to `master`, not `phase-2`. `root.yaml` uses `targetRevision: HEAD`, which
resolves to the default branch.

```bash
git push origin phase-2:master
```

**2. Confirm the live CR, not the file.** Git and the cluster disagreeing is a
silent failure in this project.

```bash
kubectl -n personalization-blue get flinkdeployment personalization -o jsonpath='
scaling.enabled = {.spec.flinkConfiguration.job\.autoscaler\.scaling\.enabled}
upgradeMode     = {.spec.job.upgradeMode}
{"\n"}'
```

Note the escaping. Dots inside a **key name** are escaped, as in
`job\.autoscaler\.scaling\.enabled`. Dots that separate **path steps** are not,
as in `.spec.job.upgradeMode`. Escaping a path separator returns an empty string
and no error, which reads exactly like a missing field.

**3. Start the Load Ramp, and size it from measurement.**

```bash
apps/gradlew -p apps :generator:run \
  --args="--click-rate=4000 --shopper-count=10000 --product-change-rate=400 --click-max-skew-seconds=1"
```

Why these numbers, and why they are not Drill E's:

- **CepOperator is the bottleneck.** It takes every Click and costs the most per
  Click. Parallelism follows `ceil(clickRate ÷ perSubtaskCapacityAtTarget)`.
- `--product-change-rate` is the wrong lever now. It only loads the Interval
  Join, and that vertex has spare capacity.
- `--shopper-count` **must** rise with `--click-rate`. Session length is
  `e^(6 × clickRate ÷ shopperCount)`. At 4000 Clicks over 2000 Shoppers that is
  `e^12`, about 163,000 Clicks per Session. Session state would exhaust the host
  and you would be debugging memory, not autoscaling. 10000 Shoppers puts it back
  near 4 Clicks per Session.

**4. Wait, and expect several minutes.** The metrics window is 3 minutes. After
each rescale the autoscaler clears its metrics and stabilizes for a further
minute. Reaching the final parallelism can take two or three rescales.

**5. Read the scaling report from the operator log.**

```bash
kubectl -n flink-operator logs deploy/flink-kubernetes-operator \
  -c flink-kubernetes-operator --since=15m | grep -i 'scaling\|rescale\|stabiliz'
```

Look for `Scaling execution enabled, begin scaling vertices` followed by
`In-place scaling triggered`. In Drill E the same line read
`Scaling execution disabled by config`.

**6. Confirm the pod count is derived, not set.**

```bash
kubectl -n personalization-blue get pods
```

6 slots over `taskmanager.numberOfTaskSlots: 2` gives 3 pods. Check that the
original TaskManager kept its name and age.

**7. Prove it was scaling and not a failover.**

`GET /jobs/:jobid/rescales/history` **does not exist on Flink 2.2.0.** See the
Notes. Use these two instead:

```bash
JID=$(curl -s localhost:30011/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -s "localhost:30011/jobs/$JID/exceptions?maxExceptions=5" | python3 -c "
import sys,json;d=json.load(sys.stdin)
print('exception entries:', len(d.get('exceptionHistory',{}).get('entries',[])))"
curl -s "localhost:30011/jobs/$JID/checkpoints" | python3 -c "
import sys,json;d=json.load(sys.stdin);print(d['counts']);print(d['latest'].get('restored'))"
```

A rescale restores from a checkpoint and leaves **no exception**. A failover
restores from a checkpoint and **always** leaves one. The pair is what separates
them.

**8. Answer the overrides question.**

```bash
kubectl -n personalization-blue get flinkdeployment personalization \
  -o jsonpath='{.spec.flinkConfiguration.pipeline\.jobvertex-parallelism-overrides}{"\n"}'
```

Expect **Empty**

```
pipeline.jobvertex-parallelism-overrides =     (empty)
```

**Empty means the autoscaler never edited your `FlinkDeployment`. It changed the
running job directly instead.** It stayed empty through all three rescales.

There were two ways it could have applied "CepOperator goes from 2 to 6":

| Path | What it does | Does the CR change? |
|---|---|---|
| Config rewrite | Operator writes `pipeline.jobvertex-parallelism-overrides: "0ce5a3d5…:6"` into `spec.flinkConfiguration`, then redeploys the job so the new config is read at startup | **Yes.** Job restarts |
| In-place rescale | Operator calls `PUT /jobs/:jobid/resource-requirements` on the running JobManager. The JobMaster asks for more slots. The ResourceManager makes pods | **No.** Job keeps running |

Flink takes the second path when `jobmanager.scheduler: Adaptive` is set and the
version supports in-place rescaling. Both are true here. So the CR was never
touched, and the field stayed empty.

## Observed result

### The live CR after the push

```
scaling.enabled = true
upgradeMode     = savepoint
```

Both edits landed. The job did **not** restart on this change. Autoscaler keys
are read by the operator, not by the JobManager, so turning scaling on needs no
redeploy.

### First attempt: no scale-up, and the reason

The Load Ramp was restarted at Drill E's settings, 800 Clicks and 400 Product
Changes per second. The operator then logged this every 15 seconds:

```
o.a.f.a.ScalingExecutor  All vertex processing rates are within target.
o.a.f.a.ScalingExecutor  All job vertices are currently running at their target parallelism.
```

The job graph had gone **down**, not up:

```
Source: click-stream                              1
EventTimeSessionWindows -> Map                    1
CepOperator                                       2
Interval Join                                     1
Co-Keyed-Process                                  1
```

`spec.job.parallelism` was still 2. Five vertices were running at 1. So the
autoscaler had already applied a scale-**down** in place, during an idle period
before the Load Ramp restarted.

Busy time explained it:

| Vertex | Parallelism | Busy | Records/sec in |
|---|---|---|---|
| Source: click-stream | 1 | 5.1% | 799.8 |
| Source: product-change-stream | 1 | 4.5% | 400.1 |
| EventTimeSessionWindows -> Map | 1 | 1.8% | 800.4 |
| **CepOperator** | 2 | **32.0%** | 799.1 |
| Interval Join | 1 | 6.1% | 1200.3 |

**Load was arriving correctly.** 799.8 Clicks and 400.1 Product Changes per
second, exactly as requested. The busiest vertex in the job was at 32% against a
60% target. There was nothing to scale up for.

**CepOperator staying at 2 is the tell that the autoscaler was thinking.** At 32%
on two subtasks, dropping to one would put it at 64%, above target. So it scaled
every other vertex to 1 and left that one alone.

### Why Drill E disagreed at the same load

| | Drill E, 14:33 | Drill F, first attempt |
|---|---|---|
| Click rate | 800/sec | 800/sec |
| CepOperator capacity per subtask | 551/sec | ~1250/sec |
| Recommendation | `2 -> 5`, then `2 -> 6` | none, within target |

The operator got about twice as fast at the same load. Two things changed. The
job had been under load for only about 8 minutes during Drill E, on a JVM that
had just started, so JIT compilation and RocksDB caches were cold. It was also
still draining Kafka backlog.

**Drill E measured a catch-up recommendation, not a steady-state one.** That is
the `job.autoscaler.catch-up.duration: 5m` term, and it is why Drill E's numbers
would not reconcile: 850.59 ÷ 0.6 gives 3 subtasks, and the autoscaler asked for
5. The extra 2 were for clearing backlog.

At 800 Clicks per second in steady state this job wants parallelism 1 to 2.
**Drill E's load level was never enough to force a real scale-up.**

### Sizing the load from measurement

From the table above, CepOperator at parallelism 2 and 32% busy handled 799
Clicks/sec. That is about 400 per subtask at 32%, so one subtask at full speed is
worth roughly 1250 Clicks/sec, and 750 Clicks/sec at the 60% target.

`ceil(clickRate ÷ 750) = 6` needs a Click rate between 3751 and 4500. The Load
Ramp was restarted at `--click-rate=4000 --shopper-count=10000`.

### The scale-up

```
16:03:57 SCALINGREPORT  Scaling execution enabled, begin scaling vertices:
  { Vertex ID 0ce5a3d5245a8fc303aa7f34d82ae94f | Parallelism 2 -> 6
    | Processing capacity 2187.86 -> 6992.00 | Target data rate 2371.89 }
16:03:57 MemoryScaling  Scaling factor: 1.0, Adjusting memory from 190000144 bytes to 190000144 bytes.
16:03:57 SCALING        In-place scaling triggered
16:04:28 ScalingMetricCollector  Job updated at 2026-09-08 16:04:11. Clearing metrics.
16:04:28 ScalingMetricCollector  Stabilizing until 2026-09-08 16:05:11

16:08:14 SCALINGREPORT  Scaling execution enabled, begin scaling vertices:
  { Vertex ID 977c935ff4c0f85b5dc0ce5735367a6a | Parallelism 1 -> 2
    | Processing capacity 86048.27 -> 87986.00 | Target data rate 27735.91 }
  { Vertex ID 5d4d0bfa95bd89679658645fb09a51ca | Parallelism 1 -> 4
    | Processing capacity 3353.03 -> 12430.00 | Target data rate 3919.24 }
16:08:14 SCALING        In-place scaling triggered
```

**Two rescales, 4 minutes 17 seconds apart.** This is the "two or three rescales
over several minutes" the plan predicted, seen from the inside. The autoscaler
cannot fix everything at once: after each change it clears its metrics and
stabilizes for a minute, so the next bottleneck only becomes visible on the
following decision.

Resolving the second report through Drill E's mapping:

| Vertex ID | Name | Change |
|---|---|---|
| `977c935f…` | Co-Keyed-Process | `1 -> 2` |
| `5d4d0bfa…` | Interval Join | `1 -> 4` |

The Interval Join was the vertex sitting at 75.3% busy in the reading taken
during the stabilization window. It scaled on the next decision, exactly as
expected.

`0ce5a3d5…` is **CepOperator**, from Drill E's mapping. Vertex IDs are a hash of
the job graph, so Drill E's list stayed valid.

`Scaling execution enabled, begin scaling vertices` replaces Drill E's
`Scaling execution disabled by config`. That single line is the switch reporting
its own position.

### Pods

```
NAME                               READY   STATUS    RESTARTS   AGE
personalization-798f5f6474-fzbcx   1/1     Running   0          108m
personalization-798f5f6474-sflzj   1/1     Running   0          108m
personalization-taskmanager-1-1    1/1     Running   0          108m
personalization-taskmanager-1-2    1/1     Running   0          2m42s
personalization-taskmanager-1-3    1/1     Running   0          2m42s
```

**Three TaskManagers.** 6 slots over 2 slots per TaskManager. Nothing set that
number.

**`taskmanager-1-1` is untouched.** Same name, 108 minutes old, `RESTARTS 0`. Its
subtasks were cancelled and redeployed underneath it. The two new pods are 2m42s
old. Both JobManagers are also untouched.

### Scaling, not failover

```
exception entries: 0
counts: {'restored': 2, 'total': 651, 'in_progress': 0, 'completed': 649, 'failed': 2}
restored: {'id': 635, 'is_savepoint': False,
           'external_path': 's3://checkpoints/phase-6/3f8f4ae5ad2b9d233013c44ce4a257b5/chk-635'}
```

State was restored **twice** and the exception history is **empty**. Once for the
scale-down, once for the scale-up. A failover cannot restore without leaving an
exception, so both restores were rescales.

Job uptime ran continuously past both events. The two `failed` checkpoints date
from job startup, as in Phase 5.

### Where it settled

After the third rescale, at job uptime 6749 seconds:

```
vertex                                         par   busy     recs/s in
Source: click-stream                             1     2.9%      2887.9
Source: product-change-stream                    1    18.3%       400.3
Source: promo-rule-stream                        1    13.4%       100.1
EventTimeSessionWindows -> Map                   1    21.8%      2888.3
CepOperator                                      6   100.0%      2958.8
Interval Join                                    4    54.3%      3189.8
Co-Keyed-Process                                 2    20.2%     22715.6
Co-Process-Broadcast -> async wait -> Sink       1     3.4%       144.4
```

Every vertex is now inside `target.utilization: 0.6` **except CepOperator**,
which is at 100.0%.

### The pod count did not change on the third rescale, and that is the point

```
NAME                               READY   STATUS    RESTARTS   AGE
personalization-798f5f6474-fzbcx   1/1     Running   0          112m
personalization-798f5f6474-sflzj   1/1     Running   0          112m
personalization-taskmanager-1-1    1/1     Running   0          112m
personalization-taskmanager-1-2    1/1     Running   0          6m40s
personalization-taskmanager-1-3    1/1     Running   0          6m40s
```

Still three TaskManagers, and the two new ones are the same pods created by the
first rescale.

**Interval Join went from 1 to 4 and Co-Keyed-Process from 1 to 2, and no pod was
created.** That is slot sharing doing its job.

Slots needed is the **largest parallelism of any single vertex**, not the sum. It
is still 6, set by CepOperator. Parallelisms of 4 and 2 fit inside those same 6
slots:

```
slot 1  CepOperator#1  IntervalJoin#1  CoKeyedProcess#1  Source#1  ...
slot 2  CepOperator#2  IntervalJoin#2  CoKeyedProcess#2
slot 3  CepOperator#3  IntervalJoin#3
slot 4  CepOperator#4  IntervalJoin#4
slot 5  CepOperator#5
slot 6  CepOperator#6
```

Pod count is `ceil(6 ÷ 2)` = 3, before and after. **A rescale that changes
parallelism does not necessarily change the pod count.** Anyone watching
`kubectl get pods` alone would have concluded that the third rescale never
happened.

### Scaling, not failover: final reading

```
exception entries: 0
counts: {'restored': 3, 'total': 670, 'in_progress': 1, 'completed': 667, 'failed': 2}
restored: {'id': 659, 'is_savepoint': False,
           'external_path': 's3://checkpoints/phase-6/3f8f4ae5ad2b9d233013c44ce4a257b5/chk-659'}
```

**Three restores, zero exceptions.** One per rescale: the scale-down to 1, the
scale-up of CepOperator, and the scale-up of the Interval Join and
Co-Keyed-Process. The job ran continuously through all three.

The two `failed` checkpoints date from job startup, as in Phase 5.

## Gate

| Check | Result |
|---|---|
| Parallelism rose | **Pass.** CepOperator 2 to 6, Interval Join 1 to 4, Co-Keyed-Process 1 to 2, over three rescales |
| Pod count followed | **Pass.** 3 TaskManagers, derived from 6 slots over 2 per pod |
| It was the autoscaler, not a failover | **Pass.** `In-place scaling triggered` twice in the operator log, 3 checkpoint restores, **0** exceptions |
| Original TaskManager not replaced | **Pass.** `taskmanager-1-1`, 112m, `RESTARTS 0` |
| Overrides field answered | **Pass. Empty.** No `ignoreDifferences` needed |
| Step 7 gap check | **Not run.** See Outstanding |

**Drill F passes on the scaling gates.** The plan's Step 4 gate, a rescale record
with `triggerCause: UPDATE_REQUIREMENT`, could not be used at all. See the Notes.

## Outstanding

**The gap check has not been run.** Task 5 Step 7 needs a `before.txt` snapshot
taken ahead of a rescale, then a `compare` after it. Both rescales in this Drill
happened before a baseline existed. Running it needs one more rescale, which
means stopping and restarting the Load Ramp, which is Drill G anyway. **Fold the
gap check into Drill G rather than forcing an extra rescale here.**

## Notes

**The rescale history endpoint does not exist on Flink 2.2.0.** All of these
return 404 on this cluster:

```
/jobs/:jobid/rescales            404
/jobs/:jobid/rescaling           404
/jobs/:jobid/rescale-history     404
/jobs/:jobid/rescales/history    404
```

The real path is `GET /jobs/:jobid/rescales/history`, and it returns
`triggerCause`, `terminalState`, `schedulerStates` and per-vertex
`preRescaleParallelism` / `postRescaleParallelism`, exactly as the plan
described. It arrives in **Flink 2.3**, per that release's notes. This cluster
reports `2.2.0 5a33689 @ 2025-11-27`.

**`web.adaptive-scheduler.rescale-history.size: "10"` is therefore inert.** Task
3 added it, `--dry-run=server` accepted it, and the operator applied it without
complaint. Flink ignores configuration keys it does not know. Nothing anywhere
reports that a key did nothing.

The plan anticipated a 404 and said to find the right path. The right path does
not exist yet on this version. The substitute in Step 7 above, zero exceptions
plus a checkpoint restore, answers the same question with weaker resolution: it
proves no failover happened, but it does not name the trigger. Combined with the
operator's `In-place scaling triggered` line it is enough.

**CepOperator is pinned at the ceiling.** It sits at parallelism 6 and **100.0%
busy**, far above `target.utilization: 0.6`. It reached 90.1% shortly after the
rescale and saturated from there. `job.autoscaler.vertex.max-parallelism`
is `"6"`, so 6 is the most it may ask for, not the amount it needs.

This is exactly the case flagged at the end of Drill E. **A job pinned at the
ceiling stops producing recommendations and looks like a stalled autoscaler.** It
is not stalled. It is capped.

The cap is a safety limit sized by the arithmetic in spec 4.3, not a tuning knob.
Every kind node advertises the full 23.5 GiB of the host, so the scheduler will
never mark a pod `Pending` for memory. Nothing else would stop a raised ceiling
from taking down the machine.

**Adding subtasks did not add proportional capacity.** The autoscaler projected
CepOperator's capacity would go from 2187.86 to 6992.00, which is linear in
parallelism. Measured afterwards, 6 subtasks at 90.1% busy were handling about
2650 Clicks/sec, so full speed is roughly 2950. That is **2.4 times short of the
projection**.

The likely cause is host CPU. Three TaskManagers with `cpu: 1` each now compete
for a single physical host that also runs 6 kind nodes, both JobManagers, Kafka,
MinIO and the generator. Task 0 already recorded host headroom halved since the
spec was written. This is reasoning from the deployment shape, not a measurement,
and confirming it would need per-container CPU throttling counters.

**What this means generally:** the autoscaler extrapolates linearly from measured
per-subtask capacity. That assumption holds when new subtasks land on genuinely
new CPU. On a single-host lab it stops holding once the host saturates, and the
autoscaler cannot tell the difference. It is the same blind spot as the CFS
throttling warning in Task 4 Step 4b: a throttled job is not a busy job, and busy
time cannot separate them.

**The Interval Join is the next bottleneck and had not moved yet.** At the last
reading it sat at parallelism 1 and **75.3% busy**, above target, while the
autoscaler was still inside its post-rescale stabilization window
(`Stabilizing until 16:05:11`). Expect it to scale on the following decision.
This is the "two or three rescales, several minutes" behaviour the plan
predicted, seen from the inside.

**The generator did not reach 4000 Clicks/sec.** It was asked for 4000 and
delivered about 2650, measured from `numRecordsInPerSecond` on the Click source.
Task 1 verified 2507.9/sec against a requested 2500 and never tested higher.
**The ceiling removed in Task 1 was the 1000/sec one; a real ceiling remains
somewhere near 2600.** The scale-up still reached 6, because CepOperator's true
per-subtask capacity was lower than the estimate used for sizing.

Always read the achieved rate off the source vertex rather than trusting
`--click-rate`.

**Two jsonpath traps, both silent.** Escaping a path separator, as in
`{.spec.job\.upgradeMode}`, returns an empty string and exit code 0. It reads
identically to a field that is genuinely absent. Escape the dots inside a key
name, never the dots between path steps.

**Next Drill.** Drill G stops the Load Ramp and watches parallelism fall. Read
`job.autoscaler.scale-down.interval` and the TaskManager idle timeout off the
live configuration first, rather than assuming them, because the wait is governed
by whichever is longer. Fold this Drill's outstanding gap check into it.
