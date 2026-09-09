# Phase 6 Drill G: stop the traffic and watch the job shrink

Date: 2026-09-08
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Deployment: `FlinkDeployment personalization` in `personalization-blue`,
`mode: native`, image `lab/personalization-pipeline:0.1-0bd7f52`
Start state: 2 JobManagers, **3 TaskManagers**, CepOperator and Interval Join at
parallelism 6, Co-Keyed-Process at 2
End state: 2 JobManagers, **1 TaskManager**, every vertex at parallelism 1,
reached in two steps (6 to 3 to 1)
Autoscaler: `scaling.enabled: true`, `metrics.window: 3m`,
`stabilization.interval: 1m`, `target.utilization: 0.6`,
`vertex.max-parallelism: 6`, **`scale-down.interval: 5m`** (added for this Drill)

Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

Drill F let the autoscaler grow the job. This Drill takes the load away. Shrinking
is the half most demonstrations skip, and it behaves nothing like growing.

## What this Drill checks

**Parallelism falls first. Pods follow later, and sometimes not at all.**

Two separate things have to happen before a pod disappears:

1. **A TaskManager is released only when it holds zero subtasks.** A half-used
   one is kept, wasting a slot. Whether the scheduler packs the survivors onto
   fewer TaskManagers or spreads them out decides whether any pod is ever freed.
2. **An idle TaskManager is held for a timeout** before the ResourceManager
   releases it, so a freed pod can be reused instead of recreated seconds later.

Pods lingering after parallelism drops is expected. It is not a failure.

### The timer that actually governs this Drill

`job.autoscaler.scale-down.interval` **defaults to 1 hour.** From the operator's
configuration reference:

> If greater than 0, scale-down operations are delayed to merge multiple
> scale-downs within the interval, reducing job restarts and improving
> availability. Scale-down executes directly if the interval is less than or
> equal to 0.

Scaling up is cheap to get wrong and expensive to delay. Scaling down is the
reverse: shrink too eagerly and a traffic dip costs you a restart. So Flink
deliberately sits on a scale-down decision for an hour.

**This Drill set it to `5m`** so the wait is minutes rather than an hour. The
default is recorded here because knowing the timer exists is the learning, not
sitting through it.

**This also explains something from earlier in the day.** Between Drill E and
Drill F the job sat idle for 81 minutes and came back at parallelism 1. Longer
than 60 minutes, so the scale-down fired. The mechanism was there all along.

### The setting that decides how big each step is

`job.autoscaler.scale-down.max-factor` **defaults to 0.6**:

> The maximum scale-down factor is 0.6, meaning the job can only be scaled down
> to 60% of its original parallelism. A factor of 1 means no limit.

So a vertex at parallelism 6 cannot drop below `6 × 0.6 = 3.6`, which rounds to
**3**. Even with zero traffic. **Shrinking happens in steps, not in one jump**,
and each step waits out the interval again.

Mechanism is in [the Phase 6 knowledge doc](../knowledge/phase-6-autoscaling.md).

## Procedure

**1. Record the settings you are drilling against.**

```bash
kubectl -n personalization-blue get flinkdeployment personalization -o json | python3 -c "
import sys,json
c=json.load(sys.stdin)['spec']['flinkConfiguration']
for k in sorted(c):
    if 'autoscaler' in k: print('%-46s %s'%(k,c[k]))"
```

**Do not use `/jobmanager/config` for this.** It returns only the keys that were
explicitly set, 62 of them on this cluster. Defaults are absent, so
`slotmanager.taskmanager-timeout` and
`jobmanager.adaptive-scheduler.prefer-minimal-taskmanagers` cannot be read there
at all. Neither can `job.autoscaler.*`, because those are consumed by the
operator and never reach the JobManager.

Take defaults from the operator's configuration reference, and take the packing
behaviour from what the pods actually do.

**2. Take the gap-check baseline before stopping anything.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-g-before.txt
```

The comparison needs a BEFORE taken while output is still being produced. Once
the rescale has happened the window is gone and cannot be recovered without
rerunning the Drill.

**3. Record the starting shape.**

```bash
JID=$(curl -s localhost:30011/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -s "localhost:30011/jobs/$JID" | python3 -c "
import sys,json
for v in json.load(sys.stdin)['vertices']: print('%-46s %s'%(v['name'][:46],v['parallelism']))"
kubectl -n personalization-blue get pods
```

**4. Stop the Load Ramp,** then prove it stopped.

`Ctrl+C` in the generator terminal, then:

```bash
ps aux | grep "[l]ab.personalization.generator" | wc -l
```

Must be `0`. In Task 1 a `pkill` reported success and killed nothing, and the
survivor corrupted a rate measurement.

**5. Wait.** The metrics window has to refill with idle data (3 minutes), then
the scale-down decision waits out `scale-down.interval`.

**Expect the backlog to add to this.** CepOperator ran at 100% busy through
Drill F, so Kafka lag had been building. The job drains that before its
utilization actually falls. The extra delay is the backlog, not a stall.

**6. Read the scale-down from the operator log.**

```bash
kubectl -n flink-operator logs deploy/flink-kubernetes-operator \
  -c flink-kubernetes-operator --since=20m | grep -aE 'begin scaling|In-place scaling|Stabilizing'
```

**7. Prove it was scaling and not a failover.** `GET /jobs/:jobid/rescales/history`
does not exist on Flink 2.2.0, exactly as in Drill F.

```bash
curl -s "localhost:30011/jobs/$JID/exceptions?maxExceptions=5" | python3 -c "
import sys,json;d=json.load(sys.stdin)
print('exceptions:', len(d.get('exceptionHistory',{}).get('entries',[])))"
curl -s "localhost:30011/jobs/$JID/checkpoints" | python3 -c "
import sys,json;print(json.load(sys.stdin)['counts'])"
```

A rescale restores from a checkpoint and leaves no exception. A failover restores
and always leaves one.

**8. Watch the pods,** for at least as long as the idle timeout.

```bash
kubectl -n personalization-blue get pods -w
```

**9. Run the gap check.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-g-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-g-before.txt /tmp/drill-g-after.txt
```

## Observed result

### Settings drilled against

```
job.autoscaler.catch-up.duration               5m
job.autoscaler.enabled                         true
job.autoscaler.metrics.window                  3m
job.autoscaler.scale-down.interval             5m      <- added for this Drill
job.autoscaler.scaling.enabled                 true
job.autoscaler.stabilization.interval          1m
job.autoscaler.target.utilization              0.6
job.autoscaler.vertex.max-parallelism          6
```

**Adding `scale-down.interval` did not restart the job.** Same job ID, uptime ran
straight through the change, same pods. Autoscaler keys are read by the operator,
not the JobManager, so they need no redeploy. This repeats what Drill F found
when `scaling.enabled` was flipped.

### Starting shape

```
CepOperator                                      6
Interval Join                                    6
Co-Keyed-Process                                 2
everything else                                  1

personalization-taskmanager-1-1   Running   0   123m
personalization-taskmanager-1-2   Running   0    17m
personalization-taskmanager-1-3   Running   0    17m
```

Baseline: `100129 records, 94285 identities`.

### The generator stopped

```
ps aux | grep "[l]ab.personalization.generator" | wc -l
0
```

### Two scale-downs, not one

```
begin scaling vertices:
  { Vertex ID 977c935f… (Co-Keyed-Process) | Parallelism 2 -> 1
    | Processing capacity 153800.56 -> 61520.22 | Target data rate 1563.85 }
In-place scaling triggered

16:40:12 begin scaling vertices:
  { Vertex ID 0ce5a3d5… (CepOperator)   | Parallelism 6 -> 3
    | Processing capacity Infinity -> Infinity | Target data rate 0.00 }
  { Vertex ID 5d4d0bfa… (Interval Join) | Parallelism 6 -> 3
    | Processing capacity Infinity -> Infinity | Target data rate 0.00 }
16:40:12 MemoryScaling  Scaling factor: 1.5, Adjusting memory from 255235312 bytes to 382852968 bytes.
16:40:12 SCALING        In-place scaling triggered
16:40:27 ScalingMetricCollector  Job updated at 2026-09-08 16:40:20. Clearing metrics.
16:40:28 ScalingMetricCollector  Stabilizing until 2026-09-08 16:41:20
```

**Co-Keyed-Process shrank first, while traffic was still draining.** Its report
carries a real `Target data rate 1563.85`, so it was measured against genuine
remaining load.

**The second report shows an idle job.** `Target data rate 0.00`, and
`Processing capacity Infinity`. With no records arriving, busy time is zero, so
the measured true processing rate is unbounded. Infinity here is arithmetic on an
idle operator, not an error.

**`Scaling factor: 1.5` is memory following parallelism.** Fewer subtasks share
the same TaskManager memory, so each gets more. 255 MB per subtask became 383 MB.

### Why it stopped at 3, not 1

An idle job needs parallelism 1. It went to 3 and stayed.

`job.autoscaler.scale-down.max-factor` is 0.6. A vertex may not drop below 60% of
its current parallelism in one decision:

```
6 × 0.6 = 3.6  ->  3
```

Exactly what happened, on both vertices. **The cap overrode the want.** An idle
job needs parallelism 1, which is the `vertex.min-parallelism` default. It got 3
because that is as far as one decision may go.

Note the rounding: 60% of 6 is 3.6 and the result was **3**, so the
implementation floors rather than ceils. That is inferred from this one
observation, not read from the source. Divisibility does not explain it, since
120 has both 3 and 4 as divisors.

The next step is bounded the same way. `0.6 × 3 = 1.8`, which floors to **1**, so
the shrink should reach 1 in one more step rather than two, after waiting out
`scale-down.interval` again.

**The shrink then completed, and it confirms the floor reading.** A later check
found every vertex at parallelism **1** and a single TaskManager. The sequence was
6 to 3 to **1**, not 6 to 3 to 2 to 1, because `0.6 × 3 = 1.8` floors to 1.

```
every vertex   par=1
personalization-taskmanager-1-1   Running   0   177m
```

Slots needed is 1, so `ceil(1 ÷ 2)` = 1 pod. **`taskmanager-1-1` survived the
entire session**: the same pod, `RESTARTS 0`, through four scale-ups and three
scale-downs.

### Pods did fall, and the scheduler packed

```
NAME                               READY   STATUS    RESTARTS   AGE
personalization-798f5f6474-fzbcx   1/1     Running   0          146m
personalization-798f5f6474-sflzj   1/1     Running   0          146m
personalization-taskmanager-1-1    1/1     Running   0          146m
personalization-taskmanager-1-3    1/1     Running   0           40m
```

**Three TaskManagers became two.** `taskmanager-1-2` was released.

The arithmetic: slots needed is the largest parallelism of any single vertex,
now 3. Over `taskmanager.numberOfTaskSlots: 2` that is `ceil(3 ÷ 2)` = **2 pods**.

**So the scheduler packed rather than spread.** Three subtasks over two
TaskManagers leaves one slot idle, and the third TaskManager was emptied
completely and released. Had the subtasks been spread one per TaskManager, all
three pods would have stayed and nothing would have been freed.

`jobmanager.adaptive-scheduler.prefer-minimal-taskmanagers` could not be read off
this cluster, since the config endpoint reports only explicitly-set keys. The
observed behaviour is packing.

**The two survivors are the oldest and the newest**, `-1-1` at 146m and `-1-3` at
40m. The middle pod went. Release order is not by age.

### Scaling, not failover

```
exceptions: 0
counts: {'restored': 6, 'total': 878, 'in_progress': 0, 'completed': 874, 'failed': 4}
```

**Six restores across the whole session, zero exceptions.** Every parallelism
change went through a checkpoint restore and none through a failure.

Failed checkpoints went from 2 to 4. The two new ones coincide with the two
rescales. A checkpoint in flight when parallelism changes is aborted, and the
next one succeeds. That is expected, not a fault.

### Gap check

```
==> before: 94285 identities
==> after:  134754 identities
 ok no gap
  ! DUPLICATES: 5844 identities appear more than once in AFTER
```

**No gap.** 40,469 new Recommendations were produced during the drain, so the
comparison had real material rather than two identical sets.

**The duplicate count did not move.** 5844 before, 5844 after. The scale-down
created none. See the Notes.

## Gate

**Drill G passes.**

| Check | Result |
|---|---|
| Parallelism fell | **Pass.** CepOperator and Interval Join 6 to 3 to 1, Co-Keyed-Process 2 to 1 |
| Read from the scaling record, not pod counts | **Pass.** Two `In-place scaling triggered` events in the operator log |
| Pod behaviour recorded | **Pass.** 3 TaskManagers to 2 to 1, `taskmanager-1-1` the sole survivor |
| The packing decision explained | **Pass.** Packed. `ceil(3 ÷ 2)` = 2 pods, one TaskManager emptied and released |
| Scaling, not failover | **Pass.** 6 restores, **0** exceptions |
| Gap check | **Pass.** No gap. Duplicates unchanged at 5844 |

The plan's Step 4 gate, a rescale record with `triggerCause: UPDATE_REQUIREMENT`,
could not be used. Same Flink 2.2.0 limitation as Drill F.

## Notes

**The plan's Step 3 timing was wrong, and by a wide margin.** It says wait
"3 minutes, plus stabilization, plus cooldown", roughly 5 minutes. The governing
timer is `job.autoscaler.scale-down.interval`, which defaults to **1 hour** and
is not mentioned in the plan at all. Without changing it this Drill would have
looked broken for 55 minutes.

**The plan's Step 1 cannot be run as written.** It says to read the idle timeout
and the packing setting from the live configuration and not to assume defaults.
`/jobmanager/config` returns only the 62 keys that were explicitly set. A default
that was never overridden is indistinguishable from a key that does not exist.
There is no endpoint on this cluster that reports effective defaults.

**The 5844 duplicates are source replay, and they are not this Drill's doing.**
The count is identical in BEFORE and AFTER, so the scale-down produced none.
Diagnosed on 2026-09-08:

```
identity: shopper-4 @ 1788799494280
  partition=0 offset=102    {"productId":"P8","discountPercent":17.0,"reason":"price-drop", ...}
  partition=0 offset=5192   {"productId":"P8","discountPercent":0.0, "reason":"cart-abandoned", ...}
```

`PipelineConfig.java:54` defaults `startFromEarliest = true`, and the manifest
never overrides it, so `KafkaSources.java:17` uses `OffsetsInitializer.earliest()`.
**Every stateless restart re-reads `clickstream` from offset 0 and re-emits every
Recommendation.** Task 3's clean start was deliberately `upgradeMode: stateless`.

Three things confirm it:

- Offset distance between the two copies is 2190 to 5516, never adjacent. A
  checkpoint replay produces near-neighbours; a whole-topic re-emission produces
  this.
- `discountPercent` differs in 4712 of the 4713 mismatched pairs. `generatedAt`
  is a window end and is deterministic from event time, so the identity repeats.
  The enrichment does not, because promo-rule broadcast state and price-drop join
  state arrived in a different order on the second pass.
- 1023 of the 4672 records from 2026-09-07 are duplicated, 22% of that day.

**Exactly-once was never violated.** It guarantees no duplicates across a restore
from checkpoint. It says nothing about a deliberate stateless restart that
rewinds the source.

**The instrument needs bounding, and the pipeline needs a decision.**
`recommendation-snapshot.sh` reads the whole topic, which spans three phases and
at least one full replay. Give `cmd_snapshot` an optional start timestamp that
becomes `kcat -o s@<ms>`, so a Drill compares its own run. Separately,
`--start-from-earliest=false` would stop the replays, at the cost that a cold
start skips whatever is already in the topic.

**The initializer only applies to a cold start.** A job that restores from a
checkpoint or savepoint takes its offsets from state and ignores it entirely. So
Phase 7's promoted green, which ADR 0006 restores from blue's savepoint via
`scripts/promote-green.sh`, is unaffected either way. The exposure is a green
deployed cold with no savepoint, which is a failure path rather than the
promotion path.

`OffsetsInitializer.committedOffsets(...)` is the middle option, resuming from
the group offsets that `KafkaSources.java:15` already sets a group id for. It is
a softer guarantee than a savepoint, since ADR 0006 records that Flink writes
those offsets for monitoring only.

**Phase 4 already paid the cost of `false` once.** The same flag feeds the
promo-rule broadcast source. With `latest`, a rule published before the job
subscribed is never read, so every discount stays `0.0` until the next rule
arrives, and `PromoRuleApplier`'s `rule != null` guard is what turns that into a
zero rather than a crash. One flag, three sources, and the right answer is not
obviously the same for all three. **An ADR with a known trade, not a bug fix.**

**`compare` exits 141 when it lists duplicates.** 141 is 128 + 13, a `SIGPIPE`
from an internal pipeline in the script. The summary lines print correctly and
the result is sound. Do not read a non-zero exit here as a failed gap check, and
consider fixing the script.

**Scaling down is not symmetric with scaling up.** Worth stating plainly, because
the asymmetry is deliberate at three separate points:

| | Scaling up | Scaling down |
|---|---|---|
| Delay before acting | none beyond the metrics window | `scale-down.interval`, default **1 hour** |
| Step size | straight to the computed target | at most 40% off, per `scale-down.max-factor` 0.6 |
| Pod effect | new pods appear as soon as slots are needed | pods leave only when one is fully emptied, then after an idle timeout |

Every one of those favours keeping capacity. An autoscaler that shrinks eagerly
turns a traffic dip into a restart.

**Next task.** Task 7 installs Karpenter with the kwok provider. It depends on
nothing in Drills E to G and touches no Flink object. The one hard rule carried
forward: **Flink pods must never tolerate the kwok taint.** A TaskManager
scheduled onto a fake node reports `1/1 Running` with no JVM behind it, and the
job hangs waiting for slots that never register.
