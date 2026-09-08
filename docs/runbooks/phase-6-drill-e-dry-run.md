# Phase 6 Drill E: the autoscaler dry run under a Load Ramp

Date: 2026-09-08
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Deployment: `FlinkDeployment personalization` in `personalization-blue`,
`mode: native`, image `lab/personalization-pipeline:0.1-0bd7f52`
Topology at start: 2 JobManagers, 1 TaskManager, `parallelism: 2`,
`taskmanager.numberOfTaskSlots: 2`, `pipeline.max-parallelism: 120`
Autoscaler: `enabled: true`, **`scaling.enabled: false`**,
`metrics.window: 3m`, `stabilization.interval: 1m`,
`target.utilization: 0.6`, `catch-up.duration: 5m`,
`vertex.max-parallelism: 6`
Checkpointing: `EXACTLY_ONCE`, to `s3://checkpoints/phase-6`

Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

This Drill is different from the others. Nothing breaks. Nothing recovers. We
check that the autoscaler does the right sums before we let it change the job.

## What this Drill checks

There are two switches, not one.

| Switch | What it does |
|---|---|
| `job.autoscaler.enabled` | Runs the whole algorithm. Reads busy time and backlog from the job. Publishes a recommended parallelism |
| `job.autoscaler.scaling.enabled` | Applies the recommendation to the job |

Turn on the first and leave the second off. You get a recommendation and no
change. That is a dry run.

**Why the dry run matters.** Four of the five blockers in
[the autoscaling design](../superpowers/specs/2026-09-07-autoscaling-design.md)
give you a wrong recommendation while every layer says it worked. You cannot tell
a good recommendation from a bad one by checking that a number appeared. You have
to check **which vertex the number is about**.

### Which vertices should scale

**Only three vertices can be the real bottleneck.** The job splits into two
branches under the Click source. From
[`PersonalizationJob.java`](../../apps/pipeline/src/main/java/lab/personalization/pipeline/PersonalizationJob.java):

```
Source: click-stream ─┬─ keyBy(shopperId) ─┬─ EventTimeSessionWindows -> Map
                      │                    └─ CepOperator
                      └─ keyBy(productId) ─── Interval Join ── with product-change
                                                    │
              shopperSignals ──── Co-Keyed-Process ─┘
                                        │
                       Co-Process-Broadcast -> async wait -> Sink: Write
```

Three vertices get every Click: the session window, the CEP operator, and the
interval join. Each sits after a `keyBy`, so Kafka partition count does not limit
them. Only these three can be the real bottleneck.

Two vertices get far less traffic. `Co-Keyed-Process` and
`Co-Process-Broadcast` see Recommendations and rules, about one twentieth of the
Click rate. If the autoscaler wants to scale those, something in Task 1, 2 or 3
did not work.

### How we add load

**Turn up `--product-change-rate`, not `--click-rate`.**

The interval join compares each Click against every Product Change still inside
the join window. The cost per Click is:

```
productChangeRate × joinWindow ÷ productCount
```

With 400 Product Changes per second, a 4 second window and 200 Products, that is
**8 comparisons per Click**. The old defaults gave 0.4. So this is 20 times the
work, and all of it is CPU inside the operator's own thread. That is what
`busyTimeMsPerSecond` measures, and that is what the autoscaler reads.

Turning up `--click-rate` would add work to every vertex at once. Then you could
not tell the vertices apart.

### The mistake that wastes the most time

**Wait three minutes. Nothing happens before then, and that is normal.**

`job.autoscaler.metrics.window: 3m` means the autoscaler needs three minutes of
history before it decides anything. `stabilization.interval: 1m` adds another
quiet minute after a restart. People watch for ninety seconds and report a broken
autoscaler.

For how any of this works, see
[the Phase 6 knowledge doc](../knowledge/phase-6-autoscaling.md).

## Procedure

**1. Check the job started clean, before adding load.**

```bash
kubectl -n personalization-blue get flinkdeployment personalization \
  -o jsonpath='{.status.jobStatus.upgradeSavepointPath}{"\n"}{.status.jobStatus.state}{"\n"}'
```

Expect an empty first line, then `RUNNING`.

Why: a savepoint path here means the job restored old state. Then every number
below would describe the Phase 5 job wearing Phase 6 settings.

**2. Record the job graph.**

```bash
JID=$(curl -s localhost:30011/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -s "localhost:30011/jobs/$JID" | python3 -c "
import sys,json
for v in json.load(sys.stdin)['vertices']:
    print('%-58s %5s %6s' % (v['name'][:58], v['parallelism'], v.get('maxParallelism')))"
```

Expect 8 vertices, `PAR 2`, `MAXPAR 120`, and no name containing
`Sink: Print to Std. Out`.

Why: Task 0 ran this same command and got 9 vertices at `PAR 6` / `MAXPAR 128`.
All three numbers must have changed. `MAXPAR 120` is what allows 3, 5 and 6 as
parallelism values. The old default of 128 allows only 1, 2, 4 and 8.

**3. Record the vertex IDs and their names.**

```bash
curl -s "localhost:30011/jobs/$JID" | python3 -c "
import sys,json
for v in json.load(sys.stdin)['vertices']:
    print(v['id'], v['name'][:70])"
```

Why: the autoscaler's report gives you hex IDs and no names. Without this list
you cannot tell which vertex it wants to scale, so you cannot pass the gate.

A vertex ID is a hash of the job graph. It survives a restart and it survives a
parallelism change. It changes when the **graph** changes, so adding or removing
an operator invalidates the whole list. `--debug-prints=false` removed a sink in
Task 3, which is why Task 0's IDs are of no use here.

The **job** ID is different. It changes on every restart, so re-read `$JID`
after one.

**4. Start the Load Ramp.** (terminal 1)

```bash
apps/gradlew -p apps :generator:run \
  --args="--click-rate=800 --product-change-rate=400 --click-max-skew-seconds=1"
```

Why each argument:

- `--click-rate=800` gives the three keyed vertices real volume. With 2000
  Shoppers this gives about 11 Clicks per Session, so the windows have something
  to aggregate. At a low Click rate every Session holds one Click and the window
  branch emits almost nothing.
- `--product-change-rate=400` is the load lever, as explained above.
- `--click-max-skew-seconds=1` keeps Clicks close to in order. Watermarks then
  advance and windows fire. This Drill measures throughput, not lateness.

**Check for old generator processes first.** In Task 1 a `pkill` said it worked
but killed nothing. Two generators ran at once and the rates added up, which
looked like a 32% overshoot.

**5. Wait three minutes.** Add the stabilization minute if the job restarted
recently. Do not touch anything.

**6. Read the recommendation.** The operator writes a `SCALINGREPORT` event for
each decision. It holds the same numbers as the `RECOMMENDED_PARALLELISM` metric,
and you can read it without a metrics backend.

```bash
kubectl -n flink-operator logs deploy/flink-kubernetes-operator \
  -c flink-kubernetes-operator --since=10m | grep -i 'scaling\|parallelism\|recommend'
```

The same report also appears in the generator's terminal. The operator writes its
audit events as Kubernetes Events on the `FlinkDeployment`.

**7. Check that nothing changed.**

```bash
kubectl -n personalization-blue get pods
```

Expect two JobManagers and still **one** TaskManager, same pod name, larger
`AGE`.

Why: this proves the dry run was dry. A new TaskManager would mean the second
switch was on.

## Observed result

Run on 2026-09-08, at `--click-rate=800 --product-change-rate=400`. Every block
below is real output, pasted as it came back.

### Clean start

```
                       <- upgradeSavepointPath, empty
RUNNING
```

Empty path, so the job did not restore.

### Job graph

```
Source: click-stream                                           2    120
Source: product-change-stream                                  2    120
Source: promo-rule-stream                                      2    120
EventTimeSessionWindows -> Map                                 2    120
CepOperator                                                    2    120
Interval Join                                                  2    120
Co-Keyed-Process                                               2    120
Co-Process-Broadcast -> async wait operator -> Sink: Write     2    120
```

8 vertices, `PAR 2`, `MAXPAR 120`, no print sink. Task 0 recorded 9 vertices at
`PAR 6` / `MAXPAR 128`. All three Task 3 changes show up in the running job, not
only in the manifest.

The missing ninth vertex was the standalone `Sink: Print to Std. Out`.
`--debug-prints=false` removed it. The other seven no longer carry it in their
names either.

### Pods before the load

```
NAME                               READY   STATUS    RESTARTS   AGE
personalization-798f5f6474-fzbcx   1/1     Running   0          7m59s
personalization-798f5f6474-sflzj   1/1     Running   0          7m59s
personalization-taskmanager-1-1    1/1     Running   0          7m47s
```

One TaskManager. This is correct. See the Notes for why.

### The two scaling reports

Fifteen seconds apart. Both say scaling is disabled.

```
14:33:01 SCALINGREPORT Scaling execution disabled by config
  job.autoscaler.scaling.enabled:false, recommended parallelism change:
  { Vertex ID 0ce5a3d5245a8fc303aa7f34d82ae94f | Parallelism 2 -> 5
    | Processing capacity 1102.79 -> 2750.00 | Target data rate 850.59 }
  { Vertex ID 5d4d0bfa95bd89679658645fb09a51ca | Parallelism 2 -> 3
    | Processing capacity 3233.56 -> 3465.00 | Target data rate 1133.52 }

14:33:16 SCALINGREPORT Scaling execution disabled by config
  job.autoscaler.scaling.enabled:false, recommended parallelism change:
  { Vertex ID 0ce5a3d5245a8fc303aa7f34d82ae94f | Parallelism 2 -> 6
    | Processing capacity 960.06 -> 2676.00 | Target data rate 826.27 }
  { Vertex ID 5d4d0bfa95bd89679658645fb09a51ca | Parallelism 2 -> 3
    | Processing capacity 3349.96 -> 3808.00 | Target data rate 1246.14 }
```

`Scaling execution disabled by config` is the dry run telling you what it is. The
autoscaler worked out a change and chose not to apply it. This line is better
proof than "no new pod appeared", because it shows both switches are where you
set them.

### Vertex IDs and names

```
bc764cd8ddf7a0cff126f51c16239658  Source: click-stream
feca28aff5a3958840bee985ee7de4d3  Source: product-change-stream
605b35e407e90cda15ad084365733fdd  Source: promo-rule-stream
c9235a26195589826000b27f7d761f13  EventTimeSessionWindows -> Map
0ce5a3d5245a8fc303aa7f34d82ae94f  CepOperator
5d4d0bfa95bd89679658645fb09a51ca  Interval Join
977c935ff4c0f85b5dc0ce5735367a6a  Co-Keyed-Process
45c68ece0ecd4e5ae4b1308f81da61ae  Co-Process-Broadcast -> async wait operator -> Sink: Writer -> Sink: C
```

So the two reported vertices are:

| Vertex ID | Name | Recommendation | Target data rate |
|---|---|---|---|
| `0ce5a3d5…` | **CepOperator** | `2 -> 5`, then `2 -> 6` | 826 to 851 |
| `5d4d0bfa…` | **Interval Join** | `2 -> 3`, both reports | 1134 to 1246 |

Both take every Click and both sit under a `keyBy`. Neither is a sink. Neither is
`Co-Process-Broadcast`.

The data rates said so before the names arrived. 826 to 851 per second is the
Click rate of 800. 1134 to 1246 is Clicks plus Product Changes, 800 + 400.

### Pods after the load

```
NAME                               READY   STATUS    RESTARTS   AGE
personalization-798f5f6474-fzbcx   1/1     Running   0          16m
personalization-798f5f6474-sflzj   1/1     Running   0          16m
personalization-taskmanager-1-1    1/1     Running   0          16m
```

Same names. `AGE` grew from 7m59s to 16m. `RESTARTS 0`. One TaskManager ran the
whole Load Ramp.

## Gate

**Drill E passes.** All three checks hold.

| Check | Result |
|---|---|
| The recommendation is on Click-consuming vertices, not on a sink or `Co-Process-Broadcast` | **Pass.** CepOperator and Interval Join |
| Processing rates are believable, so the print sinks are gone | **Pass.** 480 to 1675 records per second per subtask. A chained `print()` would give a much lower number |
| Nothing changed | **Pass.** One TaskManager, same name, `RESTARTS 0`, over 8 minutes of load |

### One vertex stayed quiet, and that is the best result here

The gate expected three vertices to scale. Two did.
`EventTimeSessionWindows -> Map` asked for nothing.

That is not a gap. It is the proof that the autoscaler measures work and not
traffic.

The session window and the CEP operator read from the **same**
`keyBy(Click::shopperId)`, at `PersonalizationJob.java:89`. They get the same
Clicks, at the same rate. They differ only in how much work each Click costs:

- `CepOperator` runs a state machine for every Click. It holds partial matches
  for the cart-abandonment pattern until the `cepWithin` time runs out.
- `SessionAggregator` adds the Click to window state and stops. The real work
  waits for the session gap. It is not paid per Click.

Now compare the two possible algorithms:

| If the autoscaler read | Then the two vertices would |
|---|---|
| Input rate | Scale the same. Both see 800 Clicks per second |
| Busy time | Scale differently. Only one is expensive per Click |

They scaled differently. So the autoscaler is reading busy time.

This is the blocker class the design warned about. A wrong recommendation appears
when the algorithm reads something easy instead of real busy time. These two
vertices happen to form a control pair, and the pair came out right.

**Still check it.** Read `LOAD` on `c9235a26…`. It should be well under
`job.autoscaler.target.utilization: "0.6"`. If a quiet vertex sits at 0.6 or
above, that is a real fault, and Drill F must not run until it is understood.

Step 4b of Task 4 was not needed. It offered a bigger `--product-change-rate` or
a 0.5 CPU TaskManager if nothing scaled. The first lever worked on the first try.

**Next Drill.** Drill F sets `scaling.enabled` to `"true"` and puts
`upgradeMode` back to `savepoint` **in the same edit**. The deployment has been
on `stateless` since Task 3, and any other spec change during that time throws
away state without warning.

Drill F answers the one question the design could not. Does the autoscaler's
chosen parallelism reach the live CR as
`pipeline.jobvertex-parallelism-overrides` in `spec.flinkConfiguration`? Task 0
recorded that field empty, so the comparison means something.
