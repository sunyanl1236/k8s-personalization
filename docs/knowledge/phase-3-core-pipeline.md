# Phase 3 knowledge: Core pipeline

Written during Phase 3, not before it. Companion to
[phase-1-data-platform.md](phase-1-data-platform.md), which covers standing
Strimzi and MinIO up in the first place. This file covers what actually came up
while building the Shopper-keyed branch against `MiniCluster`.

Design decisions and their rejected alternatives live in
[the core pipeline design](../superpowers/specs/2026-08-23-core-pipeline-design.md).
The ordered steps live in
[the implementation plan](../superpowers/plans/2026-08-23-phase-3-core-pipeline.md).
This file is only for how things actually work.

## How a NodePort Service reaches the pod, and why it never visits the ClusterIP

Came up in Task 2, while exposing MinIO's S3 API so `MiniCluster` on the host
could write checkpoints to it.

The confusion is worth recording because the obvious mental model is wrong. A
`NodePort` Service looks like it should chain: host port, then ClusterIP, then
pod. It does not. There is no process listening on the ClusterIP at all. A
Service is a set of packet-rewriting rules that `kube-proxy` programs into the
node's iptables or nftables, so nothing ever "arrives" at the ClusterIP to be
forwarded onward.

`nodePort` and `port` are two separate keys in the same rewrite table, not two
hops in one path. Both keys name the same right-hand side, which is
`targetPort`.

MinIO's ClusterIP is 10.96.241.160 and targetPort is 9000. Say the pod's IP is
10.244.1.7 (check yours with `kubectl get pod -n minio-tenant -o wide`).

Your path, from the host:

```
localhost:30014
  └─▶ kind node container, port 30014
        └─▶ rule: "dst *:30014" → rewrite dst to 10.244.1.7:9000
              └─▶ pod, listening on 9000
```

A pod inside the cluster calling the Service:

```
curl http://minio.minio-tenant.svc:80
  └─▶ DNS resolves to 10.96.241.160
        └─▶ rule: "dst 10.96.241.160:80" → rewrite dst to 10.244.1.7:9000
              └─▶ pod, listening on 9000
```

Two entry points. Both rules end at targetPort. Neither one visits the other.

Corrected diagram

                    ┌─ nodePort 30014 ─┐
host / any node ────┤                  ├──▶ pod 10.244.1.7:9000   (targetPort)
                    │                  │
in-cluster client ──┴─ port 80 ────────┘
                       (ClusterIP 10.96.241.160)

### Three consequences worth keeping

- **`targetPort` is the only one of the three that must be correct.** It sits on
  the right-hand side of both rules. `nodePort` decides how the host gets in.
  `port` is only used by in-cluster callers addressing the ClusterIP, and
  nothing in this project does that, so its value is free.
- **The rules are generated from the `selector`, not from the Service name.** A
  wrong selector produces a Service that is created without error and routes
  nowhere. Listing the endpoints prints the real `podIP:targetPort` list, so an
  empty result identifies a selector problem before `curl` is ever run.

  On Kubernetes 1.33 and later, `kubectl get endpoints` warns that the `v1
  Endpoints` API is deprecated in favour of `discovery.k8s.io/v1
  EndpointSlice`. This cluster is on 1.34.8, so the current form is:

  ```bash
  kubectl get endpointslice -n minio-tenant -l kubernetes.io/service-name=minio-s3-api
  ```

  The old command still works and still answers the question. It just will not
  forever.
- **In `kind`, a NodePort alone is not enough.** A node is a container, so the
  port must also be published to the host by `extraPortMappings` at cluster
  creation time. Phase 0 reserved 30014 and 30015 for MinIO in
  `clusters/kind/kind-cluster.yaml` before anything needed them, which is why
  Phase 3 did not require recreating the cluster.

### Observed on this cluster, 2026-08-23

The `minio-s3-api` Service, `nodePort: 30014`, `targetPort: 9000`:

```
NAME           ENDPOINTS          AGE
minio-s3-api   10.244.3.14:9000   4m58s

$ curl -i http://localhost:30014/minio/health/live
HTTP/1.1 200 OK
```

The endpoint line proves the selector matched a real pod and that `targetPort`
is right. The `200 OK` proves the whole chain from outside the cluster: host
port 30014, the `kind` `extraPortMapping`, the `zone-a` worker, the nodePort
rewrite rule, the pod on 9000.

**Neither is implied by ArgoCD reporting Synced and Healthy.** For a Service
that means only that the YAML applied and Kubernetes accepted it. A Service
whose selector matches zero pods is Synced and Healthy too.

### Which namespace the Service goes in is not a choice

A Service's selector only matches pods in its own namespace. Selectors do not
cross namespace boundaries. Put this Service in `default` with the same
`v1.min.io/tenant: personalization` label and it matches zero pods, produces an
empty endpoint list, and routes nowhere.

The MinIO pods are in `minio-tenant` because `manifests/minio/tenant.yaml` sets
that namespace on the `Tenant`. So the Service has to be there too. This is
mechanical, not a preference.

## Why `setStartingOffsets(OffsetsInitializer.earliest())` is set explicitly

Came up in Task 4, and the answer matters for Task 9's Drill rather than for
anything visible now.

**The problem.** Without that line, a second run of the job behaves differently
from the first, and nothing tells you. Task 9's Drill compares a clean run
against a crashed-and-restarted run, and its entire premise is that both read
identical input.

**Who remembers where you were.** A Kafka topic is an append-only log and every
record has a position, its offset. A consumer starting up answers one question,
and there are only three possible answers:

```java
// Where do we start reading?
int start = 0;                     // earliest: from the beginning
int start = log.size();            // latest: only what arrives from now on
int start = bookmark.get(reader);  // committed: where this reader stopped last time
```

`KafkaSource`'s **default is the third**: the consumer group's committed
offsets, falling back to earliest only if the group has never committed. Flink
commits offsets back to Kafka on each checkpoint, so the bookmark does get
written.

**The worked example.** Consumer group `personalization-phase-3`, `clickstream`
holding 1600 records.

Without the line, using the default:

| | Bookmark at start | Reads | Bookmark after |
|---|---|---|---|
| Run 1 | none yet | 0 to 1600 | 1600 |
| Run 2 | 1600 | **nothing** | 1600 |

Run 2 connects, reports no error, and prints nothing forever. That is the "it
worked yesterday and today the job is broken" mystery, and it is not a bug.

With `earliest` set explicitly, both runs read 0 to 1600, so any difference
between their outputs can only have been caused by the crash.

**Why this does not fight `--restore-from`.** Offsets held in a restored
checkpoint take priority over the initializer, which applies only to a fresh
start with no state to restore:

```
run 2, fresh start    -> initializer applies      -> offset 0
   killed at ~800, last checkpoint held offset 700
run 2, --restore-from -> checkpoint state applies -> offset 700, not 0
```

If the checkpoint did not win, recovery would restart from the beginning and
reprocess everything, which is exactly the duplication the exactly-once sink
exists to prevent. That priority **is** the recovery mechanism.

**The cost.** `earliest` replays the whole backlog. Task 3's 75-second run
printed 2.9 million Clicks with event times from a week earlier. Correct for the
Drill, useless for watching live behaviour, which is why `--start-from-earliest`
exists and defaults to `true`: the Drill's requirement is the one you get by
accident rather than the one you have to remember.


## Why the session window ends in a `ProcessWindowFunction`, not `reduce`

Came up in Task 4. There are four ways to terminate a windowed stream in Flink
and they are not interchangeable.

**`reduce` structurally cannot do this job**, for two independent reasons.

*The type.* `ReduceFunction<T>` is `(T, T) -> T`: input and output types are the
same by definition. What is needed is `Iterable<Click> -> SessionSignal`, and a
`SessionSignal` is not a `Click`. No version of `reduce` changes the type.

*The window metadata.* `SessionSignal` carries `windowStart` and `windowEnd`,
which come from `context.window().getStart()` and `getEnd()`. **Only
`ProcessWindowFunction` receives a `Context`.** `ReduceFunction` and
`AggregateFunction` see elements and accumulators, never the window they belong
to.

That second point is load-bearing beyond tidiness. Task 6 uses `windowEnd` as
the Recommendation's `generatedAt` precisely so nothing in the output derives
from wall-clock time. Without window metadata the only available timestamp is
`Instant.now()`, and Task 9's Drill, which compares a clean run against a
replayed one line for line, would fail permanently for a reason unrelated to
checkpointing.

A third, smaller reason: most-clicked Product needs a `Map<String, Integer>`
accumulator, and `reduce`'s accumulator *is* the element type, so there is
nowhere to put one. `AggregateFunction` solves this half, since its `ACC` is
free.

| Option | Type change | Window metadata | State held while the window is open |
|---|---|---|---|
| `reduce(ReduceFunction)` | no | no | 1 `Click` |
| `aggregate(AggregateFunction)` | yes | **no** | the accumulator |
| `process(ProcessWindowFunction)` | yes | yes | **every `Click` in the window** |
| `aggregate(AggregateFunction, ProcessWindowFunction)` | yes | yes | the accumulator |

**The fourth row is the production-grade form**, and this project does not use
it. It folds incrementally and then hands the result to a
`ProcessWindowFunction` at fire time purely to attach the window metadata.


## Why a Click behind the watermark is not automatically a Late Click

Came up in Task 5. This is the single easiest thing to get wrong in the whole
phase, because the obvious reading is wrong: "the watermark has passed this
Click, so the Click is late" does not hold.

**The watermark fires windows. Windows accept or refuse Clicks.** The watermark
never refuses a Click directly.

### What "late" actually means

Not "older than the watermark". It means this:

> The `SessionSignal` that this Click should have influenced was already
> published, and it cannot be taken back.

That is the only real damage. A `SessionSignal` leaves the operator carrying
`clickCount: 4`. Then a fifth Click arrives. The 4 is already downstream and is
now wrong forever.

If nothing has been published yet, a Click arriving behind the watermark costs
nothing. It joins the pile still sitting in window state, and the window
publishes the correct number later.

### Session windows work in two phases

The window assigner is stateless and knows nothing about existing windows, so
it does the only thing it can. It gives **every** Click its own fresh window
`[t, t + gap)`. That is the entire body of
`EventTimeSessionWindows.assignWindows`:

```java
return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
```

The window operator holds the state, so merging happens there, one step later.
`MergingWindowSet.addWindow` unions the fresh window with every existing window
it overlaps.

A union can only move the end later, never earlier. So:

**Merging can only rescue a Click from lateness. It can never cause it.**

### One worked trace

Watermark bound 5s, session gap 6s, one Shopper. The watermark is always
`maxSeen - 5`, where `maxSeen` is the newest `eventTime` the source has read.

| # | Click | maxSeen | watermark | its window | verdict |
|---|---|---|---|---|---|
| 1 | A at t=20 | 20 | 15 | `[20, 26)` | opens a window |
| 2 | B at t=30 | 30 | 25 | `[30, 36)` | opens a window. A's window ends at 26 and the watermark is 25, so A is still open |
| 3 | C at t=22 | 30 | 25 | `[22, 28)`, merged into `[20, 28)` | **accepted**, though it sits 3s behind the watermark |
| 4 | D at t=10 | 30 | 25 | `[10, 16)` | **Late Click** |

Row 3 is the one that surprises people. C is assigned `[22, 28)`, which then
merges with A's `[20, 26)` into `[20, 28)`. Note the end moved out to 28. C did
not just join A's window, it **extended** it, which is exactly what "session"
means.

State at watermark 25, which is what makes rows 3 and 4 differ:

```
[20, 26)  A     STILL OPEN.  end 26 > 25. Nothing published. C can join.
[30, 36)  B     STILL OPEN.  end 36 > 25.
[10, 16)  ...   GONE.        Fired when the watermark passed 16, state deleted.
                             D has no pile left to join.
```

### The two conditions, and why both are needed

Simplified from `WindowOperator.processElement`:

```java
boolean isSkippedElement = true;

for (W window : elementWindows) {
    W actualWindow = mergingWindows.addWindow(window, mergeFunction);
    if (isWindowLate(actualWindow)) {          // 1. did the window already fire?
        mergingWindows.retireWindow(actualWindow);
        continue;
    }
    isSkippedElement = false;                  // a window took it
    ...
}

if (isSkippedElement && isElementLate(element)) {   // 2. behind the watermark?
    sideOutput(element);
}
```

| Helper | Test |
|---|---|
| `isWindowLate(w)` | `w.maxTimestamp() + allowedLateness <= currentWatermark` |
| `isElementLate(e)` | `e.getTimestamp() + allowedLateness <= currentWatermark` |

Two points that matter. The check runs on `actualWindow`, the **merged** window,
not the freshly assigned one. And the guard is an `&&`.

| | window end | `isWindowLate` | `isSkippedElement` | `isElementLate` | side output |
|---|---|---|---|---|---|
| **C** at t=22 | 28 (merged) | `27.999 <= 25` → **false** | **false**, a window took it | `22 <= 25` → **true** | **no**, the `&&` fails on the first term |
| **D** at t=10 | 16 | `15.999 <= 25` → **true** | **true**, no window took it | `10 <= 25` → **true** | **yes**, both terms hold |

C's `isElementLate` is **true**. Being behind the watermark is real, and Flink
computes it. It is simply not sufficient. `isElementLate` is only consulted
after every candidate window has already refused the element, and it answers a
narrower question: this element found no home, was that because it was late or
for some other reason?

### The two lines, 6 seconds apart

```
   event time ------------------------------------------------->

        t=10        t=19        t=22       t=25       t=30
          |           |           |          |          |
          D       LATENESS        C      WATERMARK   maxSeen
                    LINE                  (maxSeen-5)
                 (maxSeen-11)

     <-- late -->|<---- behind the watermark, but still accepted ---->
```

Crossing the watermark line means nothing on its own. Crossing the lateness line
means the window already closed. The distance between them is exactly the
session gap:

```
a window closes when    watermark   >  t + 6
substitute watermark:   maxSeen - 5 >  t + 6
rearrange:              t           <  maxSeen - 11
```

So with this project's settings a Click is late only when its `eventTime` is
more than **11 seconds** behind the newest `eventTime` seen. Not 5 seconds. The
session gap buys 6 extra seconds of protection past the Click's own timestamp.

`allowedLateness` stays at its default of zero here. Setting it shifts both
helpers by exactly that amount, and the 11 second figure moves with it.

### Why Drill B uses `shopper-99`

`Catalog.SHOPPER_IDS` holds only `shopper-1` through `shopper-10`, so the
generator can never produce `shopper-99`. That id has no open Browsing Session,
so phase 2 finds nothing to merge with and no rescue is possible. The Drill sets
`eventTime` to 60 seconds ago, far past the 11 second lateness line. The verdict
cannot be ambiguous.



## Why `transient` on a `ValueState` field

Came up in Task 6, on `RecommendationDecider`. Appended verbatim from the
explanation given at the time. Only the heading depth was changed, so the
section nests under this document.

### The problem, in one sentence

Your `RecommendationDecider` object is built on one machine and has to run on a
different one. Some things can survive that trip. A live connection cannot.

### Simplest same-kind example, no Flink in it

Plain Java. Forget streaming entirely.

```java
class Printer implements Serializable {
    String name = "office-printer";
    Socket connection;              // a live TCP connection to a machine on THIS network
}
```

Now you want to send this `Printer` object to a colleague in another building.
Java serialization walks **every** field and turns it into bytes.

- `name` is a `String`. It converts to bytes fine. `"office-printer"` means the
  same thing in the other building.
- `connection` is a live socket. There is nothing sensible to write down. A
  socket is a file descriptor plus a TCP session with a machine that the other
  building cannot even reach. Java refuses, because `Socket` is not
  `Serializable`, and you get a `NotSerializableException`.

The fix:

```java
transient Socket connection;
```

`transient` means: **skip this field when writing bytes.** The mail now carries
only the name.

And here is the part that matters most. On arrival, the object is rebuilt from
those bytes. Transient fields are not restored, and their initializers do not
run either. `connection` comes back as **`null`**. The receiver has to open its
own socket, using its own network.

That is the whole pattern:

```
things that describe    -> travel as bytes
things that connect     -> transient, rebuilt on arrival
```

### Now map it back

**Why Flink serializes your function at all.** You create
`new RecommendationDecider(cooldown)` inside `main()`. That code runs on the
JobManager. But `processElement` runs inside TaskManager JVMs, which are
different processes, often on different machines. Flink cannot pass the object
by reference across a network. It serializes the object and ships the bytes.

```
JobManager                              TaskManager, subtask 7
-----------                             ----------------------
new RecommendationDecider(cooldown)
   cooldown           = 60s
   lastRecommendedProduct = null

serialize  ------------ bytes ------->  deserialize
                                           cooldown = 60s      (travelled)
                                           lastRecommendedProduct = null

                                        open(OpenContext) runs HERE, once
                                           lastRecommendedProduct =
                                               getRuntimeContext().getState(...)
```

`cooldown` is a `Duration`. It describes something, so it travels.

`lastRecommendedProduct` is a handle into RocksDB, wired to a state backend, a
key group assignment, and a checkpoint. **None of that exists on the
JobManager.** There is no state backend there to point at. It is the `Socket` of
this example.

**Which is exactly why `open` exists.** It is not a constructor. A constructor
runs before the trip. `open` runs once per subtask **after** the object has
arrived, at the one moment when `getRuntimeContext()` finally has a real state
backend behind it. That is the only place the handle can be created.

### One honest correction to what I said

I said `transient` "keeps it out of the shipped bytes", and implied you would
otherwise get an error. In this exact case you would not. The field is still
`null` when the object is serialized, and Java writes a null field without
complaint no matter what its declared type is. Dropping `transient` here would
compile and run today.

Write it anyway, for two reasons:

1. **It documents the lifecycle.** The keyword tells the next reader that this
   field is filled in at runtime, not at construction. That is real information
   about when the field is safe to touch.
2. **It protects the refactor.** The moment anyone assigns a non-serializable
   value to a field before shipping, a `KafkaProducer`, a JDBC connection, a
   cached client, the job fails at submission with `NotSerializableException`.
   That is the common version of this bug in production code, and the habit is
   what prevents it.


## How `deleteEventTimeTimer` works, and why a timer needs deleting at all

Came up in Task 6, on `RecommendationDecider`'s cooldown. Appended verbatim from
the explanation given at the time. Only the heading depth was changed, so the
section nests under this document.

### The problem: `register` never replaces, it adds

The mental model that causes the bug is thinking of a timer like setting an
alarm clock on your phone. You set it for 16:00, then change your mind and set
it for 18:00, and there is one alarm.

Flink is not like that. **Registering a second timer leaves the first one
there.** Both fire.

### What a timer actually is

Not a thread. Not a wall clock alarm. It is a **row in a sorted set** held by
the operator, and each row is a pair:

```
(key, timestamp)
```

The watermark drives it. When the watermark advances to `W`, Flink walks the set
from the lowest timestamp upward, and for every row with `timestamp <= W` it
sets the current key to that row's key, calls your `onTimer`, and removes the
row.

So:

- `registerEventTimeTimer(t)` = **insert** a row `(currentKey, t)`.
- `deleteEventTimeTimer(t)` = **remove** the row `(currentKey, t)`.

Identity is the timestamp. Registering `160` twice for the same key inserts one
row, not two, because the set deduplicates. Registering `160` and then `180`
inserts **two independent rows**, and that is exactly the bug.

### The same trace, both ways

`shopper-3`, cooldown 60s. Watch the timer set for that key.

**Without delete:**

```
t=100   emit P7,  state="P7",  register 160        set = {160}
t=120   emit P2,  state="P2",  register 180        set = {160, 180}   <-- two rows
t=160   watermark passes 160 -> onTimer fires      set = {180}
        onTimer clears state.  But state held "P2", not "P7".
t=170   P2 arrives again, state is null -> P2 emitted a second time.  WRONG.
```

The row for `160` had no idea it had been superseded. It did its job faithfully
and wiped the wrong thing.

**With delete:**

```
t=100   emit P7,  state="P7",  register 160        set = {160}
                  remember 160
t=120   emit P2,  delete 160                       set = {}
                  register 180                     set = {180}        <-- one row
                  state="P2", remember 180
t=180   watermark passes 180 -> onTimer fires      set = {}
        clears state.  P2's cooldown ran its full 60 seconds.  CORRECT.
```

### Three things about the call itself

**1. It is keyed, exactly like `ValueState`.**

```java
ctx.timerService().deleteEventTimeTimer(160_000L);
```

You never pass a Shopper id. Flink already set the current key before calling
`processElement`, and the delete applies to that key alone. `shopper-4`'s timer
at the same timestamp is untouched.

**2. It takes epoch milliseconds, and a wrong value is a silent no-op.**

The argument is a `long`, the same unit as `windowEnd().toEpochMilli()`. If you
pass a timestamp that does not match a registered row, nothing is deleted and
**nothing is reported**. No exception, no log line. So you must delete the
*exact* value you registered, not a recomputed one.

This is the reason the delete approach forces a second piece of state. You have
to remember the exact timestamp you registered:

```java
private transient ValueState<Long> pendingTimer;
```

And it must be `ValueState`, not a plain `long` field. A plain field would be
shared across every Shopper this subtask handles, so `shopper-4` would overwrite
`shopper-3`'s remembered timestamp. Same lesson as `lastRecommendedProduct`.

**3. You cannot delete a timer that already fired.** Once the watermark passes
it, the row is gone. `delete` on it is another silent no-op. In practice this is
fine here, because you only ever delete a timer that is still in the future
relative to the signal you are processing.

### What this costs you, versus the other option

| | delete-and-reregister | stale-check in `onTimer` |
|---|---|---|
| Extra state | `ValueState<Long>` for the pending timestamp | `ValueState<Long>` for the live timestamp |
| Timer rows alive per Shopper | always 1 | grows until each fires |
| `onTimer` body | clear both states | compare `timestamp` against stored value, return early if they differ |

Both need the same second `ValueState`, so the storage cost is a wash. The real
difference is how many timer rows exist. Delete keeps exactly one per Shopper.
The stale-check approach lets dead rows pile up until their watermark arrives,
and every one of them is checkpointed.

One more point in delete's favour for this project: timers are part of
checkpointed state. Task 9 restores from a checkpoint and compares the replayed
run against the original. Fewer rows in the checkpoint means fewer things that
can differ.

Pick one and tell me which, then write `processElement` and `onTimer`.


## What `s3://checkpoints/phase-3` actually means

Came up in Task 7. Appended verbatim from the explanation given at the time.
Only the heading depth was changed, so the section nests under this document.

Three separate things are packed into that one string, and only one of them is a
folder. None of them is AWS.

### Decompose the URI

```
s3://checkpoints/phase-3
│     │           │
│     │           └── key prefix inside the bucket
│     └────────────── BUCKET name, not a folder
└──────────────────── scheme: which FileSystem implementation to use
```

**`s3://` is not a location.** It is a lookup key. Flink asks its registry "who
handles the scheme `s3`?", and `flink-s3-fs-native` answers because it registered
itself through `ServiceLoader`. That is the only job the scheme does. It says
*speak the S3 protocol*, nothing about where.

**`checkpoints` is the bucket.** In an S3 URI the first segment after `://` sits
in the position a hostname occupies in an HTTP URL, and S3 puts the bucket there.
It is not a directory. You created this bucket in Phase 1, declared in
`Tenant.spec.buckets`, and confirmed it in the MinIO console.

**`phase-3` is a key prefix.** And here is the part that surprises people: **S3
has no folders at all.** A bucket holds a flat list of objects, each with a string
key. The key `phase-3/a1b2c3/chk-7/_metadata` is one string. The `/` characters
carry no meaning to the storage layer. Consoles draw folder icons by splitting on
`/`, but nothing was ever created called `phase-3`. Delete every object under it
and the "folder" vanishes, because it never existed.

### So where is S3?

There is no AWS in this project. This line is the answer:

```yaml
s3.endpoint: http://localhost:30014
```

That is the MinIO S3 API you exposed on NodePort 30014 in Task 2, the one you
proved with `curl http://localhost:30014/minio/health/live` returning 200.

```
your job                       kind cluster
--------                       ------------
s3://checkpoints/phase-3
   │
   ├── scheme  -> flink-s3-fs-native
   └── request -> http://localhost:30014  ──> NodePort 30014
                  (s3.endpoint)                    │
                                                   v
                                            minio-s3-api Service
                                                   │
                                                   v
                                            MinIO, bucket "checkpoints"
```

### Why `s3.path-style-access: true` belongs to this

Now the two config lines connect. There are two ways an S3 client can turn a
bucket name into an HTTP request.

**Virtual-host style**, the AWS default. The bucket becomes a subdomain:

```
PUT http://checkpoints.localhost:30014/phase-3/a1b2c3/chk-7/_metadata
        └──────────┘
        bucket as subdomain -> DNS lookup for "checkpoints.localhost" -> fails
```

**Path style**, what you set:

```
PUT http://localhost:30014/checkpoints/phase-3/a1b2c3/chk-7/_metadata
                          └──────────┘
                          bucket as first path segment -> no DNS involved
```

MinIO here is reached by a host and port, not by a domain you control, so
virtual-host style cannot work. Without that one line the failure says the
hostname could not be resolved, and you go looking for a networking problem that
is really a one-line config problem.

### What you will actually see in the console

After Step 5, browsing the `checkpoints` bucket:

```
checkpoints/                      <- the bucket (real)
  phase-3/                        <- prefix (drawn, not real)
    a1b2c3d4.../                  <- the job id (drawn)
      chk-7/
        _metadata
        <state files>
```

`chk-N` increments about every 10 seconds, because
`execution.checkpointing.interval: 10s`. A growing N is the proof that
checkpointing is working, and it is what Task 9's `--restore-from` will point at.


## Why the job reaches MinIO on `localhost:30014` and not an in-cluster address

Came up in Task 7, from the question "I configured a NodePort, why is it still
accessing from the external?". Appended verbatim. Only the heading depth was
changed.

Because **your Flink job is not in the cluster.** At Phase 3 it runs on your
laptop, in a plain JVM.

That is what a NodePort is for. It does not move anything inside. It opens a door
from outside, and your job is outside.

### Where the job actually runs

```bash
apps/gradlew -p apps :pipeline:run
```

That starts a `MiniCluster`, which is a whole Flink cluster (JobManager,
TaskManagers, state backend) running as threads inside one ordinary JVM process
on your host. There is no pod. `kubectl get pods` will never show it. This is
ADR 0001's dev loop: fast iteration without a deploy.

So the job is an external client of your `kind` cluster, exactly like `kcat` is.

### The chain your request actually takes

```
your JVM                      docker            kind node container        Kubernetes
--------                      ------            -------------------        ----------
PUT http://localhost:30014/checkpoints/...
      │
      └─> host port 30014
              │
              └─> extraPortMappings          (clusters/kind/*.yaml:46)
                     containerPort: 30014
                        │
                        └─> kube-proxy listening on 30014 on that node
                               │
                               └─> minio-s3-api Service (NodePort 30014)
                                      │
                                      └─> MinIO pod, 10.244.3.14:9000
```

Two hops are easy to miss:

1. **`kind` nodes are Docker containers.** A NodePort alone is not reachable from
   your host. It becomes reachable because `clusters/kind/` declares
   `extraPortMappings` for 30014, publishing the container's port to your machine.
   Without that line, `localhost:30014` would refuse the connection even with a
   perfectly healthy Service.
2. **kube-proxy never visits the ClusterIP.** It forwards straight to a pod
   endpoint. That mechanism is already written up in the Phase 3 knowledge doc's
   first section.

### You already accepted this for Kafka

```yaml
bootstrap-servers: localhost:30016   # Kafka external listener
s3.endpoint: http://localhost:30014  # MinIO S3 API
```

Identical reasoning, identical pattern. Both are NodePorts reserved at Phase 0,
both reached from a process on your host.

### What changes at Phase 5

The job becomes pods, scheduled by the Flink Kubernetes Operator. Then it is
**inside** the cluster, and it stops using the NodePort:

| | Phase 3 | Phase 5 |
|---|---|---|
| Job runs as | a JVM on your host | pods in the cluster |
| `s3.endpoint` | `http://localhost:30014` | the in-cluster Service, `http://minio.<namespace>.svc:80` style |
| Path | host port to NodePort to Service to pod | Service to pod, no NodePort |

The NodePort stays useful after Phase 5, but for you and for `kcat`, not for the
job.

**This is exactly why `s3.endpoint` is a line in `config.yaml` rather than a
constant in Java.** It is the value that must differ between the two
environments. At Phase 5 you copy the YAML into `spec.flinkConfiguration` and
change this one line. No Java is touched, and the same jar runs in both places.


## Why `pipeline.generic-types: false`

Came up in Task 7. Appended verbatim from the explanation given at the time.
Only the heading depth was changed, so the section nests under this document.

### The problem it prevents

Every record that crosses an operator boundary must become bytes. Shuffles across
a `keyBy`, values written to keyed state, everything in a checkpoint. Flink
chooses a serializer **per type**, once, while building the job graph.

If Flink recognises the type, it generates a fast, schema-aware serializer. If it
does not, it **silently falls back to Kryo**, a generic reflection-based
serializer.

Silently is the whole problem. There is no error. The job starts, runs, and
produces correct output. You find out later, and "later" is expensive:

| Cost | When you notice |
|---|---|
| Much slower serialization | when throughput does not match your parallelism |
| State that cannot be evolved | when you add a field to a record and a restore fails |
| Opaque checkpoint contents | when you try to read state with the State Processor API |

Setting `pipeline.generic-types: false` removes the fallback. Flink throws
instead:

```
UnsupportedOperationException: Generic types have been disabled in the
ExecutionConfig and type <X> is treated as a generic type.
```

A loud failure at job-graph build time, before a single record moves.

### Worked example with this project's own types

**`Click` passes.** It is a record of `String`, `String`, `Instant`, and an enum.
Flink recognises records as POJO types, and `java.time.Instant` is a first-class
Flink type. That last fact was an open question in the design and was confirmed
during Task 3. It only mattered *because* this flag is false. With the flag at
its default, `Instant` would have gone quietly to Kryo and nobody would have
checked.

**`ProductChange` failed, on purpose.** It was a sealed interface over
`PriceChange` and `StockChange`. An interface is not a POJO, so there was no
schema to generate from.

> **Resolved 2026-08-28 by
> [ADR 0008](../adr/0008-product-change-as-a-state-snapshot.md).** Phase 4 did
> not add a custom `TypeInformation`, and did not split the stream into two
> typed branches. It removed the sum type: `ProductChange` is now one record
> carrying `price`, `previousPrice`, `stock`, and `previousStock`. The
> mechanism described below is still exactly why the old model could not work,
> which is why it is kept.

```
default (generic-types: true)     ->  Kryo, silently. Job runs. Problem hidden.
this project (false)              ->  build fails, naming ProductChange.
```

Phase 4 is the phase that reads `product-change`, so Phase 4 is where this fires.
`status.md` already records it as budgeted work needing either a custom
`TypeInformation` or a split into two typed branches. That entry exists
**because** the flag is false. Otherwise Phase 4 would have shipped with Kryo and
the problem would have surfaced in Phase 5's HA Drill instead.

### Why it belongs to this project in particular

`:domain` declares zero dependencies. Not Kafka, not Flink. `CLAUDE.md` says that
emptiness "is what keeps them valid Flink POJO types".

But a rule nobody enforces is a comment. `pipeline.generic-types: false` is the
enforcement. It turns "please keep the records POJO-compatible" into a build
failure the moment someone breaks it.

It also protects Task 9 directly. That Drill compares an uninterrupted run
against one restored from a checkpoint, line for line. Kryo-serialized state is
the kind that fails or shifts across a restore, and the Drill would fail for a
reason unrelated to checkpointing.

### Why it moved into `config.yaml`

It was in `PersonalizationJob` as
`flinkConfig.set(PipelineOptions.GENERIC_TYPES, false)`. It is a Flink setting,
so it now sits with the other Flink settings, and it travels to
`spec.flinkConfiguration` at Phase 5 unchanged.

That move is also the second reason the assert after loading matters. If
`config.yaml` fails to load, you lose this line too, and the Kryo fallback
becomes silent again exactly when you are least likely to look for it.


## Why `FileSystem.initialize(flinkConfig, null)` is required, and how it fails without it

Came up in Task 7, and it is the finding most likely to bite again, because the
error names something that does not exist on this machine.

### The failure

Credentials were set on the `Configuration` handed to
`getExecutionEnvironment(...)`. The job still died at startup:

```
java.nio.file.AccessDeniedException: s3://checkpoints/phase-3/<job-id>/shared:
  org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: No AWS Credentials
  provided by DynamicTemporaryAWSCredentialsProvider
  TemporaryAWSCredentialsProvider SimpleAWSCredentialsProvider
  EnvironmentVariableCredentialsProvider IAMInstanceCredentialsProvider :
  Unable to load AWS credentials from environment variables
  (AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY ...)
```

Read that literally and the fix is `export AWS_ACCESS_KEY_ID`. That does work,
and it is the wrong fix. It leaves the config-driven design half broken, and it
puts credentials in a second place nobody is looking.

### Why it happens

Two configuration surfaces exist, and they are not connected.

```
Configuration flinkConfig
        │
        ├──> getExecutionEnvironment(flinkConfig)
        │       the JOB's config: state backend, checkpoint interval,
        │       checkpoint directory, generic types
        │
        └──> FileSystem.initialize(flinkConfig, null)
                the PROCESS-WIDE static registry: which factory handles
                which scheme, and how that factory is configured
```

`org.apache.flink.core.fs.FileSystem` holds a **static** map of scheme to
filesystem, populated once and shared by everything in the JVM. The job's
`Configuration` never reaches it. So `s3.access-key` was being set on an object
the S3 factory does not read.

With no credentials from Flink, `S3AFileSystem.bindAWSClient` fell back to
Hadoop's default provider chain, and every provider in that chain reported
nothing. The chain's last resort is EC2 instance metadata, which is why AWS
appears in an error on a laptop with no AWS account.

Trace it in the stack, bottom-up:

```
CheckpointCoordinator.<init>
  FsCheckpointStorageAccess.<init>
    Path.getFileSystem
      FileSystem.get                     <- the static registry
        AbstractS3FileSystemFactory.create
          S3AFileSystem.initialize
            bindAWSClient                <- no credentials here
```

### The fix, and where it goes

```java
flinkConfig.setString("s3.access-key", Env.require("MINIO_ACCESS_KEY"));
flinkConfig.setString("s3.secret-key", Env.require("MINIO_SECRET_KEY"));

FileSystem.initialize(flinkConfig, null);
```

After the credentials are on the `Configuration`, and before anything touches an
S3 path.

**Pass the `PluginManager`, even as `null`.** Both overloads are public in
`flink-core-2.2.0.jar`, but `initialize(Configuration)` is **deprecated**, and
javac only says so under `-Xlint:deprecation`. Gradle's default output is the
unhelpful `uses or overrides a deprecated API`, naming no line. A `null`
plugin manager means "no plugin isolation", which is exactly right under
`MiniCluster`, where there is no plugins directory to isolate from.

### The Phase 5 caveat

This is a `MiniCluster` requirement, not a universal one. A real Flink cluster's
entrypoint initializes the filesystems itself, from the `config.yaml` the
operator rendered, before any user code runs. Whether this call should be
guarded or removed when running under the Flink Kubernetes Operator is an open
Phase 5 question.

### The general shape worth remembering

Anything reached through a **static registry** cannot be configured by an object
you pass to something else. Flink's `FileSystem` is the example here. The
symptom is always the same: a component falls back to its own defaults and
reports a failure in the vocabulary of those defaults, not of your
configuration.


## Why the transactional id prefix exists, and why it must be stable

Came up in Task 8, from the question "I don't understand why we need
transactional-id-prefix". Appended verbatim from the explanation given at the
time. Only the heading depth was changed.

### The problem, before any mechanism

Your job crashes at 18 seconds, halfway through writing a batch of
Recommendations.

Kafka does not know it crashed. From the broker's side a producer that stops
talking is indistinguishable from a producer that is slow. So the transaction it
opened stays **open**.

Those records are already physically in the log. They are just marked as
belonging to an uncommitted transaction.

Now a `read_committed` consumer arrives. It cannot skip past those records and
read the ones after them, because the open transaction might still commit, and
committed records must appear in offset order. So the consumer stops at the
**Last Stable Offset**, the offset of the earliest still-open transaction, and
waits.

That is the hang. Nothing is broken. The consumer is correctly refusing to guess.

### So somebody has to declare the crashed producer dead

Two ways that can happen:

1. **The timeout expires.** After `transaction.timeout.ms`, the coordinator
   aborts it on its own. Your value is 300,000, so that is five minutes of stall.
2. **A producer claims the same identity and cleans up.** Instant, if it can
   prove it is the same logical producer.

Option 2 needs a name that survives process death. A TCP connection cannot be it.
A process id cannot be it. It has to be a string you choose.

**That string is the transactional id.** When a producer calls
`initTransactions()` with a transactional id, the coordinator looks up that id,
bumps its **producer epoch**, and aborts anything the previous epoch left open.
The old producer, if it is somehow still alive, gets `ProducerFencedException` on
its next write and dies. That is fencing.

### Worked example, with your numbers

Three partitions, `transaction.timeout.ms=300000`, checkpoint interval 10s.

```
t=0s     job starts. Sink subtask 0 uses transactional id
         "personalization-phase-3-0-0", calls initTransactions()

t=10s    checkpoint 1 completes -> transaction commits -> records visible

t=15s    8 Recommendations written into a new transaction,
         landing at offsets 40..47, uncommitted

t=18s    kill -9

         kcat -X isolation.level=read_committed
         reads offsets ..39, then STOPS. Last Stable Offset = 40.
```

Now branch on the prefix.

**Stable prefix**, which is what you have:

```
t=25s    restart. Subtask 0 recomputes the SAME id
         "personalization-phase-3-0-0"
         initTransactions() -> coordinator bumps the epoch,
                               aborts the transaction at offsets 40..47
         LSO jumps past 47. kcat resumes immediately.
```

**Prefix with a UUID or timestamp in it:**

```
t=25s    restart. Subtask 0 computes
         "personalization-phase-3-9f3a1c-0-0"
         initTransactions() -> coordinator has never seen this id.
                               Nothing to fence. Nothing aborted.

         The orphan at offsets 40..47 stays open.
         LSO stays at 40.

t=318s   timeout finally expires (18s + 300s). Coordinator aborts it.
         kcat resumes, five minutes late.
```

Same job, same code, same data. The only difference is whether the id was
reproducible.

### Why a *prefix* rather than an id

Because the sink runs at parallelism 16, not 1. If all 16 subtasks shared one
transactional id, each `initTransactions()` would bump the epoch and fence the
other 15. The job would fence itself to death on startup.

So each subtask needs its own id, and Flink builds them:

```
personalization-phase-3 - 0 - 0
└─ your prefix ───────┘  │   └─ counter
                         └─ subtask index
```

You own the prefix. Flink appends the parts it can derive **deterministically**
from the subtask index. Determinism is the whole trick: after a restart, subtask
0 is still subtask 0, so it recomputes the same id and reclaims its own orphans,
not somebody else's.

### Why you supply it instead of Flink defaulting

The prefix has two requirements that pull in opposite directions:

| Requirement | Rules out |
|---|---|
| Identical across restarts of **this** job | anything random or time-based |
| Different from every other job on this cluster | a fixed library default |

Flink cannot satisfy the second on your behalf. If two jobs shared a default
prefix, job B's subtask 0 would fence job A's subtask 0, and job A would die with
`ProducerFencedException` for no visible reason.

So it is a namespace you own. Pick one name per job, write it down, and never
generate it.

### The connection back to `transaction.timeout.ms`

These two settings are the same failure seen from two ends. Fencing is the fast
path out of a stall, and the timeout is the slow path. Five minutes was a
reasonable ceiling **because** fencing normally makes it irrelevant. If you ever
find yourself waiting out that timeout, the first thing to check is whether the
prefix changed between runs.


## Enabling transactions in Kafka: four layers, not one

Came up in Task 8, from the question "how to enable transactions in kafka?
through setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)". Appended verbatim
from the explanation given at the time. Only the heading depth was changed.

`setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)` is the switch that makes
Flink *use* transactions. It is necessary and not sufficient.

```
4. Flink sink        setDeliveryGuarantee(EXACTLY_ONCE)
                     setTransactionalIdPrefix(...)
                            │
3. Flink job         checkpointing enabled, in EXACTLY_ONCE mode
                            │   (what drives begin/commit)
                            │
2. Kafka producer    transactional.id  -> implies idempotence
                            │
1. Kafka broker      transaction coordinator + __transaction_state topic
```

### Layer 1: the broker

Nothing to enable. The transaction coordinator has been built in since Kafka
0.11, and it keeps its state in an internal topic, `__transaction_state`.

What you *do* configure is that topic's durability, and your cluster already
does, in `spec.kafka.config`:

```yaml
transaction.state.log.replication.factor: 3
transaction.state.log.min.isr: 2
```

Those two lines are the broker-side "enabling" in practice. Leave them at a
replication factor of 1 and a broker loss takes the transaction log with it, so
fencing and commit state are gone.

### Layer 2: the producer

A plain Kafka client enables transactions by setting `transactional.id` and then
driving the lifecycle by hand:

```java
producer.initTransactions();     // claim the id, fence the previous epoch
producer.beginTransaction();
producer.send(...);
producer.commitTransaction();    // or abortTransaction()
```

Setting `transactional.id` also **implies idempotence**. The client turns on
`enable.idempotence=true`, forces `acks=all`, and caps in-flight requests,
because transactions cannot be correct without them.

You never write any of this. Flink does it for you, which is the point of layer
4.

### Layer 3: checkpointing

This is the one people forget, and it is the reason the sink cannot be enabled in
isolation.

**Flink commits a transaction when a checkpoint completes.** That is the only
commit trigger. With checkpointing disabled there is nothing to commit on, and
records stay invisible to `read_committed` consumers forever.

You have this from Task 7:

```yaml
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
```

Both matter. An exactly-once sink over at-least-once checkpointing is not
exactly-once, because the checkpoint barrier alignment that makes the snapshot
consistent is what the commit is anchored to.

### Layer 4: the two Flink sink calls

`setDeliveryGuarantee` alone is rejected. From the builder's own bytecode:

```
"EXACTLY_ONCE delivery guarantee requires a transactionalIdPrefix to be set
 to provide unique transaction names across multiple KafkaSinks..."
```

So the builder fails fast rather than silently degrading, which is the good
design. The other validations it carries:

| Check | Message |
|---|---|
| prefix present | the one above |
| prefix length | "The configured prefix is too long and the resulting transactionalIdPrefix might exceed Kafka's ... size" |
| producer config | "Overwriting the '{}' is not recommended" for keys Flink manages itself |

That last one is why you set `transaction.timeout.ms` through `setProperty` and
**not** `key.serializer` or `value.serializer`. Flink owns the serializers,
because your `RecommendationSerializationSchema` already produces `byte[]`.

### The short answer

```java
.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)   // use transactions
.setTransactionalIdPrefix(prefix)                       // required, or build() throws
.setProperty("transaction.timeout.ms", "300000")        // required here, broker caps at 900000
```

plus checkpointing enabled in `EXACTLY_ONCE` mode, which you already have. Three
of those four you wrote in Step 5. The fourth was Task 7.


## The watermark stall: why default parallelism made the job look dead

Came up in Task 4, and it cost the longest single detour in the phase. It is the
failure the plan warned about, arriving exactly as predicted and still taking
time to recognise.

### The symptom

The job ran. Kafka records were consumed. No `SessionSignal` was ever emitted.
Nothing errored, nothing warned, no window ever fired. A healthy-looking process
producing nothing.

### The mechanism

Flink tracks a watermark **per split**, meaning per Kafka partition, and the
watermark that reaches a downstream operator is the **minimum** across all of
them.

```
clickstream has 3 partitions.
Default parallelism is 16, so the source has 16 subtasks.

subtask 0  -> partition 0  -> watermark advancing
subtask 1  -> partition 1  -> watermark advancing
subtask 2  -> partition 2  -> watermark advancing
subtask 3  -> (no partition assigned)  -> watermark Long.MIN_VALUE
...
subtask 15 -> (no partition assigned)  -> watermark Long.MIN_VALUE

effective watermark = min(all 16) = Long.MIN_VALUE, forever
```

Thirteen subtasks had no partition to read, so they never observed an event time,
so they never emitted a watermark above `Long.MIN_VALUE`. The minimum never moved.
Event-time windows fire when the watermark passes their end, so no window ever
fired.

Nothing about this is an error. Every component behaved correctly.

### The fix, and the fix that was rejected

```java
.withIdleness(config.watermarkIdleness())
```

`withIdleness` marks a source subtask **idle** after a period with no records.
An idle subtask is excluded from the minimum instead of pinning it, so the
watermark advances on the three subtasks that actually have data.

**Pinning parallelism to 3 would also have worked, and was rejected.** Phase 6
varies parallelism deliberately as its whole subject. A fix that only holds while
parallelism equals the partition count is a fix that Phase 6 removes.

### The general shape

Any time a job consumes fewer partitions than it has source subtasks, this is
waiting. It applies to a topic with one partition and parallelism 2. The
diagnostic question is always the same: **is the effective watermark being held
by a subtask that has nothing to read?**

If a windowed job emits nothing, check this before anything else. There is no
error message pointing at it.


## Facts the spec could not know in advance

Collected for Task 10, from work done across Tasks 0 to 9. Each of these was
either an open question in the design or something that cost real time.

| Question the spec left open | Answer, and how it was established |
|---|---|
| Is `java.time.Instant` a first-class Flink type? | **Yes.** Confirmed in Task 3. The domain records need no change, and `Click` serialises as a POJO. This mattered only because `pipeline.generic-types: false` would have turned a negative answer into a loud build failure rather than a silent Kryo fallback |
| Which `flink-connector-kafka` pairs with Flink 2.2? | **`5.0.0-2.2`.** The connector has its own version line, `<connector>-<flink-minor>`, separate from Flink's |
| Flink 2.x checkpoint config key names | `execution.checkpointing.dir`, `execution.checkpointing.mode`, `execution.checkpointing.externalized-checkpoint-retention`, `execution.state-recovery.path`. All four verified with `javap` against `flink-core-2.2.0.jar`, after a documentation search wrongly suggested two of them did not exist |
| Broker `transaction.max.timeout.ms` | **900000**, with synonym `DEFAULT_CONFIG`, so nothing overrides Kafka's 15 minute default. Flink's own default is `Duration.ofHours(1)`, four times that ceiling, so leaving `transaction.timeout.ms` unset makes the producer refuse to start. Chose **300000** |
| Does the watermark stall on idle partitions? | **Yes**, see the section above |
| Observed Browsing Session close rate | The spec predicted about one every 4 seconds at 10 Shoppers and 5 Clicks per second. Observed in Task 6: ten `SessionSignal` lines between `16:13:45.322` and `16:14:26.522`, which is 41.2 seconds, **4.12 seconds per Session**. The derivation held |

### Things that cost more than fifteen minutes

1. **The watermark stall.** Section above.
2. **The lz4 capability clash.** Flink 2.2's runtime and its own Kafka connector
   pull two different lz4 modules declaring the same Gradle capability. A hard
   error, resolved with `capabilitiesResolution` in `build.gradle`.
3. **`flink-connector-base` is not transitive.** Flink bundles it in
   `flink-dist`, so nothing pulls it in for a `MiniCluster` run.
4. **`flink-s3-fs-native` is not published to Maven Central at any version.** See
   [ADR 0008](../adr/0007-s3-filesystem-plugin.md).
5. **`FileSystem.initialize` is required and its failure names AWS.** See the
   section above on the static registry.
6. **A checkpoint directory without `_metadata` is not a checkpoint.** Restoring
   from one fails with `FileNotFoundException`, and a bucket listing cannot
   distinguish it from a good one. Read `Completed checkpoint N` from the log.
