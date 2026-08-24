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


