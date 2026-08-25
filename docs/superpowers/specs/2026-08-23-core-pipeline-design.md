# Core pipeline design (Phase 3)

Date: 2026-08-23
Status: approved (design), not yet implemented
Derives from: [implementation phases](../plans/2026-08-10-implementation-phases.md),
[the Flink job walkthrough](../../knowledge/flink-job-walkthrough.md),
[ADR 0001](../../adr/0001-minicluster-first-dev-loop.md),
[ADR 0003](../../adr/0003-interval-join-key-and-semantics.md)

## Goal

Build the Shopper-keyed branch of the pipeline, running on `MiniCluster` against
the Kafka and MinIO already in the `kind` cluster. Read `Click` events from
`clickstream`, group them into Browsing Sessions, and publish a `Recommendation`
per closed Browsing Session to the `recommendation` topic.

Two Drills prove it works:

1. Hard-kill the job mid-stream and restart from the last checkpoint. The output
   is identical to an uninterrupted run.
2. Inject a `Click` beyond the watermark bound. It lands in the Late Click side
   output instead of being dropped.

The Product-keyed branch, broadcast state, CEP, and async I/O all belong to
Phase 4. See "Out of scope" at the end.

## Decisions

### Flink 2.x (2.2), not the 1.20 LTS line

The newest line. Java records are recognised as POJO types, so the Phase 2
domain records need no change. In-place rescaling (Phase 6) and the adaptive
scheduler (Phase 5) are both present.

The cost is that most Flink material written before 2025 does not apply. The
2.0 line removed `org.apache.flink.streaming.api.windowing.time.Time`, removed
the legacy `SourceFunction` and `SinkFunction`, and renamed a group of
checkpointing configuration keys.

The Phase 5 risk this raised has since been checked and cleared. `v2_2` is a
current, undeprecated value of the Flink Kubernetes Operator's `FlinkVersion`
enumeration, with `v2_3` and `v2_4` beyond it. See
[Resolved risks](#resolved-risks) for the evidence and for the one detail left
to Phase 5.

Rejected: **Flink 1.20 LTS.** Conservative, better documented, and every
existing example compiles. Rejected because the project exists to learn the
current shape of the technology, and the version cost is paid once.

### A shared `:domain` Gradle module

Both the generator and the pipeline need the same four records. The pipeline
also needs the two `JsonCodec` directions the generator never had:
`Click fromJson(byte[])` and `toJson(Recommendation)`.

Two facts drove this.

**Copies cannot be checked against each other.** The wire format is JSON, and
`JsonCodec` is hand-written. Add a field to `Click`, and a second copy of the
record simply ignores it. No compile error, no runtime error, a null appears
downstream weeks later.

**`Recommendation` has no correct home today.** It sits in
`generator/domain/` as dead code, because the generator never produces one. The
pipeline is its only real consumer. Phase 3 is the moment that stops being
theoretical.

`:domain` declares **no dependencies at all**. Not Kafka, not Flink. This is the
rule that keeps the records usable as Flink POJO types: a Flink annotation or a
`TypeInformation` field in `Click` becomes a compile error instead of a
code-review argument.

Rejected: **a standalone `pipeline/` build with copied records.** Zero
restructuring, and Phase 2 stays untouched. Rejected on the drift argument
above.

Rejected: **`:pipeline` depends on `:generator`.** Cheapest path to a single
definition, no package rename. Rejected because the Flink job would depend on
the synthetic data producer, and would carry `kafka-clients` and the factory
classes into the Phase 5 fat jar.

### The `KeyedProcessFunction` sits downstream of the window

The phase plan asks for session windows **and** keyed state in a
`KeyedProcessFunction`, without saying what the second operator is for. A
session window already holds per-Shopper state, so the second operator has to
hold something a window structurally cannot.

A window's state dies when the window fires. A recommendation cooldown, meaning
"do not recommend this Shopper the same Product two Browsing Sessions running",
must survive window firing. That is the state the `KeyedProcessFunction` holds.

The decisive argument is Phase 4. ADR 0003 and the walkthrough both put the
branch merge in a `KeyedCoProcessFunction` keyed by `shopperId`. That is the
same operator, in the same position, holding the same state. Phase 4 changes the
class and this Phase 3 body becomes `processElement1`.

Rejected: **hand-written session logic in a `KeyedProcessFunction`, no window.**
Teaches event-time timers thoroughly. Rejected because `.sideOutputLateData(...)`
exists only on a `WindowedStream`, so the Late Click Drill would need
hand-written lateness comparison too.

Rejected: **the `KeyedProcessFunction` upstream of the window.** The state it
would hold is bookkeeping a stateless filter mostly covers, and it leaves
nothing in place for the Phase 4 merge.

### Phase 3 publishes a real `Recommendation`, not a new Signal type

A Browsing Session aggregate is a Signal by the `CONTEXT.md` definition, so
publishing it to a new `signal` topic would be the literal reading of the
glossary. Phase 3 publishes a `Recommendation` to the `recommendation` topic
instead.

Three reasons. The phase plan and Phase 1 declare exactly four topics, and a
fifth contradicts a signed-off phase. Phase 5's HA Drill requires "no gap in the
recommendation topic", so that sink must already be proven before Phase 5 leans
on it. And `Recommendation` already carries a `reason` field, which exists to
say which branch produced the record.

The record a Shopper-keyed branch can honestly produce:

| Field | Phase 3 value |
|---|---|
| `shopperId` | from the Browsing Session |
| `productId` | most-clicked Product in the Browsing Session |
| `discountPercent` | `0.0`, because no Promo Rule has been evaluated yet |
| `reason` | `"most-viewed-in-session"` |
| `generatedAt` | the window end, **not** wall-clock time |

`discountPercent` staying zero is not an oversight. Phase 4 adds broadcast Promo
Rules and fills it in.

### `EXACTLY_ONCE` delivery, not at-least-once

The phase's own done-when says the output after recovery is identical to an
uninterrupted run. On restart Flink rewinds the Kafka source to the checkpoint's
offsets and reprocesses. With at-least-once, everything written after that
checkpoint is written a second time, so "identical" is false by construction.

`KafkaSink` with `DeliveryGuarantee.EXACTLY_ONCE`, a stable transactional id
prefix, and checkpointing in `EXACTLY_ONCE` mode.

Two consequences that look like bugs and are not:

- Records become visible only when a checkpoint commits the transaction. At a 10
  second checkpoint interval the `recommendation` topic is silent for 10
  seconds, then emits a burst.
- Any consumer that verifies the output must set
  `isolation.level=read_committed`. Otherwise it sees records from aborted
  transactions, including the orphaned ones a restart is meant to fence away.

Rejected: **at-least-once with a weakened criterion.** Simpler, output visible
continuously, no transaction timeout to tune. Rejected because it does not meet
the criterion the plan already states.

### Session gap 6 seconds, derived not chosen

The session gap is not a free parameter. It has to be set against the
generator's actual per-Shopper Click rate, or no window ever fires and the job
looks broken while behaving correctly.

`Catalog.SHOPPER_IDS` holds 10 Shoppers. The default rate is 5 Clicks per
second, uniformly distributed, so each Shopper produces 0.5 Clicks per second
and the mean gap between one Shopper's Clicks is 2 seconds. A Browsing Session
closes only when a gap exceeds the session gap, with probability
`e^(-0.5 x gap)`:

| Session gap | Chance one gap is long enough | Browsing Sessions closing, all 10 Shoppers |
|---|---|---|
| 6s | 5.0% | about one every 4 seconds |
| 10s | 0.67% | about one every 30 seconds |
| 30s | 0.00003% | never. The job emits nothing |

6 seconds it is, which is also the value the walkthrough's worked example uses.

### Nothing in the output derives from wall-clock time

This rule is what makes the restart Drill possible, and breaking it fails the
Drill in a way that looks like a checkpointing bug.

If `generatedAt` were `Instant.now()`, run 1 writes a record stamped 14:02:11 and
run 2 replays the same `Click` and stamps it 14:07:43. Every other field
matches. A comparison reports every line as changed, permanently, and never
tells you anything about recovery.

The cooldown timer follows the same rule. A processing-time timer fires relative
to wall clock, so run 2 would suppress a different set of Recommendations than
run 1. The outputs genuinely differ, and correct checkpointing cannot fix it.

So `generatedAt` is the window end, and every timer is an event-time timer.

### `pipeline.generic-types: false`

Kryo does not fail. It runs, more slowly, and it writes state into checkpoints
that is fragile across schema changes. A silent Kryo fallback in Phase 3 becomes
a checkpoint that cannot be evolved in Phase 6 or 7.

Setting `pipeline.generic-types: false` makes any type Flink cannot handle with
a real serializer throw at job-graph construction. The failure names the exact
field, in the first seconds of the first run.

This matters because all four domain records carry a `java.time.Instant` field,
and the Flink 2.2 documentation consulted for this design does not state
explicitly whether `Instant` has a built-in serializer. Rather than assume,
the first run answers it. If `Instant` does fall back, the fix is one
`pipeline.serialization-config` entry, not a change to the domain records.

**A consequence for Phase 4, recorded here because nothing else records it.**
`ProductChange` is a sealed interface. A sealed interface is not a POJO by
Flink's rules, and with generic types disabled it cannot silently become a Kryo
type either. Phase 4 reads the `product-change` topic and will hit this at once.
The likely resolutions are a custom `TypeInformation` or splitting the stream
into two typed branches at deserialization. Phase 3 does not solve it. Phase 4
must budget for it.

### MinIO gets a NodePort Service, tracked in git

`MiniCluster` runs on the host. `svc/minio`, the S3 API, is cluster-internal
only, so `s3://` checkpoint writes cannot reach it. `README.md` predicted this
exact moment and named the path to take.

`clusters/kind/kind-cluster.yaml` already maps host ports 30014 and 30015,
labelled `# MinIO`, on the `zone-a` worker. Phase 0 reserved them. So no cluster
recreation is needed, which was the expensive part.

A `Service` of type `NodePort` on port 30014, committed under `manifests/minio/`
and picked up by the existing `minio-tenant` Application. No new ArgoCD
Application, the same way `kafka-topics.yaml` joined the existing
`strimzi-kafka-cluster` Application.

Two values come from the live cluster, not from this document. The selector
labels and the target port both come from reading the Operator's own Service:
`kubectl get svc minio -n minio-tenant -o yaml`.

Rejected: **`kubectl port-forward` per session.** Zero manifests, nothing added
to the cluster. Rejected because it dies with the terminal and becomes an
undocumented prerequisite for every Phase 3 and Phase 4 Drill.

## Architecture

```
KafkaSource<Click>                       topic: clickstream, 3 partitions
        |  Click                         startingOffsets: earliest (explicit)
        v
assignTimestampsAndWatermarks            bound 5s, ts = Click.eventTime
        |  Click        <-- assigned ONCE here, before any keyBy
        v
    keyBy(Click::shopperId)
        |
        v
window(EventTimeSessionWindows.withGap(Duration.ofSeconds(6)))
  .sideOutputLateData(LATE_CLICKS) ---------> DataStream<Click> --> print("LATE")
        |
        v
  ProcessWindowFunction
        |  SessionSignal
        v
    keyBy(SessionSignal::shopperId)
        |
        v
  RecommendationDecider extends KeyedProcessFunction
        |  ValueState<String> lastRecommendedProduct
        |  event-time timer clears it
        |  Recommendation
        v
KafkaSink<Recommendation>                topic: recommendation, EXACTLY_ONCE
```

### Watermarks

`WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))`, timestamp
from `Click.eventTime`. A 5 second bound against the generator's 2 second
default skew leaves headroom, so nothing becomes a Late Click by accident.

The walkthrough describes the watermark as one shared number. Inside
`KafkaSource` that is not exactly true. Flink tracks a watermark per partition,
and the operator's effective watermark is the **minimum** across all three
partitions of `clickstream`. A partition with no traffic freezes the whole job's
watermark and no window fires.

At 5 Clicks per second keyed by `shopperId` across 10 Shoppers, all three
partitions stay busy. This is the first thing to check if windows stop firing.
`withIdleness(...)` is the lever.

### Flink 2.x API note

`org.apache.flink.streaming.api.windowing.time.Time` was removed in the 2.0
line. `withGap` takes a `java.time.Duration`. Examples written before 2025 use
`Time.seconds(6)` and do not compile.

## Components

### Module layout

```
apps/                          the whole Gradle build, out of the repo root
  settings.gradle              include ':domain', ':generator', ':pipeline'
  gradlew, gradle/             wrapper, moved up from generator/
  domain/
    build.gradle               no dependencies
    src/main/java/lab/personalization/domain/
        Click, ActionType, ProductChange, PriceChange, StockChange,
        PromoRule, Recommendation, SessionSignal, JsonCodec
  generator/
    build.gradle               + implementation project(':domain')
    src/main/java/lab/personalization/generator/
        Generator, GeneratorConfig, SkewedEventStream,
        SkewedPublisher, factory/
  pipeline/
    build.gradle               Flink 2.2
    src/main/java/lab/personalization/pipeline/
        PersonalizationJob, PipelineConfig,
        ClickDeserializationSchema, RecommendationSerializationSchema,
        SessionAggregator, RecommendationDecider
```

The build lives under `apps/` so the repo root separates the Java build from
`manifests/`, `clusters/`, and `scripts/`. Gradle resolves its build root from
the working directory, so commands are `apps/gradlew -p apps :generator:run`.

All three modules share one layout, `src/main/java` plus a
`lab.personalization.*` package. No `sourceSets` overrides and no shortened
package names: `:domain` and `:pipeline` look exactly like `:generator`.

Eight files move into `:domain`. The package changes from
`lab.personalization.generator.domain` to `lab.personalization.domain`. That
rewrites 14 import lines across 5 generator files: `JsonCodec` (5), `Generator`
(3), `ProductChangeFactory` (3), `ClickFactory` (2), `PromoRuleFactory` (1).

`SessionSignal` is new and lives in `:domain`, because `CONTEXT.md` defines
Signal as domain vocabulary and Phase 4 adds more of them. It has no
dependencies, so it does not break the `:domain` rule.

### `:pipeline` dependencies

| Artifact | Scope | Why | Doc |
|---|---|---|---|
| `flink-streaming-java` | `compileOnly` + `runtimeOnly` | API. A real cluster provides it in Phase 5 | [configuration/overview][d-overview] |
| `flink-clients` | `compileOnly` + `runtimeOnly` | Required to run from a `main()` | [configuration/overview][d-overview] |
| `flink-statebackend-rocksdb` | `compileOnly` + `runtimeOnly` | Documented as not on the default classpath | [state_backends][d-backends] |
| `flink-connector-kafka` | `implementation` | A connector. Bundled into the fat jar | [configuration/maven][d-maven] |
| `flink-s3-fs-hadoop` | `runtimeOnly` | Discovered through `ServiceLoader`, never referenced from Java, so it must not be on the compile classpath. `flink-s3-fs-native` was evaluated and rejected: it is not published to Maven Central at all, see [ADR 0007](../../adr/0007-s3-filesystem-plugin.md) | [ADR 0007](../../adr/0007-s3-filesystem-plugin.md) |
| `project(':domain')` | `implementation` | | |

The packaging rule that produces the `compileOnly` plus `runtimeOnly` split is
stated in [configuration/overview][d-overview]: API and runtime modules are
provided by Flink and must not be included in the job uber jar, while
connectors, formats, and third-party libraries should be.

`compileOnly` plus `runtimeOnly` is the Gradle equivalent of Maven's `provided`:
on the classpath locally, absent from the fat jar Phase 5 ships. Note that
`generator/build.gradle` deliberately removed its provided-versus-bundled split
when it stopped being a Flink program. The pipeline brings that split back.

### Record types

```java
public record SessionSignal(
        String shopperId,
        Instant windowStart,
        Instant windowEnd,
        int clickCount,
        String topProductId) {}
```

`topProductId` is the most-clicked Product in the Browsing Session. Ties break
by lowest `productId`, so the result never depends on iteration order.

### `RecommendationDecider`

```
processElement(signal):
    if signal.topProductId equals lastRecommendedProduct  -> emit nothing
    else                                                  -> emit Recommendation
                                                             lastRecommendedProduct = topProductId
                                                             register event-time timer at windowEnd + 60s

onTimer():                                                -> lastRecommendedProduct.clear()
```

Three mechanisms in one small operator: `ValueState`, an event-time timer, and
explicit state clearing. All three are what RocksDB and the checkpoints actually
hold, so the restart Drill exercises real state rather than an empty backend.

### Late Clicks in Phase 3

The side output goes to a `print()` with a `LATE` prefix. That is enough for the
Drill, which checks one specific injected `Click`. Phase 8 decides whether it
becomes a metric or its own topic, because that phase owns the dashboard that
consumes the count.

## Configuration

Two surfaces, split the way production splits them. **Flink settings are data in
a `config.yaml`. Job settings are `--key=value` flags. Credentials are
environment variables and appear in neither.**

### Why Flink settings are not flags

Revised 2026-08-24, replacing an earlier decision to carry `--s3-endpoint`,
`--checkpoint-dir` and `--checkpoint-interval-seconds` as flags.

Production Flink on Kubernetes puts every Flink configuration key on the
`FlinkDeployment` CR under `spec.flinkConfiguration`, and the Flink Kubernetes
Operator renders that map into `config.yaml` inside the pod. The job jar carries
no environment knowledge. The same jar runs in dev and in production, and only
the data changes.

Phase 3 does the same thing one layer down:

```
Phase 3                              Phase 5
-------                              -------
apps/pipeline/conf/config.yaml       spec.flinkConfiguration: (identical keys)
        |                                    |
        v                                    v
GlobalConfiguration                  operator renders config.yaml
   .loadConfiguration(dir)                   |
        |                                    v
        v                            Flink process loads it
getExecutionEnvironment(config)      getExecutionEnvironment()
```

The keys are identical in both columns. Phase 5 copies the YAML body into
`spec.flinkConfiguration`, deletes the local file, and changes no Java.

**Verified against the 2.2.0 jar on 2026-08-24**, because the documentation does
not cover this use. `javap -p` on
`org/apache/flink/configuration/GlobalConfiguration.class` from
`flink-core-2.2.0.jar` shows `public static Configuration loadConfiguration()`,
`loadConfiguration(String)`, and `loadConfiguration(String, Configuration)`,
with a private `loadYAMLResource(File)` behind them. The class's string
constants are `config.yaml` and `FLINK_CONF_DIR`. So a `MiniCluster` run reads
the same filename, from the same environment variable, with the same parser as a
real Flink process. Nested YAML is flattened into dotted keys, and `isSensitive`
masks credentials when the loaded config is logged.

A second reason, independent of fidelity to production. A `Configuration` passed
to `getExecutionEnvironment(...)` is job level and **takes precedence over
cluster config**. Hardcoding `s3.endpoint` in Java would silently override
whatever Phase 5 sets in `spec.flinkConfiguration`, and the CR would appear to be
ignored.

### `apps/pipeline/conf/config.yaml`

Not under `src/main/resources`. Anything in `resources` is baked into the jar,
and the whole point is that the jar knows nothing about the environment.

```yaml
state.backend.type: rocksdb
execution.checkpointing.interval: 10s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.incremental: true
execution.checkpointing.num-retained: 3
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
execution.checkpointing.dir: s3://checkpoints/phase-3
pipeline.generic-types: false

s3.endpoint: http://localhost:30014
s3.path.style.access: true
```

Two of these override Flink defaults, and both were added on 2026-08-25.

**`num-retained: 3`**, default `1`. With the default, the coordinator deletes
`chk-N` the moment `chk-N+1` completes. Task 9 Step 4 kills the job while you are
reading its log for a checkpoint number, so the default leaves a race: the number
you noted can be gone before Step 6 uses it, and the resulting path error looks
like a retention bug. Confirmed working, job `70c305...` retained
`chk-6, chk-7, chk-8` where earlier runs retained one.

**`incremental: true`**, default `false`. Uploads only changed SST files per
checkpoint rather than the full state, which is the reason to run RocksDB at all.
**Effect not yet demonstrated.** The verification run reported 917,783 bytes for
checkpoints 1 through 4 unchanged, because it ran with no generator and a state
that was not changing. The key parses; the benefit should appear under real load.

### The line for what belongs here

A setting belongs in `config.yaml` if it is a Flink runtime concern that would
travel to `spec.flinkConfiguration` unchanged at Phase 5. What that rule keeps
out:

| Kept out | Why |
|---|---|
| `parallelism.default` | Phase 6 varies it on purpose. Task 4's watermark stall was fixed with `withIdleness` rather than by pinning parallelism, for this reason |
| `--restore-from` | Already writes `execution.state-recovery.path`, but stays a flag because it is per-run. Phase 5 sets it per deploy via `spec.job.initialSavepointPath` |
| bootstrap servers, topics, consumer group, transactional id prefix | Not Flink configuration keys at all. They are arguments to the Kafka source and sink builders |
| watermark bound, session gap, cooldown | Domain parameters. The 6 second gap is *derived* from the generator's rate, and derived business values do not belong in a runtime config file |
| `restart-strategy.type` | Tempting for Drill determinism, but Phase 5's whole subject is failure recovery. Deciding it here would pre-empt that phase |

One thing is absent on purpose: **credentials**, which arrive from
`MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` and are set onto the loaded
`Configuration` in code. Everything else Flink-related lives here.
`pipeline.generic-types` moved out of `PersonalizationJob` for that reason, and
so did checkpoint consistency mode and externalized retention.

**Corrected 2026-08-24.** Those last two were briefly held as programmatic
setters on `env.getCheckpointConfig()`, on the claim that no config key existed.
That claim came from a documentation search returning nothing, which is not the
same as absence. `javap` on `CheckpointingOptions` in `flink-core-2.2.0.jar`
shows both:

```
CHECKPOINTING_CONSISTENCY_MODE     -> execution.checkpointing.mode
EXTERNALIZED_CHECKPOINT_RETENTION  -> execution.checkpointing.externalized-checkpoint-retention
```

Both verified live with a negative control, a deliberately invalid value:

```
IllegalArgumentException: Could not parse value 'NONSENSE' for key
  'execution.checkpointing.mode'. Expected one of: [[EXACTLY_ONCE, AT_LEAST_ONCE]]
```

### Job flags

`PipelineConfig` parses `--key=value` arguments with defaults for everything and
no CLI library, mirroring `GeneratorConfig` so both programs are driven the same
way. What remains are job settings and per-run switches, not Flink settings.

| Flag | Default | Notes |
|---|---|---|
| `--bootstrap-servers` | `localhost:30016` | The same external listener the generator uses |
| `--flink-conf-dir` | `conf` | Directory holding `config.yaml`. Explicit rather than relying on `FLINK_CONF_DIR`, so an unset variable cannot silently fall back |
| `--watermark-bound-seconds` | `5` | Above the generator's 2s default skew |
| `--session-gap-seconds` | `6` | Derived above, not chosen |
| `--cooldown-seconds` | `60` | Event time, not wall clock |
| `--consumer-group` | `personalization-phase-3` | |
| `--transactional-id-prefix` | `personalization-phase-3` | Must be stable across restarts |
| `--output-topic` | `recommendation` | A throwaway topic makes Drill A repeatable |
| `--bounded` | `false` | Drill mode. Pins the end offset at job start. **Pass it as `--bounded=true`**: `PipelineConfig.parse` requires `--key=value` for every flag and rejects a bare switch with `Expected --key=value` |
| `--restore-from` | none | A checkpoint path. Absent means start fresh. Written onto the loaded `Configuration` as `execution.state-recovery.path`, which is what `spec.job.initialSavepointPath` becomes at Phase 5 |

### Credentials come from the environment, never from a flag

```bash
export MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=...
apps/gradlew -p apps :pipeline:run --args="--bounded=true"
```

A `--secret-key=...` argument lands in shell history and is visible in `ps`
output to every process on the machine. This is the same no-durable-secrets
reasoning `README.md` already applies to the ArgoCD and MinIO secrets.

### Why the transactional id prefix must be stable

Recovery works because the restarted sink fences the dead producer's orphaned
transaction, and fencing is by transactional id. Change the prefix between runs
and the new producer fences nothing. The orphaned transaction stays open until
`transaction.timeout.ms` expires, blocking `read_committed` consumers from
advancing past it. A `kcat` capture then appears to hang.

### Flink configuration

| Key | Value | Confidence | Doc |
|---|---|---|---|
| `state.backend.type` | `rocksdb` | confident | [state_backends][d-backends] |
| `execution.checkpointing.interval` | `10s` | **confirmed 2026-08-24**, checkpointing is enabled by setting this above 0 | [config][d-cfg22] |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | confident | locate in the 2.2 checkpointing page before writing |
| `pipeline.generic-types` | `false` | confident | [types_serialization][d-types22] |
| `execution.checkpointing.dir` | `s3://checkpoints/phase-3` | **confirmed 2026-08-24**. `CheckpointingOptions.CHECKPOINTS_DIRECTORY`, which keeps `state.checkpoints.dir` and `state.backend.fs.checkpointdir` as deprecated keys | [CheckpointingOptions.java][d-ckopts] |
| `execution.checkpointing.externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` | **confirmed 2026-08-24 from the jar**, `CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION`. The enum is `ExternalizedCheckpointRetention` in `org.apache.flink.configuration`, replacing `ExternalizedCheckpointCleanup` | [checkpoints][d-ckdoc] |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | **confirmed 2026-08-24 from the jar**, `CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE`, typed as `org.apache.flink.core.execution.CheckpointingMode` | [checkpoints][d-ckdoc] |
| `execution.state-recovery.path` | value of `--restore-from` | **confirmed 2026-08-24**, this is the 2.x replacement for `execution.savepoint.path`. Source is the SQL client page, which is where the key is documented, not a DataStream page | [sql-client][d-recovery] |
| `s3.endpoint` | from `--s3-endpoint` | **confirmed 2026-08-24** | [s3][d-s3] |
| `s3.path.style.access` | `true` | **confirmed 2026-08-24 from the jar.** `S3FileSystemFactory` mirrors both spellings, so `s3.path-style-access` is equally valid. MinIO is reached by IP and port, so virtual-host addressing cannot work | [ADR 0007](../../adr/0007-s3-filesystem-plugin.md) |
| `s3.access-key` / `s3.secret-key` | from the environment | **confirmed 2026-08-24** | [flink-s3-fs-hadoop README][d-s3native] |


Rows reading "not yet located" are ones this design states from reasoning, not
from a page that was actually opened. Open the page before writing the key.
That distinction is the whole point of the column.

All rows were checked on 2026-08-24, every one against the jar rather than the
documentation. Every Flink setting lives in `config.yaml`. Nothing is configured
programmatically.

The equivalent Java setters exist and are worth knowing for reading other
people's code: `CheckpointConfig.setCheckpointingConsistencyMode(...)` and
`setExternalizedCheckpointRetention(...)`. Two traps there. `setCheckpointingMode`
is deprecated in favour of the first. And the enum it takes is
`org.apache.flink.core.execution.CheckpointingMode`, not the identically named
`org.apache.flink.streaming.api.CheckpointingMode`, while
`ExternalizedCheckpointRetention` sits in `org.apache.flink.configuration`
rather than alongside it.

`s3.path.style.access` produces the most confusing failure when wrong. Without
it the client addresses the bucket as `http://checkpoints.localhost:30014`,
which does not resolve, and the error mentions DNS rather than S3.

Retention on cancellation is not a nicety. Under the wrong policy a graceful
stop deletes the checkpoint that `--restore-from` was going to point at.

### Both open questions, answered by the run on 2026-08-24

Neither was settled by documentation. Both were left open deliberately rather
than asserted, and both are now closed.

**Does `loadConfiguration(dir)` throw or return empty when `config.yaml` is
missing?** Still unknown, and it no longer matters, because the assert is in
place and the happy path is proven. The default `conf` resolves correctly:
Gradle's `run` task uses the subproject directory as its working directory, so
the process starts in `apps/pipeline`. Confirmed by the checkpoint coordinator
reaching `s3://checkpoints/phase-3/<job-id>/shared`, a value that could only have
come from the file.

**Does `flink-s3-fs-hadoop` register from the classpath?** The plugins page warns
that S3 filesystems must be used as plugins in a distribution, because relocations
were removed and `lib/` placement fails. A `MiniCluster` run has neither `lib/`
nor `plugins/`, just one flat classpath, so there is nothing to conflict with, but
no document confirms it. The symptom if it does not is
`UnsupportedFileSystemSchemeException: Could not find a file system implementation
for scheme 's3'`. That message means the plugin did not load. It does not mean the
endpoint or the credentials are wrong.

## Verifying the done-when criterion

Both Drills get a runbook in `docs/runbooks/`, matching the Phase 0 pattern.
Both must end with **real pasted output**. The Phase 0 runbook still holds
predicted output, `status.md` says so, and that gap is not repeated here.

### Drill A: hard kill, restart from the last checkpoint

Three preconditions decide whether the comparison means anything.

**1. Stop the generator before run 1, and leave it stopped.** `--bounded` pins
the end offset at job start. A running generator means run 2 pins a larger end
offset, the two runs read different `Click` ranges, and the comparison is
meaningless again.

**2. Set starting offsets to `earliest` explicitly.** `KafkaSource` defaults to
the consumer group's committed offsets. Run 1 commits its progress, so run 2
would start where run 1 stopped and read nothing. This does not conflict with
`--restore-from`, because offsets in a restored checkpoint take priority over
the initializer. That priority is exactly what recovery depends on.

**3. The output topic must be empty at the start.** Phase 3 is the first ever
writer to `recommendation`, so this holds the first time. Use `--output-topic`
with a throwaway name to repeat the Drill. Topic auto-creation is on, confirmed
in Phase 1 when `smoke-test` worked without a `KafkaTopic`, and this is the same
disposable pattern as `smoke-test` and `drill-check`. Do not delete and recreate
the real `recommendation` topic. ArgoCD manages it, and that would be deliberate
drift.

**The check, with no offset arithmetic.** Run 1 writes set X. Run 2 writes set
Y. The topic holds X plus Y. If recovery is correct then Y equals X, so every
distinct line appears exactly twice.

```bash
kcat -C -b localhost:30016 -t recommendation -o beginning -e \
     -X isolation.level=read_committed \
  | sort | uniq -c | awk '$1 != 2'
```

| Output | Meaning |
|---|---|
| nothing | recovery correct |
| lines with count 3 | the sink duplicated. Not exactly-once. The transaction did not roll back |
| lines with count 1 | a Recommendation was produced once and not the other time. Keyed state lost, or a window never fired |

`isolation.level=read_committed` is not optional. Without it `kcat` shows
records from aborted transactions, including the orphans the restart fenced, and
reports failures that are not real.

**Procedure.**

```bash
# 1. Stage a backlog, then STOP the generator.
apps/gradlew -p apps :generator:run --args="--click-rate=200"     # about 2 minutes, then Ctrl-C

# 2. Record the backlog size. Both runs must read this exact range.
kcat -L -b localhost:30016 -t clickstream

# 3. Run 1, clean, to completion.
apps/gradlew -p apps :pipeline:run --args="--bounded=true"

# 4. Run 2. Watch the log for at least one COMPLETED checkpoint,
#    then hard-kill. SIGKILL, not Ctrl-C.
apps/gradlew -p apps :pipeline:run --args="--bounded=true"
pkill -9 -f lab.personalization.pipeline

# 5. Find the retained checkpoint. The job log names it. The MinIO
#    console under checkpoints/phase-3 confirms it.

# 6. Resume.
apps/gradlew -p apps :pipeline:run --args="--bounded=true --restore-from=s3://checkpoints/phase-3/<job-id>/chk-N"

# 7. The check.
kcat -C -b localhost:30016 -t recommendation -o beginning -e \
     -X isolation.level=read_committed | sort | uniq -c | awk '$1 != 2'
```

**Why a hard kill, not `Ctrl-C`.** `SIGINT` is catchable, so it runs the
cooperative shutdown path. That path aborts the in-flight transaction itself and
consults the retention policy, which under the wrong setting deletes the
checkpoint. `SIGKILL` runs nothing. The transaction is left open on the broker,
the checkpoint survives, and on restart the sink fences the dead producer and
aborts the orphan. That fencing step is the entire exactly-once mechanism, and a
graceful cancel never reaches it. Phase 5's real failures, an evicted pod, a
drained node, an OOMKill, are all `SIGKILL`.

**Step 4 is the one to get right.** Killing before any checkpoint completes
leaves nothing to restore from, and step 6 fails with a path error rather than
teaching anything.

### Drill B: inject a Click beyond the bound

**Why `shopper-99`.** `Catalog.SHOPPER_IDS` holds `shopper-1` through
`shopper-10`, so the generator can never produce that id. Anything carrying it
came from the injection and nothing else.

**The arithmetic, before running it.** Bound 5s, session gap 6s. A `Click` at
event time `t` opens a window ending at `t + 6`. It is a Late Click when
`maxSeen - 5 > t + 6`, that is when `t < maxSeen - 11`. An event time 60 seconds
in the past clears that by a wide margin.

```bash
# 1. Generator and job both running normally, no --bounded.
#    Confirm Recommendations are actually flowing first.

# 2. Inject. Event time 60 seconds ago, ISO-8601 UTC, the format
#    JsonCodec already emits.
echo '{"shopperId":"shopper-99","productId":"P1","eventTime":"'"$(date -u -d '60 seconds ago' +%Y-%m-%dT%H:%M:%SZ)"'","actionType":"VIEW"}' \
  | kcat -P -b localhost:30016 -t clickstream
```

| Check | Expectation |
|---|---|
| job stdout | one `LATE` line carrying `shopper-99` |
| `recommendation` topic | no record for `shopper-99`, ever |

The second half is what proves the point. A `LATE` line alone shows the side
output fired. The absence of a Recommendation shows the `Click` did not also
reach a window. The done-when says the `Click` must land in the side output
"rather than being silently dropped", and only both halves together distinguish
routed-to-side-output from processed-normally.

### Phase 2 regression check

The module restructure touches a signed-off phase. Phase 3 is not done until the
Phase 2 verification has been re-run: `apps/gradlew -p apps :generator:run` plus `kcat -C`
against `clickstream`, `product-change`, and `promo-rule`, with real messages
observed. Not "it compiled".

## Risks

| Risk | Impact | Handling |
|---|---|---|
| ~~The Flink Kubernetes Operator may not support Flink 2.2~~ **Resolved 2026-08-23** | Phase 5 blocked, or a downgrade after Phases 3 and 4 are written | Cleared. `v2_2` is a current value of the Operator's `FlinkVersion` enum, undeprecated, with `v2_3` and `v2_4` beyond it. See [Resolved risks](#resolved-risks) |
| Three renamed Flink 2.x configuration keys | The job fails to start, or silently ignores a setting | Verify each against the 2.2 documentation at implementation |
| `flink-connector-kafka` version pairing | Dependency resolution failure | Verify. It has its own version line, not Flink's |
| `transaction.timeout.ms` against the broker's `transaction.max.timeout.ms` | The producer refuses to start, or transactions abort mid-run and lose data | Read the Strimzi broker's configured maximum. Set the Flink value under it and above the checkpoint interval |
| A bounded run finishes before it can be killed | Drill A cannot be performed | Bigger backlog first. Shorter checkpoint interval second. An artificial throttle only if both fall short |
| Silent Kryo fallback | Fragile checkpoint state, discovered several phases later | `pipeline.generic-types: false`. It fails loudly instead |
| An idle Kafka partition freezes the watermark | No window fires, the job looks dead | Documented diagnosis. `withIdleness(...)` is the lever |
| Phase 2 regression from the restructure | A signed-off phase quietly broken | The regression check above |

## Resolved risks

### Flink Kubernetes Operator support for Flink 2.2 (resolved 2026-08-23, Task 0)

**Cleared.** `spec.flinkVersion` on a `FlinkDeployment` accepts only values of
the Operator's `FlinkVersion` enumeration, and that enumeration currently reads:

| Value | Note |
|---|---|
| `v1_13`, `v1_14` | no longer supported since the 1.7 operator release |
| `v1_15` | deprecated since 1.10 |
| `v1_16` | deprecated since 1.11 |
| `v1_17`, `v1_18` | deprecated since 1.13 |
| `v1_19`, `v1_20` | current |
| `v2_0`, `v2_1`, **`v2_2`**, `v2_3`, `v2_4` | current |

Source:
<https://github.com/apache/flink-kubernetes-operator/blob/main/docs/content/docs/custom-resource/reference.md>

`v2_2` carries no deprecation marker and sits two versions inside the range, so
Flink 2.2 is a target the Operator currently supports rather than a version it
merely tolerates.

**One thing left open, and it belongs to Phase 5.** The page above is from the
`main` branch, not a tagged release. Neither the compatibility page nor the
upgrade page publishes a per-release Flink matrix, so which released Operator
version first shipped `v2_2` is not established here. The deprecation notes
reference "the 1.13 operator release", which bounds `main` at 1.13 or later
without pinning it.

Phase 5 picks an Operator version for its ArgoCD Application. The check there is
one step: open that release's own `reference.md` and confirm `v2_2` appears in
its enum. Recorded here so Phase 5 does not have to rediscover the question.

## Out of scope for this design

| Not in Phase 3 | Owner |
|---|---|
| Product-keyed branch, interval join, Unmatched Click | Phase 4 |
| Broadcast state for Promo Rules | Phase 4 |
| CEP, async I/O, the `connect` merge | Phase 4 |
| Assertion tests | Phase 4, per its own done-when |
| Any `FlinkDeployment`, containers, the S3 plugin directory | Phase 5 |
| JobManager HA, savepoints, zone spread | Phase 5 |
| Late Click as a metric or its own topic | Phase 8 |

## Documentation this phase owes

| File | Why |
|---|---|
| `docs/runbooks/phase-3-checkpoint-restart-drill.md` | Drill A, with real pasted output |
| `docs/runbooks/phase-3-late-click-drill.md` | Drill B, same standard |
| `docs/knowledge/phase-3-core-pipeline.md` | One knowledge doc per phase, matching Phases 0 and 1 |
| `docs/superpowers/plans/status.md` | Updated as work lands |
| `README.md` | Build commands become `:generator:run`, and the MinIO section's "no external NodePort" line is now wrong |

## Sources

Pages actually opened while writing this design. Claims not backed by one of
these are reasoning, and are marked as such in place.

[d-types22]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/fault-tolerance/serialization/types_serialization.md
[d-backends]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/ops/state/state_backends.md
[d-s3]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/deployment/filesystems/s3.md
[d-s3native]: https://github.com/apache/flink/blob/release-2.2.0/flink-filesystems/flink-s3-fs-hadoop/README.md
[d-cfg22]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/deployment/config.md
[d-ckopts]: https://github.com/apache/flink/blob/release-2.2.0/flink-core/src/main/java/org/apache/flink/configuration/CheckpointingOptions.java
[d-ckdoc]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/ops/state/checkpoints.md
[d-recovery]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/sql/interfaces/sql-client.md
[d-maven]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/configuration/maven.md
[d-overview]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/configuration/overview.md
[d-typesmaster]: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/serialization/types_serialization
[d-typesapi]: https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/common/typeinfo/Types.html

| Claim | Page |
|---|---|
| Java records are recognised as POJO types since Flink 1.19, needing only to be public and instantiated by their canonical constructor. Types not recognised become `GenericType` and are serialized with Kryo | [types_serialization, master][d-typesmaster] |
| `Types.POJO(...)` supports Java records | [Types javadoc][d-typesapi] |
| `pipeline.generic-types: false` disables Kryo as a fallback for generic types | [types_serialization, 2.2][d-types22] |
| `flink-statebackend-rocksdb` is a `provided`-scope dependency and is not on the default classpath | [state_backends, 2.2][d-backends] |
| `flink-connector-kafka` carries its own version line, not Flink's | [configuration/maven, 2.2][d-maven] |
| `flink-clients` is required to run a job by executing the main class. API and runtime modules stay out of the uber jar, connectors go in | [configuration/overview, 2.2][d-overview] |

**Not found, and therefore not asserted:** whether `java.time.Instant` has a
built-in Flink serializer. The 2.2 serialization page consulted does not say.
`pipeline.generic-types: false` is what turns that open question into a
first-run failure rather than a silent Kryo fallback.
