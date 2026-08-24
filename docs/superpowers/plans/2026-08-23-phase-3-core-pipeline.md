# Phase 3 core pipeline implementation plan

**Goal:** Build the Shopper-keyed branch on `MiniCluster`, reading `Click`
events from `clickstream` and publishing a `Recommendation` per closed Browsing
Session to `recommendation`, with RocksDB state checkpointed to MinIO.

**Architecture:** A root Gradle multi-project build with `:domain`,
`:generator`, and `:pipeline`. The job assigns watermarks once on the raw
stream, keys by `shopperId`, groups Clicks into session windows, routes Late
Clicks to a side output, and decides a Recommendation in a
`KeyedProcessFunction` whose keyed state outlives any single window.

**Tech Stack:** Flink 2.2 (DataStream API), `flink-connector-kafka`,
`flink-statebackend-rocksdb`, `flink-s3-fs-native`, Gradle multi-project,
Java 21 records, Strimzi Kafka, MinIO.

**Spec:** [core pipeline design](../specs/2026-08-23-core-pipeline-design.md)

## How to use this plan

This project's [CLAUDE.md](../../../CLAUDE.md) working agreement governs, and it
overrides the default shape of a plan document.

**You write every file and run every command.** Each task below gives the goal,
the concept behind it, the failure mode to watch for, a skeleton or a signature,
and the command that proves it worked. It deliberately does not give finished
implementations. Work done for you is learning lost.

**A task is done when its verification command produces real output you have
read.** Not when it compiles. Not when a pod is `Ready`. This is the same
standard Phase 1 applied to the external listener.

**This plan prescribes no commits.** Git is yours to drive, at whatever
granularity suits you, so no task ends with a `git commit` step.

Worth knowing while you do: `.gitignore` excludes `docs/`, `CLAUDE.md`,
`CONTEXT.md`, and `.claude/`, and that is deliberate. Runbooks, the knowledge
doc, and `status.md` are still written, and are still what "done" means for the
tasks that produce them. They simply never reach git.

Ask why something failed and you get the line and the mechanism, not a corrected
file.

## Progress

| # | Task | Status |
|---|---|---|
| 0 | Confirm the Flink Operator supports Flink 2.2 | done 2026-08-23, gate cleared |
| 1 | Root multi-project build with `:domain` | done 2026-08-23, Phase 2 regression passed |
| 2 | Expose MinIO's S3 API on NodePort 30014 | done 2026-08-23, `curl` returned 200 |
| 3 | `:pipeline` module reading Clicks from Kafka | done 2026-08-23, Clicks printing, Instant confirmed |
| 4 | Watermarks, session windows, and `SessionSignal` | done 2026-08-23, after a watermark stall |
| 5 | Late Click side output, and Drill B | done 2026-08-24, `LATE` line observed |
| 6 | `RecommendationDecider` | done 2026-08-24, suppression observed |
| 7 | RocksDB and checkpoints to MinIO | **next** |
| 8 | Exactly-once Kafka sink | not started |
| 9 | Bounded mode, restore, and Drill A | not started |
| 10 | Knowledge doc, README, and status | not started |

## Global constraints

Copied verbatim from the spec. Every task inherits these.

- Flink **2.2**. `org.apache.flink.streaming.api.windowing.time.Time` does not
  exist; windowing takes `java.time.Duration`.
- `:domain` declares **no dependencies at all**. Not Kafka, not Flink.
- `pipeline.generic-types: false`. Any Kryo fallback must fail loudly.
- **Nothing in the output derives from wall-clock time.** `generatedAt` is the
  window end. Every timer is an event-time timer.
- Watermark bound **5s**, session gap **6s**, checkpoint interval **10s**,
  cooldown **60s** of event time.
- Delivery guarantee `EXACTLY_ONCE`, with a **stable** transactional id prefix.
- Credentials come from environment variables, never from a `--flag`.
- Any consumer verifying output sets `isolation.level=read_committed`.
- Ubiquitous language from [CONTEXT.md](../../../CONTEXT.md): Shopper, Click,
  Browsing Session, Product, Recommendation, Late Click, Signal, Drill.

---

## Task 0: Confirm the Flink Kubernetes Operator supports Flink 2.2

**Status: done, 2026-08-23. Gate cleared, Task 1 is unblocked.**

**Files:** none. This is research, and its output is a decision.

**Why this is first.** The spec's top risk. If the Operator does not support
Flink 2.2, every later task writes code against a version Phase 5 cannot deploy.
Ten minutes now, or two phases rewritten later.

- [x] **Step 1: Find the Operator's supported-version matrix.**

The Operator versions independently of Flink and publishes which Flink versions
each release supports. Look for the compatibility or supported-versions section
of the Flink Kubernetes Operator documentation for its current release.

**Result.** There is no per-release compatibility matrix. The actual mechanism
is the `FlinkVersion` enumeration, whose values are what `spec.flinkVersion` on
a `FlinkDeployment` will accept:

| Value | Note in the reference |
|---|---|
| `v1_13`, `v1_14` | no longer supported since the 1.7 operator release |
| `v1_15` | deprecated since 1.10 |
| `v1_16` | deprecated since 1.11 |
| `v1_17`, `v1_18` | deprecated since 1.13 |
| `v1_19`, `v1_20` | current |
| `v2_0`, `v2_1`, **`v2_2`**, `v2_3`, `v2_4` | current |

Source:
<https://github.com/apache/flink-kubernetes-operator/blob/main/docs/content/docs/custom-resource/reference.md>

`v2_2` is undeprecated and sits two versions inside the range, so Flink 2.2 is
a version the Operator currently targets.

- [x] **Step 2: Record the answer in the spec's risk table.**

Done in `docs/superpowers/specs/2026-08-23-core-pipeline-design.md`, in three
places, because one edit would have left the document contradicting itself:

| Location | Change |
|---|---|
| The Flink 2.x decision | The "not yet confirmed" paragraph replaced. It contradicted the finding |
| The risk table row | Struck through and marked resolved, pointing at the evidence |
| New `## Resolved risks` section | The enum table, the source URL, and the Phase 5 follow-up |

**Gate.** If Flink 2.2 is **not** supported, stop and reopen the version
decision before Task 1. Do not carry on and hope.

**Gate result: passed.** Flink 2.2 is supported. Task 1 proceeds.

**One thing deliberately left open, and it belongs to Phase 5.** Which
*released* Operator version first shipped `v2_2` is not established. The page
read is from the `main` branch, not a tag, and no per-release Flink matrix is
published. The deprecation notes reference "the 1.13 operator release", which
bounds `main` at 1.13 or later without pinning it. Phase 5 picks an Operator
version for its ArgoCD Application, and the check there is one step: open that
release's own `reference.md` and confirm `v2_2` is in its enum.

No commit step. Task 0's whole output is edits to the design and to this plan,
and planning documents are not committed on their own here.

---

## Task 1: Root multi-project build with `:domain`

**Files:**
- Create: `apps/settings.gradle`, `apps/domain/build.gradle`
- Move: `generator/gradlew`, `generator/gradlew.bat`, `generator/gradle/` up into `apps/`
- Move: 8 files from `apps/generator/src/main/java/lab/personalization/generator/domain/` and `apps/generator/src/main/java/lab/personalization/generator/JsonCodec.java` into `apps/domain/src/main/java/lab/personalization/domain/`
- Modify: `apps/generator/build.gradle`, `generator/settings.gradle` (delete it), and 5 generator source files
- Modify: `README.md`

**Interfaces produced:** package `lab.personalization.domain` containing
`Click`, `ActionType`, `ProductChange`, `PriceChange`, `StockChange`,
`PromoRule`, `Recommendation`, `JsonCodec`.

**The concept.** A Gradle multi-project build has one `settings.gradle` at the
root naming the modules, and one `build.gradle` per module. The root owns the
wrapper. `generator/settings.gradle` currently declares `generator` as its own
root project, so it must go, or Gradle sees two competing roots.

**The failure mode to watch for.** Moving Java files without changing their
`package` line compiles fine in some setups and then fails at runtime with
`NoClassDefFoundError`, because the directory says one thing and the file says
another. Change the `package` line in all 8 moved files, and the `import` lines
in the 5 files that reference them.

- [x] **Step 1: Create the root `settings.gradle`.**

```groovy
rootProject.name = 'flink-k8s-personalization-lab'

include 'domain'
include 'generator'
```

`:pipeline` is added in Task 3, not now. Keep each task's change reviewable.

**`include` requires the directory to already exist.** Gradle 9.7 fails
configuration with "Configuring project ':domain' without an existing directory
is not allowed", before it ever looks for a build file. So `mkdir apps/domain`
first. A missing `build.gradle` inside an existing directory is fine, a missing
directory is not. Verified by hitting it on 2026-08-23.

**Create only `apps/domain`, not the source chain under it.** Git does not track
empty directories, so a pre-made
`apps/domain/src/main/java/lab/personalization/domain/` would not survive a
fresh clone. That path appears on its own in step 4, when the files move into
it, and it then matches `:generator` exactly.

- [x] **Step 2: Delete `generator/settings.gradle` and move the wrapper up.**

`gradlew`, `gradlew.bat`, and the whole `gradle/` directory move from
`generator/` to the repo root. Use `git mv` so the history stays attached.

Checkpoint before going further:

```bash
apps/gradlew -p apps projects
```

Expected: `:domain` and `:generator` both listed under the root project.

- [x] **Step 3: Write `apps/domain/build.gradle`.**

The whole file is the `java` plugin and the toolchain. **No `dependencies`
block at all.** That absence is the design constraint, not an omission. Copy the
`java { toolchain { ... } }` shape from `apps/generator/build.gradle`.

Checkpoint: `apps/gradlew -p apps projects` lists `:domain` and `:generator`.

- [x] **Step 4: Move the 8 files and fix their `package` lines.**

Target directory: `apps/domain/src/main/java/lab/personalization/domain/`.
Same layout as `:generator`, no shortened package and no `sourceSets` override.
New package: `lab.personalization.domain`.

- [x] **Step 5: Fix the 14 import lines in the generator.**

| File | Import lines to change |
|---|---|
| `JsonCodec.java` | moved in step 4, fix its own imports |
| `Generator.java` | 3 |
| `factory/ProductChangeFactory.java` | 3 |
| `factory/ClickFactory.java` | 2 |
| `factory/PromoRuleFactory.java` | 1 |

`lab.personalization.generator.domain` becomes `lab.personalization.domain`.

**The count was right, the composition was not.** Actual result on 2026-08-23:

| File | Change |
|---|---|
| `Generator.java` | 3 imports repointed |
| `factory/ProductChangeFactory.java` | 3 repointed |
| `factory/ClickFactory.java` | 2 repointed |
| `factory/PromoRuleFactory.java` | 1 repointed |
| `JsonCodec.java` | 5 imports **deleted**, now same-package after the move |

**And one the plan missed entirely.** `Generator.java` calls `JsonCodec::toJson`
with **no import**, because the two shared the `lab.personalization.generator`
package. Moving `JsonCodec` into `:domain` breaks that, so
`import lab.personalization.domain.JsonCodec;` has to be **added**. Nothing in
the import count predicts this: it is a reference that was previously invisible
because it needed no import line. Step 7's compile is what finds it.

- [x] **Step 6: Add the dependency in `apps/generator/build.gradle`.**

```groovy
dependencies {
    implementation project(':domain')
    // ... existing kafka-clients and log4j entries stay
}
```

- [x] **Step 7: Compile.**

Run: `apps/gradlew -p apps :generator:build`
Expected: `BUILD SUCCESSFUL`. A compile failure here names the exact file whose
import you missed.

Observed 2026-08-23:

```
BUILD SUCCESSFUL in 4s
11 actionable tasks: 11 executed
```

Confirms the `JsonCodec` import added in step 5 was the only invisible
same-package reference in the module.

- [x] **Step 8: The Phase 2 regression check. This is the real gate.** Passed
2026-08-23: all three topics returned their expected records after the
restructure.

Phase 2 is signed off. This task put it back in play. Compiling is not proof.

Terminal 1:
```bash
apps/gradlew -p apps :generator:run
```

Terminals 2, 3, 4:
```bash
kcat -C -b localhost:30016 -t clickstream
kcat -C -b localhost:30016 -t product-change
kcat -C -b localhost:30016 -t promo-rule
```

Expected: a new `clickstream` line roughly every 200ms, `product-change` about
once a second, `promo-rule` about every 30 seconds. Each `product-change` line
carries a `"type"` of `"PRICE"` or `"STOCK"`.

- [ ] **Step 9: Update `README.md`.**

The "Running the generator" section says `cd generator` then `apps/gradlew -p apps run`.
It is now `apps/gradlew -p apps :generator:run` from the repo root.

---

## Task 2: Expose MinIO's S3 API on NodePort 30014

**Files:**
- Create: `manifests/minio/s3-nodeport.yaml`
- Modify: `README.md`

**The concept.** `MiniCluster` runs on your host. `svc/minio` is a `ClusterIP`
Service, reachable only from inside the cluster, so `s3://` writes cannot get
to it. A `NodePort` Service opens a port on every node, and
`clusters/kind/kind-cluster.yaml` already forwards host port 30014 to the
`zone-a` worker.

**The failure mode to watch for.** A `NodePort` Service with the wrong
`selector` is created successfully, reports no error, and routes to nothing.
`curl` then hangs or refuses the connection. Read the selector off the
Operator's own Service rather than guessing it.

- [x] **Step 1: Read the existing Service.**

```bash
kubectl get svc minio -n minio-tenant -o yaml
```

Note two things: the `spec.selector` map, and the `targetPort` behind port 80.

- [x] **Step 2: Write the Service manifest.** Created as `minio-s3-api`,
naming the thing rather than the audience.

Skeleton. Fill the two values from step 1.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: minio-s3-external
  namespace: minio-tenant
spec:
  type: NodePort
  selector:
    # from step 1
  ports:
    - name: http-minio
      port: 80
      targetPort: # from step 1
      nodePort: 30014
```

A comment block at the top saying why this exists is the house style. Look at
`manifests/minio/tenant.yaml` for the tone.

- [x] **Step 3: Sync.** ArgoCD reported Synced and Healthy.

The existing `minio-tenant` Application picks this up. No new ArgoCD
Application, the same way `kafka-topics.yaml` joined the existing
`strimzi-kafka-cluster` Application. `selfHeal` is off, so sync manually.

- [x] **Step 4: Prove it from outside the cluster.** Passed 2026-08-23:
endpoints `10.244.3.14:9000`, and `curl` returned `HTTP/1.1 200 OK`.

A `Ready` Service is not proof. Phase 1's standard applies.

```bash
curl -i http://localhost:30014/minio/health/live
```

Expected: `HTTP/1.1 200 OK`. A hang or a connection refusal means the selector
or the target port is wrong, not that the port mapping is missing.

- [ ] **Step 5: Correct `README.md`.**

The MinIO section currently says "No external NodePort configured, reachable
only from inside the cluster." That is now false. Replace it with the port, what
it serves, and why Phase 3 needed it.

---

## Task 3: `:pipeline` module reading Clicks from Kafka

**Files:**
- Create: `pipeline/build.gradle`
- Create: `pipeline/src/main/java/lab/personalization/pipeline/PipelineConfig.java`
- Create: `pipeline/src/main/java/lab/personalization/pipeline/ClickDeserializationSchema.java`
- Create: `pipeline/src/main/java/lab/personalization/pipeline/PersonalizationJob.java`
- Modify: `settings.gradle`, `domain/.../JsonCodec.java`

**Interfaces consumed:** `lab.personalization.domain.Click` from Task 1.

**Interfaces produced:**
```java
static Click JsonCodec.fromJson(byte[] bytes)
record PipelineConfig(String bootstrapServers, String consumerGroup, ...)
    static PipelineConfig parse(String[] args)
class ClickDeserializationSchema implements DeserializationSchema<Click>
```

**Why this task stops at printing.** It answers three questions at once, and
each has a distinct failure signature: do the Flink 2.2 dependencies resolve, do
your hand-written records survive Flink's type system, and does the external
listener work from inside a Flink source. Adding windowing before knowing those
would make one failure look like another.

**The concept: `DeserializationSchema` is an adapter, not a parser.** The
parsing lives in `JsonCodec.fromJson` in `:domain`, which knows nothing about
Flink. The schema class is a thin wrapper so Flink can call it. This is what
keeps `:domain` dependency-free.

**The failure mode to watch for.** `pipeline.generic-types: false` makes the job
throw at graph construction if any type falls back to Kryo. The spec flags
`java.time.Instant` as the open question. **If this task throws on `Instant`,
that is the design working as intended**, not a defect. The fix is one
`pipeline.serialization-config` entry, not a change to `Click`.

- [x] **Step 1: Add `:pipeline` to `apps/settings.gradle`.** `mkdir apps/pipeline`
first, same reason as Task 1.

- [x] **Step 2: Write `apps/pipeline/build.gradle`.**

Dependency scopes from the spec's Components section. The `compileOnly` plus
`runtimeOnly` pairing is Gradle's equivalent of Maven `provided`: present when
you run locally, absent from the fat jar Phase 5 ships.

```groovy
dependencies {
    implementation project(':domain')

    // API and runtime: a real cluster provides these in Phase 5
    compileOnly  "org.apache.flink:flink-streaming-java:${flinkVersion}"
    runtimeOnly  "org.apache.flink:flink-streaming-java:${flinkVersion}"
    // same pairing for flink-clients and flink-statebackend-rocksdb

    // Connectors and filesystems: bundled
    implementation "org.apache.flink:flink-connector-kafka:${kafkaConnectorVersion}"
    implementation "org.apache.flink:flink-s3-fs-native:${flinkVersion}"
}
```

`kafkaConnectorVersion` is **not** Flink's version. The connector has its own
line. Resolved from Maven Central on 2026-08-23: **`5.0.0-2.2`**. The `4.0.x-2.0`
versions are the Flink 2.0 line and `3.4.0-1.20` the last 1.x line.

**Two deviations from this step as originally written, both deliberate.**

`flink-s3-fs-native` was left out. Nothing before Task 7 writes a checkpoint,
and it drags in a large Hadoop tree that can pull a second SLF4J binding onto
the classpath. This task exists to keep three failure signatures
distinguishable, so an unused heavy dependency works against it. Add it in Task
7.

A `resolutionStrategy.capabilitiesResolution` block was added, because
`flink-runtime:2.2.0` and `kafka-clients:4.2.0` pull two different lz4 modules
that declare the same capability, which is a hard error rather than a version
conflict. See the knowledge doc for the full explanation.

**Check the report for `FAILED`, do not trust its exit code.**
`apps/gradlew -p apps :pipeline:dependencies --configuration runtimeClasspath`
prints `BUILD SUCCESSFUL` even when entries in the tree failed to resolve,
because the task only prints and never needs the classpath.

- [x] **Step 3: Add `fromJson` to `JsonCodec`.**

Signature: `public static Click fromJson(byte[] bytes)`.

Hand-written, matching the existing `toJson` style. `Instant.parse` reads back
what `Instant.toString()` wrote, so no formatter is needed. Decide now what an
unparseable line should do, and be explicit about it: throwing kills the job on
one bad record, returning null pushes the problem downstream. Write the choice
in a comment.

- [x] **Step 4: Write `PipelineConfig`.**

Same shape as `GeneratorConfig`: a record, `--key=value` parsing, package-private,
defaults for everything, no CLI library. For this task only three fields are
needed: `bootstrapServers`, `consumerGroup`, `inputTopic`. Later tasks add the
rest.

Name it `inputTopic`, not `topic`. Task 8 adds `outputTopic`, and a bare `topic`
alongside it reads as ambiguous at exactly the moment Task 9's Drill depends on
telling them apart.

- [x] **Step 5: Write `ClickDeserializationSchema`.**

Two methods carry weight:

```java
public Click deserialize(byte[] message)              // delegates to JsonCodec
public TypeInformation<Click> getProducedType()       // TypeInformation.of(Click.class)
```

`getProducedType` is the one people forget. Without it Flink cannot know the
stream's type and falls back to a generic type, which `pipeline.generic-types:
false` then rejects.

- [x] **Step 6: Write `PersonalizationJob` with a source and a print.**

```java
Configuration config = new Configuration();
config.setString("pipeline.generic-types", "false");   // verify the 2.x setter
StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(config);

KafkaSource<Click> source = KafkaSource.<Click>builder()
        .setBootstrapServers(...)
        .setTopics(...)
        .setGroupId(...)
        .setStartingOffsets(OffsetsInitializer.earliest())   // explicit, see Task 9
        .setValueOnlyDeserializer(new ClickDeserializationSchema())
        .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "clickstream")
   .print();

env.execute("personalization-phase-3");
```

`WatermarkStrategy.noWatermarks()` here is a placeholder that Task 4 replaces.
Nothing in this task consumes event time yet.

**Verify the setter.** Flink 2.x prefers typed `ConfigOption` constants over
untyped string keys, and `setString` may be deprecated. Find the current form
once here, then use it for every config key in Tasks 7 and 8.

- [x] **Step 7: Add the `run` task and execute.**

```bash
apps/gradlew -p apps :generator:run          # terminal 1, leave it producing
apps/gradlew -p apps :pipeline:run           # terminal 2
```

Expected: `Click[shopperId=shopper-7, productId=P3, eventTime=..., actionType=VIEW]`
lines, roughly 5 per second.

Three distinct failures and what each means:

| Symptom | Cause |
|---|---|
| Dependency resolution fails on `flink-connector-kafka` | Wrong connector version line |
| Throws at startup naming a field type | A Kryo fallback. Expected if `Instant` is not first-class. Fix with `pipeline.serialization-config` |
| Runs, prints nothing, no error | Reaching Kafka but reading nothing. Check the topic name and that the generator is running |

**Observed 2026-08-23.** Clicks printed at the expected rate, 10 distinct
shoppers matching `Catalog.SHOPPER_IDS`. Three findings this step produced:

1. **`java.time.Instant` is a first-class Flink type. The open question is
   closed.** With `pipeline.generic-types: false` set, a Kryo fallback would
   have thrown at graph construction naming the field. It did not throw, and
   nothing logged a fallback. No `pipeline.serialization-config` entry is
   needed and the domain records stay untouched.
2. **`flink-connector-base` is required and nothing pulls it in.** The Kafka
   connector does not depend on it transitively, and Flink's docs note it "has
   been bundled in flink-dist since FLINK-30400", so a real cluster has it and
   MiniCluster run from Gradle does not. It fails at job construction, not at
   compile time: `NoClassDefFoundError:
   org/apache/flink/connector/base/source/reader/RecordEmitter`. Added with the
   same provided-style scope as the other runtime modules.
3. **`OffsetsInitializer.earliest()` replays the whole backlog.** The 75-second
   run printed 2.9 million Clicks with event times from a week earlier. Correct,
   and load-bearing for Task 9, but it means Task 4 computes session windows
   over a week of history before reaching live data. See Task 4's note on
   `--starting-offsets`.

- [x] **Step 8: Record the `Instant` answer.**

The spec says the first run answers whether `Instant` is a first-class Flink
type. Write the answer into the spec's Sources section, replacing the "Not
found, and therefore not asserted" paragraph with what actually happened.

---

## Task 4: Watermarks, session windows, and `SessionSignal`

**Files:**
- Create: `domain/src/main/java/lab/personalization/domain/SessionSignal.java`
- Create: `pipeline/src/main/java/lab/personalization/pipeline/SessionAggregator.java`
- Modify: `PersonalizationJob.java`, `PipelineConfig.java`

**Interfaces produced:**
```java
public record SessionSignal(String shopperId, Instant windowStart,
                            Instant windowEnd, int clickCount, String topProductId)
class SessionAggregator extends ProcessWindowFunction<Click, SessionSignal, String, TimeWindow>
```

**The concept, restated because it is the heart of the phase.** A watermark is
the job's own estimate of how far back in event time it has safely seen
everything. It is assigned **once, on the raw stream, before any `keyBy`**. If
each branch computed its own, the same Click could be judged on time by one
branch and late by another, decided only by which branch ran faster.

**The failure mode to watch for, and it is the most likely one in this whole
plan.** Flink tracks a watermark **per Kafka partition**, and the effective
watermark is the **minimum** across all three partitions of `clickstream`. One
idle partition freezes the watermark, no window ever fires, and the job looks
dead while behaving correctly. If nothing prints, check this before anything
else. `withIdleness(...)` is the lever.

- [x] **Step 1: Add `--watermark-bound-seconds` (5) and `--session-gap-seconds` (6) to `PipelineConfig`.**
Also added `--start-from-earliest` (default `true`), not in the original plan:
`earliest` replays the whole backlog, so a rate check needs a way to start live.

Both are derived values, not preferences. The spec's arithmetic: at 10 Shoppers
and 5 Clicks per second, a 6 second gap closes a Browsing Session about every 4
seconds, and a 30 second gap closes one about never.

- [x] **Step 2: Write `SessionSignal` in `:domain`.**

A Signal by the `CONTEXT.md` definition, so it is domain vocabulary. It has no
dependencies, so it does not break the `:domain` rule.

- [x] **Step 3: Replace `noWatermarks()`.**

```java
WatermarkStrategy.<Click>forBoundedOutOfOrderness(Duration.ofSeconds(bound))
        .withTimestampAssigner((click, recordTimestamp) ->
                click.eventTime().toEpochMilli())
```

The assigner reads the event time embedded in the `Click`, not the Kafka record
timestamp. Those differ by exactly the skew the generator injects, which is the
whole point of Phase 2's delayed-publish mechanism.

- [x] **Step 4: Key and window.**

**`WindowedStream` has no `print()`**, and the compile error names it exactly:
`symbol: method print() location: class WindowedStream<Click,String,TimeWindow>`.
`.window(...)` returns a `WindowedStream`, not a `DataStream`, and it offers
only `reduce`, `aggregate`, `process`, and `apply`. A window is a grouping that
has not been collapsed yet, so the API refuses to go downstream until you say
how each window becomes a single value. Only those four return a `DataStream`,
and only then does `print()` exist.

**`WindowedStream` has no `print()`.** `.window(...)` returns a
`WindowedStream`, not a `DataStream`, and it offers only `reduce`, `aggregate`,
`process`, and `apply`. A window is a grouping that has not been collapsed yet,
so the API refuses to go downstream until you say how each window becomes a
single value. The compile error names it exactly:
`symbol: method print() location: class WindowedStream<Click,String,TimeWindow>`.

The plan's throwaway `.reduce((a, b) -> b)` diagnostic was skipped; step 5's
`SessionAggregator` is the terminal, so the rate check happens in step 6 with
the real aggregator in place.

Also wired here: `--start-from-earliest`, which `PipelineConfig` parsed while
`setStartingOffsets` stayed hardcoded to `earliest()`. A flag that is accepted
and ignored is worse than one that does not exist: an unknown flag throws, an
ignored one silently replays the whole backlog and makes step 6's rate check
look like a windowing bug.

```java
.keyBy(Click::shopperId)
.window(EventTimeSessionWindows.withGap(Duration.ofSeconds(gap)))
```

`Duration`, not `Time`. `Time` was removed in the Flink 2.0 line, and every
example written before 2025 uses it.

- [x] **Step 5: Write `SessionAggregator`.**

A `ProcessWindowFunction` receives every Click in the window plus the window
metadata. Compute the most-clicked Product. **Break ties by lowest `productId`**,
so the result never depends on iteration order. A tie broken by iteration order
makes Task 9's Drill fail intermittently, and that is a miserable thing to debug.

- [x] **Step 6: Print `SessionSignal` and check the rate against the prediction.**

```bash
apps/gradlew -p apps :generator:run                                       # terminal 1
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"   # terminal 2
```

Expected: `SessionSignal` lines at roughly **one every 4 seconds**, after an
initial quiet period of about 50 seconds. A session spans ~40s of event time,
and the watermark trails the newest event time by the 5s bound while the window
extends 6s past the last Click.

---

## Task 5: Late Click side output, and Drill B

**Files:**
- Modify: `PersonalizationJob.java`
- Create: `docs/runbooks/phase-3-late-click-drill.md`

**The concept, and the distinction that matters.** Being behind the watermark is
not the same as being too late for a window. A Click one second behind the
watermark still opens or extends its own session window, because that window's
end boundary sits ahead of the watermark. A Click is a **Late Click** only when
its window has already fired.

With bound 5s and gap 6s: late when `maxSeen - 5 > t + 6`, that is when
`t < maxSeen - 11`.

**The failure mode to watch for.** `.sideOutputLateData(...)` must be called on
the `WindowedStream`, before the window function. Called elsewhere it either
does not compile or silently tags nothing, and Late Clicks return to Flink's
default of being dropped without trace.

- [x] **Step 1: Declare the `OutputTag`.**

```java
static final OutputTag<Click> LATE_CLICKS =
        new OutputTag<>("late-clicks", TypeInformation.of(Click.class)) {};
```

The trailing `{}` is not a typo. It creates an anonymous subclass so the generic
type survives erasure. Without it Flink cannot recover the type at runtime.

- [x] **Step 2: Attach it to the window and print the side output.**

```java
SingleOutputStreamOperator<SessionSignal> signals = clicks
        .keyBy(Click::shopperId)
        .window(EventTimeSessionWindows.withGap(...))
        .sideOutputLateData(LATE_CLICKS)
        .process(new SessionAggregator());

signals.getSideOutput(LATE_CLICKS).print("LATE");
```

- [x] **Step 3: Run Drill B.**

`shopper-99` is used because `Catalog.SHOPPER_IDS` holds only `shopper-1`
through `shopper-10`. The generator can never produce that id, so anything
carrying it came from your injection.

Generator and job both running normally. Confirm `SessionSignal` lines are
flowing first, then:

```bash
echo '{"shopperId":"shopper-99","productId":"P1","eventTime":"'"$(date -u -d '60 seconds ago' +%Y-%m-%dT%H:%M:%SZ)"'","actionType":"VIEW"}' \
  | kcat -P -b localhost:30016 -t clickstream
```

- [x] **Step 4: Check both halves.**

| Check | Expectation |
|---|---|
| Job stdout | one `LATE` line carrying `shopper-99` |
| `SessionSignal` output | **no** signal for `shopper-99`, ever |

The second half is what proves it. A `LATE` line alone shows the side output
fired. The absence of a signal shows the Click did not also reach a window. Only
both together distinguish routed-to-side-output from processed-normally.

- [x] **Step 5: Write the runbook, with real pasted output.**

`docs/runbooks/phase-3-late-click-drill.md`, following the shape of
`phase-0-control-plane-drill.md`: rationale per command, then an "Observed
result" section holding **actual terminal output**.

The Phase 0 runbook still holds predicted output, and `status.md` records that
as a known gap. Do not repeat it here.

---

## Task 6: `RecommendationDecider`

**Files:**
- Create: `pipeline/src/main/java/lab/personalization/pipeline/RecommendationDecider.java`
- Modify: `PersonalizationJob.java`, `PipelineConfig.java`

**Interfaces produced:**
```java
class RecommendationDecider
        extends KeyedProcessFunction<String, SessionSignal, Recommendation>
```

**Why this operator exists at all.** A session window already holds per-Shopper
state, so a second stateful operator has to hold something a window structurally
cannot. A window's state dies when the window fires. "Do not recommend this
Shopper the same Product two Browsing Sessions running" must survive that.

**Why it is worth building carefully.** Phase 4 changes this class to a
`KeyedCoProcessFunction` and this body becomes `processElement1`. It is the
merge point ADR 0003 describes, built one phase early.

**The failure mode to watch for.** A processing-time timer here silently breaks
Task 9's Drill. It fires relative to wall clock, so a replayed run suppresses a
different set of Recommendations than the original. The outputs genuinely
differ, and no amount of correct checkpointing fixes it. **Event-time timers
only.**

- [x] **Step 1: Add `--cooldown-seconds` (60) to `PipelineConfig`.**

- [x] **Step 2: Declare the state in `open`.**

```java
private transient ValueState<String> lastRecommendedProduct;

public void open(OpenContext ctx) {          // verify the 2.x signature
    lastRecommendedProduct = getRuntimeContext().getState(
            new ValueStateDescriptor<>("last-recommended-product", String.class));
}
```

**Verify the `open` signature.** The 1.x line used `open(Configuration)` and
newer Flink uses `open(OpenContext)`. Every older example shows the first form.
Check which one Flink 2.2 declares before writing it, rather than trusting the
skeleton above.

`transient` matters. The state handle is created per subtask at runtime and must
not be captured when the function object is serialized and shipped.

- [x] **Step 3: Implement the decision.**

```
processElement(signal, ctx, out):
    if signal.topProductId equals lastRecommendedProduct  -> emit nothing
    else                                                  -> emit Recommendation
                                                             update state
                                                             register event-time timer
                                                             at windowEnd + cooldown

onTimer(timestamp, ctx, out):                             -> clear the state
```

The emitted record, from the spec:

| Field | Value |
|---|---|
| `shopperId` | from the signal |
| `productId` | `topProductId` |
| `discountPercent` | `0.0`, no Promo Rule evaluated yet |
| `reason` | `"most-viewed-in-session"` |
| `generatedAt` | **`windowEnd`**, never `Instant.now()` |

`generatedAt` is the one that would fail Task 9 silently. Wall-clock stamps make
run 1 and run 2 differ on every line.

- [x] **Step 4: Wire it in and observe suppression.**

```java
signals.keyBy(SessionSignal::shopperId)
       .process(new RecommendationDecider(cooldown))
       .print("REC");
```

Run for a few minutes. Expected: `REC` lines appear, and **a Shopper whose
consecutive Browsing Sessions share a top Product produces only one**. With 10
Products and short sessions this happens often enough to see.

If every signal produces a Recommendation, the state is not being read. If none
do after the first, the timer is not clearing it.

---

## Task 7: RocksDB and checkpoints to MinIO

**Files:**
- Modify: `PersonalizationJob.java`, `PipelineConfig.java`
- Modify: `docs/superpowers/specs/2026-08-23-core-pipeline-design.md`

**The concept.** A checkpoint is a consistent snapshot of every operator's state
plus the source offsets that produced it. RocksDB keeps state on local disk
rather than on the JVM heap, so state can exceed memory. The snapshot goes to
durable storage, which here is the MinIO bucket from Phase 1.

**The failure mode to watch for.** `s3.path-style-access` is the one that
produces the most confusing error when wrong. Without it the client addresses
the bucket as `http://checkpoints.localhost:30014`, which does not resolve, and
the failure mentions DNS rather than S3. You will chase a networking problem
that is really a one-line config problem.

- [ ] **Step 1: Locate the three renamed config keys. Do not guess them.**

The Flink 2.0 line renamed a group of checkpointing keys. The spec marks three
as unverified:

| What it does | Old name | Doc to open |
|---|---|---|
| Where checkpoints are written | `state.checkpoints.dir` | the 2.2 checkpointing configuration page |
| Whether checkpoints survive cancellation | the `ExternalizedCheckpointCleanup` enum API | same page |
| Which checkpoint to restore from | `execution.savepoint.path` | the 2.2 state recovery page |

Fill the "Doc" column of the spec's configuration table with the URLs you
actually open. Rows currently read "not yet located", and that distinction is
the point of the column.

- [ ] **Step 2: Write `apps/pipeline/conf/config.yaml`, and add one flag.**

Revised 2026-08-24. The original three flags (`--s3-endpoint`,
`--checkpoint-dir`, `--checkpoint-interval-seconds`) are gone. Flink settings are
data in a `config.yaml`, loaded with
`GlobalConfiguration.loadConfiguration(dir)`, because that is what production
does: the Flink Kubernetes Operator renders `spec.flinkConfiguration` into
`config.yaml` inside the pod, and the jar carries no environment knowledge. See
the spec's Configuration section for the verification against the 2.2.0 jar and
for the file's contents.

The one flag that replaces them is `--flink-conf-dir`, defaulting to
`apps/pipeline/conf`. Explicit rather than relying on `FLINK_CONF_DIR`, so an
unset variable cannot silently fall back.

Not under `src/main/resources`. Anything there is baked into the jar.

- [ ] **Step 3: Read credentials from the environment.**

```bash
export MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=...
```

Fetch the values with the command in `README.md`'s MinIO section.

**Never a `--flag`.** An argument lands in shell history and is visible in `ps`
output to every process on the machine. Same no-durable-secrets reasoning the
README already applies.

- [ ] **Step 4: Load the `Configuration`, then add what the file cannot hold.**

Load `config.yaml` with `GlobalConfiguration.loadConfiguration(dir)`, then set
the two credentials from the environment onto the result, then apply the two
programmatic setters on `env.getCheckpointConfig()`: consistency mode and
externalized retention. Neither has a confirmed config key, so neither belongs in
the file.

**Assert one known key is present after loading.** A missing `config.yaml` that
yields an empty `Configuration` gives you a heap state backend and no
checkpointing, and the job looks healthy while doing neither.

- [ ] **Step 5: Prove checkpoints actually land in MinIO.**

A running job is not proof. The job logs a completed checkpoint whether or not
the bytes arrived where you think.

```bash
kubectl port-forward svc/personalization-console -n minio-tenant 9090:9090
```

Open `http://localhost:9090` and confirm objects exist under
`checkpoints/phase-3/<job-id>/chk-N`, with N increasing about every 10 seconds.

---

## Task 8: Exactly-once Kafka sink

**Files:**
- Create: `pipeline/src/main/java/lab/personalization/pipeline/RecommendationSerializationSchema.java`
- Modify: `PersonalizationJob.java`, `PipelineConfig.java`, `domain/.../JsonCodec.java`

**Interfaces produced:**
```java
static byte[] JsonCodec.toJson(Recommendation recommendation)
class RecommendationSerializationSchema implements KafkaRecordSerializationSchema<Recommendation>
```

**The concept.** Exactly-once here is a two-phase commit. The sink opens a Kafka
transaction, writes into it, and commits only when the checkpoint that covers
those records completes. A crash before the commit leaves the transaction open,
and the restarted job fences it away.

**Two behaviours that look like bugs and are not.**

1. Records become visible only when a checkpoint commits. At a 10 second
   interval the topic is silent for 10 seconds, then emits a burst.
2. A consumer without `isolation.level=read_committed` sees records from aborted
   transactions.

**The failure mode to watch for.** `transaction.timeout.ms` must sit **above**
the checkpoint interval and **below** the broker's `transaction.max.timeout.ms`.
Too low and transactions abort during normal running, silently discarding work.
Above the broker maximum and the producer refuses to start.

- [ ] **Step 1: Read the broker's limit.**

```bash
kubectl get kafka personalization -n kafka -o yaml
```

Look for `transaction.max.timeout.ms` in `spec.kafka.config`. If it is absent,
the broker default applies. Find that default before choosing the Flink value.

- [ ] **Step 2: Add `toJson(Recommendation)` to `JsonCodec`.**

Same hand-written style as its siblings. `Instant.toString()` gives ISO-8601 UTC
directly.

- [ ] **Step 3: Write `RecommendationSerializationSchema`.**

Returns a `ProducerRecord`. **Key it by `shopperId`**, so one Shopper's
Recommendations keep their relative order within a partition.

- [ ] **Step 4: Add `--output-topic` (`recommendation`) and `--transactional-id-prefix` (`personalization-phase-3`).**

The prefix has a stable default on purpose. Recovery fences the dead producer's
orphaned transaction **by transactional id**. Vary the prefix between runs and
the new producer fences nothing, the orphan stays open until it times out, and
`read_committed` consumers cannot advance past it. Your `kcat` capture appears
to hang.

- [ ] **Step 5: Build the sink and replace `.print("REC")`.**

```java
KafkaSink<Recommendation> sink = KafkaSink.<Recommendation>builder()
        .setBootstrapServers(...)
        .setRecordSerializer(new RecommendationSerializationSchema(topic))
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix(prefix)
        .setProperty("transaction.timeout.ms", ...)   // from step 1
        .build();
```

Checkpointing mode must also be `EXACTLY_ONCE`, set in Task 7.

- [ ] **Step 6: Observe both behaviours deliberately.**

```bash
kcat -C -b localhost:30016 -t recommendation -X isolation.level=read_committed
```

Expected: **silence for about 10 seconds, then a burst.** That sawtooth is the
transaction committing at each checkpoint. Watch it for a minute so the pattern
is familiar before Task 9 depends on it.

Then run the same command without `-X isolation.level=read_committed` and
compare. Understanding the difference now saves misreading Task 9's result.

---

## Task 9: Bounded mode, restore, and Drill A

**Files:**
- Modify: `PersonalizationJob.java`, `PipelineConfig.java`
- Create: `docs/runbooks/phase-3-checkpoint-restart-drill.md`

**The problem this solves.** The generator produces continuously. Two runs over
a live stream read different Clicks, so their outputs differ for reasons that
have nothing to do with recovery. `setBounded(OffsetsInitializer.latest())` asks
the broker for the end offset at job start and stops there, so both runs read an
identical range and the comparison means exactly one thing.

**Three preconditions, each of which silently ruins the Drill.**

1. **Stop the generator before run 1 and leave it stopped.** Otherwise run 2
   pins a larger end offset.
2. **Starting offsets must be `earliest`, explicitly.** `KafkaSource` defaults
   to the consumer group's committed offsets, so run 2 would start where run 1
   stopped and read nothing. Set in Task 3, verify it is still there.
3. **The output topic must be empty at the start.** Use `--output-topic` with a
   throwaway name to repeat the Drill. Auto-creation is on, confirmed in Phase 1
   when `smoke-test` worked without a `KafkaTopic`. Do **not** delete and
   recreate the real `recommendation` topic. ArgoCD manages it, and that is
   deliberate drift.

`--restore-from` does not conflict with precondition 2. Offsets in a restored
checkpoint take priority over the initializer, and that priority is exactly what
recovery depends on.

- [ ] **Step 1: Add `--bounded` (false) and `--restore-from` (none).**

`--bounded` switches the source between `setUnbounded` and `setBounded`.
`--restore-from` sets the state recovery path key you located in Task 7.

- [ ] **Step 2: Stage a backlog, then stop the generator.**

```bash
apps/gradlew -p apps :generator:run --args="--click-rate=200"     # about 2 minutes, then Ctrl-C
kcat -L -b localhost:30016 -t clickstream              # record the end offsets
```

Both runs must read this exact range.

**If run 2 finishes before you can kill it**, the Drill is impossible as written.
The spec's mitigation ladder, in order:

1. Stage a bigger backlog. Raise `--click-rate` or run the generator longer.
2. Shorten `--checkpoint-interval-seconds`, so a checkpoint completes earlier in
   the run.
3. Only if both fall short, add a throttle to the job in drill mode, meaning a
   step that sleeps per record. This is scaffolding, not pipeline logic, so keep
   it clearly marked as such.

Try them in that order. Step 3 changes the job; steps 1 and 2 do not.

- [ ] **Step 3: Run 1, clean, to completion.**

```bash
apps/gradlew -p apps :pipeline:run --args="--bounded --output-topic=drill-a"
```

- [ ] **Step 4: Run 2. Wait for a completed checkpoint, then hard-kill.**

```bash
apps/gradlew -p apps :pipeline:run --args="--bounded --output-topic=drill-a"
# watch the log until at least one checkpoint COMPLETES, then:
pkill -9 -f lab.personalization.pipeline
```

**`SIGKILL`, not `Ctrl-C`.** `SIGINT` is catchable, so it runs the cooperative
shutdown path: the sink aborts its own transaction, and the retention policy is
consulted, which under the wrong setting deletes the checkpoint you are about to
restore from. `SIGKILL` runs nothing. The transaction is left open, the
checkpoint survives, and on restart the sink fences the dead producer and aborts
the orphan. **That fencing step is the entire exactly-once mechanism, and a
graceful cancel never reaches it.** Phase 5's real failures, an evicted pod, a
drained node, an OOMKill, are all `SIGKILL`.

Killing before any checkpoint completes leaves nothing to restore from, and step
6 then fails with a path error rather than teaching you anything.

- [ ] **Step 5: Find the retained checkpoint.**

The job log names it. The MinIO console under `checkpoints/phase-3` confirms it.

- [ ] **Step 6: Resume.**

```bash
apps/gradlew -p apps :pipeline:run --args="--bounded --output-topic=drill-a --restore-from=s3://checkpoints/phase-3/<job-id>/chk-N"
```

- [ ] **Step 7: The check.**

Run 1 wrote set X. Run 2 wrote set Y. The topic holds X plus Y. If recovery is
correct then Y equals X, so **every distinct line appears exactly twice**.

```bash
kcat -C -b localhost:30016 -t drill-a -o beginning -e \
     -X isolation.level=read_committed \
  | sort | uniq -c | awk '$1 != 2'
```

| Output | Meaning |
|---|---|
| nothing | recovery correct |
| lines with count **3** | the sink duplicated. Not exactly-once. The transaction did not roll back |
| lines with count **1** | produced once and not the other time. Keyed state lost, or a window never fired |

`isolation.level=read_committed` is not optional. Without it `kcat` shows
records from aborted transactions, including the orphan the restart fenced, and
reports failures that are not real.

- [ ] **Step 8: Write the runbook, with real pasted output.**

`docs/runbooks/phase-3-checkpoint-restart-drill.md`. Rationale per command, and
an "Observed result" section holding **actual output**, including the empty
result of the `awk` check.

---

## Task 10: Knowledge doc, README, and status

**Files:**
- Create: `docs/knowledge/phase-3-core-pipeline.md`
- Modify: `README.md`, `docs/superpowers/plans/status.md`

**Why this is a task and not an afterthought.** `docs/knowledge/` holds one doc
per phase and Phases 0 and 1 both have one. The knowledge doc records how the
thing actually works, including what went wrong, which is the part that is gone
in a week if it is not written down now.

- [ ] **Step 1: Finish `docs/knowledge/phase-3-core-pipeline.md`.**

Started during Task 2, not at the end: it already carries "How a NodePort
Service reaches the pod, and why it never visits the ClusterIP", written when
that question actually came up. Keep adding to it as things surface, rather
than reconstructing them here from memory.

Not a restatement of the spec. Record what the spec could not know in advance:

- Whether `java.time.Instant` turned out to be a first-class Flink type, and
  what the failure looked like if not
- The three Flink 2.x config key names, with the URLs
- The `flink-connector-kafka` version that pairs with Flink 2.2
- The broker's `transaction.max.timeout.ms` and the Flink value chosen against it
- Whether the watermark ever stalled on an idle partition, and what that looked like
- The observed Browsing Session close rate against the predicted one every 4 seconds
- Anything that cost more than fifteen minutes to work out

- [ ] **Step 2: Update `README.md`.**

A "Running the pipeline" section, matching the tone of "Running the generator":
the command, the defaults, the required environment variables, and the
verification that is not "the log said it started".

Note the exactly-once sawtooth explicitly. Someone running `kcat` and seeing ten
seconds of silence will otherwise think it is broken.

- [ ] **Step 3: Update `status.md`.**

Phase 3 to done, with the same specificity the Phase 1 and 2 entries use: what
was built, what was verified with real output, and anything deliberately
deferred.

Add the Phase 4 finding the spec surfaced: `ProductChange` is a sealed
interface, which is not a Flink POJO, and with `pipeline.generic-types: false`
it cannot silently become a Kryo type either. Phase 4 hits this the moment it
reads `product-change`. It needs a custom `TypeInformation` or a split into two
typed branches, and it needs budget.

---

## Done when

All of these, with real output you have read:

| Criterion | Evidence |
|---|---|
| Phase 2 still works after the restructure | `kcat` on all three input topics, real messages |
| MinIO reachable from the host | `curl http://localhost:30014/minio/health/live` returns 200 |
| Checkpoints land in MinIO | Objects under `checkpoints/phase-3/<job-id>/chk-N`, N increasing |
| Drill A: kill and restart | The `awk` check prints nothing |
| Drill B: Late Click | A `LATE` line for `shopper-99`, and no Recommendation for it |
| Both runbooks | Contain pasted output, not predictions |

The phase plan's own criterion is the Drill A row. The rest is what makes that
row trustworthy.
