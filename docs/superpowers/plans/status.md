# Implementation status

Last updated: 2026-09-08

Live tracker: what's actually done right now, not the design (that's
[the phase plan](2026-08-10-implementation-phases.md)) and not how things work
(that's [the knowledge docs](../../knowledge/)). Update this file as work
lands, don't let it go stale.

Status legend: ✅ done · 🟡 in progress · ⬜ not started · ❌ removed by decision

## Phase 0: Cluster floor — ✅ done (sync-wave deferred)

- ✅ `kind` cluster, 3 control-plane + 3 zone-labelled workers, IP-pinned via
  `scripts/session-start.sh` after the etcd/cert bug forced a recreation
- ✅ cert-manager (`v1.21.1`), direct Helm install, recorded in
  `scripts/bootstrap-phase0.sh`
- ✅ ArgoCD (`v10.3.2`), direct Helm install, same script. UI at
  `localhost:30010`
- ✅ Root app-of-apps Application (`manifests/argocd-apps/root.yaml`),
  applied and synced
- ⬜ `sync-wave` annotations — deferred on purpose, nothing yet needs
  ordering (only one component exists per wave so far)
- ✅ Control-plane-kill drill — run on the current (recreated, IP-pinned)
  cluster, confirmed complete. Raw command output was never pasted back into
  the runbook's "Observed result" section, so
  [the runbook](../../runbooks/phase-0-control-plane-drill.md) still only
  has the predicted-behavior version, not a real transcript

## Phase 1: Data platform — ✅ done

- ✅ Strimzi operator, as an ArgoCD Application (`manifests/argocd-apps/strimzi.yaml`),
  Healthy/Synced
- ✅ `Kafka` cluster + two `KafkaNodePool`s (`controllers` 0-2, `brokers` 3-5),
  as a second Application (`manifests/argocd-apps/strimzi-kafka-cluster.yaml`
  → `manifests/strimzi/kafka-cluster.yaml`), Healthy/Synced
- ✅ External listener verified end-to-end from outside the cluster (`kcat`
  round-trip via `localhost:30016`), real output confirmed, see the plan's
  Phase 1 Drill section
- ✅ Four real `KafkaTopic` resources: `clickstream`, `product-change`,
  `promo-rule`, `recommendation` (`manifests/strimzi/kafka-topics.yaml`, same
  `strimzi-kafka-cluster` Application, no new Application needed), all
  `Ready`, confirmed via `kubectl get kafkatopic -n kafka`. Named
  `product-change` rather than the originally planned `price-change`, since
  the term covers stock-level moves too, not just price.
- ✅ MinIO, as two ArgoCD Applications (`minio-operator.yaml` for the
  Operator, `minio-tenant.yaml` → `manifests/minio/tenant.yaml` for the
  `Tenant`), same split as Strimzi, both Healthy/Synced
- ✅ MinIO checkpoint bucket (`checkpoints`, declared in `Tenant.spec.buckets`),
  confirmed via the Console UI (`personalization-console`, port-forwarded),
  not just the Tenant's own `Ready` status

## Phase 2: Domain model and generator — ✅ done

- ✅ Four domain schemas as Java records (now `apps/domain/`):
  `Click`, `ProductChange`, `PromoRule`, `Recommendation`. Design in
  [the domain schemas doc](2026-08-16-domain-schemas-design.md).
  **`ProductChange` was redesigned on 2026-08-28**, see
  [ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md): it was a
  sealed interface over `PriceChange`/`StockChange`, and is now one record
  carrying `price`, `previousPrice`, `stock`, `previousStock`. The two variants
  are deleted and the wire format's `"type"` field is gone.
- ✅ Synthetic generator (`generator/`), a plain Java `kafka-clients`
  producer, not a Flink program, per
  [the generator design doc](2026-08-16-generator-event-production-design.md).
  One reusable `SkewedEventStream<T>` driver, instantiated for `Click` and
  `ProductChange` (each with independently configurable rate and max skew,
  delayed-publish mechanism, not random jitter) and for `PromoRule` (its
  own slower cadence, skew fixed at zero, no watermark gates broadcast
  state).
- ✅ Done-when criterion met: configurable rate and configurable max skew
  confirmed via `./gradlew run` plus `kcat -C` against all three real
  topics, `clickstream`, `product-change`, `promo-rule`, real messages
  observed at the expected cadence, not just a running-process log line.

## Phase 3: Core pipeline — ✅ done

Design and plan both written and approved:
[design](../specs/2026-08-23-core-pipeline-design.md),
[implementation plan](2026-08-23-phase-3-core-pipeline.md). The plan runs 11
tasks; check its Progress table for the live position.

- ✅ Task 0: Flink Kubernetes Operator confirmed to support Flink 2.2. `v2_2` is
  a current, undeprecated value of the Operator's `FlinkVersion` enum, with
  `v2_3` and `v2_4` beyond it. This was the phase's top risk, checked first on
  purpose, since a downgrade found at Phase 5 would have invalidated Phases 3
  and 4. Which *released* Operator version first shipped `v2_2` is still open
  and is a Phase 5 question, recorded in the design's Resolved risks section.
- ✅ Task 1: Gradle multi-project restructure. The whole build moved under
  `apps/`, so the repo root now separates the Java build from `manifests/`,
  `clusters/`, and `scripts/`. New `:domain` module holds the 7 records plus
  `JsonCodec` and declares **zero** dependencies, which is what keeps the
  records valid Flink POJO types. Commands are now
  `apps/gradlew -p apps :generator:run`; Gradle resolves its build root from the
  working directory, so `-p apps` is required.
  Gate passed: `kcat` against `clickstream`, `product-change`, and `promo-rule`
  all returned their expected records after the restructure, not just a
  successful compile.
- ✅ Task 2: MinIO S3 API exposed on NodePort 30014 as `minio-s3-api`
  (`manifests/minio/`, picked up by the existing `minio-tenant` Application).
  Ports 30014 and 30015 were already reserved at Phase 0, so no cluster
  recreation. Verified from outside the cluster, not from Synced/Healthy:
  endpoints `10.244.3.14:9000` and `curl http://localhost:30014/minio/health/live`
  returning `200 OK`.
- ✅ Task 3: `:pipeline` module reads Clicks from `clickstream` on
  `MiniCluster`. Three findings: `java.time.Instant` **is** a first-class Flink
  type, so the open question from the design is closed and the records need no
  change; `flink-connector-base` is required and nothing pulls it in, since
  Flink bundles it in `flink-dist`; and Flink 2.2's runtime and its own Kafka
  connector pull two different lz4 modules declaring the same Gradle
  capability, which is a hard error needing `capabilitiesResolution`.
- ✅ Task 4: watermarks, session windows, `SessionSignal`. Cost a real
  debugging detour: windows never fired because default parallelism (16) far
  exceeds the partition count (3), so 13 source subtasks held the watermark at
  `Long.MIN_VALUE`. Fixed with `.withIdleness(...)`, not by pinning parallelism,
  since Phase 6 varies parallelism deliberately.
- ✅ Task 5: Late Click side output and Drill B. `OutputTag<Click>` attached to
  the `WindowedStream` via `.sideOutputLateData(...)`, printed with the `LATE`
  prefix. Drill B injected a hand-written Click for `shopper-99`, an id
  `Catalog.SHOPPER_IDS` cannot produce, with an `eventTime` 60 seconds back.
  Confirmed on both halves: one `LATE` line appeared, and no `SessionSignal`
  ever carried `shopper-99`. The absence is what proves the routing, since a
  `LATE` line alone does not rule out the Click also reaching a window.
  The mechanism is written up in
  [the knowledge doc](../../knowledge/phase-3-core-pipeline.md), section "Why a
  Click behind the watermark is not automatically a Late Click": lateness is
  judged on the *merged* window's end, not on the element's own timestamp, so
  with bound 5s and gap 6s a Click is late only past `maxSeen - 11`, not
  `maxSeen - 5`.
  **Known gap, same shape as the Phase 0 one:**
  [the Drill B runbook](../../runbooks/phase-3-late-click-drill.md) carries the
  real `LATE` line but its "Observed result" section still has two TODO
  placeholders, the baseline `SessionSignal` sample and the `grep 'shopper-99'`
  output. Both were confirmed live, neither was pasted in.
- ✅ Task 6: `RecommendationDecider`, a `KeyedProcessFunction` keyed by
  `shopperId` holding two `ValueState` handles, `last-recommended-product` and
  `pending-timer`. It exists because a window's state dies when the window
  fires, and "do not recommend the same Product two Browsing Sessions running"
  must outlive that.
  The stale-timer problem was the real content of this task. `registerEventTimeTimer`
  inserts a row, it does not replace one, so a Shopper who switches Product
  leaves an older timer that later clears the *newer* state. Resolved with
  `deleteEventTimeTimer` plus a second `ValueState<Long>` holding the exact
  registered timestamp, since the old timestamp is not derivable and a delete
  against a wrong value is a silent no-op. The rejected alternative was a
  staleness check inside `onTimer`, which costs the same state but lets dead
  timer rows accumulate into every checkpoint.
  Verified from real output, not from a clean compile: `generatedAt` equals
  `windowEnd` to the millisecond on every pair, `shopperId` carries a Shopper id
  rather than a Product id, and each Shopper lands on the same subtask index in
  both the window operator and the decider. Suppression itself needed a longer
  run, since with 10 Products only about one consecutive session pair in ten
  collides.
  Two write-ups landed in
  [the knowledge doc](../../knowledge/phase-3-core-pipeline.md): why `transient`
  belongs on a `ValueState` field but not on the `Duration cooldown`, and how
  `deleteEventTimeTimer` works.
  **Semantic worth knowing before Phase 4 rewrites this class.**
  `lastRecommendedProduct` is a *single slot*, so the cooldown only blocks an
  immediate repeat. An observed run had `shopper-1` receive P7, then P2, then P7
  again within 22 seconds, all inside the 60 second cooldown. That matches the
  spec's wording, "not two Browsing Sessions running". A rule of "not the same
  Product twice within 60 seconds regardless of what came between" would need
  `MapState<String, Long>` of Product to expiry instead.
- ✅ Task 7: RocksDB and checkpoints to MinIO. Verified against the bucket with a
  signed `ListObjectsV2` request, not from the absence of errors:
  `chk-5` → `chk-6` → `chk-15` under `phase-3/<job-id>/`, each carrying
  `_metadata`, one retained per job at a time.
  **Flink settings moved out of Java and into `apps/pipeline/conf/config.yaml`**,
  loaded with `GlobalConfiguration.loadConfiguration(dir)`. That is what
  production does: the Flink Kubernetes Operator renders `spec.flinkConfiguration`
  into `config.yaml` inside the pod, and the jar carries no environment knowledge.
  Phase 5 copies the YAML body into the CR and changes no Java. The three flags
  the plan originally specified (`--s3-endpoint`, `--checkpoint-dir`,
  `--checkpoint-interval-seconds`) are gone, replaced by `--flink-conf-dir`
  (default `conf`, since Gradle's `run` starts in the subproject directory).
  Credentials stay out of the file and come from `scripts/minio-env.sh`.
  Three findings, none of them in any documentation:
  1. **`flink-s3-fs-native` is not published to Maven Central at any version.**
     It ships only inside a Flink distribution's `opt/`, so it cannot be a Gradle
     dependency. Recorded in
     [ADR 0008](../../adr/0007-s3-filesystem-plugin.md), with what the detour
     bought. `flink-s3-fs-hadoop` it is, `runtimeOnly`.
  2. **`FileSystem.initialize(flinkConfig, null)` is required.** Flink's filesystem
     registry is a process-wide static that the job's `Configuration` never
     reaches, so `s3.access-key` was being set on an object the S3 factory does
     not read. It fails as `NoAuthWithAWSException` naming `AWS_ACCESS_KEY_ID`,
     which invites the wrong fix on a machine with no AWS account.
  3. **The S3 filesystem does register from a plain classpath** under
     `MiniCluster`, despite the plugins page's warning about `lib/` placement in
     a distribution.
  **Corrected 2026-08-25.** Checkpoint consistency mode and externalized
  retention were briefly held as programmatic setters on
  `env.getCheckpointConfig()`, on the claim that no config key existed. That
  claim came from a documentation search returning nothing, which is not the same
  as absence. `javap` on `CheckpointingOptions` shows
  `CHECKPOINTING_CONSISTENCY_MODE` → `execution.checkpointing.mode` and
  `EXTERNALIZED_CHECKPOINT_RETENTION` →
  `execution.checkpointing.externalized-checkpoint-retention`. Both now live in
  `config.yaml`, so **no Flink setting is configured in Java at all**. Verified
  with a negative control: an invalid value fails with
  `Could not parse value 'NONSENSE' for key 'execution.checkpointing.mode'.
  Expected one of: [[EXACTLY_ONCE, AT_LEAST_ONCE]]`.
  The lesson generalises past this one setting: for anything version-specific,
  the jar is the authority and Context7 returning nothing is not evidence.
  The equivalent Java setters still exist and carry two traps worth knowing when
  reading other code: `setCheckpointingMode` is deprecated in favour of
  `setCheckpointingConsistencyMode`, and the enums sit in different packages,
  `CheckpointingMode` in `org.apache.flink.core.execution` with a deprecated
  same-named twin in `org.apache.flink.streaming.api`, and
  `ExternalizedCheckpointRetention` in `org.apache.flink.configuration`.
  The spec now carries a four-step **checkpoint verification procedure** as a
  precondition for Drill A, deliberately including a negative control, since a
  check that cannot fail proves nothing.
  **Known gap:** `:pipeline` has no `log4j2.xml`, and log4j2 with no
  configuration defaults to `ERROR`. Every INFO line Flink emits, including
  `Completed checkpoint N`, is discarded. Verification therefore has to query S3
  rather than read logs. Worth closing before Task 9's Drill.
- ✅ Task 8: Exactly-once Kafka sink. `KafkaSink` with
  `DeliveryGuarantee.EXACTLY_ONCE`, keyed by `shopperId`, publishing to
  `recommendation`. Three flags added: `--output-topic`,
  `--transactional-id-prefix`, `--transaction-timeout-ms`.
  **`transaction.timeout.ms = 300000`, and the number is forced, not chosen.**
  The broker reports `transaction.max.timeout.ms=900000` with synonym
  `DEFAULT_CONFIG`, so nothing overrides Kafka's default of 15 minutes. Flink's
  `KafkaSinkBuilder.DEFAULT_KAFKA_TRANSACTION_TIMEOUT` is `Duration.ofHours(1)`,
  verified by `javap`, which is **four times the broker ceiling**. Leaving it
  unset is not a neutral choice: the producer refuses to start. The floor is not
  the checkpoint interval either, it is checkpoint interval plus the longest
  outage you expect to recover from, because a transaction whose checkpoint
  completed is committed by the *restarted* job, and a coordinator timeout
  aborts it first and loses those records.
  **Verified from real output, and the proof is in the offsets.** Every partition
  advanced by **2** per single record printed, because a committed transaction
  writes a control record into each partition it touched. A non-transactional
  producer advances by exactly the record count, so the by-two pattern is direct
  evidence `EXACTLY_ONCE` engaged rather than silently falling back.
  Per-Shopper ordering held across both captures, confirming the `shopperId`
  message key: `shopper-10` P10 at `01:17:30.626` then P9 at `01:17:57.626`.
  Timestamps are unordered *across* Shoppers, which is correct, since Kafka
  orders within a partition only.
  **Worth knowing before Task 9.** Running `kcat` with and without
  `isolation.level=read_committed` produced **identical payloads**. On a healthy
  run the two isolation levels are indistinguishable, because there are no
  aborted transactions to hide. The difference only appears during a failure,
  which is precisely Drill A, and is why capturing Drill A without
  `read_committed` would surface records the restart is meant to abort.
  Two API notes. `setProperty` takes a `String`, so a `Duration` needs
  `String.valueOf(...toMillis())`. And `FileSystem.initialize(Configuration)`
  from Task 7 turned out to be **deprecated**; the current form takes a
  `PluginManager`, and `null` is correct under `MiniCluster`. Gradle's default
  output says only "uses or overrides a deprecated API" with no line number, so
  `-Xlint:deprecation` was needed to find it.
  The transactional id prefix mechanism, including why it must be stable and why
  it is a prefix rather than an id, is written up in
  [the knowledge doc](../../knowledge/phase-3-core-pipeline.md).
- ✅ Task 9: Bounded mode, restore, and Drill A. `--bounded` and `--restore-from`
  added. `--bounded=true` calls `setBounded(OffsetsInitializer.latest())`, which
  ends the job at the end offsets, rather than `setUnbounded`, which stops
  emitting but leaves the job running. `--restore-from` writes
  `StateRecoveryOptions.SAVEPOINT_PATH` onto the loaded `Configuration`, guarded
  so an unset flag never sets an empty path.
  Note `--bounded` must be passed as `--bounded=true`. `PipelineConfig.parse`
  requires `--key=value` for every flag and rejects a bare switch. The plan's
  command examples said `--bounded` and were corrected.
  Two supporting changes landed here because Drill A needed them.
  `apps/pipeline/src/main/resources/log4j2.xml`, scoped so
  `Completed checkpoint N` is visible without burying the `SIGNAL`, `RECOMMEND`
  and `LATE` prints. Root stays at `WARN`; opening `org.apache.flink` wholesale
  produced 395 INFO lines in 80 seconds, of which 337 were per-subtask chatter
  from `StateBackendLoader` and `FlinkKafkaInternalProducer`. And
  `execution.checkpointing.num-retained: 3`, up from the default of 1, so the
  checkpoint noted from the log cannot be deleted between reading it and
  restoring from it.
  **`_metadata` is what makes a checkpoint restorable, and this was observed
  rather than reasoned.** Restoring from `chk-16` of an earlier job failed with
  `FileNotFoundException: Cannot find meta data file '_metadata' in directory`.
  That job had been killed while checkpoint 16 was in flight: state files
  uploaded, coordinator never got all 16 acknowledgements, metadata never
  written. `chk-15` of the same job was fine. **Pick the checkpoint from the
  log's `Completed checkpoint N` line, never from the highest number in the
  bucket**, because a directory listing cannot tell the two apart. That is the
  concrete payoff of adding `log4j2.xml` before the Drill rather than after.
  The restore path itself was confirmed reaching Flink:
  `Starting job 39da64ab... from savepoint s3://checkpoints/phase-3/.../chk-16`.
  **Evidence state, recorded plainly.** Bounded mode completes: an earlier
  bounded run left 133,844 records in `drill-a`, and a later run against a fresh
  topic produced 60,777 Recommendations with checkpoints completing at 12MB then
  24MB, the growth confirming incremental checkpointing accumulating shared
  state. The full Drill A sequence, kill after a completed checkpoint, restore
  from it, and the `sort | uniq -c | awk '$1 != 2'` comparison, was **not run to
  completion**, and `docs/runbooks/phase-3-checkpoint-restart-drill.md` was not
  written. Marked done on the user's explicit instruction on 2026-08-25.
- ✅ Task 10: Knowledge doc, README, and status.
  [The knowledge doc](../../knowledge/phase-3-core-pipeline.md) runs 14 sections
  and 1319 lines, written **as each finding surfaced** rather than reconstructed
  at the end, which is what Task 10 asked for. It covers the NodePort path, the
  `earliest` offsets decision, `ProcessWindowFunction` versus `reduce`, why a
  Click behind the watermark is not a Late Click, `transient` on `ValueState`,
  `deleteEventTimeTimer`, what `s3://checkpoints/phase-3` means,
  `pipeline.generic-types: false`, `FileSystem.initialize`, the transactional id
  prefix, the four layers of enabling Kafka transactions, the watermark stall,
  and a table of facts the spec could not know in advance.
  `README.md` gained a **Running the pipeline** section covering the required
  `source scripts/minio-env.sh`, the config split between `config.yaml` and
  flags, drill mode, and the exactly-once sawtooth, since ten seconds of silence
  on `kcat` reads as a broken job otherwise. Its MinIO entry was also corrected:
  it still claimed no external NodePort existed, which Task 2 made false.

**Observed end-to-end on 2026-08-25**, one bounded run over a backlog of about
3 million Clicks: 1m 47s wall clock, 125,893 Browsing Sessions closed, 111,988
Recommendations published, 13,905 suppressed by the cooldown, 0 Late Clicks, 5
checkpoints completed, the last at 49.5MB. A `read_committed` consumer counted
**exactly 111,988** records on the output topic, matching the job's own emitted
count. Checkpoint sizes grew 12MB, 24MB, 49MB across the run, which is
incremental checkpointing accumulating shared state.

Two decisions worth knowing without reading the whole design. Phase 3 publishes
a real `Recommendation` to the existing `recommendation` topic rather than
adding a fifth topic, with `discountPercent` at `0.0` until Phase 4 evaluates
Promo Rules. And the session gap is 6 seconds because it is *derived* from the
generator's rate, not chosen: at 10 Shoppers and 5 Clicks per second a 30 second
gap would close a Browsing Session about never, and the job would emit nothing
while behaving correctly.

**~~Surfaced for Phase 4~~ Resolved 2026-08-28 by
[ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md).**
`ProductChange` was a sealed interface, which is not a Flink POJO, and
`pipeline.generic-types: false` blocked a silent Kryo fallback. Neither of the
two options predicted here was taken. Instead the sum type was removed: one
record now carries the Product's full state plus the values it replaced. That
also gave stock its first consumer, an out-of-stock suppression rule, and made
"price drop" checkable for the first time.

**Deferred out of Phase 3, both deliberately.** Drill A's full sequence, kill
after a completed checkpoint then restore and compare, was not run to
completion, and `docs/runbooks/phase-3-checkpoint-restart-drill.md` was never
written. Drill B's runbook exists but its "Observed result" section still holds
two TODO placeholders. Phase 5's HA Drill leans on exactly the same recovery
path, so it inherits the unproven part.

## Phase 4: Advanced Flink — ✅ done

Design and plan both written and approved:
[design](../specs/2026-08-24-advanced-flink-design.md),
[implementation plan](2026-08-24-phase-4-advanced-flink.md). The plan runs 10
tasks; check its Progress table for the live position.

Tasks 0 to 3 depend on nothing Phase 3 still has open, so they can run alongside
Phase 3 Tasks 6 to 10. Task 4 rewrites `RecommendationDecider`, so it waits.

- ✅ Task 0: `flink-cep` packaging confirmed. `lib/flink-cep-2.2.0.jar` is in the
  `flink:2.2.0` image and loaded by default, and the advanced-configuration page
  names CEP explicitly as a library that sits outside `flink-dist.jar` but ships
  in `lib/`. Scope is `compileOnly` plus `runtimeOnly`, matching
  `flink-streaming-java`. Checked first on purpose, the same reasoning that made
  the Operator-version check Phase 3's Task 0.
- ✅ Task 1: `Product Change` snapshot model, and its source. Redesigned
  mid-task by [ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md),
  which is the substantial event of this phase so far. `PriceChange` and
  `StockChange` are deleted; one `ProductChange` record carries `price`,
  `previousPrice`, `stock`, `previousStock`, and the wire format's `"type"`
  field is gone. `ProductChangeFactory` is now stateful, keeping a ten-entry map
  of each Product's last event, and emits `stock` of zero one time in ten so the
  out-of-stock rule is observable.
  Gate passed: the job read `product-change` end to end, at the expected rate
  and with the expected `stock=0` share, not just a successful compile.
  Three things were verified rather than assumed. The derived accessors
  `priceDropped()` and `outOfStock()` do **not** confuse Flink's POJO
  extraction: `TypeInformation.of(ProductChange.class)` gives `PojoTypeInfo`
  with arity **6**, not 8, and a `PojoSerializer` round trip is exact. The
  generator's measured distributions over 20,000 events are `stock == 0` at
  9.6% and a price drop at 37.5%. And a first attempt at the numeric JSON
  pattern, `(-?[0-9.eE+]+)`, was wrong: `-?` anchors only the start and the
  character class holds no `-`, so `1.0E-4` matched as `1.0E` and threw
  `NumberFormatException`.
- ✅ Task 2: test infrastructure, the interval join, and `EnrichedClick`.
  Five tests green, and the live `ENRICHED` rate confirmed against the predicted
  one third of Clicks.
  Three Gradle facts had to be settled before a single test could run:
  `compileOnly` does not reach the test compile classpath (fixed with
  `testImplementation.extendsFrom compileOnly`), `applicationDefaultJvmArgs`
  applies to `run` and not `test`, and without `useJUnitPlatform()` Gradle finds
  zero tests and **reports success**. Every test result in this phase is
  therefore read from the XML report, not from `BUILD SUCCESSFUL`.
  The harness artifact question the plan left open is closed:
  `ProcessFunctionTestHarnesses` and the `*OperatorTestHarness` classes are in
  the **tests classifier** of `flink-runtime`, which `flink-test-utils` does not
  pull. It is declared explicitly and Task 4 needs it.
  Two review findings on the first implementation. The Product-keyed branch had
  been forked *below* `keyBy(shopperId)`, which compiles and costs a second full
  shuffle, and is the exact shape ADR 0003 exists to forbid. And the join was
  written with `ProductChange` on the left; with symmetric bounds that emits an
  identical pair set, but it prevents job and test sharing one
  `ProcessJoinFunction` and would invert silently if the bounds ever became
  asymmetric. Both corrected; the join interval is now
  `--join-lower-bound-seconds` / `--join-upper-bound-seconds` rather than
  hardcoded.
- ✅ Task 3: CEP abandoned cart, `ShopperSignal`, `SignalKind`. Five tests green
  and the live `CART-ABANDONED` rate confirmed.
  The pattern is VIEW, then ADD_TO_CART on the **same** Product, then no CHECKOUT
  within 30s. It supersedes the design spec's "viewed a competitor, went idle",
  which at the generator's real rates needs a 60s window spanning ten session
  gaps. `ActionType`'s own comment in `:domain` had described this pattern since
  Phase 2 and nothing else recorded it.
  **One question the docs could not answer, settled by the test.** A pattern
  ending in `notFollowedBy(...).within(...)` delivers a clean expiry through
  `processMatch`, not `processTimedOutMatch`. So "viewed, carted, never checked
  out" is a **match**, and the timed-out handler sees only genuinely incomplete
  sequences such as a VIEW never carted. Had it been the other way the emission
  would have had to move, and the two side outputs would have been tangled.
  `SameProductAs` is an `IterativeCondition`, not a `SimpleCondition`, because
  the Product is unknown at graph-construction time and is read per match from
  `ctx.getEventsForPattern("view")`. It is a named static nested class so that
  serialization ships only its two fields.
  `--cep-within-seconds` added, so every timing value in the job is now a flag.
- ✅ Task 4: the `connect` merge, `SignalMerger`, `UNMATCHED` and `OUT_OF_STOCK`.
  Nine harness tests green. `RecommendationDecider` became `SignalMerger`, a
  `KeyedCoProcessFunction` whose `processElement1` is Phase 3's old body.
  Three decisions worth keeping. The two state maps have **different lifetimes**:
  `matchesByProduct` clears on session close, while `abandonedCarts` expires on
  its own 60s event-time timer, because a cart abandonment confirms 30s after the
  VIEW and ~60% of them arrive after their own Browsing Session has closed. A
  candidate with no trigger goes to `UNMATCHED` and is **not** published, which
  narrows Phase 3's behaviour by roughly a fifth of output volume. And the request
  is built from real facts before any branch, so a suppressed record still reports
  why it would have been recommended.
  **A config fix was needed before the live run, and it will matter again.** The
  first attempt failed at deploy with `Insufficient number of network buffers:
  required 17, but only 0 available` against the default 2048. Phase 4 grew the
  graph to five `keyBy` shuffles, and `MiniCluster`'s default network memory does
  not cover them at parallelism 16. Fixed in `apps/pipeline/conf/config.yaml` with
  `taskmanager.memory.network.min` and `.max` both at `256mb`, which is 8192
  buffers, plus `taskmanager.memory.process.size: 2gb` to afford it. Pinning
  parallelism was rejected as the fix, for the same reason Phase 3 Task 4 rejected
  it: Phase 6 varies parallelism deliberately.
  After the fix, zero failed tasks and every output present: `REQUEST`,
  `UNMATCHED`, `CART-ABANDONED`, `CEP-TIMEOUT`, `PRICE-DROP-MATCH`,
  `MERGED-SIGNAL`, `SIGNAL`.
  **Phase 5 inherits this.** These values move into `spec.flinkConfiguration` on
  the `FlinkDeployment`, and a TaskManager container sized below 2gb will hit the
  same wall.
- ✅ Task 5: broadcast Promo Rules. Five harness tests green; the live check was
  not run, so the task is signed off on tests alone.
  `PromoRuleApplier` is a plain `BroadcastProcessFunction`, not a keyed one: the
  discount is a stateless multiplication, so no second `keyBy` shuffle is needed.
  The rule lives in broadcast state under **one fixed key**, so each new rule
  replaces the last; keying by `ruleId` would hold 120 entries after an hour,
  since the generator never stops emitting fresh ids.
  Three bugs the harness caught, all of which compile cleanly: reading broadcast
  state **before any rule has arrived** returns `null` and NPEs, which would crash
  every run in its first 30 seconds; an `if` with no `else` **silently drops**
  every cart-abandoned request whose Product never moved in price; and adding an
  unconditional second `collect` **doubles** every matched request. The rule is
  exactly one `collect` per input, with `priceDropMatched` deciding the discount
  rather than whether the record survives.
- ✅ Task 6: async I/O and the mocked recommendation service. Two tests green, and
  verified live on the `recommendation` topic. `AsyncDataStream.orderedWait`, its own
  operator downstream of the merge, feeding the sink; Phase 3's `RecommendationDecider`
  is deleted. A `RecommendationClientFactory` travels to the TaskManagers rather than a
  client, since a client owns an executor and is not serializable.
  Live evidence: `discountPercent` of 18.0, 10.0 and 6.0 on the topic, where every
  record before today read `0.0`; `reason` values of `price-drop` and `cart-abandoned`
  and never `most-viewed-in-session`; and `cart-abandoned` appearing at both `10.0` and
  `0.0`, which is the structural condition working.
- ✅ Task 7: end-to-end test of the assembled graph. 27 tests green, three consecutive
  fresh runs.
  **The finding worth carrying forward:** a window fires when the **watermark** passes
  its end, not when its last element arrives. A bounded fixture that stops too early
  leaves the window and the CEP match both firing at `MAX_WATERMARK`, where their
  arrival order at the merge is a race, and the test fails intermittently on `reason`.
  The fix is a **watermark pusher**: one event from another key far in the future, so
  the operator under test fires during the stream. Any bounded test whose assertion
  depends on operator ordering needs one.
- ✅ Task 8: Drill C, a Promo Rule changed mid-run and the topic changed with no
  restart. All four claims confirmed.
  The plan's second claim had to be corrected first: it checked that
  `most-viewed-in-session` records stayed at `0.0`, but since Task 4 those go to
  `UNMATCHED` and never reach the topic, so the check was unfalsifiable. The real
  discriminator is a `cart-abandoned` record still reading `0.0` after the injection,
  meaning its candidate had no price drop.
  The [runbook](../../runbooks/phase-4-promo-rule-drill.md) carries a **real
  transcript**, the first in this project to do so; the Phase 0 and Drill B runbooks
  still have only their predicted-behaviour versions.
  **Two findings from the recorded run.** A freshly started job knows **no rule at
  all**: `--start-from-earliest=false` makes the broadcast source start at *latest*, so
  a rule published before it subscribed is never read, and every discount is `0.0`
  until the next rule arrives. `PromoRuleApplier`'s `rule != null` guard is what makes
  that a zero rather than a crash. And the injection takes about **a minute** to show,
  not one checkpoint interval: a Browsing Session must close *and* have a price-drop
  match before any record can carry the new rule.
- ✅ Task 9: documents. Most landed early, as the decisions were made rather than
  retrofitted: ADR 0008, `CONTEXT.md`, the
  [Phase 4 knowledge doc](../../knowledge/phase-4-advanced-flink.md), the walkthrough,
  and supersession notes on the two Phase 2 specs. Finished on 2026-08-30 with
  [ADR 0009](../../adr/0009-unmatched-click-moves-to-the-merge.md), a superseded-in-part
  banner on ADR 0003, and the design spec's CEP coverage map corrected from "viewed a
  competitor, went idle" to the abandoned cart.
  The sweep for stale claims is clean: every remaining mention of the old wording is
  inside a document explaining what it supersedes.

**Surfaced for Phase 5 by Task 1, and nothing else records it.** The out-of-stock
suppression rule means the `recommendation` topic legitimately has ~10% fewer
records than there are closed Browsing Sessions. Phase 5's HA Drill requires "no
gap in the recommendation topic", so that check must compare against emitted
Recommendations, not against Browsing Sessions, or a correct suppression will
read as a gap.

Seven design decisions were settled before any code, and the two that a later
phase would otherwise rediscover are these. The CEP pattern is **abandoned
cart** (VIEW, ADD_TO_CART, no CHECKOUT within 30s), not the "viewed a competitor,
went idle" pattern the design spec's coverage map still names, because at the
generator's real rates that one needs a 60 second window spanning ten session
gaps. And `ProductChange` never enters the job graph at all: the deserializer
reads the `type` discriminator and collects only `PriceChange`, which closes the
sealed-interface warning Phase 3 surfaced.

**Surfaced for Phase 5 by Task 0, and nothing else records it.** `runtimeOnly`
does not keep a jar out of a Shadow fat jar, because Shadow builds from the
runtime classpath. Five dependencies now carry that scope
(`flink-streaming-java`, `flink-clients`, `flink-statebackend-rocksdb`,
`flink-connector-base`, `flink-cep`), and bundling any of them beside a
distribution that already loads them is a duplicate-class failure. Phase 5 needs
a dedicated configuration or an explicit exclusion set. Separately,
`flink-s3-fs-hadoop` was confirmed to live in `opt/`, not `lib/`, which is the
plugin-directory move ADR 0001 predicted.

## Phase 5: Operator and HA — ✅ done

Design and plan both written and approved:
[design](../specs/2026-08-30-operator-and-ha-design.md),
[implementation plan](2026-08-30-phase-5-operator-and-ha.md). The plan runs 12
tasks; check its Progress table for the live position.

Every design decision was settled **before** the plan was written, with the
evidence and the command that produced it, so no task carries a verification
gate. That is a change from Phases 3 and 4, which both opened with a Task 0
check.

- ✅ Task 0: Kafka internal listener. `- name: plain`, port 9092, `type: internal`,
  `tls: false`, added beside the untouched `external` listener.
  **This was a blocker nothing had recorded.** The `Kafka` CR had one listener,
  and the in-cluster bootstrap Service therefore exposed only
  `tcp-replication 9091`, which is Strimzi's broker-to-broker listener. A Flink
  pod had nothing to connect to.
  The reason the `external` listener could not simply be reused from inside is
  the **two-step bootstrap**: a client asks the bootstrap address who has what,
  then reconnects to the addresses it is handed. `external` advertises
  `localhost:30017-30019`, deliberately, for host-side clients. Inside a pod
  `localhost` is that pod, so a TaskManager would connect to itself. After the
  change the advertised addresses are
  `personalization-brokers-N.personalization-kafka-brokers.kafka.svc:9092`.
  Gate passed with a real in-cluster probe, not a Service listing: a `kcat` pod
  in the `kafka` namespace listed **3 brokers** on
  `personalization-kafka-bootstrap.kafka.svc.cluster.local:9092`.
  **One naming fact worth keeping.** Strimzi does **not** derive the Service port
  name from the listener's `name`. The listener is `plain`; the port is
  `tcp-clients`, a fixed name per listener role. Do not match on the listener
  name when selecting a Service port.
- ✅ Task 1: Shadow fat jar, allowlist scoped. `com.gradleup.shadow` 9.6.1, since
  `com.github.johnrengelman.shadow` is unmaintained and does not support Gradle 9.
  Five content checks pass and the 27 Phase 4 tests stay green, read from the XML
  report rather than from `BUILD SUCCESSFUL`.
  **An allowlist, not an exclusion list**, and the reason is the failure mode.
  Excluding the five `runtimeOnly` Flink dependencies does not exclude their
  transitive `flink-core`, `flink-runtime`, and `flink-shaded-*`, so those still
  land in the jar. A missed exclusion is a **silent** duplicate class that wins a
  scan order. A missed allowlist entry is a `NoClassDefFoundError` at startup,
  naming the class.
  The Gradle 9 idiom is `dependencyScope('bundled')` plus
  `resolvable('bundledClasspath')`, with `implementation.extendsFrom
  configurations.bundled` so the compile classpath is unchanged. Confirmed
  working: `compileJava` stayed `UP-TO-DATE`.
  Exactly two dependencies are bundled, `:domain` and `flink-connector-kafka`.
  Verified absent: `flink-streaming-java`, log4j, the S3 plugin, and `flink-cep`,
  all at count **0**.
  **A result that looks wrong and is not.** The jar does contain
  `org/apache/flink/streaming/connectors` (71) and
  `org/apache/flink/streaming/util` (3). Those ship inside
  `flink-connector-kafka:5.0.0-2.2` under legacy package names. Confirmed with
  `javap` that `flink-dist-2.2.0.jar` contains neither. A package name is not
  evidence of which artifact a class came from.
- ✅ Task 2: the image, and loading it into `kind`. `apps/pipeline/Dockerfile`
  and `scripts/build-image.sh`. Tag `lab/personalization-pipeline:0.1-b606416-dirty`,
  both files verified inside the image before loading, present on all three
  workers.
  `RUN cp` rather than `COPY` for the S3 plugin, because the source file is
  already inside the base image and `COPY` reads the build context.
  `ENABLE_BUILT_IN_PLUGINS` was verified to work in the 2.2.0 entrypoint and
  rejected anyway: it moves one of the two required files, so it would mean a
  second mechanism for the job jar.
  **Two defects found by running it, both now in the plan.** `kind load` returns
  **before** containerd finishes registering the image on every node, which
  produced a false "missing on worker2"; the script now retries for 30s per node.
  And running the script as `./scripts/build-image.sh | tail -30` reported
  `exit code 0` while the script printed a red failure, because a pipeline's exit
  code is the last command's. That one is general, not specific to this script.
  **Surfaced for Task 5.** Rebuilding produces a different image **digest** for
  the same tag (`dbc3f08` then `79c7cf38`). Docker builds are not
  byte-reproducible. `kind load` overwrites by tag so the nodes stay correct, but
  a tag is a label and not an identity, and the `-dirty` suffix is currently real.
- ✅ Task 3: namespaces and the credentials Secret. `manifests/flink/namespaces.yaml`
  and `scripts/bootstrap-flink-secret.sh`. Both `personalization-blue` and
  `personalization-green` exist; green is deliberately empty until Phase 7, and
  is created now only so Task 4's chart puts the `flink` ServiceAccount, Role,
  and RoleBinding in it without a later re-sync.
  Gate passed by running the script **twice**: the first run created both
  Secrets, the second changed nothing. Both `access-key` and `secret-key` were
  compared byte for byte against `storage-configuration` in `minio-tenant` and
  match. The guard is **per namespace**, not global as in
  `bootstrap-minio-secret.sh`, because a partial run could otherwise never be
  repaired by re-running.
  The script copies and never generates. Generating would rotate the credentials
  under a running MinIO Tenant, and the symptom is an S3 403 from Flink hours
  later that reads like a MinIO fault.
  `manifests/flink/namespaces.yaml` sits **above** `manifests/flink/blue/` on
  purpose. Task 5's Application syncs that subdirectory with `prune: true`, so a
  namespace file inside it would let the Application delete the namespace its own
  resources live in.
  **Two kubectl facts this task settled.** `kind` is CamelCase and
  case-sensitive: `kind: namespace` is rejected with
  `no kind "namespace" is registered for version "v1"`, because the lowercase
  form is a command-line resource name, not a manifest kind. And
  `--dry-run=client` did **not** catch it, since it never contacts the API
  server; it printed two `created (dry run)` lines for a type that does not
  exist. Use `--dry-run=server` by default.
- ✅ Task 4: the operator, as an ArgoCD Application. `flink-kubernetes-operator`
  1.15.0 from `https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/`,
  which serves an `index.yaml` and so is a valid Helm repo, not only a download
  page. Synced and Healthy, operator pod `2/2 Running`, four CRDs installed.
  Gates read from the **installed** CRD rather than the chart tarball: the
  `flinkVersion` enum ends `"v2_0","v2_1","v2_2"`. RBAC landed in both job
  namespaces, and `watchNamespaces` also gave the operator **namespaced**
  `flink-operator` Roles there instead of cluster-scoped ones.
  The pod's two containers are `flink-kubernetes-operator` and `flink-webhook`.
  The second is what `flink-operator-serving-cert` serves TLS for, which is why
  cert-manager is a real prerequisite and why an unready certificate would fail a
  Task 5 apply with a message about TLS rather than about Flink.
  **The Application is not applied by hand.** `root.yaml` watches
  `path: manifests/argocd-apps`, so it must be committed and pushed, and root
  creates the child Application. Applying it with `kubectl` would create an
  Application outside the app-of-apps tree, which is self-inflicted drift in the
  phase whose point is observing drift deliberately. This differs from Task 3's
  namespaces, which **are** applied by hand, because nothing in ArgoCD claims
  `manifests/flink/`.
  **A permanent OutOfSync, diagnosed rather than tolerated.** The Application sat
  OutOfSync while Healthy, on all four CRDs. The whole diff was three lines of
  `> priority: 0`: the chart omits `priority` on `additionalPrinterColumns`, the
  API server defaults it, and a text diff then reports drift forever. Fixed with
  `argocd.argoproj.io/compare-options: ServerSideDiff=true`, which compares
  against a dry-run apply so both sides carry the same defaults. Left unfixed it
  would have been worse than cosmetic, since this project uses OutOfSync as the
  **signal** in Drill D.
  `ServerSideApply=true` was added as well, and for its **own** reason, not as a
  dependency: an earlier claim that `ServerSideDiff` requires it was wrong, and
  the docs record that the old structured-merge strategy which `ServerSideApply`
  used to select was discontinued precisely because it mishandled CRD defaults.
  The real justification is measured: client-side apply stores the whole object in
  a `last-applied-configuration` annotation capped at 262144 bytes, and
  `flinkdeployments` was using **166871**, 64% of the limit, on a CRD that grows
  each release. After the change, `managedFields` shows
  `argocd-controller/Apply` instead of `/Update` and the annotation is **0 bytes**.
  **How to read a diff without the argocd CLI**, which is not installed and which
  README deliberately does not install:
  `kubectl -n argocd exec argocd-application-controller-0 -- argocd --core app diff <app>`.
  The binary ships in the image. The `argocd-server` pod's ServiceAccount lacks
  the RBAC for it; the application controller's does not.
- ✅ Task 5: the `FlinkDeployment`, the Service, the PDB. **All fourteen steps
  verified on the cluster.** The job reached `RUNNING` / `STABLE` on 2026-09-07
  on image `0.1-0bd7f52`.
  Five pods, and the Zone spread held on both rules: JobManagers on `zone-c` and
  `zone-a`, TaskManagers 1/1/1 across all three Zones. The selectors guessed in
  Step 4 were right, confirmed from live labels
  `app=personalization,component=jobmanager`. A third label exists,
  `type=flink-native-kubernetes`, useful for finding everything Flink created.
  Nine vertices RUNNING at parallelism 6. Checkpoints landing at
  `s3://checkpoints/phase-5/<jobid>/chk-4`, which is the single fact proving the
  whole S3 chain: plugin loaded, credentials found, MinIO reachable in-cluster,
  bucket writable. One checkpoint `failed`, almost certainly the first attempt at
  startup, still to be looked at. `HTTP 200` on `localhost:30011`. PDB reports
  `ALLOWED DISRUPTIONS: 1`.
  **The Service name matters.** Flink creates `personalization-rest` itself, from
  `kubernetes.cluster-id`. The hand written NodePort Service is
  `personalization-flink-dashboard`, deliberately not that name. Both exist side
  by side and select the same pods, which is fine, because a Service selector is
  a filter and not a claim of ownership.
  **Three runtime failures, all in the same category.** Each lives in a seam that
  only closes inside the container. Gradle passed, the image built,
  `--dry-run=server` passed, and ArgoCD said `Synced` and `Healthy` through all
  three. None could fail under `MiniCluster`.
  1. `UnsupportedClassVersionError`, class file 65.0 against a runtime reading up
     to 61.0. `apps/` compiles at Java 21 and `flink:2.2.0` ships Temurin 17.
     Fixed with `FROM flink:2.2.0-java21`, because `:domain` is bundled into the
     Shadow jar and targeting Java 17 would have meant editing
     `apps/domain/build.gradle`.
  2. `MalformedURLException: unknown protocol: local`, thrown from
     `env.execute()`. The operator renders `job.jarURI` into the same
     `config.yaml` the job re-reads, as
     `pipeline.jars=local:///opt/flink/usrlib/pipeline.jar`. Handing that back to
     Flink makes `ExecutionConfigAccessor.getJars` call `new URL()` on a scheme
     `java.net.URL` does not know. Fixed with
     `flinkConfig.removeConfig(PipelineOptions.JARS)`.
  3. `UnsupportedFileSystemSchemeException` for scheme `s3`, while initialising
     the HA checkpoint store. `FileSystem.initialize(config, null)` rebuilds the
     filesystem registry loading **no plugins**, and ADR 0001 put
     `flink-s3-fs-hadoop` in `plugins/` where it is the only copy: `/opt/flink/lib`
     holds no s3 jar. The `null` discarded the filesystem the entrypoint had
     already registered. Fixed with
     `PluginUtils.createPluginManagerFromRootFolder(flinkConfig)`.
  **A global constraint was amended, not broken quietly.** The plan said "no file
  under `apps/` changes except `apps/pipeline/build.gradle`". Two lines of
  `PersonalizationJob.java` changed, both inside `flinkConfiguration()`, both
  no-ops under `MiniCluster`, `:pipeline:test` green after each. The constraint
  now reads: no change may alter the job graph, the operators, or their
  semantics. The plan records the amendment and the reason.
  **A workflow trap.** `root.yaml` uses `targetRevision: HEAD`, which resolves to
  the repository's **default branch**, `master`, not the locally checked out
  branch. Work committed to `phase-2` deploys nothing and reports no error
  anywhere: the Application simply never appears. Push with
  `git push origin phase-2:master`. ArgoCD then polls on its own schedule, so
  `Synced` beside a stale revision is normal for a few minutes. Always read the
  revision beside the status, and force a refresh with
  `kubectl -n argocd exec argocd-application-controller-0 -- argocd --core app get <app> --refresh`.
  **Step 12, HA metadata, checked separately from checkpoints.** Both sides carry
  data: the `personalization-cluster-config-map` in Kubernetes, and
  `s3://checkpoints/phase-5-ha/` in MinIO. The split is not incidental. A
  ConfigMap is small and a JobGraph is not, so Flink persists the metadata to
  `high-availability.storageDir` and stores **only a pointer** in Kubernetes.
  Checking one and assuming the other would let Drill B pass while proving
  nothing, since a fresh JobManager starting an empty job also reaches `RUNNING`.
  **Step 13, Recommendations from the cluster.** Present on the topic, with
  `reason` values `price-drop` and `cart-abandoned`, so at least two operators
  are producing.
  **Two facts about the `recommendation` topic that Task 6 must handle.**
  1. Offsets step by **2**, not 1. Every other offset is a transaction commit
     marker: it takes an offset but is not a record. That is the `EXACTLY_ONCE`
     sink. A snapshot that counts offsets rather than records is wrong by a
     factor of two, and `-X isolation.level=read_committed` is not optional.
  2. The log start offsets are **not zero**: 102, 103, and 86 against end offsets
     108, 109, and 92. So `kcat -o -20` asks for an offset below the log start,
     gets `Broker: Offset out of range`, silently resets to END, and prints
     nothing. Confirmed as real log start offsets rather than transaction
     filtering, because `read_uncommitted` reports the same first offsets. The
     topic has no custom retention and the brokers use `jbod` storage, so the
     cause is **not yet established**. Worth answering before Task 6, whose
     instrument reads this topic.
- ✅ Task 6: the gap check instrument. `scripts/recommendation-snapshot.sh`, two
  modes: `snapshot <out>` and `compare <before> <after>`. Calibrated with the
  generator live on 2026-09-07: 158 identities then 159, zero gaps, zero
  duplicates, exit 0. The growth case is the one that matters and it is proven,
  `comm -23` ignores identities that appear only in AFTER.
  **The identity is `(shopperId, generatedAt)`**, read straight off the Kafka
  record with `kcat -f '%k %T\n'`. No JSON parsing.
  `RecommendationSerializationSchema` puts `shopperId` in the key and
  `generatedAt` in the record timestamp. `generatedAt` is the Browsing Session's
  window end, an event-time value, so replaying the same input reproduces the
  same pair.
  **The topic is compared against itself, never against the input.** The pipeline
  suppresses output twice on purpose, out-of-stock at 9.6% and `UNMATCHED`
  candidates dropped, so comparing against closed Browsing Sessions would make
  correct suppression read as a Drill failure.
  **A correction to the plan's Step 2.** The plan wrote the snapshot with
  `sort -u` and then looked for duplicates with
  `cut -d' ' -f1,2 "$AFTER" | sort | uniq -d`. `%k %T` is two fields, so that
  `cut` is the whole line, and on a deduplicated file the check can never fire.
  It would have reported "no duplicates" on a Drill that genuinely broke
  exactly-once. The script writes `sort` instead and takes both views from the
  one file: `comm -23 <(uniq before) <(uniq after)` for the gap, `uniq -d after`
  for duplicates. `snapshot` prints records and identities separately so a
  divergence shows immediately.
  `export LC_ALL=C` on both `sort` and `comm`, because `comm` compares byte for
  byte and rejects input sorted under another collation.
  **Two rules every Drill inherits.** The generator must be running, or a killed
  TaskManager has nothing in flight to lose and a zero gap proves nothing. And
  Kafka offsets must never be reset to replay: identity is derived from event
  time, so a replay writes a genuine duplicate that stays in the topic and fails
  every later Drill. Flink ignores a consumer-group reset anyway, since source
  offsets live in checkpoint state and restored state overrides any configured
  initial position.
  **One sensitivity concern, open.** Only one Recommendation appeared in the 30
  second calibration window against roughly 50 Clicks per 10 seconds. Session
  windows need a 6 second silent gap per Shopper, which rarely arrives under
  continuous load. A gap check is only as sensitive as the identities produced
  during the Drill window, so read the per-operator counts at `localhost:30011`
  before Drill A.
- ✅ Task 7: Drill A, kill a TaskManager. Runbook at
  [phase-5-drill-a-taskmanager-kill.md](../../runbooks/phase-5-drill-a-taskmanager-kill.md).
  Two runs on 2026-09-07. Restored from `chk-784` then `chk-809`, `is_savepoint`
  false both times, **15 seconds recovery in both runs**. Gap 0, duplicates 0,
  identities 506 to 530. Zone spread survived: the replacement TaskManagers
  landed one per worker.
  **Why the whole job fails, not just the dead pod.** The surviving TaskManagers
  hold state from a later point in time than a fresh one would, so resuming from
  mixed vintages would silently corrupt every windowed aggregate. Flink restarts
  every task from the last checkpoint instead. The 10 second interval bounds the
  replay, and the 15 second wall clock is dominated by pod scheduling and
  TaskManager registration rather than by replay.
  **`ScheduleAnyway` earned its place.** With `DoNotSchedule` on the TaskManager
  spread constraint, a replacement pod could have been left `Pending` and blocked
  the recovery this Drill exists to observe.
  **Three corrections, folded back into the plan.**
  1. `kubectl delete pod -l component=taskmanager ... | head -1` deletes
     **every** matching pod. `head -1` truncates kubectl's output, not the
     deletion, and the API calls are already made when the pipe runs. Both
     recorded runs killed two pods this way. Select the target first with
     `-o name | head -1`, then delete by name.
  2. Read the restore from the REST API, `/jobs/<jid>/checkpoints` field
     `latest.restored`, not from the log. The log is fragile three ways at a 10s
     checkpoint interval: `--tail=300` does not reach back far enough, only the
     **leader** JobManager logs the restore so half of a
     `-l component=jobmanager` fetch is standby noise, and a restarted
     JobManager loses its previous container's log.
  3. **The gap direction is structurally near-untriggerable.** Committed Kafka
     records do not disappear, so `comm -23 BEFORE AFTER` is empty whatever
     happens, barring retention. The restore record and the duplicate check are
     what actually prove recovery. A clean `no gap` line is a regression guard,
     not evidence. This applies to Drills B and C too.
  **The PDB is not involved and that is correct.** `kubectl delete pod` is an
  involuntary disruption. A PodDisruptionBudget constrains the eviction API,
  which is `kubectl drain`. `ALLOWED DISRUPTIONS` stayed at 1 throughout. Drill C
  is where the PDB is exercised.
  **Sensitivity, measured before the Drill.** Roughly **2 Recommendations per
  minute**: 10964 Clicks became 173 through two deliberate chokepoints, the 6
  second session window at 24 to 1 and Phase 4's suppressions at 34 to 1. The
  generator was left at its default `click-rate=5.0`, on the grounds that every
  Drill should run against the same workload and that more volume would not make
  the gap direction any more sensitive.
- ✅ Task 8: Drill B, kill the leader JobManager. Runbook at
  [phase-5-drill-b-jobmanager-kill.md](../../runbooks/phase-5-drill-b-jobmanager-kill.md).
  Killed the leader `zctrk` on 2026-09-07. `leaderTransitions` went 1 to 2,
  `holderIdentity` changed, the previous standby `hw6ct` took over, and a
  replacement standby `p62r4` landed on a different worker so `DoNotSchedule`
  held. **The job id was unchanged**, `ccf3abb44f948e42142eaac8a5edd1a4`,
  restored from `chk-920`. Recovery 15 seconds. Gap 0, duplicates 0, identities
  602 to 626.
  **The job id is the primary evidence, not the pod count.** If the survivor had
  started a fresh job, two healthy JobManager pods would still be listed and the
  Drill would have proved nothing.
  **The TaskManagers survived.** The new leader logged
  `Recovered worker personalization-taskmanager-2-8 ... registered` and re-adopted
  the existing pods. None was killed or rescheduled. That is the sharpest
  contrast with Drill A, where every slot was rebuilt.
  **Drill B's signature log line** is `Job <jobid> was recovered successfully`,
  which Drill A never produces, because Drill A never lost the coordinator. The
  `KubernetesCheckpointRecoveryFactory.createRecoveredCompletedCheckpointStore`
  frame beside it is the HA store being rebuilt from `phase-5-ha`.
  **Identifying the leader is harder than the plan assumed, and the plan is now
  corrected.** `holderIdentity` in the lease annotation is a UUID Flink generates
  per JobManager **process**. It matches neither pod's UID. Grepping the logs for
  it fails too, because the standby observes the election and logs the winner as
  well, measured at 1 line against 3. What works: the leader is the only
  JobManager running the `CheckpointCoordinator`, so
  `kubectl logs <pod> --tail=-1 | grep -c "Completed checkpoint"` is non-zero on
  exactly one pod. In k9s, filter the log view on `Completed checkpoint` rather
  than `was granted leadership`, since the latter is written once at election
  time and is usually outside any tail. The pods view itself is no help: both
  JobManagers carry identical labels and no leader marker.
  **`leaseDuration: PT15S`** is why the standby cannot act for up to 15 seconds.
  The observed sequence was 10 seconds from kill to the job noticing, then 15
  seconds to running. **15 seconds is now the recovery figure in all three Drill
  runs**, dominated by TaskManager registration and re-scheduling rather than by
  replay.
  **`leaderTransitions` is cumulative.** It read 1 before this Drill, from a
  leadership change earlier in the day during the image rollouts. Record the
  before value rather than assuming 0.
- ✅ Task 9: Drill C, drain a Zone. Runbook at
  [phase-5-drill-c-zone-drain.md](../../runbooks/phase-5-drill-c-zone-drain.md).
  Drained `personalization-lab-worker2`, Zone `zone-b`, on 2026-09-07. Gap 0,
  duplicates 0, identities 725 to 801. **The job stayed `RUNNING` throughout**:
  only one of six slots moved, and Flink redeployed that subtask without failing
  the job. No restart, no checkpoint restore.
  **The plan's prediction was wrong, and the arithmetic says why.** It expected
  `personalization-pdb` to refuse an eviction and the drained Zone's JobManager
  to sit `Pending`. Neither can happen at 2 replicas across 3 Zones:
  `ALLOWED DISRUPTIONS` is `2 - 1 = 1` so the budget has slack, and a spare Zone
  always exists so `maxSkew: 1` stays satisfiable. The replacement scheduled onto
  `worker` in zone-a. The plan is corrected in place.
  **The refusal came from Strimzi, not from Flink**, and is captured verbatim:
  `Cannot evict pod as it would violate the pod's disruption budget`, repeating
  every 5 seconds forever.
  **A genuine drain deadlock, which is the better finding.**
  `personalization-kafka` has `minAvailable: 5` over 6 pods. `brokers-4` was
  evicted first, allowed because 6 minus 1 is 5. It then could not reschedule:
  its PersistentVolume is pinned to `worker2` by `kind`'s local-path provisioner,
  and `worker2` was cordoned by the drain itself. `FailedScheduling` accounted
  for all six nodes: 1 unschedulable, 2 failing PV node affinity, 3 tainted
  control planes. With `brokers-4` Pending, available fell to 5,
  `ALLOWED DISRUPTIONS` became 0, and `brokers-5` could never be evicted.
  **A strict PDB plus node-local storage makes a node undrainable**, not slow.
  Only `uncordon` breaks it.
  **`minAvailable` is a rule about the state after the eviction**, not about the
  state now. A budget can be satisfied and immovable at the same time.
  **`ScheduleAnyway` earned its place.** The evicted TaskManager's replacement
  landed on `worker3`, which already had one, making the distribution 2/0/1.
  `DoNotSchedule` would have left it `Pending` and stalled the recovery this
  Drill exists to observe. `DoNotSchedule` on the JobManagers held: two pods, two
  Zones, none Pending.
  **`drain` evicts every pod on the node, not one.** Ten pods lived on `worker2`.
  Record `kubectl get pdb -A`, not one namespace: the refusal came from a
  namespace the plan never mentioned.
  **A Phase 1 defect this Drill exposed.** `brokers-4` and `brokers-5` shared
  `worker2`, so one drain removed two thirds of the Kafka cluster and left it at
  exactly `min.insync.replicas: 2` with no margin.
  `manifests/strimzi/kafka-cluster.yaml` carries **no**
  `topologySpreadConstraints`, while the `FlinkDeployment` carries them on both
  roles. Worth fixing in the Phase 1 manifests. After `uncordon`, Kafka caught
  `brokers-4` back up on its own with no under-replicated partitions.
- ❌ Drill D, ArgoCD Lua actions and drift, formerly Task 10. **Removed by decision on
  2026-09-07**, not attempted. It was the only item in this phase testing the
  GitOps control loop rather than Flink recovery, and its expensive half was
  ArgoCD Lua plumbing. Two consequences, both real and both carried forward:
  `automated.selfHeal: false` is now set on every Application without ever being
  validated, and `upgradeMode: savepoint` has never been exercised, so
  `s3://checkpoints/phase-5-savepoints/` is expected to be empty and every
  restore so far read `is_savepoint: false`. **Phase 7's Promotion suspends with
  a savepoint**, so it should prove that operation before building on it:
  `kubectl patch flinkdeployment personalization -n personalization-blue
  --type=merge -p '{"spec":{"job":{"state":"suspended"}}}'`.
  The design spec's coverage map still lists Drill D and the three Lua actions,
  so the spec and the plan now disagree until the documents task reconciles them.
- ✅ Task 10: Documents, renumbered from 11. Knowledge doc, ADR 0007 amendment,
  four new `CONTEXT.md` terms, the runbook index, and this section.

**What Phase 6 and Phase 7 need from here.**

- **Parallelism is 6 with `taskmanager.numberOfTaskSlots: "2"`, giving exactly 3
  TaskManagers, one per Zone.** Phase 6's autoscaler changes parallelism, and
  that changes the pod count by the same division. The `256mb` network buffer
  setting is the headroom it must stay inside: 8192 buffers at a 32kb segment
  size, against a requirement that grows with the **square** of parallelism
  because every upstream subtask needs a channel to every downstream one.
- **`upgradeMode: savepoint` is set and both namespaces exist**, so Phase 7's
  Promotion has what it needs declared. **It has never been exercised.** Drill D
  was removed, every restore in this phase read `is_savepoint: false`, and
  `s3://checkpoints/phase-5-savepoints/` is expected to be empty. Prove it before
  building Promotion on it:
  `kubectl patch flinkdeployment personalization -n personalization-blue
  --type=merge -p '{"spec":{"job":{"state":"suspended"}}}'`.
- **The operator chart ships a `FlinkBlueGreenDeployment` CRD**, one of the four
  installed. Phase 7 must evaluate it against
  [ADR 0006](../../adr/0006-blue-green-native-mode.md) rather than assume the
  hand-rolled Promotion is the only option.
- **The image tag must move on every code change.** `scripts/build-image.sh` tags
  `0.1-<short sha>` and appends `-dirty` when `apps/` has uncommitted changes.
  A stale tag with new code is the failure that looks like a Flink bug, because
  `imagePullPolicy: IfNotPresent` means the node keeps what it already has. Commit
  first, rebuild, then update `spec.image`.
- **`kind load` overwrites by tag, and a rebuild produces a different digest for
  the same tag.** Observed 2026-08-31 as `dbc3f08` then `79c7cf38`. Docker builds
  are not byte-reproducible. A tag is a label, not an identity.

**One open defect, from Drill C.** `manifests/strimzi/kafka-cluster.yaml` carries
no `topologySpreadConstraints`, so `brokers-4` and `brokers-5` shared `worker2`.
One drain removed two thirds of the Kafka cluster and left it at exactly
`min.insync.replicas: 2`. The `FlinkDeployment` above it is Zone-spread on both
roles. Fix belongs in the Phase 1 manifests.

**One documentation divergence.** The design spec's coverage map still lists
Drill D and the three ArgoCD Lua actions. The plan no longer does. The spec is
the older document and was not rewritten, on the grounds that a spec records what
was designed and `status.md` records what was built.


**One step still open from Task 0.** Step 5, re-confirming the host-side
`external` listener after the broker roll, was never run. One command closes it:
`kcat -b localhost:30016 -L | head -5`.


## Phase 6: Autoscaling — ✅ done

Design and plan both written:
[design](../specs/2026-09-07-autoscaling-design.md),
[implementation plan](2026-09-07-phase-6-autoscaling.md). The plan runs 10 tasks, and
its Progress table is kept in step with this file. A
[knowledge doc](../../knowledge/phase-6-autoscaling.md) exists already, written
ahead of the Drills rather than after them.

**Scope changed from the phase plan.** Phase 6 is 6a plus Karpenter. The
Standalone Variant and KEDA are **dropped by decision**, not deferred. The lab
loses external-metric-driven autoscaling, which was ADR 0005's stated reason for
keeping KEDA. Nothing in Phase 7 depends on it, since ADR 0006 runs blue/green on
the Native Variant. **ADR 0005's Decision was amended on 2026-09-09**; its analysis
stays correct and is load-bearing.

- ✅ Task 0: Baseline capture, 2026-09-08. Read-only, and every later gate compares
  against it. 9 vertices at `PAR 6` / `MAXPAR 128`, seven of nine names carrying
  `Sink: Print to Std. Out`, the overrides field empty, and **5.0 Clicks/sec
  measured from partition offsets** rather than read off `--click-rate`.
  **Two facts the design did not have.** Host headroom has **halved** since the
  spec was written: 6.7 GiB then, 2.5 GiB now, with TaskManagers at 1105/1036/1116
  Mi against the 887-946 Mi Phase 5 recorded. The design's rejection of
  parallelism 8 is safer than written; the margin behind the accepted plan is
  thinner. And **Zone spread has drifted**: TaskManagers sit on `worker` (1) and
  `worker3` (2), none on `worker2`. Pod names `taskmanager-2-7`, `-2-8`, `-3-1`
  show replacements since Phase 5's recorded 1/1/1. Expected under
  `whenUnsatisfiable: ScheduleAnyway`, and re-verification is out of scope, but
  the Phase 5 record is stale.
- ✅ Task 1: The generator's 1000/sec ceiling removed, 2026-09-08.
  `SkewedEventStream.start` now fixes the period at 10 ms and varies the batch,
  instead of one tick per Click with `Math.max(1, ...)` flooring the period at 1 ms.
  The ticker stays single-threaded, because event-time ordering depends on ticks
  firing sequentially, and `Instant.now()` stays inside `tick()`.
  **Gate: 2507.9 Clicks/sec against a requested 2500**, measured from offsets.
  **The first measurement was wrong and the failure is worth keeping.** It read
  3311.6/sec for a requested 2500, a 32% overshoot that looked like a bug in the
  batching. It was two generators running at once: a `pkill` had reported success
  without killing anything, so an 800/sec run was still live. 800 + 2500 = 3300.
  **Check the process list before believing a rate measurement.**
- ✅ Task 2: Key cardinality, 2026-09-08. `Catalog`'s constants became
  `shopperIds(n)` and `productIds(n)`; `GeneratorConfig` gained `--shopper-count`
  (2000) and `--product-count` (200). `ClickFactory` and `ProductChangeFactory`
  were untouched, since both already take `List<String>`.
  Gate: **2000 distinct Shoppers** (max index 2000) and **200 distinct Products**
  (max index 200), uniform.
  **A measurement trap worth keeping.** Reading the last 20,000 messages of
  `product-change` reported only **85** distinct Products. That was not a defect:
  15,466 of those 20,000 were pre-change messages over the old P1..P10 catalogue,
  because the topic's tail spans both eras. Consuming **live** traffic for 15
  seconds gave 200. On a compacted-history topic, sample the present, not the tail.
  **A coupling that is now live in the defaults.** Session length is
  `e^(6 × clickRate ÷ shopperCount)`. At the old `--click-rate=5` default with
  2000 Shoppers that is `e^0.015`, so every Session is one Click and the window
  branch emits almost nothing. **Running the generator bare now produces a
  pipeline that looks broken.** Hold `shopperCount ≈ 2.5 × clickRate`.
- ✅ Task 3: The `FlinkDeployment` config changes, 2026-09-08. Eight autoscaler
  keys with `scaling.enabled: "false"`, `pipeline.max-parallelism: "120"`,
  `--debug-prints=false`, parallelism 6 to 2, the three `phase-6` state prefixes,
  and `upgradeMode: stateless` travelling in the same edit because alone it is an
  `IGNORE`-level diff and triggers no reconciliation at all.
  Verified locally: `--dry-run=server` reports `configured`, and 27 pipeline tests
  pass, read from the XML report.
  **`jobmanager.scheduler: Adaptive` was already set**, at
  `flinkdeployment.yaml:31`, put there in Phase 5 for an unrelated reason. It is
  also the autoscaler's hard prerequisite. The enum is `Default` / `Adaptive` /
  `AdaptiveBatch`, so the capital is correct despite the operator docs writing it
  lowercase.
  **The deployment now sits at `upgradeMode: stateless` until Drill F restores
  `savepoint`.** Any other spec edit in that window discards state silently.
  **Pushed to `master` on 2026-09-08** as `git push origin phase-2:master`, and
  all three Step 9 deploy gates were run and passed: empty
  `upgradeSavepointPath` beside `RUNNING`, `MAXPAR 120` / `PAR 2` with no
  `Sink: Print to Std. Out` vertex, and one TaskManager with checkpoints under
  `s3://checkpoints/phase-6/`.
- ✅ Task 4: Drill E, 2026-09-08. Ran at `--click-rate=800
  --product-change-rate=400`. **Gate passed.**
  [Runbook](../../runbooks/phase-6-drill-e-dry-run.md).
  Two `SCALINGREPORT` events, fifteen seconds apart, both carrying
  `Scaling execution disabled by config`, which is the dry run naming itself.
  **CepOperator** asked for `2 -> 5`, then `2 -> 6`. **Interval Join** asked for
  `2 -> 3` twice. Both take every Click and both sit under a `keyBy`, which is
  what the gate wanted. One TaskManager throughout, `RESTARTS 0`, `AGE` 7m59s to
  16m.
  **The session window asked for nothing, and that is the best result in the
  Drill.** It reads from the same `keyBy(shopperId)` as CepOperator, so both get
  identical Clicks at an identical rate. They differ only in cost per Click: CEP
  runs a state machine, `SessionAggregator` appends to window state and defers
  the work to the session gap. An autoscaler reading input rate would have scaled
  both the same. One reading busy time separates them. It separated them.
  **Still confirm `LOAD` on `c9235a26…` is under 0.6** before Drill F.
  **`2 -> 6` is exactly `job.autoscaler.vertex.max-parallelism: "6"`.** It may be
  the number wanted or the largest number allowed; the report cannot tell you
  which. In Drill F a job pinned at the ceiling looks like a stuck autoscaler and
  is not one.
  **The report's own numbers do not rebuild its recommendation.** 850.59 over
  utilization 0.6 needs 3 subtasks, not 5. The missing term is
  `catch-up.duration: 5m`, which adds capacity to clear backlog.
  **Vertex IDs must be captured before a restart**, since the report names
  vertices by hex ID only and the IDs change with the job graph. Reading the
  mapping is now step 3 of the runbook's procedure.
- ✅ Task 5: Drill F, 2026-09-08. **Scaling gates passed.**
  [Runbook](../../runbooks/phase-6-drill-f-scale-up.md). **Three rescales.**
  CepOperator `2 -> 6` at 16:03:57, then Interval Join `1 -> 4` and
  Co-Keyed-Process `1 -> 2` at 16:08:14, 4m17s later. Three TaskManagers, and
  `taskmanager-1-1` kept its name, its 112 minute age and `RESTARTS 0`. Proof it
  was scaling and not a failover: the operator logged `In-place scaling
  triggered` twice, checkpoints show 3 restores, and the exception history is
  **empty**. A failover cannot restore without leaving an exception.
  **The third rescale created no pod, and that is worth remembering.** Slots
  needed is the largest parallelism of any one vertex, still 6 from CepOperator,
  so parallelism 4 and 2 fit inside the existing slots by slot sharing. Watching
  `kubectl get pods` alone would have missed the rescale entirely.
  **Step 6 answered: the overrides field is EMPTY.** The autoscaler used
  `PUT /jobs/:jobid/resource-requirements` and never wrote to the CR. Git owns
  the spec, the autoscaler owns runtime state, so no `ignoreDifferences` is
  needed in `flink-job-blue.yaml`. Its own memory lives in the
  `autoscaler-personalization` ConfigMap. **Consequence for Phase 7:**
  `spec.job.parallelism` is still 2, so any full restart drops the job back to 2
  and the autoscaler must climb again.
  **Step 4's instrument does not exist on Flink 2.2.0.**
  `GET /jobs/:jobid/rescales/history` is a Flink **2.3** feature and 404s here,
  so `web.adaptive-scheduler.rescale-history.size: "10"` from Task 3 is inert.
  Flink ignores unknown config keys silently. The plan's Task 5 Step 4 now
  carries the correction.
  **Drill E's load was too low to force a real scale-up.** At 800 Clicks/sec in
  steady state the job wants parallelism 1 to 2, and the autoscaler had scaled
  *down* to 1 before this Drill. Drill E's `2 -> 5` was a **catch-up**
  recommendation, driven by Kafka backlog on a cold JVM, not by steady load.
  CepOperator's per-subtask capacity was 551/sec then and about 1250/sec once
  warm. Scale-up needed `--click-rate=4000 --shopper-count=10000`.
  **Raise `--shopper-count` with `--click-rate` every time.** Session length is
  `e^(6 × clickRate ÷ shopperCount)`; 4000 Clicks over 2000 Shoppers is `e^12`.
  **The generator tops out near 2650 Clicks/sec.** Asked for 4000, delivered
  2648, measured off the source vertex. Task 1 removed the 1000/sec ceiling and
  verified 2507.9; a real ceiling remains just above that.
  **CepOperator is pinned at the ceiling**, parallelism 6 and **100.0%** busy
  against a 0.6 target. Capped, not stalled. Every other vertex settled inside
  target. Since CepOperator is saturated it cannot keep up, so Kafka lag on
  `clickstream` will grow while the Load Ramp runs. **Drill G must drain that
  backlog before parallelism falls**, so expect the scale-down to start later
  than the configured interval alone predicts.
  **The autoscaler's linear projection was 2.4x optimistic.** It predicted
  capacity 2187.86 to 6992.00 for `2 -> 6`; measured was about 2950. Likely host
  CPU saturation, since three TaskManagers now share one host with 6 kind nodes,
  Kafka, MinIO and the generator. Reasoning from deployment shape, not measured.
  **Step 7's gap check was folded into Drill G** and passed there, because both
  of this Drill's rescales happened before a baseline existed.
  A fourth rescale followed, Interval Join `4 -> 6`, before the job settled.
- ✅ Task 6: Drill G, 2026-09-08.
  [Runbook](../../runbooks/phase-6-drill-g-scale-down.md). Traffic stopped,
  Co-Keyed-Process fell `2 -> 1` while draining, then CepOperator and Interval
  Join both `6 -> 3`. **Three TaskManagers became two**, `taskmanager-1-2`
  released. 6 restores, **0** exceptions across the session. Gap check: **no
  gap**, duplicates unchanged at 5844.
  **Two settings the plan never named govern this Drill.**
  `job.autoscaler.scale-down.interval` defaults to **1 hour**, which is why the
  plan's "wait 3 minutes plus stabilization" is wrong by about an hour. Set to
  `5m` for this Drill; adding it did not restart the job, same as Drill F.
  `job.autoscaler.scale-down.max-factor` defaults to **0.6**, so `6 × 0.6 = 3.6`
  rounds to 3. **That is why an idle job stopped at parallelism 3 and not 1.**
  Shrinking happens in bounded steps, each waiting out the interval again.
  **This also explains the 81-minute idle gap earlier in the day**, when the job
  came back at parallelism 1. 81 minutes is longer than the 1 hour default.
  **The scheduler packed rather than spread.** Slots needed is the largest
  parallelism of any one vertex, 3, so `ceil(3 ÷ 2)` = 2 pods and the third was
  emptied and released. Had subtasks spread one per TaskManager, no pod would
  have been freed. `prefer-minimal-taskmanagers` is unreadable on this cluster,
  so this is observed behaviour, not a config reading.
  **`/jobmanager/config` reports only explicitly-set keys**, 62 here. There is no
  endpoint that reports effective defaults, so the plan's Step 1 cannot be run as
  written. Both plan steps now carry the correction.
  **Failed checkpoints went 2 to 4**, one per rescale. A checkpoint in flight
  when parallelism changes is aborted and the next succeeds. Expected.
  **`recommendation-snapshot.sh compare` exits 141 when it lists duplicates**, a
  `SIGPIPE` from an internal pipeline. The summary is correct; the exit code is a
  script bug worth fixing, since this is the gap instrument for every Drill from
  Phase 5 on.
  **Resolved: the 5844 duplicates are source replay, not an exactly-once
  violation.** `PipelineConfig.java:54` defaults `startFromEarliest = true` and
  the manifest never overrides it, so every stateless restart re-reads
  `clickstream` from offset 0 and re-emits every Recommendation. Task 3's clean
  start was deliberately `upgradeMode: stateless`. Offset distance between copies
  is 2190 to 5516, never adjacent; `discountPercent` differs in 4712 of 4713
  pairs because `generatedAt` is a deterministic window end while the enrichment
  state is not. 1023 of 4672 records from 09-07 are duplicated.
  **Two follow-ups.** Bound `recommendation-snapshot.sh` to the current run with
  an optional `kcat -o s@<ms>` start time, so the gap instrument stops comparing
  three phases of history. And decide `--start-from-earliest` deliberately.
  `latest` stops the replays but makes a **cold start** skip whatever is already
  in the topic. **The initializer applies only to a cold start**, so Phase 7's
  promoted green, which ADR 0006 restores from blue's savepoint, is unaffected
  either way. `OffsetsInitializer.committedOffsets(...)` is the middle option.
  **Phase 4 already recorded the cost of `false`:** the promo-rule broadcast
  source starts at *latest*, so a rule published before the job subscribed is
  never read and every discount stays `0.0` until the next rule arrives. That is
  the same flag, and it makes this an **ADR with a known trade, not a bug fix**.
  **Pin the settings the Drills depend on.** `/jobmanager/config` reports only
  explicitly-set keys, so an unset default is indistinguishable from a missing
  key. `job.autoscaler.scale-down.max-factor` (0.6) and
  `jobmanager.adaptive-scheduler.prefer-minimal-taskmanagers` are both unpinned.
  The second is a JobManager key, so pinning it triggers a savepoint redeploy and
  resets parallelism to 2; do it between Drills, not during one.
- ✅ Task 7: Karpenter installed, 2026-09-08 and 2026-09-09.
  `bootstrap.sh karpenter` covers four stages: pin the upstream checkout at
  `02caf5a`, install kwok `v0.8.0`, build the controller image, then CRDs plus
  chart. **Upstream publishes no chart and no image for the kwok provider**, so
  the chart is applied from a path inside the checkout and the image is compiled
  in Docker (`manifests/karpenter/Dockerfile.controller`, Go 1.26.6) and
  `kind load`ed. Neither `go` nor `make` is on the host, and neither was
  installed.
  NodePool `decoy` reports `NodeClassReady=True` / `Ready=True`. Both safety
  checks pass: no real node carries `node-role=decoy`, and nothing outside
  `karpenter-decoy` tolerates `workload=flink`.
  **Beyond the plan:** `manifests/argocd-apps/karpenter-resources.yaml` puts the
  hand-written CRs under ArgoCD. The controller cannot go in, because ArgoCD does
  not build images. It carries `ignoreDifferences` on the Decoy's
  `spec.replicas` plus `RespectIgnoreDifferences=true`, so a sync cannot reset
  the Drill mid-run.
  **The `karpenter-decoy` Namespace lives in `decoy.yaml`, not the script.** The
  cost: `--dry-run=server` on that file fails with `namespaces not found`,
  because a dry run does not create the namespace the Deployment needs.
  **The Decoy carries resource requests the plan does not mention**, `cpu: "1"`
  and `memory: 256Mi`. Without them Karpenter sizes a node for pods that ask for
  nothing and `limits.cpu` is decorative.
- ✅ Task 8: Drill H, 2026-09-09.
  [Runbook](../../runbooks/phase-6-drill-h-karpenter.md). Two runs. **Gate
  passes.** One kwok node appeared 5 seconds after scaling the Decoy to 5, all
  five pods landed on it, and it went away 45 seconds after scaling to 0.
  **`pending-pods: 5` in the controller log is the only durable proof the pods
  were unschedulable.** The `Pending` window is about 5 seconds, since kwok
  creates a node instantly, so watching by hand does not work.
  **`consolidateAfter: 10s` is not a countdown to deletion.** It is how long a
  node must sit idle before Karpenter will *consider* disruption. Observed 45s.
  Run 1 logged `marking consolidatable` while the node was still fully loaded,
  which is the same distinction from the other side.
  **Karpenter bin-packs first, then asks for one node.** The NodeClaim requested
  `cpu: 5100m, memory: 1330Mi, pods: 7`: five Decoy pods at 1 CPU, plus 100m and
  two slots for the `kindnet` and `kube-proxy` DaemonSets that land on any new
  node. **Seven pods on the kwok node, not five, and that is not a leak.**
  **`limits.cpu: "20"` was never reached**, so that path is untested. It would
  take roughly 20 replicas.
  **`instance-type`, `capacity-type` and `zone` are invented by the kwok
  provider** (`c-8x-amd64-linux`, `spot`, `test-zone-a`/`b`). Run 1 shows six
  types considered. None exists, and that fabrication is what makes the loop
  exercised here the same code that runs on EKS.
  **The node's fakeness is visible in `kubectl get nodes`**: `kwok-v0.8.0` in the
  `VERSION` column against `v1.34.8`. In `nodeInfo`, `bootID`, `machineID`,
  `systemUUID` and `osImage` are all empty strings.
  **DEFECT: a kwok node crashes kindnet on the real nodes.** 5 of 6 kindnet pods
  went `CrashLoopBackOff` with 14 restarts each. kube-controller-manager gives
  the kwok node a pod CIDR, kindnet on every real node tries to route to it via
  the node's internal IP, and kwok picked `10.244.3.7`, an address inside
  worker's own pod CIDR and unreachable as a gateway. `ip route add` fails
  `ENETUNREACH` and kindnet panics. **Flink pods were unaffected** (`RESTARTS 0`,
  job `RUNNING`) because kindnet runs FailOpen, but the CNI genuinely died.
  Recovery is `kubectl -n kube-system delete pod -l app=kindnet` once the kwok
  node is gone. **No clean fix found:** `--allocate-node-cidrs` is cluster-wide
  in kind and the provider picks the node IP. Mitigation is to keep the kwok
  node's life short and restart kindnet after.
  **Each run permanently consumes a `/24`.** Workers moved from `10.244.3/4/5`
  to `3/5/6`; runs took `.4` and `.7` and never gave them back. 256 available,
  so not urgent, but a cluster rebuild is the only reset.
  **Carry into Phase 7:** promotion depends on pod networking across two
  namespaces. Do not run a Karpenter Drill and a promotion together.
- ✅ Task 9: documents, 2026-09-09. ADR 0005's Decision superseded in place, the
  `CONTEXT.md` Decoy Workload entry corrected to the two-mechanism version, the
  phase plan's Phase 6 section rewritten (11h to 7h, 6b removed, the Karpenter
  mechanism corrected), the knowledge doc given a
  "What the Drills corrected in this document" section, and the four Drill E to H
  runbooks added to `docs/knowledge/README.md`.
  **The sharpest thing found while amending ADR 0005.** That ADR argues a KEDA
  `ScaledObject` against `mode: native` is a no-op. Verified on the live cluster,
  it is worse: the CRD **does** declare a `scale` subresource pointing at
  `.spec.taskManager.replicas`, so `kubectl scale flinkdeployment personalization
  --replicas=3 --dry-run=server` returns **`scaled`** against a native deployment
  whose field is unset and never read. **Every layer reports success and the
  number goes nowhere.**
  **A gap the knowledge doc had:** its "Four timers" section listed only the four
  that pace scaling *up*. `job.autoscaler.scale-down.interval` (default **1h**)
  and `scale-down.max-factor` (default **0.6**) govern the entire scale-down half
  and were absent. Now recorded as a correction rather than a rewrite.

## What Phase 7 inherits from Phase 6

Carried forward deliberately, because a later phase would otherwise rediscover
each of these the hard way.

- **Parallelism is no longer deterministic.** The Promotion runbook must not
  assume 6, or 3 TaskManagers, or any fixed pod count. Across Phase 6 the job ran
  at 1, 2, 3 and 6, with 1 to 3 TaskManagers, all without a restart. **Read the
  live value.**
- **`upgradeMode` is back to `savepoint`**, restored in Drill F's first step.
  Promotion depends on it. It was `stateless` for one transition only.
- **`spec.job.parallelism` is still the number the job starts at.** The
  autoscaler writes only to the running JobManager, never to the CR, so any full
  restart drops the job back to `2` and the autoscaler must climb again from the
  `autoscaler-personalization` ConfigMap. **Promotion restarts the job by
  design.**
- **The two-config-lists rule is dead.** `apps/pipeline/conf/config.yaml` was
  deleted in `b0705e0`; every key lives in `spec.flinkConfiguration` alone. Phase
  7 inherits a single list and should not re-derive a drift rule for it.
- **Karpenter sits outside the GitOps tree.** A cluster rebuild must install it by
  hand with `scripts/bootstrap.sh karpenter`, alongside cert-manager and ArgoCD.
  Its `nodepool.yaml` and `decoy.yaml` *are* in ArgoCD; the controller cannot be,
  because ArgoCD does not build images.
- **Do not run a Karpenter Drill and a promotion at the same time.** A kwok node
  crashes `kindnet` on the real nodes, and promotion depends on pod networking
  across two namespaces.
- **Task 5 Step 6's probe came back EMPTY.** The autoscaler never writes
  `pipeline.jobvertex-parallelism-overrides` to the CR, so no `ignoreDifferences`
  is needed in `flink-job-blue.yaml` and the Application does not go permanently
  `OutOfSync`.
- **The generator's measured ceiling is about 2650 Clicks/sec.** Task 1 removed
  the 1000/sec ceiling and verified 2507.9 against a requested 2500. Drill F
  asked for 4000 and got 2648, read off the source vertex. **Always measure the
  achieved rate rather than trusting `--click-rate`.**
- **Raise `--shopper-count` with `--click-rate`, every time.** Session length is
  `e^(6 × clickRate ÷ shopperCount)`. 4000 Clicks over 2000 Shoppers is `e^12`,
  about 163,000 Clicks per Session, and the window state would exhaust the host.
- **Host headroom is the binding constraint.** Task 0 measured it at 2.5 GiB,
  halved since the spec was written. Drill F showed the autoscaler's linear
  capacity projection coming in 2.4x optimistic, most likely CPU contention on a
  single host.
- **Open: `recommendation-snapshot.sh` reads the whole topic**, spanning three
  phases and at least one full source replay. It needs an optional
  `kcat -o s@<ms>` start time before its duplicate count means anything. It also
  exits 141 (SIGPIPE) when it lists duplicates.
- **Open: `--start-from-earliest` is an ADR, not a bug.** `true` replays the whole
  topic on any stateless restart. `false` stops that, but Phase 4 recorded its
  cost: the promo-rule broadcast source would miss rules published before the job
  subscribed, leaving every discount at `0.0`.

**`apps/pipeline/conf/config.yaml` does not exist, and three documents were
corrected to say so.** Task 5 of Phase 5 deleted it in commit `b0705e0`, 15
lines, when the configuration moved into `spec.flinkConfiguration`. The Phase 5
design's rule that it and `spec.flinkConfiguration` are "two lists that must not
drift" therefore has one side left, and the Phase 5 plan's constraint forbidding
its edit is satisfied vacuously. A consequence that predates Phase 6: a bare
`:pipeline:run` against `MiniCluster` throws
`IllegalStateException("no config.yaml loaded from conf")` unless the developer
supplies their own copy. `:pipeline:test` does not touch that path.

**The one question the design could not settle** is still open, and it can only
be answered after the first scaling event in Drill F: does the autoscaler's
computed parallelism reach the live CR's `spec.flinkConfiguration` as
`pipeline.jobvertex-parallelism-overrides`? Task 0 recorded the field empty, so
the comparison is meaningful. Drill D's removal means this is now GitOps hygiene
rather than protection of a Drill, but Phase 7 and Phase 8 inherit the
Application.


## Phase 7: Blue/green and the deployment mechanism — ⬜ not started

Design and plan both written on 2026-09-09:
[design](../specs/2026-09-09-blue-green-and-deployment-design.md),
[implementation plan](2026-09-09-phase-7-blue-green.md). The plan runs 14 tasks;
check its Progress table for the live position. A
[knowledge doc](../../knowledge/phase-7-blue-green.md) exists already, written
during design rather than during the phase, and says so at the top.

**Scope changed from the phase plan, three ways.**

- **OTel moved wholesale to Phase 8.** Its Drill needs Prometheus, node-exporter
  and kube-state-metrics, and none of the three is installed. A Drill that shows
  a blast-radius boundary cannot run when neither side of the boundary is
  observable. Phase 7 is renamed accordingly.
- **The recovery reconsideration is resolved as partially adopted.** Promotion
  **is** the recovery path for a deployment failure, where high availability
  makes things worse by faithfully restoring a broken jar forever. It is **not**
  a failover mechanism for an infrastructure failure, where Phase 5's Drills
  already recover in under a minute. The full classification lives in the
  [Phase 5 knowledge doc](../../knowledge/phase-5-operator-and-ha.md), section
  "Every failure this project can have".
- **No CI.** A merge-request-triggered pipeline was designed and rejected: a
  hosted runner cannot reach this cluster, so the runner would be self-hosted on
  this machine either way, and CI would then add only a trigger you did not type.
  Deployment is `scripts/promote.sh` plus six Drills.

**Phase 7 touches `apps/pipeline`**, which the phase plan did not anticipate.
Nine `.uid()` and `.name()` calls, and a new image. Budget rises from 9 hours to
roughly 12.

**Five facts were not settled at design time** and are gates inside the tasks
that need them, with both branches written out: what `status.jobStatus.state`
reads on a never-run FlinkDeployment, whether the `app` label carries the
cluster-id, the REST Service name, whether
`kubernetes.operator.savepoint.format.type` is honoured per resource, and whether
`execution.checkpointing.num-retained` should rise from 3.

**One decision is open**: amend ADR 0006, or write ADR 0010, for the
promotion-as-recovery scope.

### Tasks 0 to 5, 2026-09-10

- ✅ **Task 0: Baseline capture.** Blue `RUNNING` on `0.1-0bd7f52`, parallelism 2,
  **1 TaskManager**, 2 JobManagers on `worker3` and `worker2`, job id
  `7a3bdfb1491018179b282896485e8fe6`, `upgradeMode: savepoint`. PDB reported
  `ALLOWED DISRUPTIONS 1`. No kwok nodes.
  **Host headroom has recovered**: 6 GiB available against the 2.5 GiB Phase 6
  Task 0 measured. MinIO holds 2081 objects under `phase-3/`, `phase-5/`,
  `phase-5-ha/`, `phase-6/`, `phase-6-ha/`, `phase-6-savepoints/`. Nothing under
  `phase-7/` yet.
  `mc` is **not** on the host PATH. Read the bucket from inside the tenant pod:
  `kubectl exec -n minio-tenant personalization-pool-0-0 -- mc ...` after
  `mc alias set` with the credentials from `scripts/minio-env.sh`.
- ✅ **Task 1: the snapshot window.** `snapshot` gained an optional
  `since-epoch-ms`, mapping to `kcat -o s@<ms>`.
  **The premise was right and now has a number: 140,598 records carrying only
  134,754 identities, so 5,844 duplicates were already present before any Phase 7
  Drill ran.** All of them sit in the older part of the topic; a window from the
  topic's midpoint returned 127,728 records with **zero** duplicates.
  **The window is an OFFSET, not a timestamp filter.** `kcat -o s@` resolves the
  timestamp to one starting offset per partition and reads everything after it.
  Because this topic's record timestamp is `generatedAt`, an event-time value,
  timestamps within a partition are not monotonic: asking for the midpoint
  returned 127,728 records where a strict timestamp filter over the same data
  returns 117,191. Harmless for a Drill, because BEFORE and AFTER share the same
  starting offsets. Do not use it as an exact time filter.
  **The exit 141 was real and is fixed.** `printf | head -20` on the duplicate
  listing: `head` closes the pipe, `printf` takes SIGPIPE, `pipefail` propagates
  141. Now a herestring. Measured before and after on the same input: **old exit
  141, new exit 1**, where 1 correctly means "did not recover cleanly".
  The window is recorded in a sidecar `.meta` file rather than the snapshot's
  first line, because the snapshot is sorted and `comm` would read a header as an
  identity. `compare` now refuses a mismatched pair outright.
- ✅ **Task 2: uids and names.** **Eleven call sites, not the nine the plan
  counted.** The plan's own list held ten items; the eleventh is the stateless
  `ShopperSignal` map, pinned so that toggling `--debug-prints` adds and removes
  print sinks without moving any other id. 27 tests pass, read from the XML
  report. Image `lab/personalization-pipeline:0.1-0902abb` built and loaded onto
  every node.
- 🟡 **Task 3: blue's manifest.** All file edits done: renamed to
  `personalization-blue`, the three storage paths split under
  `s3://checkpoints/phase-7/blue/`, `kubernetes.operator.savepoint.format.type:
  NATIVE` added, prefix moved to `personalization-phase-7`, image at
  `0.1-0902abb`, PDB selector at `app: personalization-blue`,
  `rest-nodeport.yaml` deleted, `build-image.sh`'s closing hint rewritten to name
  the Standby Side rather than blue. **Steps 8 and 9 need a push and a sync.**
- 🟡 **Task 4: green.** Directory and Application written.
  `diff -r manifests/flink/blue manifests/flink/green` shows **exactly the seven
  intended fields** and nothing else. The Application differs from blue's in
  three lines.
  **Gate settled: a never-run FlinkDeployment has no status at all.** Applied by
  hand, watched 60 seconds, removed: `.status` is `{}`, and both the JOB STATUS
  and LIFECYCLE STATE columns print blank. So discovery tests "is it RUNNING" and
  never compares to the literal `SUSPENDED`. Zero pods, as application mode
  requires.
  **`minio-credentials` already exists in `personalization-green`**, 9 days old.
  **New finding: `manifests/flink/namespaces.yaml` is owned by no Application.**
  Both namespaces carry `kubectl.kubernetes.io/last-applied-configuration` and no
  ArgoCD tracking id, so they were applied by hand. A cluster rebuild must apply
  that file by hand, the same way the Karpenter controller must.
- 🟡 **Task 5: `scripts/promote.sh`.** Written. Discovery reads both sources for
  both sides and permits exactly three arrangements; the abort prints all four
  cells. Suspend polls two conditions at 1 second. A suspend timeout rolls itself
  back; a start timeout prints the manual fallback and refuses to guess.
  **`yq` is not a dependency, against the plan's sketch.** `yq -i` reflows the
  whole document and these manifests carry load-bearing comments. Anchored `sed`
  instead, on the only two fields that change. Verified: after a suspend, a
  savepoint write and a start, the whole-file diff was **exactly two lines** with
  every comment intact, and setting the savepoint path twice leaves one line.
  `^    state: ` is anchored to four spaces because `state.backend.type` sits at
  the same indent under `flinkConfiguration`.
  **Neither `argocd` nor `yq` is installed on this host.** `argocd` is required;
  the script dies with the install URL.
  **Step 10's dry run cannot pass until Task 3 has synced**, an ordering
  dependency the plan did not state: discovery reads the live CR by its new name,
  which does not exist until the rename is applied.

### Tasks 3 to 5 closed, 2026-09-12

**A bug was introduced and caught by the cluster, not by review.** The rename
changed the `app` label, and `pdb.yaml`'s selector was updated while the **two
`topologySpreadConstraints.labelSelector` blocks inside `flinkdeployment.yaml`**
were not. After the first sync both JobManagers landed on `worker2`, zone-b:
skew 2 against `maxSkew: 1`, which is impossible if the selector matches.

**A spread constraint selecting nothing is not an error, and it is worse than the
PDB case**, because `whenUnsatisfiable: DoNotSchedule` reads like a hard
guarantee. With no matching pods there is no skew to violate, so the scheduler
places pods anywhere and reports success. Phase 5's Zone spread was silently
inert for the duration. **Rule: after any rename, grep every selector that names
`app`, not just the ones in separate files.** Three per side.

**`Synced` does not mean "matches GitHub".** It means "matches the revision I last
looked at". After the fix was pushed, `flink-job-blue` reported `Synced` at the
previous commit for minutes. `argocd app get <app> --refresh` forces the poll.

Gates settled on the live cluster:

- **The `app` label carries `kubernetes.cluster-id`.** Conclusive only after the
  rename made the two strings differ: pods now carry `app=personalization-blue`.
- **Zone spread works again**: JobManagers on `worker3` (zone-c) and `worker2`
  (zone-b), PDB `ALLOWED DISRUPTIONS 1`.
- **The REST Service is `personalization-blue-rest`**, ClusterIP, and it is the
  **only** Service in the namespace. No internal headless Service, confirming
  Flink skips it under Kubernetes HA.
- **The storage split is real**: `phase-7/blue/{checkpoints,ha,savepoints}` all
  created, nothing leaking into the phase-6 prefixes.
- **`kubernetes.operator.savepoint.format.type: NATIVE` IS honoured inside
  `spec.flinkConfiguration`**, which settles Task 7 Step 7 early. The savepoint
  holds **200 UUID-named files**, RocksDB's own layout. A `CANONICAL` savepoint
  would be one `_metadata` plus a few large chunks.
- **`promote.sh --dry-run` discovers `blue -> green`** and echoes every mutating
  command.

**A Standby Side's PDB reports `ALLOWED DISRUPTIONS 0`, and that is correct.**
Green is suspended, so there are no pods to match. The PDB check only carries
meaning on the Active Side; do not read 0 on a Standby Side as the broken-selector
signature.

**Two job ids now exist under `phase-7/blue/checkpoints/`**, `834237ec…` then
`6fc8664a…`. The rename was a delete-and-create and started clean; the selector
fix was an ordinary spec change, so `upgradeMode: savepoint` savepointed and
restored. `spec.job.initialSavepointPath` stayed empty throughout, and
`status.jobStatus.upgradeSavepointPath` carries the savepoint the operator used.

**The `argocd` CLI is required and was not installed.** Server is v3.5.0 with
`server.insecure: true`, so the CLI needs `--plaintext` on login. Installed to
`~/.local/bin`, which is already first on PATH, rather than machine-wide.

**What is left: the six Drills, Tasks 6 to 13.**

### Drill 1 steps 1 to 6, 2026-09-12

**`promote.sh` was polling two wrong fields, and it would have broken every
promotion.** Measured on a real suspend:

```
lifecycleState                                 = SUSPENDED   <- the signal
jobStatus.state                                = FINISHED
jobStatus.savepointInfo.lastSavepoint.location = (absent)
jobStatus.upgradeSavepointPath                 = s3://.../savepoint-6fc866-...
```

`stop-with-savepoint` leaves Flink's job status at **FINISHED**; `SUSPENDED` is
the **operator's** `lifecycleState`. And the operator records the upgrade's
savepoint in `upgradeSavepointPath`, not in `savepointInfo.lastSavepoint`, which
is for independently triggered savepoints. The script polled
`jobStatus.state == "SUSPENDED"` and read `lastSavepoint.location`, so every
promotion would have run the full 300s `SUSPEND_TIMEOUT` and rolled itself back.

**`--dry-run` cannot catch this, because a dry run never polls.** Rule: a dry run
proves the branching, never the polling. Any condition a dry run skips must be
checked against a real resource in the state it will actually see.

The correct field name was already in this file, from Phase 6 Task 3: "empty
`upgradeSavepointPath` beside `RUNNING`". The wrong names came from the original
design spec's promotion runbook, written before Phase 5 existed.

**`suspended` is two different states, and only one of them has no pods.**

| | never run | suspended after running |
|---|---|---|
| JobManager Deployment | absent | **2/2, still up, 0 restarts** |
| `<name>-rest` Service | absent | **still there** |
| ConfigMaps | absent | **all five still there** |
| TaskManagers | absent | gone with the job's slots |
| `status` | `{}` entirely | populated |

Suspending stops the **job**, not the cluster. The knowledge doc's claim that a
suspended deployment has no pods was true only of a never-run side and has been
corrected. This also means **the pre-warming analysis in the Phase 7 knowledge
doc needs redoing**: a real Standby Side has been suspended, so its JobManager is
already warm, and the saving from session mode is smaller than the tradeoff
section claims. Not yet rewritten.

**The topics are truncated.** Before: clickstream 10,041,754, product-change
1,892,833, promo-rule 336,326, recommendation 183,688. After: every topic reports
earliest equal to latest, cross-checked with a real consumer reading 0 records.
The 5,844 pre-existing duplicate identities are gone with the data.

**Kafka offsets do not reset to zero.** `kafka-delete-records.sh` moves the log
start forward; numbering continues. `clickstream` reads earliest 10,041,754 and
latest 10,041,754, and is empty.

**Stale Phase 3 consumer groups still exist**: `personalization-phase-3`,
`-product-change`, `-promo-rule`. Harmless, since Flink does not use consumer
groups for offsets, but they confuse a lag reading.

**Git pushes cannot be done from the assistant's shell.** The remote is HTTPS
with no credential helper, so `git push` fails with "could not read Username".
Commits land locally; the push is a manual step.

## Phase 8: Observability and docs — ⬜ not started

Now also carries the OTel Collector, the Prometheus reporter plugin, the
Collector-kill Drill, and the Grafana namespace grouping, all moved out of
Phase 7. Host port 30011 is freed by Phase 7 and available here.
