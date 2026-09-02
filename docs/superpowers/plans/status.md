# Implementation status

Last updated: 2026-08-31

Live tracker: what's actually done right now, not the design (that's
[the phase plan](2026-08-10-implementation-phases.md)) and not how things work
(that's [the knowledge docs](../../knowledge/)). Update this file as work
lands, don't let it go stale.

Status legend: ✅ done · 🟡 in progress · ⬜ not started

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

## Phase 5: Operator and HA — 🟡 in progress

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
- ⬜ Tasks 4 to 11

**One step still open from Task 0.** Step 5, re-confirming the host-side
`external` listener after the broker roll, was never run. One command closes it:
`kcat -b localhost:30016 -L | head -5`.


## Phase 6: Autoscaling — ⬜ not started

## Phase 7: Blue/green and OTel — ⬜ not started

Reconsideration flagged, not yet designed: whether Blue/Green should also
serve as a recovery mechanism, not just zero-downtime deployment. See the
plan's Phase 7 section.

## Phase 8: Observability and docs — ⬜ not started
