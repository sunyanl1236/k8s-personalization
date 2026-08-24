# Implementation status

Last updated: 2026-08-24

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

- ✅ Four domain schemas as Java records (`generator/src/main/java/lab/personalization/generator/domain/`):
  `Click`, `ProductChange` (sealed interface over `PriceChange`/`StockChange`),
  `PromoRule`, `Recommendation`. Design in
  [the domain schemas doc](2026-08-16-domain-schemas-design.md).
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

## Phase 3: Core pipeline — 🟡 in progress

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
- 🟡 Task 7: RocksDB and checkpoints to MinIO. Next up.
- ⬜ Tasks 8 to 10

Two decisions worth knowing without reading the whole design. Phase 3 publishes
a real `Recommendation` to the existing `recommendation` topic rather than
adding a fifth topic, with `discountPercent` at `0.0` until Phase 4 evaluates
Promo Rules. And the session gap is 6 seconds because it is *derived* from the
generator's rate, not chosen: at 10 Shoppers and 5 Clicks per second a 30 second
gap would close a Browsing Session about never, and the job would emit nothing
while behaving correctly.

**Surfaced for Phase 4, and nothing else records it.** `ProductChange` is a
sealed interface, which is not a Flink POJO. Phase 3 sets
`pipeline.generic-types: false`, so it cannot silently fall back to Kryo either.
Phase 4 hits this the moment it reads `product-change`, and needs either a
custom `TypeInformation` or a split into two typed branches. Budget for it.

## Phase 4: Advanced Flink — ⬜ not started

## Phase 5: Operator and HA — ⬜ not started

## Phase 6: Autoscaling — ⬜ not started

## Phase 7: Blue/green and OTel — ⬜ not started

Reconsideration flagged, not yet designed: whether Blue/Green should also
serve as a recovery mechanism, not just zero-downtime deployment. See the
plan's Phase 7 section.

## Phase 8: Observability and docs — ⬜ not started
