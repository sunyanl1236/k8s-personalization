# Advanced Flink design (Phase 4)

Date: 2026-08-24
Status: approved (design), not yet implemented
Derives from: [implementation phases](../plans/2026-08-10-implementation-phases.md),
[core pipeline design](2026-08-23-core-pipeline-design.md),
[domain schemas design](2026-08-16-domain-schemas-design.md),
[the Flink job walkthrough](../../knowledge/flink-job-walkthrough.md),
[ADR 0001](../../adr/0001-minicluster-first-dev-loop.md),
[ADR 0003](../../adr/0003-interval-join-key-and-semantics.md)

## Goal

Add the Product-keyed branch and the four remaining Flink concepts to the job
Phase 3 builds. Still on `MiniCluster`, still against the Kafka and MinIO in the
`kind` cluster. No `FlinkDeployment`, no containers. Those are Phase 5.

Five concepts, which is what the phase plan's done-when counts:

1. Interval join on its own `keyBy(productId)` branch, with `Unmatched Click`
   as its side output
2. Broadcast state for Promo Rules
3. CEP for a multi-step behaviour pattern
4. The `connect` merge, a `KeyedCoProcessFunction` keyed by `shopperId`
5. Async I/O to a mocked recommendation service

Done when each has one assertion test, and a Promo Rule changes mid-run with the
`recommendation` topic changing without a restart.

## Prerequisite

Phase 3 Tasks 6 to 10 land first. Phase 4 changes `RecommendationDecider`'s
output type, so that class must exist and must have passed its own Drill before
this phase touches it. Nothing in Phase 3 needs to change in anticipation. See
[Effect on Phase 3](#effect-on-phase-3) for why building Task 6 against a Phase
4 shape would be worse, not better.

## Verified before designing

Everything in this section was checked against the Flink 2.2 documentation, not
recalled. Anything below stated as reasoning rather than fact says so.

| Claim | Where |
|---|---|
| `IterativeCondition` with `ctx.getEventsForPattern(name)` compares a candidate against earlier matched events | [cep][d-cep] |
| A pattern may end in `notFollowedBy` when `within(...)` is set | [cep][d-cep] |
| `TimedOutPartialMatchHandler.processTimedOutMatch(...)` routes expired partial matches to a side output | [cep][d-cep] |
| `CEP.pattern(...)` accepts a keyed stream | [cep][d-cep] |
| Unordered async I/O reorders records **between** watermarks, and with watermarks present its overhead approaches ordered mode | [asyncio][d-async] |
| `AsyncFunction.timeout(...)` must be overridden or a timeout fails the job | [asyncio][d-async] |
| A custom `TypeInformation` can be attached with no annotation, via `pipeline.serialization-config` with `{type: typeinfo, class: ...}` | [types_serialization][d-types] |
| `ProcessFunctionTestHarnesses` provides `forKeyedCoProcessFunction` and `forBroadcastProcessFunction` style factories | [testing][d-testing] |

## Decisions

### `intervalJoin` cannot emit `Unmatched Click`, so the side output moves

This is the one finding that contradicts a signed-off document, so it comes
first.

**The problem.** `intervalJoin` is an inner join. `ProcessJoinFunction
.processElement(...)` is invoked only when a pair matches. A Click with no
nearby Price Change never reaches the function, so there is no callback in which
to call `ctx.output(UNMATCHED, click)`.

ADR 0003's diagram says otherwise:

```
   intervalJoin
        |
   matched: enriched click
   unmatched: side output      <-- not reachable from ProcessJoinFunction
```

**The decision.** The `UNMATCHED` side output moves to the merge operator, and
its granularity changes from one record per Click to one record per Browsing
Session candidate Product.

The merge already holds, per Shopper, which Products had a price-drop match.
When a Browsing Session closes and its candidate Product is absent from that
map, the merge emits to the `UNMATCHED` side output. The population is real and
means what `CONTEXT.md` intends. Only the grain changes.

**Consequence.** ADR 0003 needs a written amendment, or a new ADR superseding
that part of it. Phase 8's dashboard counts Unmatched per Browsing Session, not
per Click, and its axis label has to say so.

Rejected: **buffer every Click for the join interval and check for an enriched
twin.** Recovers per-Click grain exactly. Rejected because it re-implements the
join's own bookkeeping, which is the thing ADR 0003 rejected to keep the real
operator in play, and it adds a third shuffle on the clickstream for a side
output nothing reads until Phase 8.

### The `product-change` topic is read as `PriceChange`, not `ProductChange`

**The problem.** `ProductChange` is a sealed interface. A sealed interface is
not a POJO by Flink's rules, and Phase 3 sets `pipeline.generic-types: false`,
so it cannot silently become a Kryo type either. A `DataStream<ProductChange>`
throws at job-graph construction, before the first record.

**The decision.** The deserializer reads the `type` discriminator that
`JsonCodec` already writes on the wire and collects only `PriceChange`, which is
a plain record and a valid Flink POJO. `StockChange` records are read and not
collected. `ProductChange` never enters the job graph.

```java
public class PriceChangeDeserializationSchema
        implements DeserializationSchema<PriceChange> {

    @Override
    public void deserialize(byte[] message, Collector<PriceChange> out) {
        ProductChange change = JsonCodec.productChangeFromJson(message);
        if (change instanceof PriceChange price) {
            out.collect(price);
        }
    }

    @Override
    public PriceChange deserialize(byte[] message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeInformation<PriceChange> getProducedType() {
        return TypeInformation.of(PriceChange.class);
    }
}
```

It stays a `DeserializationSchema`, matching `ClickDeserializationSchema` and
keeping `setValueOnlyDeserializer(...)` on the `KafkaSource` builder. The
`Collector` overload is the one that can emit zero records, which is how a
`StockChange` is dropped. The single-value `deserialize` is then unreachable,
because Flink calls the `Collector` form when it is overridden.

**Why dropping `StockChange` is not a loss.** Nothing consumes it. `CONTEXT.md`
names the domain term **Price Change**, ADR 0003 describes the join as
clickstream against a price-change stream, and no later phase lists stock
levels. The sealed interface is still doing its real job in `JsonCodec`, where
the exhaustive `switch` guarantees both variants get written. The pipeline
simply does not need the sum type on the read side.

Rejected: **a custom `TypeInfoFactory` registered via
`pipeline.serialization-config`.** It works, and it keeps `:domain`
Flink-free because that route needs no `@TypeInfo` annotation. Rejected on cost.
A `TypeInfoFactory` alone is not enough. It must return a `TypeInformation`,
which must supply a `TypeSerializer`, which for a sum type writes a tag and
delegates. Any `TypeSerializer` reaching state or a checkpoint also needs a
`TypeSerializerSnapshot`, and Phases 5 through 7 restore from checkpoints and
savepoints repeatedly, so that snapshot is a compatibility contract this project
actually exercises. Roughly 150 lines and about 3 hours, in a 13 hour phase
whose stated overrun risk is CEP.

Rejected: **one flat `ProductChangeRecord` POJO with a kind discriminator, split
into typed streams after the source.** Consistent with the `ShopperSignal`
decision below, and it keeps `StockChange` available. Rejected because it pays
the unused-field cost a second time in one phase for a stream with no consumer.

### The CEP pattern is an abandoned cart, not the pattern the spec names

**The problem.** Both patterns already written down are wrong for this
generator, and neither document computed its own selectivity.

Per Shopper the Click rate is 0.5 per second. `ActionType` is uniform over 3
values and the Product uniform over 10, so Clicks of one kind on one Product
arrive at 0.0167 per second per Shopper.

| Pattern | Source | `within` | Match rate | Verdict |
|---|---|---|---|---|
| VIEW(A) then VIEW(B≠A) | walkthrough | 5s | 83% of Clicks | Degenerate. Every Shopper always matches |
| VIEW(P)×3, competitor, idle | design spec | 60s | about 1 per 37s | 60s spans 10 session gaps. Comparison shopping stretched over ten Browsing Sessions is not comparison shopping |
| VIEW(P), ADD_TO_CART(P), no CHECKOUT(P) | `ActionType`'s own comment | 30s | 24% of VIEW Clicks | Selective and frequent. Abandoning a cart across several Browsing Sessions is still an abandoned cart |

**The decision.** The abandoned cart. `ActionType`'s comment in `:domain`
already named it, and nothing outside that comment recorded it:

> Minimal set for Phase 4's CEP pattern (view, then cart, then no checkout
> within a window). "Left without checking out" is the absence of a CHECKOUT
> event within the pattern's own bound, not a fourth value here.

```java
Pattern.<Click>begin("view")
        .where(SimpleCondition.of(c -> c.actionType() == ActionType.VIEW))
    .followedBy("cart")
        .where(new SameProductCondition(ActionType.ADD_TO_CART, "view"))
    .notFollowedBy("checkout")
        .where(new SameProductCondition(ActionType.CHECKOUT, "view"))
    .within(Duration.ofSeconds(30));
```

`SameProductCondition` is an `IterativeCondition` reading
`ctx.getEventsForPattern("view")` to compare `productId`.

**The `within` value does not need tuning, which is worth knowing before
spending time on the knob.** With `x = 0.0167 * W`, the match probability is
`(1 - e^-x) * e^-x`:

| `within` | x | P(match) |
|---|---|---|
| 30s | 0.50 | 23.9% |
| 41s | 0.69 | 25.0%, the maximum |
| 60s | 1.00 | 23.3% |

No value moves it much. 30 seconds, because it is the smallest value near the
plateau, which keeps state lifetime short.

**Consequence.** The design spec's concept coverage map still reads "viewed a
product repeatedly, viewed a competitor, went idle". This design supersedes that
line, the way ADR 0003 supersedes the spec's architecture diagram.

Rejected: **raise the generator's Click rate so the spec's pattern becomes
selective.** Rejected because Phase 3 derived the 6 second session gap from the
current rate. Changing the rate invalidates a signed-off, already-drilled
decision to save rewriting one line of a coverage map.

### Four inputs, two-input operators

**The problem.** Every DataStream operator takes at most two inputs.
`connect(DataStream)` gives `KeyedCoProcessFunction`. `connect(BroadcastStream)`
gives `BroadcastProcessFunction`. There is no three-input form. Phase 4 has four
things to combine: `SessionSignal` and the CEP Signal, both keyed by
`shopperId`; the enriched Click, keyed by `productId` and re-keyed; and the
broadcast Promo Rules, keyed by nothing.

**A constraint that removes one option.** `union` needs one identical type. A
sealed-interface supertype would repeat the `ProductChange` problem exactly. Any
union here has to be one flat record with a discriminator, where some fields go
unused per kind.

**The decision.** Union the two Shopper-keyed Signals into `ShopperSignal`.
`connect` that with the re-keyed enriched Clicks in a `KeyedCoProcessFunction`,
which is what ADR 0003 requires. Apply the broadcast afterwards in a separate,
stateless `BroadcastProcessFunction`.

Three reasons. The phase plan names `connect` plus `KeyedCoProcessFunction` as a
thing to build, so collapsing everything into one union drops a concept from the
coverage map. The discount step needs no keyed state, so a plain
`BroadcastProcessFunction` avoids a second `keyBy(shopperId)` shuffle the keyed
variant would force. And the split makes the concept count come out at exactly
five, matching the done-when as written, with `Unmatched Click` folded into the
interval join.

Rejected: **one union of all three Signals into a single
`KeyedBroadcastProcessFunction`.** Two operators instead of four, and
`CONTEXT.md`'s definition of Signal ("Session aggregates, CEP pattern matches,
and enriched Clicks are all Signals") arguably endorses it. Rejected because
`connect` of two DataStreams then never appears in the job, which is the same
argument ADR 0003 used to reject a hand-rolled join.

Rejected: **apply the broadcast on the Product branch, before the merge.**
Stamps the discount at match time, which is defensible for a price-drop bonus.
Rejected because the merge then sees several stamped matches per Browsing
Session and must decide which discount survives, which is extra logic for no
gain.

### The Browsing Session close is the only emission trigger

**The problem.** Three branches produce Signals at three different rates. If
each could emit, the output rate and the cooldown Phase 3 built would both stop
meaning anything.

**The decision.** Only `SignalKind.BROWSING_SESSION` emits. The other two
branches write into keyed state that the session close reads.

| Input | Effect |
|---|---|
| `processElement2(EnrichedClick)` | put `productId` into `priceDropMatches` |
| `processElement1(CART_ABANDONMENT)` | put `productId` into `abandonedCarts` |
| `processElement1(BROWSING_SESSION)` | read both maps for the candidate Product, emit, clear both maps |

Both are `MapState<String, Boolean>`, `productId -> true`.

Three reasons. One trigger means one Recommendation per Browsing Session, so the
cooldown logic Phase 3 Task 6 builds carries over unchanged, even though the
class it lives in changes its output type. The output rate stays tied to
session closes, which is the rate Phase 3 sized its Drill around. And `reason`
becomes the field carrying which branches contributed, which is what the Phase 3
design says `reason` exists for.

`reason` precedence, most specific first: `"cart-abandoned"`, then
`"price-drop"`, then Phase 3's `"most-viewed-in-session"`.

**Both lookups are narrowed to the candidate Product, and that narrowing is
load-bearing.** The candidate is the most-clicked Product of the Browsing
Session. A Shopper abandons about 1.6 carts per Browsing Session across 10
Products, so an unnarrowed check would put `reason = "cart-abandoned"` on
roughly 80 percent of Recommendations and drown the other two values. Narrowed
to the candidate it lands near 15 percent.

**State is bounded by the clear on session close.** An event-time timer is the
safety net for a Shopper whose session never closes, so an `EnrichedClick` for
an idle Shopper cannot pin state forever.

### Promo Rules: structural condition, latest rule wins

**The problem.** `PromoRule(ruleId, description, discountPercent)` has no
condition field. The [domain schemas design](2026-08-16-domain-schemas-design.md)
calls that deliberate: the condition lives in where the rule is applied in the
job graph. Nothing then said what that position is. Separately, the generator
emits a fresh `ruleId` every 30 seconds, so any keep-everything policy grows
`MapState` without bound. After one hour it holds 120 rules.

**The decision.** Two parts.

**Condition.** A Recommendation earns the discount only when its candidate
Product had a price-drop match inside the join interval. Unmatched candidates
keep `discountPercent` at `0.0`. This is the structural condition the schema
design intended, and it is what the walkthrough already describes as the rule's
"extra 5 percent" applying to matches. The generator's own description string,
`"N% off, price-drop bonus"`, says the same thing.

**Composition and retention.** `MapState<String, PromoRule>` under one fixed
key. Each arriving rule replaces the previous one. Retention needs no policy,
because state holds exactly one entry forever.

A map holding one entry looks like the wrong structure, and it is not a choice.
Broadcast state in Flink is always a map: `ctx.getBroadcastState(...)` takes a
`MapStateDescriptor` and there is no value-typed form. Using one fixed key is
how a single-valued broadcast rule is expressed.

The decisive reason for the structural condition is that it is the only option
under which the interval join affects the output. If every Recommendation got
the discount, the whole Product-keyed branch could be deleted and the
`recommendation` topic would look identical apart from `reason`.

**Expected split, and a correction to an earlier figure.** The 33 percent
interval-join match rate is per Click. Per Recommendation it is much higher,
because the candidate is the most-clicked Product and gets several chances to
match: roughly 80 percent of Recommendations carry a discount and 20 percent sit
at `0.0`. Both populations are non-empty in every run, which is what the
assertion test and the Drill need, and the high rate makes a mid-run rule change
visible within seconds.

Rejected: **max over all active rules, keeping every rule in state.** Exercises
`MapState` iteration properly, which a single entry does not. Rejected because
it needs a retention policy invented for a generator that will never stop
emitting new ids, and unbounded broadcast state is a worse lesson than a thin
one.

Rejected: **unconditional, every Recommendation gets the active rule.**
Demonstrates "change a rule, output changes, no restart" most directly.
Rejected because it makes the interval join decorative.

### Async I/O uses `orderedWait`

**The problem.** Phase 3's restart Drill asserts the output after recovery is
identical to an uninterrupted run. The Flink 2.2 documentation is explicit that
unordered mode does not give that:

> Watermarks establish an *order boundary*. Records are emitted unordered only
> **between** watermarks.

Several Recommendations can fall between two watermarks, so two runs would
differ by line order and the Drill would fail for a reason unrelated to
checkpointing.

**The decision.** `AsyncDataStream.orderedWait`. The usual reason to accept
unordered is throughput, and the same documentation page removes that incentive:

> in the presence of watermarks, the *unordered* mode introduces some of the
> same latency and management overhead as the *ordered* mode does

So unordered would cost a Drill and buy almost nothing.

**Two determinism constraints that follow, and hold for any mock.**

1. The reply must be a pure function of the request. A random or
   wall-clock-dependent reply breaks the same Drill, for the same reason
   `generatedAt` had to become the window end rather than `Instant.now()`.
2. `timeout(...)` must be overridden, or a timeout fails the job. But a timeout
   firing during normal running would itself be nondeterministic. So normal
   latency stays well under the timeout, and the timeout path is proven by an
   assertion test with an injected slow client, never by the running job.

### The recommendation service is in-process, behind an interface

**The problem.** The real weight of this choice lands in Phase 5, not here.
Under `MiniCluster` any mock works. Once the job becomes a `FlinkDeployment`, an
HTTP mock needs a Deployment, a Service, an ArgoCD Application, and an endpoint
flag, added to a phase budgeted at 9 hours that does not mention it.

**The decision.** An interface with a deterministic in-process implementation.

```java
public interface RecommendationClient {
    CompletableFuture<String> suggest(RecommendationRequest request);
}
```

The default implementation computes its reply from the request alone, with
latency simulated on a dedicated executor. No wall clock, no randomness. It
ships inside the job jar, so Phase 5 needs no new manifests.

The interface is the seam, and it exists for one concrete reason: a test injects
a slow or failing implementation to cover the `timeout(...)` path, which is
otherwise unreachable.

The `RichAsyncFunction` holds no state, so the documented restriction on keyed
state inside async functions never arises.

Rejected: **a real HTTP service.** Exercises a genuine network boundary,
connection pooling, and real timeouts. Rejected on Phase 5 scope, and because
HTTP latency is not deterministic, which puts the restart Drill at risk again.

Rejected: **in-process with no interface.** Least code. Rejected because there
is then no seam to inject a slow client, so the timeout path cannot be tested.

## Architecture

```
                      clickstream (watermarked once, before the fork)
                                       |
        +------------------------------+------------------------------+
        |                              |                              |
  keyBy(shopperId)              keyBy(shopperId)              keyBy(productId)
        |                              |                              |
  session window                CEP: abandoned cart           intervalJoin  <--- keyBy(productId)
  SessionAggregator             within 30s                    between(-2s, +2s)   price-change
        |                              |                              |
   SessionSignal                 timed-out partials            EnrichedClick
        |                          -> side output                     |
     map to                             |                       keyBy(shopperId)
  ShopperSignal                   ShopperSignal                       |
        |                              |                              |
        +-------------- union ---------+                              |
                        |                                             |
                        +------------ connect ------------------------+
                                       |
                        KeyedCoProcessFunction  (ADR 0003 merge)
                          processElement1: ShopperSignal
                          processElement2: EnrichedClick
                          holds the Phase 3 cooldown
                                       |
                          RecommendationRequest      +--> UNMATCHED side output
                                       |
                                    connect <------------- promo-rule (broadcast)
                                       |
                        BroadcastProcessFunction (stateless per record)
                                       |
                          RecommendationRequest + discountPercent
                                       |
                          AsyncDataStream.orderedWait
                            RichAsyncFunction -> RecommendationClient
                                       |
                                 Recommendation
                                       |
                            KafkaSink, EXACTLY_ONCE
```

Watermarks are still assigned once on the raw stream, before the fork, per ADR
0003. Assigning them per branch would let the two sides drift and make the
merge's event time meaningless.

Three side outputs now exist, and they carry three different populations:

| Side output | Source | Meaning |
|---|---|---|
| `LATE_CLICKS` | session window, Phase 3 | Click behind the watermark, missed its window |
| `CEP_TIMED_OUT` | `TimedOutPartialMatchHandler` | viewed and never carted inside 30s |
| `UNMATCHED` | merge operator | candidate Product found no Price Change in the interval |

## New types

All in `:domain`, which keeps its zero dependencies. All flat records, which is
what Flink's POJO rules require.

```java
public record EnrichedClick(String shopperId, String productId, Instant clickTime,
                            double newPrice, Instant priceChangeTime) {}

public enum SignalKind { BROWSING_SESSION, CART_ABANDONMENT }

public record ShopperSignal(String shopperId, SignalKind kind, Instant eventTime,
                            String productId, int clickCount) {}

public record RecommendationRequest(String shopperId, String candidateProductId,
                                    boolean priceDropMatched, boolean cartAbandoned,
                                    double discountPercent, String reason,
                                    Instant generatedAt) {}
```

`clickCount` is meaningful only for `BROWSING_SESSION`. That unused field is the
price of the union, and Flink's rejection of sealed interfaces leaves no
alternative.

`SessionSignal` survives unchanged. A chained `.map()` converts it, which costs
no shuffle, so Phase 3's `SessionAggregator` and its Drill stay exactly as
built.

`Recommendation` is unchanged. `RecommendationRequest.candidateProductId` is the
Product the job proposes; `Recommendation.productId` is what the service
answered. They are allowed to differ, and that difference is the whole reason
the async call exists.

`generatedAt` is the Browsing Session's window end, never `Instant.now()`, for
the reason Phase 3 established.

## Dependencies

| Artifact | Scope | Why |
|---|---|---|
| `flink-cep:2.2.0` | **open, see below** | The Pattern API |
| `flink-test-utils:2.2.0` | `testImplementation` | Bounded-job tests |
| `flink-streaming-java:2.2.0` test-jar | `testImplementation` | `ProcessFunctionTestHarnesses` |
| JUnit 5 | `testImplementation` | No test framework exists yet |
| AssertJ | `testImplementation` | Readable assertions on collected output |

**`flink-cep`'s scope is genuinely open and gets checked first.** It is not
bundled in `flink-dist` the way `flink-connector-base` turned out to be, so the
`compileOnly` plus `runtimeOnly` pattern the other Flink modules use may be
wrong here. Whether it needs `implementation` depends on how Phase 5 builds its
fat jar and its operator image. This is the same shape of problem Task 3 hit,
so it is checked before any CEP code is written, the way Task 0 checked the
Operator version.

The exact artifact coordinate for `ProcessFunctionTestHarnesses` is reasoning
from the documentation, not a verified fact. It gets confirmed when that task
starts.

## Testing

Six tests, all at parallelism 1.

| Test | Tool | Proves |
|---|---|---|
| `MergeFunctionTest` | `ProcessFunctionTestHarnesses.forKeyedCoProcessFunction` | emission only on session close, candidate narrowing, `reason` precedence, `UNMATCHED` side output, state cleared |
| `PromoRuleBroadcastTest` | `ProcessFunctionTestHarnesses.forBroadcastProcessFunction` | a new rule replaces the old one and the next matched request carries the new number |
| `IntervalJoinTest` | bounded job | a Click 1s from a Price Change matches, one 4s away does not |
| `CartAbandonmentCepTest` | bounded job | view then cart with no checkout matches; view, cart, checkout does not; view with no cart lands in the timed-out side output |
| `AsyncRecommendationTest` | bounded job | ordered output, and the timeout path with an injected slow client |
| `PersonalizationJobTest` | bounded job, whole graph | the wiring: union, re-key, merge, broadcast, async, in one run |

**Why parallelism 1.** Otherwise Task 4's bug returns in a new costume. Idle
subtasks pin the watermark at `Long.MIN_VALUE` and nothing fires. In production
`.withIdleness(...)` handles it. In a test, parallelism 1 is simpler and removes
a source of flakiness.

**Why bounded input.** A bounded source emits `Watermark.MAX_WATERMARK` when it
finishes. That closes every session window and fires every CEP timeout, so a
test never waits or guesses. Without it these tests would hang.

**Why the tool per concept is not a free choice.** Two of the five concepts are
functions this project writes, so a harness can wrap them. The other three are
subgraphs the library builds from `.intervalJoin(...)`, `CEP.pattern(...)`, and
`AsyncDataStream.orderedWait(...)`, with no single user function to instantiate.
Those need a running job.

**Why the sixth test exists.** Harness tests prove each function's logic and see
none of the wiring. The graph is four operators deep with a union, a re-key, a
broadcast, and an async stage. Wiring errors live exactly there, and no harness
test can reach them. Phase 3 has no unit test at all, so nothing is duplicated.

## Drill

Change a Promo Rule while the job runs, and watch the `recommendation` topic
change without a restart.

1. Start the generator and the job. Consume `recommendation` with
   `isolation.level=read_committed`, which Phase 3 established is required
   whenever the sink is `EXACTLY_ONCE`.
2. Record the `discountPercent` on Recommendations carrying
   `reason = "price-drop"` or `"cart-abandoned"`.
3. Wait for the generator's next Promo Rule, which arrives every 30 seconds with
   a discount between 5 and 20 percent.
4. Confirm subsequent discounted Recommendations carry the new number, that the
   job never restarted, and that Recommendations reading
   `"most-viewed-in-session"` still sit at `0.0`.

The last clause is what separates broadcast state working from a rule applying
to everything.

## Effect on Phase 3

No change is needed now, and that is a deliberate call rather than an oversight.

Task 6's `RecommendationDecider` should be built exactly as its plan says,
emitting `Recommendation` directly. Phase 4 splits it into a
`RecommendationRequest` plus the async stage. That is one small refactor of one
class. Building Task 6 against the Phase 4 shape instead would mean Tasks 6
through 10 carry a `RecommendationRequest` type with no async stage to consume
it, and Task 9's Drill could not exercise the sink it is meant to prove.

## Documents this phase must change

| Document | Change |
|---|---|
| [ADR 0003](../../adr/0003-interval-join-key-and-semantics.md) | Amendment, or a superseding ADR: `intervalJoin` cannot emit the `Unmatched Click` side output, so it moves to the merge and changes grain |
| [design spec](2026-07-25-flink-k8s-personalization-design.md) | The coverage map's CEP line still says "viewed a product repeatedly, viewed a competitor, went idle" |
| [walkthrough](../../knowledge/flink-job-walkthrough.md) | Step 9 puts the async call inside the merge function. Async I/O is its own operator |
| `CONTEXT.md` | The glossary says **Price Change** while the topic is `product-change` and the type is `ProductChange`. Add **Cart Abandonment**. Restate **Unmatched Click** at its new grain |
| [status.md](../plans/status.md) | Phase 4 progress, and closing the `ProductChange` warning Phase 3 surfaced |

A new knowledge doc, `docs/knowledge/phase-4-advanced-flink.md`, follows the
one-per-phase pattern.

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| CEP overruns, as the design spec predicts | The phase passes 16 hours | The spec pre-agrees the fallback: trim CEP to a single pattern before cutting anything from the Kubernetes HA side. This design already uses a single pattern, so the remaining trim is dropping `TimedOutPartialMatchHandler` and its side output |
| `flink-cep` packaging is wrong for Phase 5's fat jar | Rework after Phase 4 is written | Checked before any CEP code, as this phase's Task 0 |
| Async I/O reorders output and fails Phase 3's Drill | A Drill fails for a reason that is not a bug | `orderedWait`, plus a mock whose reply is a pure function of the request |
| The union's unused `clickCount` field spreads | Wide records with optional fields everywhere | The union exists once, on the Shopper side only, because Flink's type system forces it. The Product side stays typed |

## Out of scope

| Item | Belongs to |
|---|---|
| Any `FlinkDeployment`, containers, the S3 plugin directory | Phase 5 |
| JobManager HA, savepoints, zone spread | Phase 5 |
| Autoscaling, varying parallelism | Phase 6 |
| Blue/green promotion, OTel | Phase 7 |
| Dashboards over the three side outputs | Phase 8 |
| A consumer for `StockChange` | No phase currently needs one |

[d-cep]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/libs/cep.md
[d-async]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/operators/asyncio.md
[d-types]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/fault-tolerance/serialization/types_serialization.md
[d-testing]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/testing.md
[d-join]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/operators/joining.md
[d-broadcast]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/fault-tolerance/broadcast_state.md
[d-process]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/datastream/operators/process_function.md
