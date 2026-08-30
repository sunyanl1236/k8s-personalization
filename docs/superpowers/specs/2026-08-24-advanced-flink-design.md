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

### `Product Change` is one state snapshot, not a sum type

Recorded in full as [ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md).
Summarised here because the rest of this design depends on it.

**The problem.** `ProductChange` was a sealed interface. Asked directly, Flink
2.2 answers `GenericTypeInfo` and `KryoSerializer` for it, while both variants
resolve to `PojoTypeInfo`. `pipeline.generic-types: false` turns that Kryo
fallback into a hard failure at job-graph construction, so a sum type can never
be a stream element type here.

Two further findings turned a workaround into a redesign. Nothing in the project
consumes `StockChange`: it is named twenty-five times across the documents and
never once read. And `PriceChange` carries no previous price, so "price *drop*"
has never been checkable, even though the generator stamps every Promo Rule with
`"N% off, price-drop bonus"`.

**The decision.** One record carrying the Product's full state and the state it
replaced. `PriceChange` and `StockChange` are deleted.

```java
public record ProductChange(String productId, Instant eventTime,
                            double price, double previousPrice,
                            int stock, int previousStock) {}
```

No discriminator, no nullable field, every field always a real value. On a
Product's first event the previous values equal the current ones.

| Question | Expression |
|---|---|
| Did the price drop? | `price < previousPrice` |
| Is the Product out of stock? | `stock == 0` |
| Did stock move? | `stock != previousStock` |

**Stock gets a job**, which is what justifies keeping it: a Recommendation is
suppressed when its candidate Product is out of stock, routed to a fourth side
output rather than dropped.

**The join must not filter on the drop.** This is the consequence most easily
got wrong. If only price drops reach the interval join, the merge never learns
the stock of a Product that went out of stock without a price move, and the
suppression rule silently fails for exactly the Products it exists to catch. So
the join matches **any** Product update near a Click, and `EnrichedClick`
carries the values the merge then tests.

**Two generator changes**, both deliberate synthetic-data choices in the spirit
of Phase 3's derived session gap. `ProductChangeFactory` keeps a ten-entry map
of each Product's last price and stock. And stock is `0` with probability `0.1`
rather than the natural `random.nextInt(501)`, which would fire the suppression
rule about once in five hundred events and make it unobservable.

**The wire format drops `"type"`**, since a discriminator restating what the
fields already say is a second source of the same fact.

The deserializer becomes one schema with no narrowing:

```java
public class ProductChangeDeserializationSchema
        implements DeserializationSchema<ProductChange> {

    @Override
    public ProductChange deserialize(byte[] message) {
        return JsonCodec.productChangeFromJson(message);
    }

    @Override
    public TypeInformation<ProductChange> getProducedType() {
        return TypeInformation.of(ProductChange.class);
    }
}
```

Alternatives and their costs are in ADR 0008. The main one rejected was a custom
`TypeInfoFactory` plus `TypeSerializer` plus `TypeSerializerSnapshot`, about 150
lines and a checkpoint-compatibility contract for Phases 5 through 7.

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
| `processElement2(EnrichedClick)` | record the Product's `stock`; if `price < previousPrice`, put the `EnrichedClick` into `matchesByProduct` |
| `processElement1(CART_ABANDONMENT)` | put `productId` into `abandonedCarts` |
| `processElement1(BROWSING_SESSION)` | read both maps for the candidate Product, emit, clear both maps |

Both are `MapState<String, Boolean>`, `productId -> true`.

Three reasons. One trigger means one Recommendation per Browsing Session, so the
cooldown logic Phase 3 Task 6 builds carries over unchanged, even though the
class it lives in changes its output type. The output rate stays tied to
session closes, which is the rate Phase 3 sized its Drill around. And `reason`
becomes the field carrying which branches contributed, which is what the Phase 3
design says `reason` exists for.

A candidate Product whose recorded `stock` is `0` is suppressed before any of
this, routed to the `OUT_OF_STOCK` side output and never reaching the sink.

For everything that does emit, `reason` precedence is, most specific first:
`"cart-abandoned"`, then `"price-drop"`, then Phase 3's
`"most-viewed-in-session"`.

**Both lookups are narrowed to the candidate Product, and that narrowing is
load-bearing.** The candidate is the most-clicked Product of the Browsing
Session. A Shopper abandons about 1.6 carts per Browsing Session across 10
Products, so an unnarrowed check would put `reason = "cart-abandoned"` on
roughly 80 percent of Recommendations and drown the other two values. Narrowed
to the candidate it lands near 15 percent.

**The two maps have different lifetimes, and that is deliberate.** Decided
2026-08-28, after working the timing through.

A cart abandonment is confirmed `cep-within` (30s) after the VIEW, because an
absence cannot be proven any sooner. A Browsing Session closes 6s after its last
Click and spans about 40s. So a VIEW in the first ~16 seconds of a session
confirms in time and later ones do not: roughly **40% land in their own session,
60% arrive after it closed**.

Those 60% are **not lost**. The write lands after the session-close clear, so the
entry survives and the Shopper's *next* session close reads it. The effect is
attribution one session late, not disappearance.

The resolution is that the two facts are not the same kind of fact:

| Fact | Scope | Why |
|---|---|---|
| price-drop match | **this Browsing Session** | it is about a specific Click near a specific price move |
| cart abandonment | **this Shopper, for a while** | "abandoned a cart on P1 recently" survives a session boundary |

So `matchesByProduct` clears on session close. `abandonedCarts` expires on its
own event-time timer, **60 seconds** after the abandonment, which is derived
rather than picked. A Shopper produces about 0.04 cart abandonments per second:

| TTL | Retained | Share of Recommendations reading `"cart-abandoned"` |
|---|---|---|
| clear on session close | ~0.4 | too few, and 60% silently discarded |
| **60s** | **~2.4 across 10 Products** | **~21%**, matching the design's expectation |
| 5 min | ~12 | most Products in the set, swamping the other reasons |

60 seconds is also the existing cooldown, so the job has one retention horizon
rather than two.

**The argument that actually carries this decision is bounded staleness, not
loss.** With clear-on-session-close, how long an abandonment survives depends on
when the Shopper next browses. A Shopper who wanders off leaves the entry in
state until they return, possibly hours later, and it is then applied as though
fresh. State stays bounded, at most one entry per Product, but it goes stale
without limit. A timer makes retention a stated 60 seconds instead of an
emergent property of Shopper behaviour.

Rejected: **clear both maps on session close.** Simpler, no timer, and it loses
nothing. Rejected only on that staleness argument, which makes it a close call
rather than a clear one.

**State stays bounded** by the session-close clear on one map and the timer on
the other, so an `EnrichedClick` for a Shopper who never closes a session cannot
pin state forever.

Rejected: **shorten `cep-within` to about 10s** so abandonments confirm before
their session closes. No merge changes at all. Rejected because "did not check
out within 10 seconds" is a weak claim about hesitation: it changes what the
Signal means in order to make a plumbing problem go away.

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

**Expected split.** Under ADR 0008 the join matches any Product update, while
the discount needs a genuine price drop, so the two rates differ. At the
generator's defaults of 1.0 Product Changes per second over 10 Products with a 4
second window:

| Population | Share |
|---|---|
| Clicks matching any Product update | 33% |
| Clicks matching a genuine price drop | about 12% |
| Recommendations carrying a discount | about 41% |
| Recommendations suppressed, out of stock | about 10% |
| Browsing Sessions reaching `UNMATCHED` | about 20% |

Per Recommendation the figures beat the per-Click ones because the candidate is
the most-clicked Product and gets several chances to match. Every population is
non-empty in every run, which is what the assertion tests and the Drills need.

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

Four side outputs now exist, and they carry four genuinely different
populations:

| Side output | Source | Meaning |
|---|---|---|
| `LATE_CLICKS` | session window, Phase 3 | Click behind the watermark, missed its window |
| `CEP_TIMED_OUT` | `TimedOutPartialMatchHandler` | viewed and never carted inside 30s |
| `UNMATCHED` | merge operator | candidate Product found no Product Change in the interval |
| `OUT_OF_STOCK` | merge operator | candidate Product had `stock == 0`, so the Recommendation was suppressed |

The fourth exists because a suppression nobody can count is a bad rule.

## New types

All in `:domain`, which keeps its zero dependencies. All flat records, which is
what Flink's POJO rules require.

```java
public record EnrichedClick(String shopperId, String productId, Instant clickTime,
                            double price, double previousPrice,
                            int stock, Instant changeTime) {}

public enum SignalKind { BROWSING_SESSION, CART_ABANDONMENT }

public record ShopperSignal(String shopperId, SignalKind kind,
                            Instant eventTime, String productId) {}

public record RecommendationRequest(String shopperId, String candidateProductId,
                                    boolean priceDropMatched, boolean cartAbandoned,
                                    double discountPercent, String reason,
                                    Instant generatedAt) {}
```

Four fields, and every one is meaningful for **both** kinds: `eventTime` is the
window end for a Browsing Session and the pattern's completion time for a cart
abandonment, while `productId` is the candidate Product in one case and the
abandoned Product in the other.

An earlier draft also carried `clickCount`, on the reasoning that `SessionSignal`
has it. Nothing downstream reads it: neither `RecommendationRequest` nor
`Recommendation` carries a click count, so it would have crossed a shuffle on
every record for no consumer. `SessionSignal` keeps it for the session branch's
own output. The rule is that a transport type carries what its **consumer**
reads, not the union of what its producers happen to know.

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
| `flink-cep:2.2.0` | `compileOnly` + `runtimeOnly` | The Pattern API. Provided in the image's `lib/`, see below |
| `flink-test-utils:2.2.0` | `testImplementation` | Bounded-job tests |
| `flink-runtime:2.2.0:tests` | `testImplementation` | `ProcessFunctionTestHarnesses` and the `*OperatorTestHarness` classes. **Confirmed 2026-08-28**, see below |
| JUnit 5 | `testImplementation` | No test framework exists yet |
| AssertJ | `testImplementation` | Readable assertions on collected output |

**`flink-cep`'s scope, resolved 2026-08-24.** Two sources agree that the official
distribution provides it, so the job must not bundle it.

The `flink:2.2.0` image holds `lib/flink-cep-2.2.0.jar`. The
[advanced configuration page][d-advanced] explains the shape behind that:

> the Flink Core Dependencies do not contain any connectors or libraries (i.e.
> **CEP**, SQL, ML) ... The `/lib` directory of the Flink distribution
> **additionally** contains various JARs including commonly used modules ...
> These are loaded by default

So CEP is not inside `flink-dist.jar`, but it ships as a separate jar in `lib/`
and is on the classpath by default. Scope is `compileOnly` plus `runtimeOnly`,
matching `flink-streaming-java`.

**Two things this check surfaced that Phase 5 needs, and nothing else records.**

`flink-s3-fs-hadoop` is in `opt/`, not `lib/`. It is shipped and **not** on the
classpath. ADR 0001 predicted it becomes a plugin directory in Phase 5, and the
image confirms it.

More importantly, `runtimeOnly` alone will not keep these jars out of Phase 5's
fat jar. Gradle's Shadow plugin builds from the runtime classpath, so every
`runtimeOnly` dependency gets bundled. Today that set is
`flink-streaming-java`, `flink-clients`, `flink-statebackend-rocksdb`,
`flink-connector-base`, and now `flink-cep`. Bundling any of them alongside a
distribution that already loads them is the duplicate-class problem the
`compileOnly` split exists to avoid. Phase 5 needs a dedicated configuration or
an explicit Shadow exclusion set. The scope declaration expresses the intent;
it does not by itself enforce it.

The `lib/` guarantee also holds only for an unmodified image. The same page
notes those jars "can be removed from the classpath just by removing them from
the `/lib` folder", so a slimmed base image in Phase 5 would invalidate this.

**The artifact holding `ProcessFunctionTestHarnesses` is resolved, 2026-08-28.**
It is not `flink-streaming-java`, and `flink-test-utils` does not bring it: that
pulls only the plain `flink-runtime`. Both `ProcessFunctionTestHarnesses` and
the `*OperatorTestHarness` base classes live in the **tests classifier** of
`flink-runtime`, so it must be declared:

```groovy
testImplementation "org.apache.flink:flink-runtime:${flinkVersion}:tests"
```

Verified by locating the classes inside `flink-runtime-2.2.0-tests.jar` and then
compiling and running a test that imports them.

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
| [design spec](2026-07-25-flink-k8s-personalization-design.md) | ~~The coverage map's CEP line~~ **corrected 2026-08-30** to the abandoned cart |
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
| The union type accretes fields its consumer does not read | Wide records with optional fields, copied across a shuffle for nobody | `ShopperSignal` carries only what the merge reads, four fields, all meaningful for both kinds. Adding one because a producer happens to know it is the failure mode |

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
[d-advanced]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/configuration/advanced.md
