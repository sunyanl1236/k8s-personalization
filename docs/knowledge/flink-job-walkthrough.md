# Flink job walkthrough: what the pipeline does, with one worked example

How the whole job fits together, taught through one worked example whose numbers
carry from the first section to the last.

Terminology follows [CONTEXT.md](../../CONTEXT.md). **Product Change** covers
both price and stock moves; ADR 0003 still says "Price Change" and has not been
reworded.

## The feature, in one sentence

A live recommendation engine. It watches what a Shopper does right now, checks
that against what is happening in the catalogue right now, and pushes back a
personalized Recommendation while they are still on the page.

```
Click ──────────┐
Product Change ─┼──>  [ Flink job ]  ──> Recommendation
Promo Rule ─────┘
```

## Why the job merges signals at all

The product decides **who gets a discount, and why**. Discounts cost margin:

- give one to a Shopper who would have bought anyway, and it is wasted
- give one to somebody never interested, and it is wasted twice

No single signal finds the Shoppers a small discount actually converts:

| Signal alone | What it lets you say | Why that is weak |
|---|---|---|
| Browsing Session | "you looked at P1 a lot" | a reminder, not an offer |
| Price drop | "P1 got cheaper" | that is a mass email |
| Cart Abandonment | "you left P1 behind" | might be full price, might be unbuyable |

Each branch answers a different question:

| Branch | Question |
|---|---|
| Session window | what are they interested in **right now**? |
| Interval join | is there a **reason to act now**? |
| CEP | is there **hesitation**? |
| Broadcast rule | what can marketing **afford today**? |
| Stock | can we **actually sell it**? |

Merged, they make one sentence no branch could produce alone:

> You kept coming back to **P1** this visit, put it in your cart, and its price
> just dropped. Here is an extra **10% off**. It is in stock.

`reason` records which of those held, as a ladder of how strong the case is:

| `reason` | Evidence | Reaches the sink? |
|---|---|---|
| `"cart-abandoned"` | focused, carted, did not buy, price moved | yes, with a discount |
| `"price-drop"` | focused, price moved near their Click | yes, with a discount |
| `"most-viewed-in-session"` | focused only, **no trigger** | no, routed to `UNMATCHED` |
| *(candidate unbuyable)* | `stock == 0` | no, routed to `OUT_OF_STOCK` |

- **A Recommendation is published only when a business event justifies it.**
  Intent alone is not enough.
- That narrows Phase 3, which published one per closed Browsing Session. The job
  now stays quiet rather than sending an offer it cannot justify, costing roughly
  a fifth of the output volume.
- `reason` also earns its keep afterwards: it is what lets you measure conversion
  per signal type, and find out whether the cart-abandonment path is worth the
  margin it costs.

## The running example

Two Shoppers, two Products, one Promo Rule. Every later section uses these
numbers.

| Event time | Shopper | Product | Action |
|---|---|---|---|
| 10:00:00 | 42 | P1 | VIEW |
| 10:00:02 | 43 | P2 | VIEW |
| 10:00:03 | 42 | P2 | VIEW |
| 10:00:05 | 43 | P1 | VIEW |
| 10:00:06 | 42 | P1 | ADD_TO_CART |

| Event time | Product | What moved |
|---|---|---|
| 10:00:01 | P1 | price dropped |
| 10:00:04 | P2 | price dropped |

Promo Rule, broadcast to every worker before any of this: a Click within 2
seconds of a price drop earns an extra 5 percent off.

## The whole job graph

Every section below zooms into one part of this.

```
clickstream ──► clicks : DataStream<Click>
                  │   watermarks assigned ONCE, here, before any fork
                  │
     ┌────────────┼────────────────────────┬──────────────────────┐
     │            │                        │                      │
 keyBy(shopperId) │                  keyBy(shopperId)       keyBy(productId)
     │            │                        │                      │
 session window   │                    CEP within 30s      intervalJoin(-2s,+2s)
   gap 6s         │                        │                      │
     │  └► LATE_CLICKS                     │ └► CEP_TIMED_OUT     │
     │                                     │                      │
 SessionSignal                             │                 EnrichedClick
     │                                     │                      │
   .map                                    │                keyBy(shopperId)
     │                                     │                      │
 ShopperSignal ◄──────── union ──────► ShopperSignal              │
 (BROWSING_SESSION)                   (CART_ABANDONMENT)          │
                    │                                             │
               keyBy(shopperId)                                   │
                    │                                             │
                    └──────────── connect ◄───────────────────────┘
                                    │
                            SignalMerger
                       processElement1: ShopperSignal
                       processElement2: EnrichedClick
                       holds the 60s event-time cooldown
                                    │
                     RecommendationRequest ──► UNMATCHED
                                    │      └─► OUT_OF_STOCK
                                    │
                              connect ◄──────── promo-rule (broadcast)
                                    │
                            PromoRuleApplier
                          fills discountPercent
                                    │
                     AsyncDataStream.orderedWait
                       ──► recommendation service (mocked)
                                    │
                     Recommendation ──► KafkaSink ──► recommendation topic
```

Four side outputs leave that graph, carrying four different populations.
Conflating them is the most common misreading of this job.

| Side output | Leaves from | The question it answers |
|---|---|---|
| `LATE_CLICKS` | session window | did this Click arrive **before its window fired**? A *timing* failure |
| `CEP_TIMED_OUT` | `TimedOutPartialMatchHandler` | was a VIEW **ever followed by a cart**? |
| `UNMATCHED` | `SignalMerger` | did the candidate find **any nearby Product Change**? A *matching* outcome, not a failure |
| `OUT_OF_STOCK` | `SignalMerger` | was the candidate unbuyable, so the Recommendation was suppressed? |

`UNMATCHED` leaves the merge rather than the join, and that is forced:
`intervalJoin` is an inner join, so `ProcessJoinFunction` is never called for a
Click that found nothing and there is no callback in which to emit it. See
[ADR 0009](../adr/0009-unmatched-click-moves-to-the-merge.md).

## Why the job forks

Two questions need the same Clicks grouped two different ways:

- **What has this Shopper been doing?** Needs one Shopper's Clicks together,
  regardless of Product.
- **Did this Click happen near a change on the same Product?** Needs one
  Product's Clicks together, regardless of Shopper.

One grouping cannot serve both, and `CONTEXT.md` says why: a Product Change is
**a fact about a Product, never about a Shopper**. It carries no `shopperId` to
join on.

**`keyBy(shopperId)`** answers the session question, and never sees Product
Change data at all:

| Shopper | Their Clicks |
|---|---|
| 42 | 10:00:00 P1, 10:00:03 P2, 10:00:06 P1 |
| 43 | 10:00:02 P2, 10:00:05 P1 |

**`keyBy(productId)`** answers the matching question, and cannot reconstruct
either Shopper's session:

| Product | Clicks | Product Change |
|---|---|---|
| P1 | 10:00:00 by 42, 10:00:05 by 43, 10:00:06 by 42 | 10:00:01, price dropped |
| P2 | 10:00:02 by 43, 10:00:03 by 42 | 10:00:04, price dropped |

## Watermarks

**The problem:**

- Clicks do not arrive in the order they happened.
- Working off arrival order would make a session's length reflect network speed
  rather than Shopper behaviour, so the job uses the event time inside each Click.
- But then the job can never be certain it has seen everything for a moment,
  since a delayed Click could always still show up. It needs a rule for when to
  stop waiting.

**The rule.** A watermark is the job's moving estimate of *how far back in event
time it has safely seen everything*: here, the largest event time seen so far
minus a 2 second bound.

- **Minus, not plus.** A watermark is a claim, and the job cannot claim to have
  seen a point in time it has not reached.
- **One shared number**, not per Shopper and not per Product. Any Click from
  anyone can push it forward, and it never moves backward.

| Step | Click | Event time | Watermark judging it | Late? |
|---|---|---|---|---|
| 1 | Shopper A | 10:00:05 | none yet | no |
| 2 | Shopper B | 10:00:12 | 10:00:03 | no |
| 3 | Shopper A | 10:00:08 | 10:00:10 | **yes** |

Row 3 is judged late because of Shopper B's earlier Click, a different person,
not because of anything Shopper A did.

**Assigned once, before the fork.** Per-branch watermarks could disagree about
what counts as recent, decided only by which branch ran faster, so the same Click
would be on time in one branch and late in the other.

## Session windows, and what "late" actually means

A Browsing Session is a run of one Shopper's Clicks with no gap longer than the
session gap. Nothing to do with a browser tab, cookie, or login.

**The end boundary anchors to the most recent Click, not the first**, because the
question is "how long since activity", not "how long since this started". With a
6 second gap:

| Click | Gap from previous | New boundary |
|---|---|---|
| 10:00:00 | first | 10:00:06 |
| 10:00:03 | 3s | 10:00:09 |
| 10:00:07 | 4s | 10:00:13 |
| 10:00:11 | 4s | 10:00:17 |

Anchoring to the first Click would fire at 10:00:06, before the 10:00:07 Click
arrived, splitting one real session in two.

**Behind the watermark is not the same as too late for a window.** Two different
checks:

- one compares a number shared across the whole stream
- the other compares the end boundary of the one window this Click belongs to

| Click | Event time | Watermark | This Click's window | Outcome |
|---|---|---|---|---|
| A, 1st | 10:00:05 | none | new, ends 10:00:11 | accepted, opens |
| B, 1st | 10:00:12 | 10:00:03 | new, ends 10:00:18 | accepted, opens |
| D, 1st | 10:00:09 | 10:00:10 | new, ends 10:00:15 | accepted, opens |
| A, 2nd | 10:00:08 | 10:00:10 | existing, extends to 10:00:14 | accepted, extends |
| C, 1st | 10:00:05 | 10:00:20 | new, ends 10:00:11 | **rejected, Late Click** |

- Rows 3 and 4 sit behind the watermark by one second each and are still
  accepted, because their own window boundary is still ahead of it.
- Only the last row's window would already be behind the watermark before it
  could exist.
- A rejected Click is not dropped. Flink's default would drop it, but
  `.sideOutputLateData(...)` routes it to `LATE_CLICKS`. Phase 8 uses that count
  to tell whether the watermark bound is too tight.

## The Product-keyed branch: interval join and `Unmatched Click`

Each Click is joined against Product Changes on the same Product within ±2
seconds:

| Click | Nearest change on that Product | Gap | Result |
|---|---|---|---|
| 42 on P1 @ 10:00:00 | P1 dropped @ 10:00:01 | 1s | match |
| 42 on P2 @ 10:00:03 | P2 dropped @ 10:00:04 | 1s | match |
| 43 on P1 @ 10:00:05 | P1 dropped @ 10:00:01 | 4s | no match |

- **The join matches any Product update, not only price drops.** An
  `EnrichedClick` carries the Product's `stock`, and filtering to drops would
  blind the job to Products that went out of stock without a price move, breaking
  the suppression rule for exactly the Products it exists to catch.
  `price < previousPrice` is asked later, on the record.
- **`Unmatched Click` and `Late Click` are different populations.** A Late Click
  is a *timing* failure: it arrived after its window fired. An Unmatched Click is
  a *matching* outcome: it arrived on time and there was nothing nearby to pair
  with. Neither is an error.
- Because `intervalJoin` is an inner join, the unmatched population is counted at
  the merge, **once per Browsing Session candidate** rather than once per Click.

## CEP: the abandoned cart

Watches one ordered sequence per Shopper: **viewed a Product, added that same
Product to the cart, did not check out within 30 seconds.**

Shopper 42 viewed P1 at 10:00:00 and carted it at 10:00:06. No CHECKOUT follows,
so at 10:00:30 the pattern completes and a `CART_ABANDONMENT` Signal is raised
for P1.

- **The same-Product link needs an `IterativeCondition`.** The Product is unknown
  when the job is built, so the condition reads it from the match in progress via
  `ctx.getEventsForPattern("view")`. A `SimpleCondition` sees only the candidate
  event and would let any cart complete any view.
- **`followedBy`, not `next`.** Strict adjacency would require the cart to be the
  very next Click, which almost never happens.
- **An absence takes time to prove.** Nothing concludes at 10:00:06; the answer
  exists only once the window expires. A partial match that never gets a cart goes
  to `CEP_TIMED_OUT`.
- **Consequence:** a session closes 6 seconds after its last Click, so roughly
  60% of abandonments confirm *after* their own session closed and are read by the
  Shopper's next one.

## Broadcast state: the Promo Rule

- Not grouped by Shopper or Product.
- Every parallel worker holds the full current rule, and a change updates every
  worker at once with no restart. That is the property Drill C tests. How that
  differs from the merge's keyed state is in
  [the Phase 4 knowledge doc](phase-4-advanced-flink.md).
- In the running example both of Shopper 42's Clicks landed within 2 seconds of a
  price drop, so both qualify for the extra 5 percent, checked directly against
  broadcast state with no join.

## Re-keying and the merge

**`keyBy` physically decides which worker handles which record.** Same key, same
worker; different key, no guarantee, and workers share no memory. With three
workers:

| Fact about Shopper 42 | Key | Worker |
|---|---|---|
| session and cart-abandonment Signals | shopperId 42 | Worker 1 |
| P1 match | productId P1 | Worker 2 |
| P2 match | productId P2 | Worker 0 |

- **Nothing is duplicated.** Each worker computed a different fact in the only
  place it could have been computed. Worker 1 has never received a single Product
  Change; Worker 2 has no idea what else Shopper 42 did.
- **So no worker can produce the output alone.** `keyBy(shopperId)` on the join's
  output physically moves the P1 and P2 matches to Worker 1, where the session
  data already is.
- **Skip the merge** and the job publishes three disconnected facts, leaving
  something downstream to stitch them together with none of Flink's tools: no
  keyed state, no exactly-once on failure, no event time.

**`connect`, not `union`.** `union` needs one identical type; a Browsing Session
Signal and an enriched Click carry different payloads. `connect` gives one
function two typed entry points over the same keyed state:

| Arrives via | What it is | State afterwards |
|---|---|---|
| `processElement2` | P1 match, stock 40 | `matchesByProduct = {P1}` |
| `processElement1` | cart abandonment on P1 | `+ abandonedCarts = {P1}` |
| `processElement2` | P2 match | `matchesByProduct = {P1, P2}` |
| `processElement1` | **BROWSING_SESSION, candidate P1** | reads both, **emits**, clears the matches |

- Only `BROWSING_SESSION` emits. The other two write state the session close
  reads.
- The two maps have **different lifetimes on purpose**: a price-drop match is
  about one Click in one session and is spent once read, while "abandoned a cart
  recently" is a Shopper-level fact that survives a session boundary and expires
  on its own 60 second timer.

### Three inputs, four outcomes

`"most-viewed-in-session"` is not an input kind, it is an **output label**. Three
record types arrive:

```
processElement1   ShopperSignal, kind = BROWSING_SESSION   <- every closed session, always
processElement1   ShopperSignal, kind = CART_ABANDONMENT   <- optional extra
processElement2   EnrichedClick                            <- optional extra
```

Only the first is guaranteed, so `reason` names which extras turned up:

```java
reason = cartAbandoned ? "cart-abandoned"
       : priceDropped  ? "price-drop"
       : "most-viewed-in-session";     // neither extra arrived
```

So `reason == "most-viewed-in-session"` is **exactly** the
`!priceDropped && !cartAbandoned` case, the same condition that routes to
`UNMATCHED`. They are one case, not two.

## Async I/O, and two things called "recommendation"

| | What it is |
|---|---|
| the **recommendation service** | an external system called over the network. Another team owns it in a real company, because choosing which Product to suggest is a trained-model problem. Phase 4 mocks it |
| **`Recommendation`** | the record written afterwards, combining the service's answer with what the job already knew |

One is a program called mid-pipeline; the other is the pipeline's output.

- The call is **its own operator**, `AsyncDataStream.orderedWait(...)`, downstream
  of the merge. A `KeyedCoProcessFunction` cannot make it and stay correct.
- **`orderedWait`, not `unorderedWait`.** Unordered reorders records between
  watermarks, and Phase 3's restart Drill asserts identical output.
- The point of async I/O is keeping many requests in flight instead of stalling
  the subtask on every reply.

## End to end: Shopper 42's trip

1. Click at 10:00:00 on P1 enters `clickstream`, gets its watermark, forks.
2. Shopper-keyed: 42's session window opens.
3. Product-keyed: matched against P1's price drop one second away.
4. Click at 10:00:03 on P2 extends the session, and matches P2's drop.
5. Click at 10:00:06 carts P1. Nothing concludes yet.
6. At 10:00:30 the CEP window expires with no CHECKOUT: `CART_ABANDONMENT` on P1.
7. Both matches are re-keyed to Shopper 42, landing on the worker holding the
   session data.
8. The session closes with candidate P1, which is abandoned, price-drop matched,
   and in stock, so `SignalMerger` emits a `RecommendationRequest` with
   `reason = "cart-abandoned"`.
9. `PromoRuleApplier` fills in the extra 5 percent from broadcast state.
10. Async I/O asks the mocked service, which replies "suggest P2".
11. A `Recommendation` is published: Shopper 42, suggest P2, 5 percent off,
    reason cart-abandoned.

Shopper 43's Click on P1 at 10:00:05 is four seconds after that Product's drop,
so it never matches. If P1 is also 43's candidate when their session closes, that
session is counted in `UNMATCHED` and no Recommendation is published for it.
