# ADR 0008: `Product Change` is a state snapshot, not a sum type

Date: 2026-08-28
Status: accepted
Supersedes: the `Product Change` section of
[the domain schemas design](../superpowers/specs/2026-08-16-domain-schemas-design.md)

## Context

Phase 2 modelled a Product Change as a sealed interface over two records:

```java
public sealed interface ProductChange permits PriceChange, StockChange {}
public record PriceChange(String productId, Instant eventTime, double newPrice) implements ProductChange {}
public record StockChange(String productId, Instant eventTime, int newStock) implements ProductChange {}
```

The stated reasons were proper types for a currency amount and a unit count,
and compiler-enforced exhaustiveness over the two variants. The same document
considered and rejected a two-optional-fields shape, "in favor of the stronger
compile-time guarantee".

Three things have since come to light, and together they change the trade.

**The compile-time guarantee cannot be spent where it was meant to be spent.**
Asked directly, Flink 2.2 answers:

| Type | `TypeInformation` | Serializer |
|---|---|---|
| `PriceChange` | `PojoTypeInfo` | `PojoSerializer` |
| `StockChange` | `PojoTypeInfo` | `PojoSerializer` |
| **`ProductChange`** | **`GenericTypeInfo`** | **`KryoSerializer`** |

A sealed interface carries no fields, so Flink has nothing to build a
`PojoSerializer` from and falls back to Kryo. Phase 3 sets
`pipeline.generic-types: false`, which turns that fallback into a hard failure
at job-graph construction. So `ProductChange` can never be a stream element
type, and the exhaustive `switch` was only ever available inside `JsonCodec`,
never inside an operator.

**Nothing consumes `StockChange`.** The term appears twenty-five times across
the project's documents. Every occurrence concerns how to serialize it, how to
distinguish it from a price move, how to drop it, or how the topic came to be
named `product-change` to accommodate it. No operator, metric, dashboard, or
phase reads it, and it is absent from the design spec's concept coverage map.
It exists chiefly because a sum type needs a second variant.

**Mutual exclusivity was never a domain decision.** The Phase 2 document argues
for proper types and exhaustiveness. Neither requires that a price move and a
stock move cannot coincide. That property fell out of `permits PriceChange,
StockChange`, and `ProductChangeFactory` then baked it into the data with
`if (random.nextBoolean())`. A clearance, where a retailer marks a Product down
and corrects its stock in one catalogue update, is ordinary and the current
model cannot represent it at all.

**A fourth point, smaller but awkward.** `PriceChange` carries no previous
price. So "price *drop*" has never been checkable anywhere in this project,
even though the generator stamps every Promo Rule with the description
`"N% off, price-drop bonus"`, and the design spec and the walkthrough both use
the word.

## Decision

One record, carrying the Product's full state and the state it replaced.

```java
public record ProductChange(String productId, Instant eventTime,
                            double price, double previousPrice,
                            int stock, int previousStock) {}
```

`PriceChange` and `StockChange` are deleted. There is no discriminator and no
nullable field. Every field always holds a real value. On a Product's first
event, `previousPrice` equals `price` and `previousStock` equals `stock`.

This is an event that carries **state**, not one that carries a delta. Three
questions become field comparisons, answerable per record with no keyed state:

| Question | Expression |
|---|---|
| Did the price drop? | `price < previousPrice` |
| Is the Product out of stock? | `stock == 0` |
| Did stock move? | `stock != previousStock` |

**Stock gets a job.** A Recommendation is suppressed when its candidate Product
is out of stock. That rule is what justifies keeping the second attribute at
all, and it is nearly free: every `ProductChange` carries `stock`, so every
`EnrichedClick` carries it, and the merge operator reads it with no extra
stream and no extra state.

**Two supporting changes to the generator**, both deliberate synthetic-data
choices in the same spirit as Phase 3's derived 6 second session gap:

- `ProductChangeFactory` keeps a ten-entry map of each Product's last price and
  stock, so it can populate the previous values.
- Stock is `0` with probability `0.1`, otherwise `1` to `500`. The natural
  `random.nextInt(501)` would make stock zero about once in five hundred
  events, so the suppression rule would essentially never fire and could not be
  observed or drilled.

**The wire format drops `"type"`.** A discriminator that merely restates what
the fields already say is a second source of the same fact, and two sources can
disagree.

## Consequences

**The interval join must not filter on the drop.** This is the non-obvious one.
If only price drops reach the join, the merge never learns the stock of a
Product that went out of stock without a price move, and the suppression rule
silently stops working for exactly the Products it most needs to catch. So the
join matches **any** Product update near a Click, and `EnrichedClick` carries
the values the merge then tests.

Rates follow from that, at the generator's defaults of 1.0 Product Changes per
second over 10 Products with a 4 second join window:

| Population | Share |
|---|---|
| Clicks matching any Product update | 33% |
| Clicks matching a genuine price drop | about 12% |
| Recommendations carrying a discount | about 41% |
| Recommendations suppressed, out of stock | about 10% |
| Browsing Sessions reaching `UNMATCHED` | about 20% |

Every population is non-empty in every run, which is what the assertion tests
and the Drills need.

These are measured, not estimated. Twenty thousand generated events give
`stock == 0` on 9.6% and a price drop on 37.5%, against the design's targets of
10% and 37.5%. The 37.5% follows exactly from the factory's rule: price moves on
a coin flip, plus the quarter of the time when neither attribute was chosen and
price is forced, so 0.75, halved because a new uniform price is equally likely
to be above or below the old one.

**A fourth side output appears**, `OUT_OF_STOCK`. A suppression nobody can count
is a bad rule, so the suppressed requests are routed rather than dropped. The
job's four side outputs then carry four genuinely different populations: Late
Click, CEP timed out, Unmatched Click, and out of stock.

**Compiler-enforced exhaustiveness is lost in `JsonCodec`.** There is no longer
anything to `switch` on. That is a real loss. It buys the ability to represent a
combined change, removes a wire-format discriminator that could contradict the
data, and makes "price drop" mean a price drop.

**`Recommendation.reason` gains a value** and the precedence becomes, most
specific first: `"cart-abandoned"`, `"price-drop"`, `"most-viewed-in-session"`.
Out-of-stock candidates never reach the sink, so `"out-of-stock"` is a side
output label rather than a `reason`.

**The topic keeps the name `product-change`.** It is created, `Ready`, and
referenced by Phase 1's manifests and knowledge doc. Renaming it would mean
touching `manifests/strimzi/kafka-topics.yaml` and re-running Phase 1's
verification for no gain, and the name is now more accurate than before, not
less: one record really does describe the whole Product.

**`CONTEXT.md` changes.** The headword becomes **Product Change** rather than
**Price Change**, which also closes the drift where the glossary said one thing
while the topic and the type said another.

## Alternatives rejected

**Keep the sealed interface, write a custom `TypeInformation`.** The only option
that preserves exhaustiveness inside operators. It needs a `TypeInfoFactory`, a
`TypeInformation`, a `TypeSerializer` writing a tag byte and delegating, and a
`TypeSerializerSnapshot`. Registration via `pipeline.serialization-config` with
`{type: typeinfo, class: ...}` would avoid an annotation and keep `:domain`
free of Flink. Rejected on cost: roughly 150 lines and three hours in a 13 hour
phase whose stated overrun risk is CEP, plus a checkpoint-compatibility
contract that Phases 5 through 7 would have to honour as they restore
repeatedly.

**One flat record with a `ChangeKind` enum and primitive fields.** Consistent
with the `ShopperSignal` decision, so the codebase would have a single rule for
every type crossing an operator boundary. Rejected twice over: an enum still
asserts that exactly one attribute changed, which is the artificial constraint
this ADR removes, and reading the inapplicable field returns `0.0` or `0`
silently rather than failing.

**Nullable `Double` and `Integer` fields, no previous values.** Verified to be a
valid Flink POJO. Rejected because `null` reads as "not applicable" rather than
as data, and because carrying the previous value costs one map of ten entries
while making the discount condition mean what every document already claims it
means.

**Delete `StockChange` and keep only price.** The disciplined minimum, and it
would have been right if stock had stayed unused. Rejected once the suppression
rule gave stock a genuine consumer.
