# Domain schemas design

Date: 2026-08-16
Status: approved (design), not yet implemented
Derives from: [CONTEXT.md](../../../CONTEXT.md)'s domain vocabulary, and the
mechanics established in
[flink-job-walkthrough.md](../../knowledge/flink-job-walkthrough.md).

## Goal

Fix the four domain schemas, `Click`, `Product Change`, `Promo Rule`,
`Recommendation`, as Java records, per Phase 2's task to "fix the schemas...
against the vocabulary in CONTEXT.md." Scoped to the record shapes
themselves, not the generator's event-production logic, which is a separate,
later step.

## Decisions

### `Click`

```java
public enum ActionType { VIEW, ADD_TO_CART, CHECKOUT }
public record Click(String shopperId, String productId, Instant eventTime, ActionType actionType) {}
```

`ActionType` is the minimal set that supports Phase 4's CEP pattern, viewed
then added to cart then no checkout within a window. "Left without checking
out" is the *absence* of a `CHECKOUT` event within the pattern's `within()`
bound, not a fourth value. Nothing else in the pipeline, the session windows
or the interval join, reads `actionType` at all, so a richer taxonomy would
add realism without adding concept coverage. Considered and rejected: a
6-value taxonomy (`VIEW`, `ADD_TO_CART`, `REMOVE_FROM_CART`,
`CHECKOUT_START`, `CHECKOUT_COMPLETE`, `SEARCH`), closer to a real
clickstream, not needed by anything currently in the phase plan.

### `Product Change`

> **Superseded 2026-08-28 by
> [ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md).** The sealed
> interface below cannot be a Flink stream element type: Flink 2.2 resolves
> `ProductChange` to `GenericTypeInfo` and `KryoSerializer`, which
> `pipeline.generic-types: false` rejects. So the compile-time guarantee this
> section pays for was only ever available inside `JsonCodec`, never inside an
> operator. `ProductChange` is now one record carrying price, stock, and the
> values they replaced. The rest of this section is kept as the record of what
> was decided in Phase 2 and why.


```java
public sealed interface ProductChange permits PriceChange, StockChange {}
public record PriceChange(String productId, Instant eventTime, double newPrice) implements ProductChange {}
public record StockChange(String productId, Instant eventTime, int newStock) implements ProductChange {}
```

A sealed interface over two records, rather than one record with a
`changeType` discriminator field and a generic numeric value. Keeps a
currency amount and a unit count as their own proper types, and gets
compiler-enforced exhaustiveness: any `switch` over `ProductChange` that
doesn't handle both variants fails to compile, rather than silently missing
a case at runtime.

**Consequence to carry into implementation**: the `product-change` Kafka
topic now carries two distinct shapes on one stream. Whatever writes to it
(the generator) and whatever reads it (Phase 3's interval join) both need a
serializer that can distinguish `PriceChange` from `StockChange` on the
wire, not a serializer for one fixed type. Considered and rejected: one
record with a shared `double newValue` field, simpler to serialize, but
blurs a price and a stock count into one generic type; also considered a
`Double`/`Integer` two-optional-fields shape as a middle ground, rejected in
favor of the stronger compile-time guarantee.

### `Promo Rule`

```java
public record PromoRule(String ruleId, String description, double discountPercent) {}
```

Deliberately simple. The discount condition itself, e.g. "viewing a product
within 2 seconds of its own price drop," lives in the Flink job's own logic
in Phase 4, not in this record's data. A more data-driven design would put
the matching window into `PromoRule` directly, considered and explicitly
deferred: more flexible, more complexity, not needed to demonstrate
broadcast state working.

### `Recommendation`

```java
public record Recommendation(String shopperId, String productId, double discountPercent, String reason, Instant generatedAt) {}
```

Who it's for, what's suggested, whatever discount applies, and why, since
Phase 8's dashboard work depends on being able to explain a recommendation,
not just emit one.

## Out of scope for this design

- The generator's actual event-production logic (rates, skew injection).
- `PromoRule`'s broadcast-state wiring and the interval join's matching
  window, both Phase 4 concerns.
- The `product-change` topic's (de)serializer implementation, flagged above
  as a consequence, not solved here.
