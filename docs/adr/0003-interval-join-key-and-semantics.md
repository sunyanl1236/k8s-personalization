# ADR 0003: The interval join gets its own `keyBy(productId)` branch, and stays an inner join

Date: 2026-08-10
Status: accepted

## Context

The design spec's architecture diagram places the clickstream x price-change
interval join downstream of `keyBy(userId)`.

That cannot compile into a correct job. Flink's `intervalJoin` is defined on a
`KeyedStream` and joins against another `KeyedStream` whose key selector
produces the same key. A price-change event has no `userId`. It is a fact about
a product, keyed by `productId` by nature.

A second problem is semantic. `intervalJoin` is an inner join. It emits only on
a match. A click on a product whose price did not change inside the interval
produces no output at all and disappears from that branch. For a
personalization pipeline that silently discards most traffic, which is not what
the diagram implies.

## Decision

Two parts.

**Keying.** The interval join branch forks from the *raw* clickstream, in
parallel with the `keyBy(userId)` branch rather than below it. It applies its
own `keyBy(productId)` and joins against the price-change stream, also keyed by
`productId`. The join output is then re-keyed to `userId` to rejoin the
recommendation path.

```
                     raw clickstream (watermarked)
                              |
              +---------------+----------------+
              |                                |
        keyBy(userId)                    keyBy(productId)
              |                                |
   session windows,                  intervalJoin  <-- keyBy(productId)
   keyed state,                            |          price-change stream
   broadcast rules,                        |
   CEP                                matched: enriched click
              |                       unmatched: side output
              |                                |
              |                          keyBy(userId)
              |                                |
              +---------------+----------------+
                              |
                    connect + KeyedCoProcessFunction
                              |
                       async I/O -> sink
```

**Semantics.** Keep the real `intervalJoin` operator, because exercising it is
a stated goal of the project. Unmatched clicks are not dropped silently. They
are routed to the existing late/low-confidence side output, which already feeds
the recommendation stage.

The two branches merge with `connect` plus a `KeyedCoProcessFunction`, not
`union`. `union` requires identical element types, and the enriched and
unenriched sides carry different payloads.

## Consequences

The job graph has two shuffles on the clickstream instead of one. That is real
network cost and it is the honest cost of the operator.

Watermarks must be assigned once, on the raw stream, before the fork. Assigning
them per branch would let the two sides drift and would make the merge's event
time meaningless.

The side output now carries two distinct populations: genuinely late events,
and on-time events with no price match. They need a discriminator field so
Phase 8 dashboards can tell them apart.

## Alternatives rejected

- **Broadcast the price-change stream instead.** Removes `intervalJoin` from
  the coverage map and overloads broadcast state, which is already carrying the
  promo rules.
- **Redefine the second stream as user-scoped.** Keeps the diagram as drawn but
  makes the domain less realistic. Price changes are not user-scoped events.
- **Hand-rolled left-outer `KeyedCoProcessFunction` instead of `intervalJoin`.**
  Semantically nicer, but it means never exercising the operator the project
  exists to learn. Reconsider as a Phase 8 stretch comparison if time allows.
