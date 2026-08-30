# ADR 0009: `Unmatched Click` is counted at the merge, per Browsing Session

Date: 2026-08-30
Status: accepted
Supersedes: the side-output half of
[ADR 0003](0003-interval-join-key-and-semantics.md)

## Context

ADR 0003 settled the Product-keyed branch: the interval join forks from the raw
clickstream with its own `keyBy(productId)`, and unmatched Clicks are not
silently dropped. Its diagram puts both outcomes on the join:

```
   intervalJoin
        |
   matched: enriched click
   unmatched: side output
```

**The second line is not implementable.** `intervalJoin` is an inner join.
`ProcessJoinFunction.processElement(...)` is invoked only when a pair matches, so
a Click with no nearby Product Change never reaches the function at all. There is
no callback in which to call `ctx.output(UNMATCHED, click)`.

This was found while writing the Phase 4 design, before any code, by reading the
operator's contract rather than by a failing test.

## Decision

The `UNMATCHED` side output moves to `SignalMerger`, and its grain changes from
**one record per Click** to **one record per Browsing Session candidate Product**.

The merge already holds, per Shopper, which Products matched. When a Browsing
Session closes and its candidate Product is absent from that map, the merge emits
to `UNMATCHED` instead of to the sink:

```java
if (priceDropped || cartAbandoned) {
    out.collect(request);
} else {
    ctx.output(UNMATCHED, request);
}
```

The record carried is a `RecommendationRequest`, not a `Click`, because that is
what the merge has at that point and it names the candidate directly.

## Consequences

**The population still means what `CONTEXT.md` intends**, and that entry was
reworded to match: a closed Browsing Session whose candidate Product found no
Product Change inside the join interval, although its Clicks arrived on time.

**Phase 8 counts per Browsing Session, not per Click.** Its axis label has to say
so, or the chart silently compares against a different denominator than the Late
Click series beside it.

**A candidate with no trigger is not published at all.** This is the part worth
being explicit about, because it narrows Phase 3. That version published a
`Recommendation` for every closed Browsing Session, with `reason` of
`"most-viewed-in-session"` and `discountPercent` of `0.0`. Now such a Shopper
produces a record in `UNMATCHED` and nothing on the topic.

The trade: the job stays quiet rather than sending an offer it cannot justify, at
the cost of roughly a fifth of the output volume. Intent alone is not a business
event.

**Phase 5's HA Drill has to account for it.** That Drill asks for "no gap in the
recommendation topic". Two rules now withhold output that Phase 3 published:

| Rule | Share of session closes withheld |
|---|---|
| no trigger, `UNMATCHED` | about 20% |
| candidate out of stock, `OUT_OF_STOCK` | about 10% |

The check must compare against **emitted Recommendations**, not against closed
Browsing Sessions, or two correctly working rules read as a failure.

**The unmatched population cannot be reconstructed per Click later** without
reintroducing the rejected alternative below. If Phase 8 ever needs per-Click
grain, that is a new decision, not a configuration change.

## Alternatives rejected

**Buffer every Click for the join interval and check for an enriched twin.**
Recovers per-Click grain exactly: hold each Click for the join window, and emit it
as unmatched if no `EnrichedClick` for it arrives.

Rejected on two counts. It re-implements the join's own bookkeeping, which is
precisely what ADR 0003 rejected in order to keep the real operator in play — the
project exists to exercise `intervalJoin`, not to hand-roll one beside it. And it
adds a third shuffle of the entire clickstream to populate a side output that
nothing reads until Phase 8.

**A left-outer interval join.** Would emit every Click with a null right side.
Rejected because the DataStream API has no such thing: `intervalJoin` is inner
only, and windowed `coGroup` matches on shared window boundaries rather than on a
per-element interval, which is a different operator with different semantics.

**Drop the population entirely.** Simplest, and honest if nobody reads it.
Rejected because `CONTEXT.md` defines `Unmatched Click` as a first-class term and
Phase 8 charts pipeline lateness against business-relevant non-matches as two
distinct signals. Deleting one of them removes half that comparison.
