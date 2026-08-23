# Flink job walkthrough: what the pipeline does, with one worked example

Written ahead of Phase 2 through 4, not during them, unlike the other files in
this folder. This captures the conceptual understanding built before any of
that code exists, so it is a companion to
[the phase plan](../superpowers/plans/2026-08-10-implementation-phases.md),
not a record of a debugging session.

One running example is used in every section below, so the same numbers carry
through the whole pipeline. Terminology note: `Product Change` is the correct
term below, covering both price and stock moves. `CONTEXT.md`, the phase
plan, and ADR 0003 still say `Price Change` as of this writing. That rename
has not been applied to those files yet.

## The feature, in one sentence

A live recommendation engine. It watches what a shopper does right now,
checks that against what is happening in the catalogue right now, and pushes
back a personalized recommendation while the shopper is still on the page.

```
Click ──────────┐
Product Change ─┼──>  [ Flink job ]  ──> Recommendation
Promo Rule ─────┘
```

## The running example

Two shoppers, two products, one promo rule.

Clicks, in the order they happen:

| Event time | Shopper | Product |
|---|---|---|
| 10:00:00 | 42 | P1 |
| 10:00:02 | 43 | P2 |
| 10:00:03 | 42 | P2 |
| 10:00:05 | 43 | P1 |

Product changes:

| Event time | Product | Change |
|---|---|---|
| 10:00:01 | P1 | price dropped |
| 10:00:04 | P2 | price dropped |

Promo rule, already loaded before any of this happens, broadcast to every
parallel piece of the job at once: viewing a product within 2 seconds of its
own price drop earns an extra 5 percent off.

## Why the job forks into two branches

**The problem.** The job needs to answer two different questions about the
same clicks, and each question needs the clicks grouped a different way.

- What has this one shopper been doing over the last few minutes? Needs every
  click from one shopper sitting together, regardless of product.
- Did this click happen near a change on the same product? Needs every click
  about one product sitting together, regardless of shopper.

One grouping cannot serve both. A `Product Change` has no shopper attached to
it at all, `CONTEXT.md`'s own definition: a fact about a Product, never about
a Shopper. There is no shopper key to join it against. Group by product
instead, and one shopper's clicks scatter across many different product
groups, so nothing is left in one place to reconstruct that shopper's
session.

So the job reads the raw `clickstream` once, then forks it into two
independently keyed copies.

**Branch one, `keyBy(shopperId)`:**

| Shopper | Their clicks |
|---|---|
| 42 | 10:00:00 on P1, 10:00:03 on P2 |
| 43 | 10:00:02 on P2, 10:00:05 on P1 |

Answers the session question. Cannot touch `Product Change` data at all.

**Branch two, `keyBy(productId)`:**

| Product | Clicks | Product Change |
|---|---|---|
| P1 | 10:00:00 by 42, 10:00:05 by 43 | 10:00:01, price dropped |
| P2 | 10:00:02 by 43, 10:00:03 by 42 | 10:00:04, price dropped |

Answers the product-change question. Cannot reconstruct either shopper's
session, since their clicks are split across product groups.

## Watermarks

**The problem.** Clicks do not arrive in the order they happened, because of
network delay. Working off arrival order instead of event time would give
wrong answers, for example a session length that reflects network speed
instead of shopper behavior. So the job works off the event time embedded in
each click. That creates a second problem: the job can never be fully sure
it has seen everything for a given moment, since a delayed click could always
still show up. It needs a rule for when to stop waiting.

**The rule.** The watermark is the job's own moving estimate of "how far back
in event time have I safely seen everything." One example rule: watermark
equals the largest event time seen so far, minus a 2 second bound.

Minus, not plus. The watermark is a claim, and the job cannot claim to have
seen a point in time it has not reached yet.

| Newest event time actually seen | Watermark rule | Resulting watermark | What it claims |
|---|---|---|---|
| 10:00:13 | minus 2s | 10:00:11 | safely seen through 10:00:11, held back 2s from the front |
| 10:00:13 | plus 2s (wrong) | 10:00:15 | would falsely claim a point never reached |

**It is one shared number, not per shopper, not per product.** Every click
that arrives, from any shopper, can push it forward if that click's event
time sets a new record. It never moves backward. A later click gets checked
against whatever this shared number already was, built from every earlier
click, not from anything about its own arrival time.

| Step | Click | Event time | Largest event time seen, before this click | Watermark used to judge this click | Late? | Largest event time seen, after |
|---|---|---|---|---|---|---|
| 1 | Shopper A | 10:00:05 | none yet | none yet | No | 10:00:05 |
| 2 | Shopper B | 10:00:12 | 10:00:05 | 10:00:03 | No | 10:00:12 |
| 3 | Shopper A | 10:00:08 | 10:00:12 | 10:00:10 | Yes | stays 10:00:12 |

Row 3: Shopper A's own second click gets judged late because of Shopper B's
earlier click, a different person, not because of anything Shopper A did.

**Assigned once, before the fork, not separately per branch.** If each
branch computed its own watermark from its own copy of the clicks, they could
disagree about what counts as recent enough, since they do not necessarily
process at the same speed. The same click could be judged on time by one
branch and late by the other, decided only by which branch happened to be
faster at that instant. Sharing one watermark across both branches, computed
before either one exists, removes that possibility.

## Session windows

**Definition**, from `CONTEXT.md`: a run of one shopper's clicks with no gap
longer than the session gap. Nothing to do with a web browser tab, a cookie,
or a login session. Computed purely from `shopperId` and event timestamps in
the click data.

**The window's end boundary anchors to the most recent click, not the
first.** The session gap answers "how long since the most recent activity,"
not "how long since this session started." Anchoring to the first click
would ask a different question and cut real, unbroken sessions into pieces.

Session gap 6 seconds. Shopper 42 clicks at 10:00:00, 10:00:03, 10:00:07,
10:00:11, each gap under 6 seconds, so all four belong to one session.

| Click, event time | Gap from previous | Boundary before | New boundary, most recent click plus 6s |
|---|---|---|---|
| 10:00:00 | first click | none yet | 10:00:06 |
| 10:00:03 | 3s | 10:00:06 | 10:00:09 |
| 10:00:07 | 4s | 10:00:09 | 10:00:13 |
| 10:00:11 | 4s | 10:00:13 | 10:00:17 |

Anchoring to the first click instead would fire the window at 10:00:06,
before the 10:00:07 click ever arrives, wrongly splitting one session.

## Being behind the watermark is not the same as being too late for a window

**The distinction.** "Is this click behind the shared watermark" and "does
this click still get counted" are two different checks. The first compares
one number shared across the whole stream. The second compares the end
boundary of the one window this specific click belongs to, whether that
window already exists or is being created right now by this very click.

Session gap 6s, bound 2s, continuing the trace from the watermark section.

| Step | Click | Event time | Shared watermark before | This click's window, boundary | Boundary vs watermark | Outcome |
|---|---|---|---|---|---|---|
| 1 | Shopper A, 1st | 10:00:05 | none yet | new, 10:00:11 | not reached | accepted, opens |
| 2 | Shopper B, 1st | 10:00:12 | 10:00:03 | new, 10:00:18 | not reached | accepted, opens |
| 3 | Shopper D, 1st | 10:00:09 | 10:00:10 | new, 10:00:15 | not reached | accepted, opens |
| 4 | Shopper A, 2nd | 10:00:08 | 10:00:10 | existing, extends to 10:00:14 | not reached | accepted, extends |
| 5 | Shopper C, 1st | 10:00:05 | 10:00:20 (advanced by other clicks) | new, 10:00:11 | already passed | rejected, `Late Click` |

Rows 3 and 4 both had a click behind the shared watermark by the same one
second, and both were accepted, one opening a fresh window, one extending an
existing one, because in both cases the click's own window boundary still
sat ahead of the watermark. Row 5 had the same click's raw event time behind
the watermark by much more, and even a freshly opened window for it would
already sit behind the watermark before it could exist. Only row 5 is
rejected.

**What happens to a row 5 click.** It does not silently disappear. Flink's
default is to drop an element like this if no side output is configured.
This project's design, per the phase plan, routes it instead to a `Late
Click` side output: `OutputTag<Click>`, attached with
`.sideOutputLateData(...)`, pulled out downstream with `.getSideOutput(...)`.
Not an error case. At real traffic volume some fraction of clicks always
arrive too late for their own session, and Phase 8's dashboard uses this
count as a pipeline health signal, whether the watermark bound is too tight.

## The product-keyed branch: interval join and `Unmatched Click`

Branch two joins each click against `Product Change` events on the same
product, inside a time window, for example plus or minus 2 seconds.

- Shopper 42's click on P1 at 10:00:00, 1 second before P1's price drop at
  10:00:01. Inside the window. A match.
- Shopper 42's click on P2 at 10:00:03, 1 second before P2's price drop at
  10:00:04. Inside the window. A match.
- Shopper 43's click on P1 at 10:00:05, 4 seconds after that same drop.
  Outside the window. Becomes an `Unmatched Click`.

`Unmatched Click` and `Late Click` are easy to conflate. They belong to two
different branches and check two different things. `Late Click` is a timing
question on the shopper-keyed branch: did this click arrive before its
session window already fired. `Unmatched Click` is a matching question on
the product-keyed branch: did this click, which arrived perfectly on time,
find a nearby `Product Change` to pair with. Neither is dropped. Both are
routed to their own side output, kept separate so Phase 8's dashboard can
show pipeline lateness and business-relevant non-matches as two distinct
signals.

## Broadcast state: the promo rule

Not grouped by shopper or product. Every parallel worker holds the full,
current rule set in memory, and a rule change updates every worker at once,
with no job restart. In the running example, both of shopper 42's clicks
landed within 2 seconds of a price drop on the same product, so both qualify
for the rule's extra 5 percent, checked directly against the broadcast state,
no join required for this part.

## CEP: the multi-step pattern

Watches for one specific ordered sequence over time on the shopper-keyed
branch, for example: viewed a product, then viewed a second product within 5
seconds. Shopper 42 viewed P1 at 10:00:00, then P2 at 10:00:03, a 3 second
gap. The pattern matches. A comparison-shopping signal is raised for shopper
42.

## Two things with the same name: `Recommendation` vs. the recommendation service

Easy to conflate, and worth stating plainly what each is, and what it is not.

- The **recommendation service** is a separate, external system, called over
  the network. In a real company it already exists, built and maintained by
  a different team, since deciding which specific product to suggest is
  usually a trained-model problem, not something to reimplement inside a
  streaming job. In this lab it does not exist for real, so Phase 4 mocks it,
  to give something real to call while building the actual mechanism, async
  I/O, that does not block the rest of the pipeline while waiting on the
  reply.
- **`Recommendation`** is the data record the job writes after that reply
  comes back. It combines the service's answer with the shopper ID, a
  timestamp, and whatever else the job knows, and that combined record is
  what gets published to the `recommendation` topic.

The service produces an answer. `Recommendation` is the record that carries
that answer forward. One is a program called mid-pipeline. The other is the
pipeline's own output.

## Re-keying the product branch back to shopper, and why it is not duplication

**The problem.** `keyBy` does not just group data on paper, it physically
decides which worker handles which piece of data. Same key, same worker.
Different key, no guarantee of the same worker, and no shared memory between
workers.

With a 3-worker example:

| Data about shopper 42 | Key used | Worker it lands on |
|---|---|---|
| Session and CEP signal | shopperId = 42 | Worker 1 |
| P1 match | productId = P1 | Worker 2 |
| P2 match | productId = P2 | Worker 0 |

Each worker holds a genuinely different fact, not a copy of another worker's
fact. Worker 1 knows the session shape, since it only ever receives clicks
keyed by shopper, and it never receives `Product Change` data at all, that
stream is only ever routed by product. Worker 2 and Worker 0 each know
whether one specific product's clicks landed near that product's own price
change, since they receive every click on that product from every shopper,
and they have no visibility into any other product or that shopper's overall
session. Nothing is duplicated. Each fact exists in exactly one place, the
only place it could have been computed.

**Why they still need to end up together.** The job's required output is one
`Recommendation` per shopper, and no single worker above has enough
information to produce that alone. Re-keying branch two's output by
`shopperId` physically moves the P1 and P2 matches to Worker 1, the same
place the session and CEP data already live. Only after that move can one
function see all of it at once.

Without this step, the job would instead publish three disconnected facts,
and something downstream would have to notice they share a `shopperId` and
stitch them back together itself, rebuilding the same correlation problem
outside Flink, with none of Flink's tools for it, no windowing, no
exactly-once state on failure.

## `connect`, not `union`

Once everything sits on one worker, per shopper, `connect` combines the two
differently-typed branches into one function with two entry points,
`processElement1` for branch one, `processElement2` for branch two, both
reading and writing the same shared per-key state. `union` requires both
streams to carry the same type and interleaves them into one processing
method with no way to tell which branch an element came from, wrong here
since a session signal and a price-drop match are not interchangeable.

Shopper 42's state on Worker 1, built up as things arrive:

| Arrives via | What it is | Shared state after |
|---|---|---|
| `processElement1` | session signal, P1 and P2 viewed | session = [P1, P2] |
| `processElement2` | P1 match | + priceDropMatches = [P1] |
| `processElement1` | CEP comparison-shopping flag | + comparisonShopping = true |
| `processElement2` | P2 match | + priceDropMatches = [P1, P2] |

## End to end: shopper 42's full trip through the job

1. Click at 10:00:00 on P1 enters `clickstream`, gets its watermark, forks.
2. Shopper-keyed branch: shopper 42's session window opens.
3. Product-keyed branch: matched against P1's price drop, 1 second away.
4. Click at 10:00:03 on P2 arrives. Session window extends, 3 second gap,
   still one session. Product-keyed branch: matched against P2's price drop,
   1 second away.
5. CEP: P1 then P2 within 5 seconds, comparison-shopping signal raised.
6. Broadcast promo rule: both matches qualify for the extra 5 percent.
7. Branch two's two matches get re-keyed to shopper 42, moved to the same
   worker as the session and CEP data.
8. One `connect`-based function now holds all of it: the session, the CEP
   flag, both price-drop matches.
9. That function calls the recommendation service asynchronously: shopper
   42, comparing P1 and P2, both recently discounted. Mocked reply: suggest
   P2.
10. The function packages the reply with everything else into one
    `Recommendation`: shopper 42, suggest P2, 5 percent price-drop bonus,
    reason comparison-shopping. Published to `recommendation`.

Shopper 43's click on P1 at 10:00:05, 4 seconds after that product's price
drop, misses the join window and becomes an `Unmatched Click` instead, a
normal outcome, not an error, and not part of shopper 43's eventual
`Recommendation` reasoning for this product.
