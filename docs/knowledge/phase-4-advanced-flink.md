# Phase 4 knowledge: Advanced Flink

Written during Phase 4, not before it. Companion to
[phase-3-core-pipeline.md](phase-3-core-pipeline.md), which covers the
Shopper-keyed branch on `MiniCluster`. This file covers what actually came up
while adding the Product-keyed branch, CEP, the merge, broadcast state, and
async I/O.

Design decisions and their rejected alternatives live in
[the advanced Flink design](../superpowers/specs/2026-08-24-advanced-flink-design.md).
The ordered steps live in
[the implementation plan](../superpowers/plans/2026-08-24-phase-4-advanced-flink.md).
This file is only for how things actually work.

## Broadcast state and keyed state are not the same shape

Came up in Task 5, and the two are easy to conflate because both end up being a
`MapState`.

| | `SignalMerger.matchesByProduct` | `PromoRuleApplier.RULE_STATE_DESCRIPTOR` |
|---|---|---|
| Kind | keyed state | broadcast (operator) state |
| Scoped by a Flink key? | yes, implicitly per Shopper | no |
| How many maps exist | one **per Shopper** | one **per worker**, all identical |
| Map key | `productId`, carries meaning | a constant, carries none |
| Written by | any `processElement` | only `processBroadcastElement` |

In the merge you never write `shopperId` into the map, because Flink has already
narrowed to that Shopper before the code runs. In the applier there is no
narrowing at all: the operator is not keyed, so every worker holds the same copy.
That is what makes a rule change take effect everywhere at once, with no restart.

### Why the rule is a map with one fixed key, not a `ValueState`

**Because the API offers nothing else.** The Context has exactly one accessor:

```java
public abstract <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V>);
```

There is no `ValueStateDescriptor` overload. Note also that `Context` returns a
writable `BroadcastState` while `ReadOnlyContext` returns a
`ReadOnlyBroadcastState`: the **compiler**, not a convention, is what stops
`processElement` from writing. If each worker could write its own copy, the
copies would diverge and the state would no longer be broadcast.

The reason behind the API is **rescaling**. State comes in two families, organised
around how they redistribute when parallelism changes:

| Family | Forms | Scoped to |
|---|---|---|
| keyed | `ValueState`, `MapState`, `ListState`, … | one key |
| operator | `ListState`, `BroadcastState` | one subtask |

`ValueState` is keyed state, and this operator has no key. Operator state has no
value form because a single opaque value has no defined redistribution rule:
`ListState` splits or unions, `BroadcastState` copies the whole map to every new
subtask. Phase 6 varies parallelism deliberately, and that broadcast
redistribution is what guarantees a new subtask starts holding the current rule.

**The key value is a constant**, so every rule overwrites the same slot. Keying by
`ruleId` would accumulate instead: the generator emits a fresh id every 30 seconds
and never stops, so state would hold 120 rules after an hour and grow without
bound. A fixed key makes retention a non-question rather than a policy to invent.

**The alternative that compiles but is worse:** holding the rule in a keyed
`ValueState` inside `SignalMerger`. It would store the rule once **per Shopper**,
and updating it would mean touching every key, which a broadcast record cannot do
because it has no key to route by. That is the problem broadcast state exists to
solve.

### Two mistakes the broadcast harness catches

Both were made in Task 5 and both compile cleanly:

- **Reading the state before any rule has arrived.** It is empty until the first
  `processBroadcastElement`, so `state.get(key)` returns `null`. The generator
  emits a rule every 30 seconds, so an unguarded read crashes the job in its first
  30 seconds of every run.
- **Emitting conditionally.** `priceDropMatched` decides the **discount**, not
  whether the record survives. An `if` with no `else` silently drops every
  cart-abandoned request whose Product never moved in price, with no error and no
  side output. The rule is exactly one `collect` per input.

## Flink ignores a record's derived accessors

Came up in Tasks 1 to 4, on four different records.

`ProductChange` has `priceDropped()` and `outOfStock()`; `EnrichedClick` has the
same two; `ShopperSignal` has two static factories. None of them is a field, and
the worry was that Flink's POJO extraction would treat `priceDropped()` as a
getter for a field that does not exist.

It does not. Asked directly:

| Type | `TypeInformation` | Arity | Fields |
|---|---|---|---|
| `ProductChange` | `PojoTypeInfo` | **6**, not 8 | the six record components |
| `EnrichedClick` | `PojoTypeInfo` | **7**, not 9 | the seven record components |
| `ShopperSignal` | `PojoTypeInfo` | **4** | plus `SignalKind` as `EnumTypeInfo` |

Extraction walks **fields** and then looks for their accessors, so a method with
no matching field is invisible. That is what lets a record carry a derived
question without storing a second source of the same fact.

`SignalKind` resolving to `EnumTypeInfo` rather than `GenericTypeInfo` is why the
discriminator is an enum and not a `String`: enums have a real Flink serializer.

## A CEP pattern ending in `notFollowedBy` is a match, not a timeout

Came up in Task 3, and the documentation does not answer it.

The pattern is *view, then cart the same Product, then no checkout within 30
seconds*. It was unclear whether Flink delivers a clean expiry through
`processMatch` or treats "view, cart, nothing" as a timed-out partial match.

**It is `processMatch`.** A window that expires without the forbidden event is a
completed match. `processTimedOutMatch` sees only genuinely incomplete sequences,
such as a VIEW that was never carted.

That split is what keeps the two outputs meaningful: matches become
`CART_ABANDONMENT` Signals, and the timed-out partials go to `CEP_TIMED_OUT`. Had
it been the other way round the emission would have had to move into
`processTimedOutMatch` and the two populations would have been tangled.

Two related mechanics worth recording:

- **The same-Product link needs an `IterativeCondition`.** The Product is unknown
  when the job graph is built, so the condition reads it from the match in
  progress via `ctx.getEventsForPattern("view")`. A `SimpleCondition` sees only
  the candidate event and would let any cart complete any view.
- **An absence takes time to prove.** Nothing can be concluded when the cart
  Click arrives; the answer exists only once the window expires. That is why
  roughly 60% of abandonments confirm after their own Browsing Session has closed.

## `orderedWait` versus `unorderedWait`

Came up in Task 6, when wiring async I/O.

**The problem.** Async replies do not come back in the order they were sent. A
200ms call issued first finishes after a 20ms call issued second. So the operator
must decide what to do with a reply whose predecessor has not arrived yet.

```
sent:      A ──► B ──► C
replies:        C(20ms)   A(200ms)   B(50ms)

unorderedWait  emits  C, A, B      as each completes
orderedWait    emits  A, B, C      buffering C until A and B are out
```

| | `unorderedWait` | `orderedWait` |
|---|---|---|
| Emits | as soon as a reply lands | in **input** order |
| Buffers | nothing | completed replies waiting on earlier ones |
| Latency | lowest | a slow record delays everything behind it |
| Checkpoint size | smaller | larger, records held longer |

### The part that decides it here

With **event time**, unordered is not as unordered as it sounds. The Flink 2.2
docs:

> Watermarks establish an *order boundary*. Records are emitted unordered only
> **between** watermarks.

So watermarks still partition the stream into ordered blocks, and:

> in the presence of watermarks, the *unordered* mode introduces some of the same
> latency and management overhead as the *ordered* mode does

**The usual reason to pick unordered mostly evaporates once you have watermarks**,
which this job does.

Meanwhile the cost is concrete. Phase 3's restart Drill asserts the output after
recovery is byte-identical to an uninterrupted run. Under `unorderedWait`, records
between two watermarks come out in whatever order the mock replies, so two runs
differ by line order and the Drill fails for a reason that has nothing to do with
checkpointing.

So: **`orderedWait`.** It costs almost nothing here and it protects a Drill. If
this job had no watermarks and throughput mattered, unordered would be the right
call.

## Phase 4's graph exhausts `MiniCluster`'s default network buffers

Came up when first running the job after Task 4, and it failed at deploy before
producing a single record:

```
java.io.IOException: Insufficient number of network buffers: required 17,
but only 0 available. The total number of network buffers is currently set to
2048 of 32768 bytes each.
```

Phase 4 grew the job to **five `keyBy` shuffles**: one for the Shopper-keyed
branches, one for the Product-keyed branch, one on the Product Change side, the
re-key of the join output, and the merge's own. At the default parallelism of 16
each shuffle wants roughly 16 x 16 channels, and 2048 buffers no longer covers it.

2048 is not a coincidence: it is the default `taskmanager.memory.network.min` of
64mb divided by the 32KB buffer size. The fraction-based derivation lost to the
minimum, because `MiniCluster`'s total Flink memory is small.

The fix is in `apps/pipeline/conf/config.yaml`, where Flink settings live as data:

```yaml
taskmanager.memory.process.size: 2gb
taskmanager.memory.network.min: 256mb
taskmanager.memory.network.max: 256mb
```

256mb of 32KB buffers is 8192. Setting **min and max to the same value** is the
documented way to pin the size rather than let it be derived from a fraction.

**Pinning parallelism was rejected as the fix**, for the same reason Phase 3 Task
4 rejected it when the watermark stalled: Phase 6 varies parallelism deliberately,
so a job that only runs at one parallelism is not a job this project can use.

**Phase 5 inherits this.** These values move into `spec.flinkConfiguration` on the
`FlinkDeployment`, and a TaskManager container sized under 2gb hits the same wall.
