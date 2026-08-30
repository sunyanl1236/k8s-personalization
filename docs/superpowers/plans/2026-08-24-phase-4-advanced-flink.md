# Phase 4 advanced Flink implementation plan

**Goal:** Add the Product-keyed branch and the four remaining Flink concepts to
the Phase 3 job, still on `MiniCluster`, so a Promo Rule can change mid-run and
the `recommendation` topic changes without a restart.

**Architecture:** The raw watermarked clickstream forks three ways. Two
Shopper-keyed branches (session window, CEP) union into one `ShopperSignal`. One
Product-keyed branch runs an interval join against `ProductChange` and re-keys its
output to `shopperId`. The two sides `connect` in a `KeyedCoProcessFunction`
that emits only on a Browsing Session close. A stateless
`BroadcastProcessFunction` then applies the active Promo Rule, and
`AsyncDataStream.orderedWait` calls a mocked recommendation service before the
sink.

**Tech Stack:** Flink 2.2 (DataStream API), `flink-cep`,
`flink-connector-kafka`, `flink-statebackend-rocksdb`, JUnit 5, AssertJ,
`flink-test-utils`, Gradle multi-project, Java 21 records.

**Spec:** [advanced Flink design](../specs/2026-08-24-advanced-flink-design.md)

## How to use this plan

This project's [CLAUDE.md](../../../CLAUDE.md) working agreement governs, and it
overrides the default shape of a plan document.

**You write every file and run every command.** Each task gives the goal, the
concept behind it, the failure mode to watch for, a skeleton or a signature, and
the command that proves it worked. It deliberately does not give finished
implementations. Work done for you is learning lost.

**A task is done when its verification command produces real output you have
read.** Not when it compiles. Not when a test class exists.

**This plan prescribes no commits.** Git is yours to drive.

Ask why something failed and you get the line and the mechanism, not a corrected
file.

## Prerequisite

**Phase 3 Tasks 6 to 10 must be done before Task 4 of this plan.** Tasks 0 to 3
here touch nothing Phase 3 still has open, so they can run in parallel with the
rest of Phase 3 if you want. Task 4 modifies `RecommendationDecider`, which
Phase 3 Task 6 creates, so it cannot start earlier.

## Progress

| # | Task | Status |
|---|---|---|
| 0 | Confirm how `flink-cep` is packaged | done 2026-08-24, gate cleared, provided in `lib/` |
| 1 | `Product Change` snapshot model, and its source | done 2026-08-28, redesigned mid-flight per ADR 0008, rate and `stock=0` share confirmed live |
| 2 | Test infrastructure, the interval join, and `EnrichedClick` | done 2026-08-28, 5 tests green, live rate confirmed |
| 3 | CEP: abandoned cart and `ShopperSignal` | done 2026-08-28, 5 tests green, live rate confirmed |
| 4 | The `connect` merge and the `UNMATCHED` side output | done 2026-08-29, 9 tests green, live run confirmed after a network-buffer config fix |
| 5 | Broadcast Promo Rules | done 2026-08-29, 5 tests green. Step 7 live check not run |
| 6 | Async I/O and the mocked recommendation service | done 2026-08-30, 2 tests green, live verified on the topic |
| 7 | End-to-end test of the assembled graph | done 2026-08-30, 27 tests green |
| 8 | Drill C: change a Promo Rule mid-run | done 2026-08-30, all four claims confirmed, runbook carries the real transcript |
| 9 | Documents: ADR amendment, glossary, knowledge doc, status | done 2026-08-30 |

## Global constraints

Copied verbatim from the spec and inherited from Phase 3. Every task inherits
these.

- Flink **2.2**. `org.apache.flink.streaming.api.windowing.time.Time` does not
  exist; windowing and CEP take `java.time.Duration`.
- `:domain` declares **no dependencies at all**. Not Kafka, not Flink, not
  JUnit.
- `pipeline.generic-types: false`. Any Kryo fallback must fail loudly.
- **No sealed interface and no abstract supertype may enter the job graph.**
  Every type crossing an operator boundary is a flat record or an enum. See
  [ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md).
- **Never filter the Product Change stream down to price drops.** The merge
  needs stock from every update, drop or not.
- **Nothing in the output derives from wall-clock time.** `generatedAt` is the
  Browsing Session's window end. Every timer is an event-time timer. Every mock
  reply is a pure function of its request.
- Watermarks are assigned **once, on the raw stream, before the fork**.
- Watermark bound **5s**, session gap **6s**, join interval **-2s to +2s**
  (`--join-lower-bound-seconds` / `--join-upper-bound-seconds`), CEP
  `within` **30s**, checkpoint interval **10s**, cooldown **60s** of event time.
- Async I/O uses `orderedWait`, never `unorderedWait`.
- Delivery guarantee `EXACTLY_ONCE`, with a **stable** transactional id prefix.
- Any consumer verifying output sets `isolation.level=read_committed`.
- Every test runs at **parallelism 1** over **bounded** input.
- Ubiquitous language from [CONTEXT.md](../../../CONTEXT.md): Shopper, Click,
  Browsing Session, Product, Product Change, Promo Rule, Recommendation, Signal,
  Late Click, Unmatched Click, Cart Abandonment, Out of Stock, Drill.

---

## Task 0: Confirm how `flink-cep` is packaged

**Status: done, 2026-08-24. Gate cleared, Task 1 is unblocked.**

**Files:** none. This task's whole output is a decision recorded in the spec.

**Why this is Task 0.** Task 3 in [the Phase 3 plan](2026-08-23-phase-3-core-pipeline.md)
lost time to exactly this class of problem: `flink-connector-base` is bundled in
`flink-dist`, so nothing pulls it transitively and the compile failed in a way
that did not name the cause. `flink-cep` may or may not be in the same position,
and the answer decides its Gradle scope here **and** whether Phase 5's fat jar
must bundle it.

**The concept.** A Flink distribution splits its modules in two. `lib/` is on
the classpath of every job automatically, so a job jar must **not** bundle those
(bundling them causes class-loader conflicts). `opt/` is shipped but **not** on
the classpath, so a job that uses one of those must either bundle it or have an
operator copy it into `lib/`. Getting this backwards fails at runtime with
`NoClassDefFoundError`, or with a much worse duplicate-class problem, never at
compile time.

- [x] **Step 1: Look in the official image.**

The plan's original `ls lib opt | grep -i cep` was replaced, because `ls` over
two directories loses which one each match came from, which is the only thing
this task needs to know.

```bash
docker run --rm flink:2.2.0 sh -c \
  'find lib opt -iname "*cep*"; echo "--- lib ---"; ls lib; echo "--- opt ---"; ls opt'
```

Result: `lib/flink-cep-2.2.0.jar`. Provided, so the scope is `compileOnly` plus
`runtimeOnly`, matching `flink-streaming-java`.

- [x] **Step 2: Cross-check against the documentation.**

The [advanced configuration page][d0-advanced] states it, and names CEP
explicitly:

> the Flink Core Dependencies do not contain any connectors or libraries (i.e.
> **CEP**, SQL, ML) ... The `/lib` directory of the Flink distribution
> **additionally** contains various JARs including commonly used modules ...
> These are loaded by default

The two sources agree. CEP is not inside `flink-dist.jar`, and it ships as its
own jar in `lib/` which is loaded by default.

- [x] **Step 3: Record the answer.**

Done, in [the spec's Dependencies section](../specs/2026-08-24-advanced-flink-design.md#dependencies).

**Three findings beyond the gate itself, recorded because nothing else records
them.**

1. `flink-s3-fs-hadoop` is in `opt/`, not `lib/`. Shipped, and **not** on the
   classpath. ADR 0001 predicted it becomes a plugin directory in Phase 5, and
   the image confirms it.
2. **`runtimeOnly` will not keep these jars out of Phase 5's fat jar.** Gradle's
   Shadow plugin builds from the runtime classpath, so every `runtimeOnly`
   dependency gets bundled: `flink-streaming-java`, `flink-clients`,
   `flink-statebackend-rocksdb`, `flink-connector-base`, and now `flink-cep`.
   Bundling any of them next to a distribution that already loads them is the
   duplicate-class problem the `compileOnly` split exists to prevent. Phase 5
   needs a dedicated configuration or an explicit exclusion set. The scope
   declares the intent; it does not enforce it.
3. The `lib/` guarantee holds only for an unmodified image. The same page notes
   those jars "can be removed from the classpath just by removing them from the
   `/lib` folder", so a slimmed base image in Phase 5 would invalidate this.

[d0-advanced]: https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/configuration/advanced.md

---

## Task 1: `Product Change` snapshot model, and its source

**Status: done, 2026-08-28. Task 2 is unblocked.**

**Redesigned 2026-08-28.** Steps 1 to 4 were implemented against the old sealed
model and are partly superseded by
[ADR 0008](../../adr/0008-product-change-as-a-state-snapshot.md). What survives
is called out per step.

**Files:**
- Rewrite: `apps/domain/src/main/java/lab/personalization/domain/ProductChange.java`
- **Delete**: `PriceChange.java`, `StockChange.java`
- Modify: `apps/domain/src/main/java/lab/personalization/domain/JsonCodec.java`
- Modify: `apps/generator/src/main/java/lab/personalization/generator/factory/ProductChangeFactory.java`
- Rename: `PriceChangeDeserializationSchema.java` -> `ProductChangeDeserializationSchema.java`
- Modify: `PipelineConfig.java`, `PersonalizationJob.java`

**Interfaces produced:**
```java
public record ProductChange(String productId, Instant eventTime,
                            double price, double previousPrice,
                            int stock, int previousStock)
public static Click clickFromJson(byte[] bytes)
public static ProductChange productChangeFromJson(byte[] bytes)
public class ProductChangeDeserializationSchema implements DeserializationSchema<ProductChange>
```

**The problem, before the mechanism.** A sealed interface cannot be a Flink
stream element type. Asked directly, Flink 2.2 answers `GenericTypeInfo` and
`KryoSerializer` for `ProductChange`, while `PriceChange` and `StockChange` both
resolve to `PojoTypeInfo`. `pipeline.generic-types: false` turns that fallback
into a hard failure while the job graph is being built.

**Why the fix is a redesign and not a cast.** Two further facts, both in ADR
0007. Nothing in the project reads `StockChange`. And `PriceChange` carries no
previous price, so "price drop" was never checkable, though the generator stamps
every Promo Rule `"N% off, price-drop bonus"`.

**The shape to build.** One record carrying the Product's state and the state it
replaced. No discriminator, no nullable field, every field always real. On a
Product's first event the previous values equal the current ones.

**The failure mode to watch for.** It is tempting to filter the stream to price
drops right at the source. **Do not.** If only drops reach the interval join,
the merge never learns the stock of a Product that went out of stock without a
price move, and Task 4's suppression rule silently fails for exactly the
Products it exists to catch. The join takes every Product update; the drop test
happens later, on `EnrichedClick`.

- [x] **Step 1: `numberField` in `JsonCodec`.** Done, and still needed.

The pattern first specified here, `(-?[0-9.eE+]+)`, was wrong: `-?` anchors only
the start and the class holds no `-`, so `1.0E-4` matched as `1.0E` and threw
`NumberFormatException`. The corrected grammar below was probed against eight
shapes.

```java
private static Pattern numberField(String name) {
    return Pattern.compile("\"" + name + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?(?:[eE][-+]?[0-9]+)?)");
}
```

- [x] **Step 2: rename `fromJson` to `clickFromJson`.** Done, and still correct.
Three readers cannot share one name, since Java does not overload on return
type.

- [x] **Step 3: rewrite `ProductChange`, delete the two variants.**

Six fields. Deleting `PriceChange` and `StockChange` will break
`ProductChangeFactory` and `JsonCodec`, which is the point: the compiler walks
you to every site that assumed a sum type.

- [x] **Step 4: rewrite both directions in `JsonCodec`.**

`toJson` loses its pattern `switch`, and `productChangeFromJson` loses its type
`switch`. Both get shorter. The wire format drops `"type"`:

```json
{"productId":"P1","eventTime":"...","price":19.99,"previousPrice":24.99,"stock":0,"previousStock":40}
```

You lose compiler-enforced exhaustiveness here. ADR 0008 accepts that trade
knowingly; it is not an oversight to fix later.

- [x] **Step 5: teach `ProductChangeFactory` to remember.**

A `Map<String, ProductChange>` of the last event per Product, ten entries. The
new event's previous values come from that map, or equal its own current values
when the Product has not been seen.

**Stock is `0` with probability `0.1`**, otherwise `1` to `500`. The natural
`random.nextInt(501)` makes stock zero about once in five hundred events, so
Task 4's suppression rule would essentially never fire and could not be drilled.
This is a deliberate synthetic-data choice, the same kind as Phase 3's derived
6 second session gap.

- [x] **Step 6: rename the deserializer and delete the narrowing.**

`ProductChangeDeserializationSchema` now needs only the single-value
`deserialize(byte[])` and `getProducedType()`. The `Collector` overload existed
solely to emit zero records for a `StockChange`, and nothing is dropped any
more.

- [x] **Step 7: `--product-change-topic` in `PipelineConfig`.** Done, unchanged.

- [x] **Step 8: build the second source and print it.**

Its own consumer group, and its own `WatermarkStrategy<ProductChange>` with
`forBoundedOutOfOrderness(...)`, a timestamp assigner reading
`ProductChange::eventTime`, and `.withIdleness(...)`. All three are required.
Without idleness, one idle partition of `product-change` pins the watermark at
`Long.MIN_VALUE` and Task 2's join fires nothing, which looks like a broken join
rather than a watermark problem. This is Phase 3 Task 4's bug in a new place.

Honour `config.bounded()` here as the Click source does, or bounded mode hangs
with one source finished and the other running forever.

**Keep the stream in a variable.** Task 2 forks it into the interval join, and
you cannot fork what you chained straight into `.print(...)`.

- [x] **Step 9: verify against the real topic.**

```bash
apps/gradlew -p apps :generator:run                                       # terminal 1
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"   # terminal 2
```

Expected: `ProductChange` lines at **one per second**, matching
`product-change-rate`. Nothing is dropped now, so half the rate would mean the
old narrowing survived somewhere.

Check two things in the printed records, not just the rate. About one in ten
should carry `stock=0`. And `previousPrice` should differ from `price` on most
records but equal it on the first event seen for each Product.

## Task 2: Test infrastructure, the interval join, and `EnrichedClick`

**Status: done, 2026-08-28. Task 3 is unblocked.**

**Files:**
- Create: `apps/domain/src/main/java/lab/personalization/domain/EnrichedClick.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/ProductChangeJoiner.java`
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/IntervalJoinTest.java`
- Modify: `apps/pipeline/build.gradle`
- Modify: `PersonalizationJob.java`

**Interfaces consumed:** `ProductChange` source from Task 1.

**Interfaces produced:**
```java
public record EnrichedClick(String shopperId, String productId, Instant clickTime,
                            double price, double previousPrice,
                            int stock, Instant changeTime)
class ProductChangeJoiner extends ProcessJoinFunction<Click, ProductChange, EnrichedClick>
```

**The concept.** An interval join asks, for each element on the left, which
elements on the right have an event time inside a window **relative to that
element**. It is not a windowed join: there is no shared window boundary, every
Click carries its own interval. Both sides must be keyed by the same key, and
`ProductChange` has no `shopperId`, so this branch forks from the raw stream and
applies its own `keyBy(productId)`, per
[ADR 0003](../../adr/0003-interval-join-key-and-semantics.md).

**The failure mode to watch for, and it is the one that will actually bite.**
Three separate things silently produce zero output here:

1. **No watermarks on the right side.** Task 1 step 5 already handled this. If
   you skipped it, nothing ever fires.
2. **Idle partitions.** The same bug Phase 3 Task 4 hit. `product-change` has
   its own partitions and the same `.withIdleness(...)` treatment is needed.
3. **`between(...)` bounds with the wrong sign.** `between(lowerBound,
   upperBound)` is relative to the **left** element's timestamp, and the lower
   bound is normally negative. `between(Duration.ofSeconds(2),
   Duration.ofSeconds(-2))` compiles and matches nothing.

**Test infrastructure lands here because this is the first test.** Three
non-obvious Gradle facts, each of which produces a confusing failure:

- **`compileOnly` does not reach the test compile classpath.**
  `flink-streaming-java` is `compileOnly` in this build, so a test importing
  `StreamExecutionEnvironment` fails to compile with "cannot find symbol" even
  though the main source set compiles fine.
- **`applicationDefaultJvmArgs` does not apply to `test`.** It configures the
  `run` task only. Without the same `--add-opens` flags on the test JVM, Flink
  fails at runtime with `InaccessibleObjectException` on `java.util`.
- **JUnit 5 needs `useJUnitPlatform()`.** Without it Gradle finds zero tests and
  reports success, which is the worst possible failure.

- [x] **Step 1: Add the test configuration to `apps/pipeline/build.gradle`.**

```groovy
configurations {
    testImplementation.extendsFrom compileOnly
}

test {
    useJUnitPlatform()
    jvmArgs applicationDefaultJvmArgs
}

dependencies {
    testImplementation "org.apache.flink:flink-test-utils:${flinkVersion}"
    testImplementation "org.junit.jupiter:junit-jupiter:5.11.4"
    testImplementation "org.assertj:assertj-core:3.27.3"
    testRuntimeOnly "org.junit.platform:junit-platform-launcher"
}
```

`testImplementation.extendsFrom compileOnly` is the line that solves the first
problem above. It is a deliberate choice, not boilerplate: it says "whatever a
real cluster provides at runtime, a test JVM has to provide for itself".

- [x] **Step 2: Prove the test task actually runs.**

Write a throwaway test asserting `1 + 1 == 2` and run:

```bash
apps/gradlew -p apps :pipeline:test
```

Expected: `1 test completed`. If it says `NO-SOURCE` or reports success with no
test count, `useJUnitPlatform()` is missing or the file is in the wrong source
root. Delete the throwaway test once it has told you what you needed.

- [x] **Step 3: Write `EnrichedClick` in `:domain`.**

An enriched Click is a Signal by the `CONTEXT.md` definition, so it is domain
vocabulary. It carries both event times, not just the Click's, because Phase 8's
dashboard needs the gap between them and recomputing it downstream is
impossible once one of them is dropped.

- [x] **Step 4: Write the failing test.**

```java
@Test
void clickWithinTwoSecondsOfProductChangeMatches() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant t = Instant.parse("2026-08-24T10:00:00Z");
    // Click on P1 at t, ProductChange on P1 at t+1s  -> expect one EnrichedClick
    // Click on P2 at t, ProductChange on P2 at t+4s  -> expect nothing
    // and assert the EnrichedClick carries price, previousPrice and stock
}
```

Fill in bounded sources with `env.fromData(...)`, the same watermark strategy
shape as production, the `keyBy`/`intervalJoin`/`between`/`process` chain, and
collect with `.executeAndCollect()`. Assert with AssertJ that exactly one
`EnrichedClick` came out and that it names `P1`.

**Why bounded input is what makes this work.** A bounded source emits
`Watermark.MAX_WATERMARK` when it finishes. That releases every buffered
interval-join candidate, so the test terminates instead of waiting. An unbounded
source in a test hangs forever and looks like a deadlock.

- [x] **Step 5: Run it and watch it fail.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*IntervalJoinTest*'
```

Expected: a compile failure naming `EnrichedClick` or `ProductChangeJoiner`. Read
the message. If instead it hangs, you have an unbounded source.

- [x] **Step 6: Write `ProductChangeJoiner` and wire the branch into the job.**

```java
clicks.keyBy(Click::productId)
      .intervalJoin(productChanges.keyBy(ProductChange::productId))
      .between(Duration.ofSeconds(-2), Duration.ofSeconds(2))
      .process(new ProductChangeJoiner())
```

`clicks` here is the **raw watermarked stream**, the same variable the
Shopper-keyed branch forks from, not anything downstream of `keyBy(shopperId)`.

- [x] **Step 7: Run the test until it passes, then check the live rate.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*IntervalJoinTest*'
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected from the live run: `EnrichedClick` output at roughly **one third** of
the Click rate. The spec's arithmetic gives 33 percent, from
`1 - e^(-0.4)` where 0.4 is the expected number of Product Changes on one Product
inside a 4 second window.

Zero output with a passing test means watermarks or idleness on the
`product-change` side. Output at the full Click rate means the bounds are wrong.

---

## Task 3: CEP: abandoned cart and `ShopperSignal`

**Status: done, 2026-08-28. Task 4 is unblocked.**

**Files:**
- Create: `apps/domain/src/main/java/lab/personalization/domain/SignalKind.java`
- Create: `apps/domain/src/main/java/lab/personalization/domain/ShopperSignal.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/CartAbandonmentPattern.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/CartAbandonmentMatcher.java`
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/CartAbandonmentCepTest.java`
- Modify: `apps/pipeline/build.gradle`, `PersonalizationJob.java`

**Interfaces consumed:** the raw watermarked clickstream.

**Interfaces produced:**
```java
public enum SignalKind { BROWSING_SESSION, CART_ABANDONMENT }
public record ShopperSignal(String shopperId, SignalKind kind,
                            Instant eventTime, String productId)
class CartAbandonmentMatcher extends PatternProcessFunction<Click, ShopperSignal>
        implements TimedOutPartialMatchHandler<Click>
static final OutputTag<Click> CEP_TIMED_OUT
```

**The pattern, and why it is not the one the design spec names.** The spec's
coverage map says "viewed a product repeatedly, viewed a competitor, went idle".
The spec for this phase supersedes that, because at the generator's real rates
that pattern needs a 60 second `within`, which spans ten session gaps. The
pattern built here is the one `ActionType`'s own comment in `:domain` already
described.

```java
Pattern.<Click>begin("view")
        .where(SimpleCondition.of(c -> c.actionType() == ActionType.VIEW))
    .followedBy("cart")
        .where(/* IterativeCondition: ADD_TO_CART on the same Product as "view" */)
    .notFollowedBy("checkout")
        .where(/* IterativeCondition: CHECKOUT on the same Product as "view" */)
    .within(Duration.ofSeconds(30));
```

**The concept that makes the middle two steps work.** `SimpleCondition` sees
only the candidate event. It cannot express "the same Product as the earlier
step", because it has no access to the earlier step. `IterativeCondition` does:
`ctx.getEventsForPattern("view")` returns what matched that named step so far.
Compare `productId` against `getEventsForPattern("view").iterator().next()`.

**The failure modes to watch for.**

- **`notFollowedBy` without `within(...)`.** Flink rejects a pattern ending in
  `notFollowedBy` unless a time constraint bounds it, and rightly so: without a
  bound the absence could never be confirmed. The error names the pattern, not
  the missing `within`.
- **`followedBy` versus `next`.** `next` demands strict adjacency, meaning the
  ADD_TO_CART must be the immediately following Click for that Shopper. At this
  generator's rates that almost never happens. `followedBy` allows other Clicks
  in between, which is what an abandoned cart actually looks like.
- **CEP on an unkeyed stream.** `CEP.pattern(...)` accepts a plain
  `DataStream`, and then matches span all Shoppers. It compiles. It is wrong.
  Key by `shopperId` first.

**The escape hatch, agreed in advance so it is not a judgement call under time
pressure.** The design spec names CEP as the concept most likely to overrun, and
pre-agrees the fallback: trim CEP before cutting anything from the Kubernetes HA
side. This plan already uses a single pattern, so the remaining trim is
**dropping `TimedOutPartialMatchHandler` and the `CEP_TIMED_OUT` side output**,
which is step 5's second half and the third case in step 3. That loses one side
output from Phase 8's dashboard and nothing else. Take it if this task passes
four hours, and record it in `status.md`.

- [x] **Step 1: Add `flink-cep` at the scope Task 0 established.**

```groovy
compileOnly "org.apache.flink:flink-cep:${flinkVersion}"
runtimeOnly "org.apache.flink:flink-cep:${flinkVersion}"
```

The pair, not `implementation`. Task 0 confirmed `lib/flink-cep-2.2.0.jar` is in
the official image and loaded by default, so the job must not bundle it.
`compileOnly` gives you the Pattern API at compile time; `runtimeOnly` puts it
on the classpath under `MiniCluster`, where there is no distribution to provide
it.

- [x] **Step 2: Write `SignalKind` and `ShopperSignal` in `:domain`.**

Four fields, all meaningful for both kinds. `eventTime` is the window end for a
Browsing Session and the pattern's completion time for a cart abandonment;
`productId` is the candidate Product in one case and the abandoned Product in
the other.

**Do not add `clickCount` because `SessionSignal` has it.** Nothing downstream
reads a click count, so it would cross a shuffle on every record for no
consumer. A transport type carries what its consumer reads.

The record is flat with an enum discriminator rather than a sealed interface
over the two kinds, and that is forced: a sealed interface fails exactly the way
`ProductChange` did, giving `GenericTypeInfo` and a `KryoSerializer` that
`pipeline.generic-types: false` rejects.

- [x] **Step 3: Write the failing test, five cases.**

```java
@Test void viewThenCartWithNoCheckoutMatches()
@Test void viewThenCartThenCheckoutDoesNotMatch()
@Test void aCartOnADifferentProductDoesNotMatch()           // the IterativeCondition
@Test void oneShoppersCartDoesNotCompleteAnothersPattern()  // the keyBy
@Test void aViewThatIsNeverCartedLandsInTheTimedOutSideOutput()
```

Bounded input, parallelism 1, event times spaced a few seconds apart inside the
30 second window. The third case is what proves `TimedOutPartialMatchHandler` is
wired, and it is the one most likely to be skipped.

- [x] **Step 4: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*CartAbandonmentCepTest*'
```

- [x] **Step 5: Write the pattern and the matcher.**

`processMatch(...)` builds a `ShopperSignal` with `kind = CART_ABANDONMENT` and
`productId` from the `"view"` step.
`processTimedOutMatch(...)` calls `ctx.output(CEP_TIMED_OUT, ...)`.

`eventTime` on the Signal comes from the matched Clicks, never from
`Instant.now()`. The global constraint about wall-clock time applies here as
much as it does to `generatedAt`. The ADD_TO_CART Click's own `eventTime` is
used, being the last real observation in the match.

**One unknown, resolved 2026-08-28 by the test rather than by reading.** It was
not clear whether Flink delivers a pattern ending in `notFollowedBy(...)
.within(...)` through `processMatch` when the window expires cleanly, or whether
it treats "view, cart, no checkout" as a timed-out partial instead. It is
`processMatch`: a clean expiry is a **match**, and `processTimedOutMatch` sees
only genuinely incomplete sequences such as a VIEW that was never carted. If it
had been the other way, the emission would have had to move into
`processTimedOutMatch`.

- [x] **Step 6: Wire the branch and check the live rate.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*CartAbandonmentCepTest*'
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected: `CART_ABANDONMENT` Signals on roughly **24 percent of VIEW Clicks**.
With 5 Clicks per second overall and a third of them VIEW, that is about 0.4
Signals per second.

If it fires on nearly every Click, you used `followedBy` without the
same-Product `IterativeCondition`, so any ADD_TO_CART matches any VIEW.

---

## Task 4: The `connect` merge and the `UNMATCHED` side output

**Status: done, 2026-08-29. Task 5 is unblocked.**

**The live run needed a config fix first, and the cause was not this task's
code.** The first attempt failed at deploy, before a single Recommendation was
produced:

```
java.io.IOException: Insufficient number of network buffers: required 17,
but only 0 available. The total number of network buffers is currently set to
2048 of 32768 bytes each.
```

Phase 4 has grown the graph to five `keyBy` shuffles, and `MiniCluster`'s default
network memory no longer covers them at the default parallelism of 16. The lever
is `taskmanager.memory.network.*` in `apps/pipeline/conf/config.yaml`, not
pinning parallelism, for the same reason Phase 3 Task 4 refused to pin it: Phase
6 varies parallelism deliberately.

**Fixed 2026-08-29** in `apps/pipeline/conf/config.yaml`, which is where Flink
settings live as data:

```yaml
taskmanager.memory.process.size: 2gb
taskmanager.memory.network.min: 256mb
taskmanager.memory.network.max: 256mb
```

256mb of 32KB buffers is 8192, against the 2048 the default `network.min` gave.
Setting min and max to the same value is the documented way to pin the size
rather than let it be derived from a fraction of a small total.

After the fix every output appeared with **zero** failed tasks and no buffer
complaint: `REQUEST`, `UNMATCHED`, `CART-ABANDONED`, `CEP-TIMEOUT`,
`PRICE-DROP-MATCH`, `MERGED-SIGNAL`, `SIGNAL`. This also unblocks Tasks 5, 6 and
Drill C, which could not have been verified live either.

**Requires Phase 3 Task 6 to be done.**

**Files:**
- Create: `apps/domain/src/main/java/lab/personalization/domain/RecommendationRequest.java`
- Rename and rewrite: `RecommendationDecider.java` -> `SignalMerger.java`
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/MergeFunctionTest.java`
- Modify: `PersonalizationJob.java`

**Interfaces consumed:** `ShopperSignal` and `SignalKind` from Task 3,
`EnrichedClick` from Task 2, the cooldown logic from Phase 3 Task 6.

**Interfaces produced:**
```java
public record RecommendationRequest(String shopperId, String candidateProductId,
                                    boolean priceDropMatched, boolean cartAbandoned,
                                    double discountPercent, String reason,
                                    Instant generatedAt)
class SignalMerger extends KeyedCoProcessFunction<String, ShopperSignal, EnrichedClick, RecommendationRequest>
static final OutputTag<RecommendationRequest> UNMATCHED
static final OutputTag<RecommendationRequest> OUT_OF_STOCK
```

**The problem this operator solves.** `keyBy` physically decides which worker
handles which data. The Browsing Session for Shopper 42 lives on whichever
worker owns `shopperId = 42`. The price-drop match for Product P1 lives on
whichever worker owns `productId = P1`. Those are different workers with no
shared memory. Neither one alone can produce a Recommendation. Re-keying the
join output to `shopperId` physically moves it to the worker that already holds
the session, and only then can one function see all of it.

**Why this is a `connect` and not a `union`.** `union` requires one identical
type. `ShopperSignal` and `EnrichedClick` carry genuinely different payloads.
`connect` gives one function two typed entry points, `processElement1` and
`processElement2`, over the same keyed state.

**Finding that supersedes ADR 0003, and it changes this task.** ADR 0003's
diagram shows `unmatched: side output` leaving the interval join. That is not
implementable. `intervalJoin` is an **inner** join, so
`ProcessJoinFunction.processElement(...)` is never invoked for a Click that did
not match, and there is no callback in which to emit it. The `UNMATCHED` side
output therefore lives **here**, and its grain changes from one record per Click
to one record per Browsing Session candidate.

**The emission rule.**

| Input | Effect |
|---|---|
| `processElement2(EnrichedClick)` | record the Product's `stock`; if `price < previousPrice`, put the `EnrichedClick` into `matchesByProduct` |
| `processElement1(CART_ABANDONMENT)` | put `productId` into `abandonedCarts` |
| `processElement1(BROWSING_SESSION)` | read both maps **for the candidate Product only**, emit, clear both maps |

Both are `MapState<String, Boolean>` holding `productId -> true`. The candidate
Product is `ShopperSignal.productId`, which for a `BROWSING_SESSION` is the
most-clicked Product of that Browsing Session.

**Why the narrowing to the candidate Product is load-bearing.** A Shopper
abandons about 1.6 carts per Browsing Session across 10 Products. An unnarrowed
check would put `reason = "cart-abandoned"` on roughly 80 percent of
Recommendations and drown the other two values. Narrowed, it lands near 15
percent.

- [x] **Step 1: Write `RecommendationRequest` in `:domain`.**

`discountPercent` is `0.0` here and stays that way until Task 5.
`candidateProductId` is what the job proposes; `Recommendation.productId` in
Task 6 is what the service answered. They are allowed to differ, and that
difference is the entire reason async I/O exists in this pipeline.

- [x] **Step 2: Write the failing harness test.**

```java
KeyedTwoInputStreamOperatorTestHarness<String, ShopperSignal, EnrichedClick, RecommendationRequest> harness =
        ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new SignalMerger(), ShopperSignal::shopperId, EnrichedClick::shopperId, Types.STRING);
```

**Resolved in Task 2, 2026-08-28.** `flink-test-utils` does **not** bring the
harness classes; it pulls only the plain `flink-runtime`. Both
`ProcessFunctionTestHarnesses` and the `*OperatorTestHarness` base classes live
in the tests classifier, already declared in `build.gradle`:

```groovy
testImplementation "org.apache.flink:flink-runtime:${flinkVersion}:tests"
```

Five behaviours to assert:

1. An `EnrichedClick` alone emits nothing.
2. A `BROWSING_SESSION` whose candidate had a match emits with
   `priceDropMatched = true`.
3. A `BROWSING_SESSION` whose candidate had **no** match goes to the `UNMATCHED`
   side output.
4. A `CART_ABANDONMENT` on the candidate Product sets `reason = "cart-abandoned"`,
   which outranks `"price-drop"`.
5. After a `BROWSING_SESSION` fires, both maps are empty, so the next session
   for the same Shopper starts clean.

Case 5 is the one that catches the unbounded-state bug, and it is the one people
skip.

- [x] **Step 3: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*MergeFunctionTest*'
```

- [x] **Step 4: Rewrite `RecommendationDecider` as `SignalMerger`.**

Phase 3's body becomes `processElement1`, exactly as the
[core pipeline design](../specs/2026-08-23-core-pipeline-design.md) predicted.
The cooldown state and its event-time timer carry over unchanged. Only the
output type changes, from `Recommendation` to `RecommendationRequest`.

A candidate Product whose recorded `stock` is `0` is suppressed **before** any
of this. It goes to `OUT_OF_STOCK` and never reaches the sink, because
recommending something nobody can buy is the rule ADR 0008 added stock to
prevent. Routed rather than dropped: a suppression nobody can count is a bad
rule.

For everything that does emit, `reason` precedence is, most specific first:
`"cart-abandoned"`, then `"price-drop"`, then Phase 3's
`"most-viewed-in-session"`.

- [x] **Step 5: Give the two maps their different lifetimes.**

**Settled 2026-08-28.** A cart abandonment confirms 30s after the VIEW, because
an absence cannot be proven sooner, while a Browsing Session closes 6s after its
last Click. About **60% of abandonments arrive after their own session has
closed**, and are therefore read by the Shopper's *next* session close rather
than their own. They are not lost, just attributed one session late.

- `matchesByProduct` **clears on session close.** It is about a specific Click
  near a specific price move, so it is session-scoped.
- `abandonedCarts` **expires on its own event-time timer, 60 seconds** after the
  abandonment. "Abandoned a cart on P1 recently" is a Shopper-level fact that
  survives a session boundary.

60s is derived, not picked: at ~0.04 abandonments per second per Shopper it
retains about 2.4 across 10 Products, giving the ~21% share of
`"cart-abandoned"` the design expects. Five minutes would retain ~12 and swamp
the other reasons.

**Why a timer rather than clearing both maps.** Clearing both is simpler and
loses nothing, so this is a close call. It is chosen for bounded staleness: with
clear-on-close, an abandonment survives until the Shopper next browses, which for
an inactive Shopper is unbounded and would then be applied as though fresh.

Event-time timers, never processing-time, for the reason the global constraints
give. Both mechanisms together keep state bounded for a Shopper whose session
never closes.

- [x] **Step 6: Union, re-key, connect, and verify live.**

```java
DataStream<ShopperSignal> shopperSignals = sessionSignals.map(/* to ShopperSignal */)
        .union(cartAbandonments);

shopperSignals.keyBy(ShopperSignal::shopperId)
        .connect(enrichedClicks.keyBy(EnrichedClick::shopperId))
        .process(new SignalMerger());
```

`SessionSignal` itself does not change. A chained `.map()` converts it, which
costs no shuffle, so Phase 3's `SessionAggregator` and its Drill stay as built.

```bash
apps/gradlew -p apps :pipeline:test --tests '*MergeFunctionTest*'
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected: `RecommendationRequest` at roughly **one every 4 seconds**, the same
rate Phase 3 saw for `SessionSignal`, because the trigger has not changed.
Roughly 80 percent carry `priceDropMatched = true` and roughly 15 percent read
`"cart-abandoned"`. `UNMATCHED` output at roughly one per 20 seconds.

If the emission rate jumped, something other than `BROWSING_SESSION` is
emitting.

---

## Task 5: Broadcast Promo Rules

**Status: done, 2026-08-29. Task 6 is unblocked.** Step 7's live check was not
run; the task is signed off on its five harness tests.

**Files:**
- Modify: `apps/domain/src/main/java/lab/personalization/domain/JsonCodec.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/PromoRuleDeserializationSchema.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/PromoRuleApplier.java`
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/PromoRuleBroadcastTest.java`
- Modify: `PipelineConfig.java`, `PersonalizationJob.java`

**Interfaces consumed:** `RecommendationRequest` from Task 4, `numberField` from
Task 1.

**Interfaces produced:**
```java
public static PromoRule promoRuleFromJson(byte[] bytes)
class PromoRuleApplier extends BroadcastProcessFunction<RecommendationRequest, PromoRule, RecommendationRequest>
static final MapStateDescriptor<String, PromoRule> PROMO_RULES
```

**The concept.** Keyed state is partitioned: each worker holds the slice for its
own keys. Broadcast state is the opposite. Every worker holds the **full** copy,
and every worker receives every broadcast element. That is what lets a rule
change take effect everywhere at once with no restart, which is the property
this phase's done-when actually tests.

**A map holding one entry is not a mistake.** Broadcast state in Flink is
**always** a map: `ctx.getBroadcastState(...)` takes a `MapStateDescriptor` and
there is no value-typed form. One fixed key is how a single-valued broadcast
rule is expressed.

**Why the rule replaces rather than accumulates.** The generator emits a fresh
`ruleId` every 30 seconds and never stops. Keeping every rule means broadcast
state holds 120 entries after an hour and grows forever. One fixed key makes
retention a non-question.

**Why the discount is conditional.** Only a request whose candidate Product had
a price-drop match earns the discount. If every Recommendation got it, the whole
Product-keyed branch could be deleted and the output topic would look identical
apart from `reason`. The generator's own description string says the same thing:
`"N% off, price-drop bonus"`.

**The failure mode to watch for.** `processBroadcastElement` gets a writable
`Context`. `processElement` gets a `ReadOnlyContext`. Trying to write broadcast
state from `processElement` does not compile, and that is deliberate: if each
worker could write its own copy, the copies would diverge and the state would no
longer be broadcast. When the compiler refuses, it is telling you something
true.

- [x] **Step 1: Write `promoRuleFromJson` and `PromoRuleDeserializationSchema`.**

`discountPercent` is unquoted on the wire, so it needs the `numberField` pattern
from Task 1, not `stringField`.

- [x] **Step 2: Add `--promo-rule-topic` to `PipelineConfig`, default `promo-rule`.**

- [x] **Step 3: Write the failing harness test.**

```java
BroadcastOperatorTestHarness<RecommendationRequest, PromoRule, RecommendationRequest> harness =
        ProcessFunctionTestHarnesses.forBroadcastProcessFunction(
                new PromoRuleApplier(), PromoRuleApplier.PROMO_RULES);
```

**The broadcast harness does not use `processElement1` / `processElement2`.**
Unlike the two-input harness, it mirrors the function's own method names:
`harness.processElement(...)` for the request stream and
`harness.processBroadcastElement(...)` for the rules. It also exposes
`getBroadcastState(descriptor)`, which is how the test asserts that a new rule
**replaces** rather than accumulates.

Four behaviours:

1. With no rule broadcast yet, a matched request still emits, at `0.0`. A
   pipeline that swallows records until the first rule arrives would lose the
   first 30 seconds of every run.
2. After `promo-1` at 10 percent, a matched request carries `10.0`.
3. After `promo-2` at 15 percent, the next matched request carries `15.0`, and
   the state still holds exactly one entry.
4. An **unmatched** request carries `0.0` regardless of the active rule.

Case 4 is the one that proves the condition is structural rather than universal.

- [x] **Step 4: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*PromoRuleBroadcastTest*'
```

- [x] **Step 5: Write `PromoRuleApplier`.**

Records are immutable, so `processElement` emits a **copy** of the request with
the discount filled in, not a mutation.

- [x] **Step 6: Broadcast the source and connect it.**

```java
BroadcastStream<PromoRule> rules = env.fromSource(promoRuleSource, WatermarkStrategy.noWatermarks(), "promo-rule")
        .broadcast(PromoRuleApplier.PROMO_RULES);

requests.connect(rules).process(new PromoRuleApplier());
```

`noWatermarks()` is correct and deliberate. Promo Rules are not watermark-gated,
which is why the domain schema gave `PromoRule` no `eventTime` field at all. A
watermark strategy here would hold back the whole job waiting for a stream that
emits once every 30 seconds.

No `keyBy` before this operator. The discount step needs no keyed state, so a
plain `BroadcastProcessFunction` avoids a second `shopperId` shuffle that the
keyed variant would force.

- [x] **Step 7: Verify live.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*PromoRuleBroadcastTest*'
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected: matched requests carry a non-zero `discountPercent` in the 5 to 20
range, and the number changes about every 30 seconds. Unmatched requests stay at
`0.0` throughout.

---

## Task 6: Async I/O and the mocked recommendation service

**Status: done, 2026-08-30. Task 7 is unblocked.** The `recommendation` topic now
carries real discounts and real reasons; the Phase 3 `RecommendationDecider` path
is removed from the job.

**Files:**
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/RecommendationClient.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/DeterministicMockClient.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/AsyncRecommendationLookup.java`
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/AsyncRecommendationTest.java`
- Modify: `PersonalizationJob.java`

**Interfaces consumed:** `RecommendationRequest` carrying a discount, from Task 5.

**Interfaces produced:**
```java
public interface RecommendationClient {
    CompletableFuture<String> suggest(RecommendationRequest request);
}
class DeterministicMockClient implements RecommendationClient
class AsyncRecommendationLookup extends RichAsyncFunction<RecommendationRequest, Recommendation>
```

**Two things share a name, and confusing them wastes an afternoon.** The
**recommendation service** is an external system that decides which Product to
suggest, normally a trained model owned by another team. `Recommendation` is the
record this job writes after the reply comes back. One is a program called
mid-pipeline. The other is the pipeline's own output.

**The concept.** A blocking call inside a `ProcessFunction` stalls that
subtask's whole input for the duration. At 100ms per call and one subtask,
throughput caps at 10 records per second no matter how much CPU is free. Async
I/O keeps many requests in flight against one subtask, and Flink checkpoints the
in-flight ones so exactly-once still holds across a restart.

**Correction to the walkthrough.** Step 9 of
[the walkthrough](../../knowledge/flink-job-walkthrough.md) says the merge
function "calls the recommendation service asynchronously". It cannot.
`AsyncDataStream.orderedWait(...)` builds its **own** operator. Task 9 fixes
that sentence.

**Why `orderedWait` and not `unorderedWait`.** Phase 3's restart Drill asserts
the output after recovery is identical to an uninterrupted run. The Flink 2.2
documentation states that unordered mode emits records unordered *between*
watermarks, so two runs would differ by line order and the Drill would fail for a
reason unrelated to checkpointing. The usual reason to accept that is throughput,
and the same page removes the incentive: with watermarks present, unordered
carries much the same overhead as ordered.

**The failure modes to watch for.**

- **Not overriding `timeout(...)`.** The default fails the whole job. Override it
  to complete rather than throw.
- **Blocking inside `asyncInvoke`.** Calling `future.get()` there defeats the
  entire operator and turns it into a slow synchronous call. Register a callback
  with `thenAccept(...)` and return immediately.
- **A reply that is not a pure function of the request.** Random or wall-clock
  input breaks the restart Drill exactly the way `Instant.now()` on `generatedAt`
  would have.

- [x] **Step 1: Write the `RecommendationClient` interface and the mock.**

The interface exists for one concrete reason: a test injects a slow
implementation to reach the `timeout(...)` path, which is otherwise unreachable.
That is the only justification needed, and it is enough.

`DeterministicMockClient` computes its suggestion from the request alone and
simulates latency on a dedicated executor. No `Random`, no `Instant.now()`, no
`Thread.sleep` on the caller's thread.

- [x] **Step 2: Write the failing test, two cases.**

```java
@Test void repliesArriveInInputOrder()
@Test void aSlowClientHitsTheTimeoutPathAndStillEmits()
```

The second constructs `AsyncRecommendationLookup` with a client that never
completes inside the timeout, and asserts a `Recommendation` still comes out
rather than the job failing.

- [x] **Step 3: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*AsyncRecommendationTest*'
```

- [x] **Step 4: Write `AsyncRecommendationLookup`.**

Open the client in `open(...)`, close it in `close(...)`. The function holds no
keyed state, so the documented restriction on keyed state inside async functions
never arises.

`generatedAt` on the emitted `Recommendation` comes from the request, which
carries the Browsing Session's window end. Not from the moment the reply
arrived.

- [x] **Step 5: Wire it, then rewire the sink.**

```java
AsyncDataStream.orderedWait(priced, new AsyncRecommendationLookup(),
                            1000, TimeUnit.MILLISECONDS, 100)
```

The Kafka sink Phase 3 Task 8 built now consumes this operator's output instead
of `RecommendationDecider`'s. The sink itself, its `EXACTLY_ONCE` guarantee, and
its transactional id prefix are unchanged.

- [x] **Step 6: Verify end to end against the real topic.**

```bash
apps/gradlew -p apps :generator:run                                       # terminal 1
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"   # terminal 2
kcat -b localhost:30016 -t recommendation -C -o end \
     -X isolation.level=read_committed                                    # terminal 3
```

Expected: real `Recommendation` JSON, arriving in **10 second bursts** rather
than continuously. That burst pattern is the `EXACTLY_ONCE` sink committing on
each checkpoint, not a bug, and Phase 3 Task 8 already established it.

Without `isolation.level=read_committed` you see records from aborted
transactions too, which makes the next task's Drill unreadable.

---

## Task 7: End-to-end test of the assembled graph

**Status: done, 2026-08-30. Task 8 is unblocked.**

**The finding that cost two failed attempts.** A window fires when the WATERMARK
passes its end, not when its last element arrives. A fixture ending at `+47` with
a 5s bound only reaches watermark `+42`, so a window ending at `+53` fires at
`MAX_WATERMARK` alongside the CEP match, and their arrival order at the merge is a
race. The fix is a **watermark pusher**: one Click from another Shopper far in the
future, so the window under test closes during the stream. Any bounded test whose
assertion depends on operator ordering needs one.

**Files:**
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/PersonalizationJobTest.java`
- Modify: `PersonalizationJob.java`

**The problem.** The five tests written so far each prove one piece and none of
them see the wiring. The graph is now four operators deep with a union, a
re-key, a broadcast, and an async stage. Wiring errors live exactly there, and
no harness test can reach them. Phase 5 deploys this graph to Kubernetes, where
a wiring bug costs an image rebuild to diagnose.

**The refactor this task needs first.** `main(...)` currently builds the graph
and calls `env.execute(...)` in one method, so a test cannot assemble the graph
against bounded sources. Extract the graph-building into a method that takes its
sources and returns the `Recommendation` stream:

```java
static DataStream<Recommendation> buildGraph(
        DataStream<Click> clicks,
        DataStream<ProductChange> productChanges,
        DataStream<PromoRule> promoRules,
        PipelineConfig config,
        RecommendationClient client)
```

`main` then wires Kafka sources into it and adds the sink. The test wires
`env.fromData(...)` into it and collects. Passing the client in is what lets the
test use a fast deterministic one.

**Why this is a real seam and not test-only scaffolding.** Phase 7's blue/green
work runs the same graph in two namespaces, and Phase 6 varies parallelism
against it. A `main` that cannot be assembled without Kafka makes both harder.

- [x] **Step 1: Extract `buildGraph(...)` and confirm nothing changed.**

```bash
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected: identical behaviour to Task 6. A pure refactor is done when the output
is unchanged, not when it compiles.

- [x] **Step 2: Write the end-to-end test.**

Construct a fixture, by hand and small enough to reason about:

- One Shopper, one Browsing Session that closes.
- Two Clicks on `P1`, one VIEW then one ADD_TO_CART, no CHECKOUT, so CEP fires.
- One `ProductChange` on `P1` within 2 seconds of a Click, whose `price` is
  below its `previousPrice` and whose `stock` is non-zero, so the join matches
  and the discount applies.
- One `PromoRule` at 10 percent.

Assert exactly one `Recommendation` comes out, that `discountPercent` is `10.0`,
that `reason` is `"cart-abandoned"` because it outranks `"price-drop"`, and that
`generatedAt` equals the Browsing Session's window end rather than any wall-clock
value.

The `generatedAt` assertion is the one that would catch a regression Phase 3
Task 9's Drill also catches, but three phases earlier and in one second.

- [x] **Step 3: Run the whole suite.**

```bash
apps/gradlew -p apps :pipeline:test
```

Expected: **6 tests completed, 0 failed**. Read the count. A suite that runs
five tests silently is the failure mode Task 2 step 2 warned about.

---

## Task 8: Drill C: change a Promo Rule mid-run

**Status: done, 2026-08-30. All four claims confirmed, twice.** The
[runbook](../../runbooks/phase-4-promo-rule-drill.md) carries a **real transcript**,
unlike the Phase 0 and Drill B runbooks, which still have only their
predicted-behaviour versions.

**Files:**
- Create: `docs/runbooks/phase-4-promo-rule-drill.md`

**What the Drill proves, stated as the thing that would otherwise be unproven.**
Every test so far ran against bounded input in a single JVM. None of them proves
the property the phase's done-when actually names: that a rule change reaches
every subtask of a **running** job, with real Kafka in between, and no restart.

**Why a hand-injected rule and not the generator's own.** The generator emits a
random discount between 5 and 20 percent every 30 seconds, so you would be
watching a number change and inferring causation. Injecting a known value with
`kcat` makes the expected result exact before you run it, which is the standard
Phase 1 set for the external listener check.

- [x] **Step 1: Start the generator, the job, and a committed-read consumer.**

```bash
apps/gradlew -p apps :generator:run --args="--promo-rule-interval-seconds=3600"
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
kcat -b localhost:30016 -t recommendation -C -o end \
     -X isolation.level=read_committed
```

The one hour rule interval silences the generator's own rules for the duration,
so the only rule change in the window is the one you inject.

- [x] **Step 2: Record the baseline.**

Note the `discountPercent` on Recommendations reading `"price-drop"` or
`"cart-abandoned"`, and confirm those reading `"most-viewed-in-session"` sit at
`0.0`.

- [x] **Step 3: Inject a known rule.**

```bash
echo '{"ruleId":"drill-1","description":"drill","discountPercent":42.0}' \
  | kcat -b localhost:30016 -t promo-rule -P
```

`42.0` is outside the generator's 5 to 20 range on purpose. If you see it, it
came from you.

- [x] **Step 4: Confirm all four claims.**

1. Discounted Recommendations now carry `42.0`, within about 10 seconds, which
   is one checkpoint interval.
2. `"cart-abandoned"` records whose candidate had **no** price drop still read
   `0.0`. **This is the claim that separates broadcast state working from a rule
   applying to everything**, and it is the one worth being slow about.
   *Corrected 2026-08-30:* this step used to check `"most-viewed-in-session"`
   records, but since Task 4 those go to `UNMATCHED` and never reach the topic.
3. The job never restarted. Check its console for a restart line, and note that
   Recommendations kept flowing across the change with no gap.
4. `kubectl get kafkatopic -n kafka` still shows all four topics `Ready`.

- [x] **Step 5: Write the runbook, with the real transcript.**

Follow the shape of
[the Phase 0 drill runbook](../../runbooks/phase-0-control-plane-drill.md), one
rationale per command.

**Paste the actual command output into the "Observed result" section.**
`status.md` records that the Phase 0 runbook still carries only the
predicted-behavior version because raw output was never pasted back. Do not
repeat that here.

---

## Task 9: Documents

**Files:**
- Create: `docs/adr/0009-unmatched-click-moves-to-the-merge.md`
- Create: `docs/knowledge/phase-4-advanced-flink.md`
- Modify: `docs/adr/0003-interval-join-key-and-semantics.md`
- Modify: `docs/superpowers/specs/2026-08-16-domain-schemas-design.md` (status note, superseded by ADR 0008)
- Modify: `docs/superpowers/specs/2026-08-16-generator-event-production-design.md` (status note, the `"type"` field is gone)
- ~~Create: `docs/adr/0008-product-change-as-a-state-snapshot.md`~~ **done 2026-08-28**
- ~~Modify: `CONTEXT.md`~~ **done 2026-08-28**
- Modify: `docs/knowledge/flink-job-walkthrough.md`
- Modify: `docs/superpowers/specs/2026-07-25-flink-k8s-personalization-design.md`
- Modify: `docs/superpowers/plans/status.md`

**Why this is a task and not a footnote.** Four documents now say things the
running code contradicts. A design document that disagrees with the code is
worse than no document, because the next phase trusts it. Phase 5 reads ADR 0003
and the walkthrough.

- [x] **Step 1: Write ADR 0009.**

`intervalJoin` is an inner join, so `ProcessJoinFunction` is never invoked for a
non-matching Click and the side output cannot originate there. Record the
decision to move it to the merge, the grain change from per-Click to per
Browsing Session candidate, and the alternative that was rejected: buffering
every Click for the join interval, which re-implements the join's bookkeeping
and adds a third shuffle.

- [x] **Step 2: Add a status line to ADR 0003.**

Do not rewrite its diagram. Mark it superseded in part, pointing at ADR 0009,
the same way ADR 0003 itself handles the design spec's architecture diagram.

- [x] **Step 3: Update `CONTEXT.md`.**

Four changes:

- **Price Change**: note that the topic is `product-change` and the type is
  `ProductChange`, because it covers stock-level moves too, while the Flink job
  reads only the price variant.
- **Unmatched Click**: restate at its new grain, one per Browsing Session
  candidate Product, not one per Click.
- **Cart Abandonment**: new term. The CEP Signal raised when a Shopper views a
  Product, adds it to the cart, and does not check out inside 30 seconds.
  _Avoid_: abandoned basket, drop-off.
- **Signal**: confirm it still covers all three producers, since the union type
  now makes that literal in code.

- [x] **Step 4: Fix the walkthrough's step 9.**

The merge function does not call the service. It emits a
`RecommendationRequest`, and a separate `AsyncDataStream.orderedWait` operator
makes the call. Fix the step and the surrounding paragraph.

Also replace the CEP section's two-step pattern with the abandoned cart, and its
`Unmatched Click` paragraph with the new grain.

- [x] **Step 5: Fix the design spec's coverage map.**

The CEP row still reads "viewed a product repeatedly, viewed a competitor, went
idle". Replace it and add one line saying the Phase 4 design supersedes it, with
the reason: at the generator's real rates that pattern needs a 60 second
`within`, which spans ten session gaps.

- [x] **Step 6: Write the Phase 4 knowledge doc.**

One doc per phase, following `phase-3-core-pipeline.md`. Cover the five concepts
as they were actually built, not as they were planned. Include the two numbers
that were derived rather than chosen, the 30 second CEP `within` and the plateau
that makes it insensitive to tuning, and the candidate-Product narrowing that
keeps `"cart-abandoned"` from swamping the other reasons.

- [x] **Step 7: Update `status.md`.**

Mark Phase 4 done. Close the `ProductChange` warning Phase 3 surfaced, recording
which of the three resolutions was taken and why. Add anything Phase 5 needs and
nothing else records, in the same "surfaced for the next phase" style Phase 3
used.

- [x] **Step 8: Confirm nothing is stale.**

```bash
grep -rn "viewed a competitor\|unmatched: side output\|Price Change" \
     docs/ CONTEXT.md
```

Read each hit. Every remaining one should be either inside ADR 0003's
now-superseded section or inside ADR 0008's description of what it supersedes.
