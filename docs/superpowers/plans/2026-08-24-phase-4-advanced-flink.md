# Phase 4 advanced Flink implementation plan

**Goal:** Add the Product-keyed branch and the four remaining Flink concepts to
the Phase 3 job, still on `MiniCluster`, so a Promo Rule can change mid-run and
the `recommendation` topic changes without a restart.

**Architecture:** The raw watermarked clickstream forks three ways. Two
Shopper-keyed branches (session window, CEP) union into one `ShopperSignal`. One
Product-keyed branch runs an interval join against `PriceChange` and re-keys its
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
| 0 | Confirm how `flink-cep` is packaged | not started |
| 1 | `JsonCodec` reverse direction, and the `PriceChange` source | not started |
| 2 | Test infrastructure, the interval join, and `EnrichedClick` | not started |
| 3 | CEP: abandoned cart and `ShopperSignal` | not started |
| 4 | The `connect` merge and the `UNMATCHED` side output | not started |
| 5 | Broadcast Promo Rules | not started |
| 6 | Async I/O and the mocked recommendation service | not started |
| 7 | End-to-end test of the assembled graph | not started |
| 8 | Drill C: change a Promo Rule mid-run | not started |
| 9 | Documents: ADR amendment, glossary, knowledge doc, status | not started |

## Global constraints

Copied verbatim from the spec and inherited from Phase 3. Every task inherits
these.

- Flink **2.2**. `org.apache.flink.streaming.api.windowing.time.Time` does not
  exist; windowing and CEP take `java.time.Duration`.
- `:domain` declares **no dependencies at all**. Not Kafka, not Flink, not
  JUnit.
- `pipeline.generic-types: false`. Any Kryo fallback must fail loudly.
- **No sealed interface and no abstract supertype may enter the job graph.**
  Every type crossing an operator boundary is a flat record or an enum.
- **Nothing in the output derives from wall-clock time.** `generatedAt` is the
  Browsing Session's window end. Every timer is an event-time timer. Every mock
  reply is a pure function of its request.
- Watermarks are assigned **once, on the raw stream, before the fork**.
- Watermark bound **5s**, session gap **6s**, join interval **-2s to +2s**, CEP
  `within` **30s**, checkpoint interval **10s**, cooldown **60s** of event time.
- Async I/O uses `orderedWait`, never `unorderedWait`.
- Delivery guarantee `EXACTLY_ONCE`, with a **stable** transactional id prefix.
- Any consumer verifying output sets `isolation.level=read_committed`.
- Every test runs at **parallelism 1** over **bounded** input.
- Ubiquitous language from [CONTEXT.md](../../../CONTEXT.md): Shopper, Click,
  Browsing Session, Product, Price Change, Promo Rule, Recommendation, Signal,
  Late Click, Unmatched Click, Drill.

---

## Task 0: Confirm how `flink-cep` is packaged

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

- [ ] **Step 1: Look in the official image.**

```bash
docker run --rm flink:2.2.0 sh -c 'ls lib opt | grep -i cep'
```

If the file lands under `lib/`, `flink-cep` is provided and the scope is
`compileOnly` plus `runtimeOnly`, matching `flink-streaming-java`. If it lands
under `opt/` or nowhere, the job jar owns it and the scope is `implementation`,
matching `flink-connector-kafka`.

- [ ] **Step 2: Cross-check against the documentation.**

Read the project-configuration page for 2.2 and find which modules it lists as
already provided:

<https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/dev/configuration/overview.md>

Two independent sources agreeing is the standard this project has been using
since Phase 0's kubeadm API-version surprise. If they disagree, the image wins,
because the image is what Phase 5 actually runs.

- [ ] **Step 3: Record the answer.**

Edit the Dependencies table in
[the spec](../specs/2026-08-24-advanced-flink-design.md), replace the word
`open` with the real scope, and delete the paragraph that says the scope is
open. Add one line saying which of the two sources you checked.

**Gate:** do not start Task 1 until the table says a real scope. A wrong scope
found in Phase 5 means rebuilding an image and re-running an HA Drill.

---

## Task 1: `JsonCodec` reverse direction, and the `PriceChange` source

**Files:**
- Modify: `apps/domain/src/main/java/lab/personalization/domain/JsonCodec.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/PriceChangeDeserializationSchema.java`
- Modify: `apps/pipeline/src/main/java/lab/personalization/pipeline/PipelineConfig.java`
- Modify: `apps/pipeline/src/main/java/lab/personalization/pipeline/ClickDeserializationSchema.java`
- Modify: `apps/pipeline/src/main/java/lab/personalization/pipeline/PersonalizationJob.java`

**Interfaces produced:**
```java
public static Click clickFromJson(byte[] bytes)
public static ProductChange productChangeFromJson(byte[] bytes)
public class PriceChangeDeserializationSchema implements DeserializationSchema<PriceChange>
```

**The problem, before the mechanism.** `JsonCodec` today can write all four
records but can read exactly one: `fromJson(byte[]) -> Click`. Phase 3 only ever
needed that direction for one topic. Phase 4 reads two more topics, so the
reverse direction has to grow, and Java cannot overload on return type alone.
Three readers therefore need three names.

**The rename, and why it happens now.** `fromJson` is fine while there is one
reader and becomes actively misleading the moment there are three. Rename it to
`clickFromJson` in this task. It has exactly two call sites, `JsonCodec` itself
and `ClickDeserializationSchema`, so it is a two-minute change now and a
permanent wart if deferred.

**The concept this task exists to prove.** `ProductChange` is a sealed
interface. A sealed interface is not a POJO by Flink's rules, and
`pipeline.generic-types: false` blocks the Kryo fallback, so a
`DataStream<ProductChange>` throws while the job graph is being built, before a
single record is read. The spec's answer is to never let the sum type into the
graph: read the `type` discriminator that `JsonCodec` already writes, and
collect only `PriceChange`, which is a plain record and a valid POJO.

**The failure mode to watch for.** If you write
`DeserializationSchema<ProductChange>` and filter afterwards with `.filter(...)`
plus a cast, it compiles and then throws at `env.execute()` with a message about
a generic type. The filter is too late. The type has to be narrowed **inside**
the deserializer, where the stream's element type is decided.

- [ ] **Step 1: Add a numeric field pattern to `JsonCodec`.**

The existing `stringField(...)` helper only matches quoted values. `newPrice`
and `discountPercent` are written unquoted, so they need their own pattern.

```java
private static Pattern numberField(String name) {
    return Pattern.compile("\"" + name + "\"\\s*:\\s*(-?[0-9.eE+]+)");
}
```

Same tolerance argument as the existing regex approach: field order and
incidental whitespace must not matter, because Drill C injects a hand-typed
Promo Rule with `kcat`.

- [ ] **Step 2: Rename `fromJson` to `clickFromJson`, and write `productChangeFromJson`.**

`productChangeFromJson` switches on the `type` field the generator writes,
`"PRICE"` or `"STOCK"`, and returns the matching variant. Make the switch
**exhaustive with a default that throws**, naming the unknown value. A silent
`null` here surfaces three operators downstream as a `NullPointerException` with
no clue where it came from.

- [ ] **Step 3: Write `PriceChangeDeserializationSchema`.**

Stay on `DeserializationSchema`, matching `ClickDeserializationSchema`, so
`setValueOnlyDeserializer(...)` on the `KafkaSource` builder still works.

```java
@Override
public void deserialize(byte[] message, Collector<PriceChange> out) {
    // collect only when the parsed value is a PriceChange
}

@Override
public PriceChange deserialize(byte[] message) {
    throw new UnsupportedOperationException();
}

@Override
public TypeInformation<PriceChange> getProducedType() {
    return TypeInformation.of(PriceChange.class);
}
```

The `Collector` overload is the one that can emit **zero** records, which is how
a `StockChange` gets dropped. The single-value form is unreachable once the
`Collector` form is overridden, and throwing is more honest than returning
`null`.

- [ ] **Step 4: Add `--product-change-topic` to `PipelineConfig`, default `product-change`.**

Follow the existing `inputTopic` naming discussion in the Phase 3 plan: a bare
`topic` would be ambiguous now that there are three of them.

- [ ] **Step 5: Build the second `KafkaSource` and print it.**

A second source, its own consumer group suffix, and the same watermark strategy
shape as the clickstream. `PriceChange` needs watermarks too, because the
interval join in Task 2 is an event-time operator and will not fire without
them on **both** sides.

```java
env.fromSource(priceChanges, priceChangeWatermarks, "product-change")
   .print("PRICE");
```

- [ ] **Step 6: Verify against the real topic.**

```bash
apps/gradlew -p apps :generator:run                                       # terminal 1
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"   # terminal 2
```

Expected: `PRICE` lines at roughly **one every two seconds**. The generator's
`product-change-rate` default is 1.0 per second and about half of what it emits
is a `StockChange`, which this deserializer drops.

If you see roughly one per second, the `StockChange` branch is being collected
too. If you see none, check the topic name before anything else.

---

## Task 2: Test infrastructure, the interval join, and `EnrichedClick`

**Files:**
- Create: `apps/domain/src/main/java/lab/personalization/domain/EnrichedClick.java`
- Create: `apps/pipeline/src/main/java/lab/personalization/pipeline/PriceDropJoiner.java`
- Create: `apps/pipeline/src/test/java/lab/personalization/pipeline/IntervalJoinTest.java`
- Modify: `apps/pipeline/build.gradle`
- Modify: `PersonalizationJob.java`

**Interfaces consumed:** `PriceChange` source from Task 1.

**Interfaces produced:**
```java
public record EnrichedClick(String shopperId, String productId, Instant clickTime,
                            double newPrice, Instant priceChangeTime)
class PriceDropJoiner extends ProcessJoinFunction<Click, PriceChange, EnrichedClick>
```

**The concept.** An interval join asks, for each element on the left, which
elements on the right have an event time inside a window **relative to that
element**. It is not a windowed join: there is no shared window boundary, every
Click carries its own interval. Both sides must be keyed by the same key, and
`PriceChange` has no `shopperId`, so this branch forks from the raw stream and
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

- [ ] **Step 1: Add the test configuration to `apps/pipeline/build.gradle`.**

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

- [ ] **Step 2: Prove the test task actually runs.**

Write a throwaway test asserting `1 + 1 == 2` and run:

```bash
apps/gradlew -p apps :pipeline:test
```

Expected: `1 test completed`. If it says `NO-SOURCE` or reports success with no
test count, `useJUnitPlatform()` is missing or the file is in the wrong source
root. Delete the throwaway test once it has told you what you needed.

- [ ] **Step 3: Write `EnrichedClick` in `:domain`.**

An enriched Click is a Signal by the `CONTEXT.md` definition, so it is domain
vocabulary. It carries both event times, not just the Click's, because Phase 8's
dashboard needs the gap between them and recomputing it downstream is
impossible once one of them is dropped.

- [ ] **Step 4: Write the failing test.**

```java
@Test
void clickWithinTwoSecondsOfPriceChangeMatches() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant t = Instant.parse("2026-08-24T10:00:00Z");
    // Click on P1 at t, PriceChange on P1 at t+1s  -> expect one EnrichedClick
    // Click on P2 at t, PriceChange on P2 at t+4s  -> expect nothing
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

- [ ] **Step 5: Run it and watch it fail.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*IntervalJoinTest*'
```

Expected: a compile failure naming `EnrichedClick` or `PriceDropJoiner`. Read
the message. If instead it hangs, you have an unbounded source.

- [ ] **Step 6: Write `PriceDropJoiner` and wire the branch into the job.**

```java
clicks.keyBy(Click::productId)
      .intervalJoin(priceChanges.keyBy(PriceChange::productId))
      .between(Duration.ofSeconds(-2), Duration.ofSeconds(2))
      .process(new PriceDropJoiner())
```

`clicks` here is the **raw watermarked stream**, the same variable the
Shopper-keyed branch forks from, not anything downstream of `keyBy(shopperId)`.

- [ ] **Step 7: Run the test until it passes, then check the live rate.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*IntervalJoinTest*'
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected from the live run: `EnrichedClick` output at roughly **one third** of
the Click rate. The spec's arithmetic gives 33 percent, from
`1 - e^(-0.4)` where 0.4 is the expected number of Price Changes on one Product
inside a 4 second window.

Zero output with a passing test means watermarks or idleness on the
`product-change` side. Output at the full Click rate means the bounds are wrong.

---

## Task 3: CEP: abandoned cart and `ShopperSignal`

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
public record ShopperSignal(String shopperId, SignalKind kind, Instant eventTime,
                            String productId, int clickCount)
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

- [ ] **Step 1: Add `flink-cep` at the scope Task 0 established.**

```groovy
// scope decided by Task 0, not guessed here
"org.apache.flink:flink-cep:${flinkVersion}"
```

- [ ] **Step 2: Write `SignalKind` and `ShopperSignal` in `:domain`.**

`clickCount` is meaningful only for `BROWSING_SESSION` and is `0` otherwise.
That unused field is the price of the union, and it is forced: a sealed
interface over the two Signal kinds would fail exactly the way `ProductChange`
does.

- [ ] **Step 3: Write the failing test, three cases.**

```java
@Test void viewThenCartWithNoCheckoutMatches()
@Test void viewThenCartThenCheckoutDoesNotMatch()
@Test void viewWithNoCartLandsInTheTimedOutSideOutput()
```

Bounded input, parallelism 1, event times spaced a few seconds apart inside the
30 second window. The third case is what proves `TimedOutPartialMatchHandler` is
wired, and it is the one most likely to be skipped.

- [ ] **Step 4: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*CartAbandonmentCepTest*'
```

- [ ] **Step 5: Write the pattern and the matcher.**

`processMatch(...)` builds a `ShopperSignal` with `kind = CART_ABANDONMENT`,
`productId` from the `"view"` step, and `clickCount = 0`.
`processTimedOutMatch(...)` calls `ctx.output(CEP_TIMED_OUT, ...)`.

`eventTime` on the Signal comes from the matched Clicks, never from
`Instant.now()`. The global constraint about wall-clock time applies here as
much as it does to `generatedAt`.

- [ ] **Step 6: Wire the branch and check the live rate.**

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
| `processElement2(EnrichedClick)` | put `productId` into `priceDropMatches` |
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

- [ ] **Step 1: Write `RecommendationRequest` in `:domain`.**

`discountPercent` is `0.0` here and stays that way until Task 5.
`candidateProductId` is what the job proposes; `Recommendation.productId` in
Task 6 is what the service answered. They are allowed to differ, and that
difference is the entire reason async I/O exists in this pipeline.

- [ ] **Step 2: Write the failing harness test.**

```java
KeyedTwoInputStreamOperatorTestHarness<String, ShopperSignal, EnrichedClick, RecommendationRequest> harness =
        ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new SignalMerger(), ShopperSignal::shopperId, EnrichedClick::shopperId, Types.STRING);
```

**The exact artifact holding `ProcessFunctionTestHarnesses` is not verified.**
`flink-test-utils` may bring it transitively. If the import does not resolve,
check what is actually on the test classpath before guessing:

```bash
apps/gradlew -p apps :pipeline:dependencies --configuration testCompileClasspath | grep -i flink
```

The candidates are `flink-test-utils`, or a `tests` classifier on
`flink-runtime` or `flink-streaming-java`. Resolve it, then record the answer in
the spec's Dependencies table so it is not rediscovered in Phase 6.

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

- [ ] **Step 3: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*MergeFunctionTest*'
```

- [ ] **Step 4: Rewrite `RecommendationDecider` as `SignalMerger`.**

Phase 3's body becomes `processElement1`, exactly as the
[core pipeline design](../specs/2026-08-23-core-pipeline-design.md) predicted.
The cooldown state and its event-time timer carry over unchanged. Only the
output type changes, from `Recommendation` to `RecommendationRequest`.

`reason` precedence, most specific first: `"cart-abandoned"`, then
`"price-drop"`, then Phase 3's `"most-viewed-in-session"`.

- [ ] **Step 5: Add the safety-net timer.**

Clearing both maps on session close bounds the state for any Shopper who keeps
shopping. A Shopper whose session never closes would pin state forever. Register
an event-time timer when a map first gains an entry, and clear on
`onTimer(...)`. Event-time, not processing-time, for the reason the global
constraints give.

- [ ] **Step 6: Union, re-key, connect, and verify live.**

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

- [ ] **Step 1: Write `promoRuleFromJson` and `PromoRuleDeserializationSchema`.**

`discountPercent` is unquoted on the wire, so it needs the `numberField` pattern
from Task 1, not `stringField`.

- [ ] **Step 2: Add `--promo-rule-topic` to `PipelineConfig`, default `promo-rule`.**

- [ ] **Step 3: Write the failing harness test.**

```java
BroadcastOperatorTestHarness<RecommendationRequest, PromoRule, RecommendationRequest> harness =
        ProcessFunctionTestHarnesses.forBroadcastProcessFunction(
                new PromoRuleApplier(), PromoRuleApplier.PROMO_RULES);
```

Four behaviours:

1. With no rule broadcast yet, a matched request still emits, at `0.0`. A
   pipeline that swallows records until the first rule arrives would lose the
   first 30 seconds of every run.
2. After `promo-1` at 10 percent, a matched request carries `10.0`.
3. After `promo-2` at 15 percent, the next matched request carries `15.0`, and
   the state still holds exactly one entry.
4. An **unmatched** request carries `0.0` regardless of the active rule.

Case 4 is the one that proves the condition is structural rather than universal.

- [ ] **Step 4: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*PromoRuleBroadcastTest*'
```

- [ ] **Step 5: Write `PromoRuleApplier`.**

Records are immutable, so `processElement` emits a **copy** of the request with
the discount filled in, not a mutation.

- [ ] **Step 6: Broadcast the source and connect it.**

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

- [ ] **Step 7: Verify live.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*PromoRuleBroadcastTest*'
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected: matched requests carry a non-zero `discountPercent` in the 5 to 20
range, and the number changes about every 30 seconds. Unmatched requests stay at
`0.0` throughout.

---

## Task 6: Async I/O and the mocked recommendation service

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

- [ ] **Step 1: Write the `RecommendationClient` interface and the mock.**

The interface exists for one concrete reason: a test injects a slow
implementation to reach the `timeout(...)` path, which is otherwise unreachable.
That is the only justification needed, and it is enough.

`DeterministicMockClient` computes its suggestion from the request alone and
simulates latency on a dedicated executor. No `Random`, no `Instant.now()`, no
`Thread.sleep` on the caller's thread.

- [ ] **Step 2: Write the failing test, two cases.**

```java
@Test void repliesArriveInInputOrder()
@Test void aSlowClientHitsTheTimeoutPathAndStillEmits()
```

The second constructs `AsyncRecommendationLookup` with a client that never
completes inside the timeout, and asserts a `Recommendation` still comes out
rather than the job failing.

- [ ] **Step 3: Run it and read the failure.**

```bash
apps/gradlew -p apps :pipeline:test --tests '*AsyncRecommendationTest*'
```

- [ ] **Step 4: Write `AsyncRecommendationLookup`.**

Open the client in `open(...)`, close it in `close(...)`. The function holds no
keyed state, so the documented restriction on keyed state inside async functions
never arises.

`generatedAt` on the emitted `Recommendation` comes from the request, which
carries the Browsing Session's window end. Not from the moment the reply
arrived.

- [ ] **Step 5: Wire it, then rewire the sink.**

```java
AsyncDataStream.orderedWait(priced, new AsyncRecommendationLookup(),
                            1000, TimeUnit.MILLISECONDS, 100)
```

The Kafka sink Phase 3 Task 8 built now consumes this operator's output instead
of `RecommendationDecider`'s. The sink itself, its `EXACTLY_ONCE` guarantee, and
its transactional id prefix are unchanged.

- [ ] **Step 6: Verify end to end against the real topic.**

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
        DataStream<PriceChange> priceChanges,
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

- [ ] **Step 1: Extract `buildGraph(...)` and confirm nothing changed.**

```bash
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

Expected: identical behaviour to Task 6. A pure refactor is done when the output
is unchanged, not when it compiles.

- [ ] **Step 2: Write the end-to-end test.**

Construct a fixture, by hand and small enough to reason about:

- One Shopper, one Browsing Session that closes.
- Two Clicks on `P1`, one VIEW then one ADD_TO_CART, no CHECKOUT, so CEP fires.
- One `PriceChange` on `P1` within 2 seconds of a Click, so the join matches.
- One `PromoRule` at 10 percent.

Assert exactly one `Recommendation` comes out, that `discountPercent` is `10.0`,
that `reason` is `"cart-abandoned"` because it outranks `"price-drop"`, and that
`generatedAt` equals the Browsing Session's window end rather than any wall-clock
value.

The `generatedAt` assertion is the one that would catch a regression Phase 3
Task 9's Drill also catches, but three phases earlier and in one second.

- [ ] **Step 3: Run the whole suite.**

```bash
apps/gradlew -p apps :pipeline:test
```

Expected: **6 tests completed, 0 failed**. Read the count. A suite that runs
five tests silently is the failure mode Task 2 step 2 warned about.

---

## Task 8: Drill C: change a Promo Rule mid-run

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

- [ ] **Step 1: Start the generator, the job, and a committed-read consumer.**

```bash
apps/gradlew -p apps :generator:run --args="--promo-rule-interval-seconds=3600"
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
kcat -b localhost:30016 -t recommendation -C -o end \
     -X isolation.level=read_committed
```

The one hour rule interval silences the generator's own rules for the duration,
so the only rule change in the window is the one you inject.

- [ ] **Step 2: Record the baseline.**

Note the `discountPercent` on Recommendations reading `"price-drop"` or
`"cart-abandoned"`, and confirm those reading `"most-viewed-in-session"` sit at
`0.0`.

- [ ] **Step 3: Inject a known rule.**

```bash
echo '{"ruleId":"drill-1","description":"drill","discountPercent":42.0}' \
  | kcat -b localhost:30016 -t promo-rule -P
```

`42.0` is outside the generator's 5 to 20 range on purpose. If you see it, it
came from you.

- [ ] **Step 4: Confirm all four claims.**

1. Discounted Recommendations now carry `42.0`, within about 10 seconds, which
   is one checkpoint interval.
2. Recommendations reading `"most-viewed-in-session"` still sit at `0.0`. **This
   is the claim that separates broadcast state working from a rule applying to
   everything**, and it is the one worth being slow about.
3. The job never restarted. Check its console for a restart line, and note that
   Recommendations kept flowing across the change with no gap.
4. `kubectl get kafkatopic -n kafka` still shows all four topics `Ready`.

- [ ] **Step 5: Write the runbook, with the real transcript.**

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
- Create: `docs/adr/0007-unmatched-click-moves-to-the-merge.md`
- Create: `docs/knowledge/phase-4-advanced-flink.md`
- Modify: `docs/adr/0003-interval-join-key-and-semantics.md`
- Modify: `CONTEXT.md`
- Modify: `docs/knowledge/flink-job-walkthrough.md`
- Modify: `docs/superpowers/specs/2026-07-25-flink-k8s-personalization-design.md`
- Modify: `docs/superpowers/plans/status.md`

**Why this is a task and not a footnote.** Four documents now say things the
running code contradicts. A design document that disagrees with the code is
worse than no document, because the next phase trusts it. Phase 5 reads ADR 0003
and the walkthrough.

- [ ] **Step 1: Write ADR 0007.**

`intervalJoin` is an inner join, so `ProcessJoinFunction` is never invoked for a
non-matching Click and the side output cannot originate there. Record the
decision to move it to the merge, the grain change from per-Click to per
Browsing Session candidate, and the alternative that was rejected: buffering
every Click for the join interval, which re-implements the join's bookkeeping
and adds a third shuffle.

- [ ] **Step 2: Add a status line to ADR 0003.**

Do not rewrite its diagram. Mark it superseded in part, pointing at ADR 0007,
the same way ADR 0003 itself handles the design spec's architecture diagram.

- [ ] **Step 3: Update `CONTEXT.md`.**

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

- [ ] **Step 4: Fix the walkthrough's step 9.**

The merge function does not call the service. It emits a
`RecommendationRequest`, and a separate `AsyncDataStream.orderedWait` operator
makes the call. Fix the step and the surrounding paragraph.

Also replace the CEP section's two-step pattern with the abandoned cart, and its
`Unmatched Click` paragraph with the new grain.

- [ ] **Step 5: Fix the design spec's coverage map.**

The CEP row still reads "viewed a product repeatedly, viewed a competitor, went
idle". Replace it and add one line saying the Phase 4 design supersedes it, with
the reason: at the generator's real rates that pattern needs a 60 second
`within`, which spans ten session gaps.

- [ ] **Step 6: Write the Phase 4 knowledge doc.**

One doc per phase, following `phase-3-core-pipeline.md`. Cover the five concepts
as they were actually built, not as they were planned. Include the two numbers
that were derived rather than chosen, the 30 second CEP `within` and the plateau
that makes it insensitive to tuning, and the candidate-Product narrowing that
keeps `"cart-abandoned"` from swamping the other reasons.

- [ ] **Step 7: Update `status.md`.**

Mark Phase 4 done. Close the `ProductChange` warning Phase 3 surfaced, recording
which of the three resolutions was taken and why. Add anything Phase 5 needs and
nothing else records, in the same "surfaced for the next phase" style Phase 3
used.

- [ ] **Step 8: Confirm nothing is stale.**

```bash
grep -rn "viewed a competitor\|unmatched: side output\|Price Change" \
     docs/ CONTEXT.md
```

Read each hit. Every remaining one should be either inside ADR 0003's
now-superseded section or inside ADR 0007's description of what it supersedes.
