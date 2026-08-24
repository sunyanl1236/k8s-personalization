# Phase 3 Drill B: Late Click side output

Date: 2026-08-24
Runtime: Flink 2.2 on `MiniCluster`, `apps/pipeline`, default parallelism 16
Kafka: Strimzi `clickstream`, 3 partitions, external listener `localhost:30016`

This is the Drill that closes out Task 5. Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

What is being broken here is the job's assumption that Clicks arrive in
approximately event-time order. A Click far behind the watermark is injected by
hand, and the Drill observes where it goes.

## What's actually being exercised

Flink's default behaviour for a Click that arrives after its Browsing Session
already closed is to **drop it silently**. No log line, no visible counter.
That is how a pipeline loses data without anyone learning that it did.

Task 5 replaces that silence with an `OutputTag` side output. This Drill proves
the replacement works, and it has to prove two separate things:

1. **The side output fires.** The injected Click appears on the `LATE` stream.
2. **No window also took it.** The injected Click never appears in a
   `SessionSignal`.

Only both together are proof. Point 1 alone is satisfied by a Click that was
processed normally *and* logged, which is not what is being claimed. The
absence in point 2 is the real evidence.

The mechanism behind the numbers is in
[the Phase 3 knowledge doc](../knowledge/phase-3-core-pipeline.md), section
"Why a Click behind the watermark is not automatically a Late Click". The short
version: with a 5s watermark bound and a 6s session gap, a Click is late only
when its `eventTime` is more than **11 seconds** behind the newest `eventTime`
seen. Not 5 seconds.

## Procedure

**1. `apps/gradlew -p apps :generator:run`** (terminal 1)

Expected: the synthetic producer starts writing Clicks to `clickstream` at its
configured rate.

Rationale: the Drill needs a live watermark. Without a stream of fresh Clicks,
`maxSeen` never advances, the watermark stays put, and nothing is ever late.
The injected Click is judged relative to this traffic, not in isolation.

**2. `apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false" 2>&1 | tee /tmp/drill-b.log`** (terminal 2)

Expected: after roughly 50 seconds of quiet, `SessionSignal` lines begin
appearing at roughly one every 4 seconds.

Rationale for `--start-from-earliest=false`: the source starts at `latest`, so
it reads only records written after it connects. With `earliest` the job would
replay the entire backlog, the injected Click would be buried in millions of
lines, and the run would take minutes to reach live traffic.

Rationale for `tee`: this runbook requires a real transcript. Scrollback is not
a transcript.

**3. Gate: confirm zero `LATE` lines before injecting anything.**

Expected: none.

Rationale: the generator's own skew is designed to stay well inside the 11
second lateness line. A `LATE` line appearing on its own would mean the real
skew exceeds Phase 2's design, and the Drill would prove nothing until that is
understood. This gate must pass before step 4.

**4. Inject the Late Click** (terminal 3)

```bash
echo '{"shopperId":"shopper-99","productId":"P1","eventTime":"'"$(date -u -d '60 seconds ago' +%Y-%m-%dT%H:%M:%SZ)"'","actionType":"VIEW"}' \
  | kcat -P -b localhost:30016 -t clickstream
```

Expected: `kcat` returns silently.

Rationale, per field:

- **`shopper-99`** is not in `Catalog.SHOPPER_IDS`
  (`apps/generator/src/main/java/lab/personalization/generator/factory/Catalog.java`),
  which holds `shopper-1` through `shopper-10`. The generator can never produce
  this id, so anything carrying it came from this injection and nothing else.
  It also guarantees no open Browsing Session exists for this Shopper, so no
  window merge can rescue the Click.
- **`60 seconds ago`** is far past the 11 second lateness line. A value closer
  to the line would make the verdict depend on timing.
- **`VIEW`** must match an `ActionType` constant exactly. `JsonCodec.fromJson`
  calls `ActionType.valueOf`, which throws on anything else.
- **No message key**, so `kcat` picks a partition. This is safe: watermarks are
  tracked per partition and are monotonic, so an old timestamp cannot pull any
  watermark backwards.

Injection must happen *after* step 2 is live. With `latest` offsets, a record
written before the source connects is never read, and that looks identical to a
broken side output.

**5. Wait at least 30 seconds, then `grep 'shopper-99' /tmp/drill-b.log`**

Expected: exactly one line, and it is the `LATE` line.

Rationale for the wait: a session window only fires once the watermark passes
its end. An absence checked too early proves nothing, because the window may
simply not have fired yet.

## What would have falsified Task 5

- A `LATE` line **and** a `SessionSignal` both carrying `shopper-99`. That would
  mean the Click reached a window as well as the side output, so
  `.sideOutputLateData(...)` is not doing what is claimed.
- No `LATE` line at all, with the record confirmed present in the topic. That
  is the classic symptom of `.sideOutputLateData(...)` attached in the wrong
  place, where it tags nothing and Flink falls back to dropping silently.
- `LATE` lines appearing before any injection. That would move the failure out
  of Task 5 and into Phase 2's skew configuration.

## Observed result

**Side output fired.** One `LATE` line, carrying the injected Shopper:

```
LATE:12> Click[shopperId=shopper-99, productId=P1, eventTime=2026-08-24T14:51:36Z, actionType=VI...
```

Reading the line: `LATE` is the prefix passed to `.print("LATE")`, which is what
separates this stream from the unprefixed `SessionSignal` stream sharing the
same stdout. `:12>` is the print sink's subtask index, 12 of the 16 default
parallel instances, which confirms the record was shuffled to a real parallel
operator rather than short circuited.

<!-- TODO: paste the two transcripts below. Both were confirmed live on
     2026-08-24 but the raw output was not captured into this file, which is
     exactly the gap phase-0-control-plane-drill.md still carries. -->

**Baseline before injection** (step 2 and step 3): _paste a few `SessionSignal`
lines here, and state that no `LATE` line appeared._

**No window also took it** (step 5): _paste the `grep 'shopper-99'` output here.
It must show exactly one line, the `LATE` line above._

## Notes

`allowedLateness` is left at its default of zero throughout. Setting it would
shift both of Flink's lateness checks by that amount and move the 11 second
figure with it, invalidating the `60 seconds ago` choice in step 4 only if the
allowance grew past roughly 49 seconds.
