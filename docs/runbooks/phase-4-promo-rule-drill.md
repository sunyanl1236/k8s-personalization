# Drill C: change a Promo Rule while the job runs

**What this proves.** That a Promo Rule change reaches every subtask of a running
job, through real Kafka, and the `recommendation` topic changes **without a
restart**. That is the property Phase 4's done-when names, and no unit test can
show it: every test runs bounded input in a single JVM.

**Prerequisites.** The `kind` cluster up, all four `KafkaTopic`s `Ready`, and
`scripts/session-start.sh` already run if the host restarted.

---

## Step 1: start the generator with its own rules silenced

```bash
apps/gradlew -p apps :generator:run --args="--promo-rule-interval-seconds=3600"
```

**Why one hour.** The generator normally emits a fresh rule every 30 seconds with
a random 5 to 20 percent discount. Left alone you would be watching a number
change and inferring causation. Silencing it means the only rule change in the
window is the one you inject, so the expected result is exact before you run
anything.

It still emits one rule immediately at startup, but **the job will probably not see
it**: with `--start-from-earliest=false` the broadcast source starts at *latest* and
the rule is usually published before it subscribes. So the baseline is normally
`discountPercent` of `0.0` everywhere, which is a cleaner baseline anyway.

## Step 2: start the job, in a second terminal

```bash
source scripts/minio-env.sh
apps/gradlew -p apps :pipeline:run --args="--start-from-earliest=false"
```

**`source`, not execute.** A child process cannot set its parent's environment,
and the script refuses to run if executed. `MINIO_ACCESS_KEY` and
`MINIO_SECRET_KEY` must be in the same shell that runs Gradle.

**`--start-from-earliest=false`.** Replaying the backlog would race through hours
of history at disk speed, closing every historical Browsing Session at once. The
rate and the shape of the output would both be meaningless.

## Step 3: note the current end of the topic

```bash
kcat -b localhost:30016 -t recommendation -C -o end -e -X isolation.level=read_committed
```

**This matters more than it looks.** The topic carries history: records written by
Phase 3's `RecommendationDecider`, all reading `most-viewed-in-session` with
`discountPercent` of `0.0`, are still there. Reading the tail without bounding it
mixes them with this run's output and makes the drill unreadable.

Bound every later read by **`generatedAt`**, or by the offsets printed here.

## Step 4: watch the topic, in a third terminal

```bash
kcat -b localhost:30016 -t recommendation -C -o end -X isolation.level=read_committed
```

**`isolation.level=read_committed` is required**, not optional. The sink is
`EXACTLY_ONCE`, so records become visible only when a checkpoint commits the
transaction. Two consequences that look like faults and are not:

- the topic is **silent for the first 10 seconds**, then emits a burst
- without `read_committed` you would also see records from aborted transactions,
  including the orphans a restart is meant to fence away

Expect nothing at all for roughly the first minute: a Browsing Session must close
before anything is emitted.

## Step 5: record the baseline

From the records now arriving, note the `discountPercent` on records reading
`"price-drop"` or `"cart-abandoned"`. Expect **`0.0` on all of them**, for the
reason in step 1: the job has not seen any rule yet.

If you do see a non-zero baseline, the job caught the generator's startup rule.
Either is fine; write down which, because it is what the injection has to change.

## Step 6: inject a rule you can recognise

```bash
echo '{"ruleId":"drill-2","description":"drill","discountPercent":37.0}' \
  | kcat -b localhost:30016 -t promo-rule -P
```

**The value is outside the generator's 5 to 20 range on purpose.** If you see it,
it came from you and from nowhere else. Use a fresh number each run: the topic
keeps every earlier drill's rule.

## Step 7: confirm four claims

| # | Claim | How to see it |
|---|---|---|
| 1 | discounted Recommendations now read the injected value | allow about a minute, not one checkpoint interval, see Timing below |
| 2 | `"cart-abandoned"` records **without** a price drop still read `0.0` | the structural condition, see below |
| 3 | the job never restarted | no restart line in the job's console, and Recommendations kept flowing across the change with no gap |
| 4 | the topics are still healthy | `kubectl get kafkatopic -n kafka` shows all four `Ready` |

**Claim 2 is the one worth being slow about.** It is what separates *broadcast
state working* from *a rule being applied to everything*. The discount is earned
only by a candidate Product that had a price drop inside the join interval; a
Shopper who abandoned a cart on a Product whose price never moved must still read
`0.0` after the injection.

If every record reads the injected value, the condition has become universal and
the whole Product-keyed branch is decorative.

*Corrected 2026-08-30:* this check used to look for `"most-viewed-in-session"`
records at `0.0`. Since Task 4 those go to the `UNMATCHED` side output and never
reach the topic.

## Step 8: stop

```bash
# Ctrl+C in each terminal, or:
pkill -f 'lab.personalization.pipeline' ; pkill -f 'lab.personalization.generator'
```

---

## Observed result

Run 2026-08-30, `kind` cluster, `MiniCluster` job. **All four claims confirmed.**

### Baseline, before injection

Every record read `0.0`, and that is itself a finding:

```
{"shopperId":"shopper-2","productId":"P2","discountPercent":0.0,"reason":"price-drop","generatedAt":"2026-08-30T03:37:55.471Z"}
{"shopperId":"shopper-7","productId":"P3","discountPercent":0.0,"reason":"price-drop","generatedAt":"2026-08-30T03:38:17.671Z"}
{"shopperId":"shopper-8","productId":"P1","discountPercent":0.0,"reason":"price-drop","generatedAt":"2026-08-30T03:37:49.271Z"}
```

**A freshly started job knows no rule at all.** `--start-from-earliest=false` makes
the broadcast source start at *latest*, so it never reads a rule published before
it subscribed. The newest record on `promo-rule` was already `drill-1` at 42.0 from
an earlier run, and the job had not seen it. `PromoRuleApplier`'s `rule != null`
guard is what keeps this at `0.0` instead of an NPE, and without that guard the job
would crash in the first 30 seconds of every run.

### Injection

```
$ echo '{"ruleId":"drill-2","description":"drill","discountPercent":37.0}' \
    | kcat -b localhost:30016 -t promo-rule -P
injected 03:38:56Z
```

### Claims 1 and 2, together

Every record with `generatedAt` after the injection:

```
{"shopperId":"shopper-4","productId":"P5", "discountPercent":0.0, "reason":"cart-abandoned","generatedAt":"2026-08-30T03:39:31.270Z"}
{"shopperId":"shopper-5","productId":"P5", "discountPercent":37.0,"reason":"price-drop",    "generatedAt":"2026-08-30T03:39:51.871Z"}
{"shopperId":"shopper-2","productId":"P10","discountPercent":0.0, "reason":"cart-abandoned","generatedAt":"2026-08-30T03:39:49.671Z"}
{"shopperId":"shopper-6","productId":"P4", "discountPercent":37.0,"reason":"cart-abandoned","generatedAt":"2026-08-30T03:40:03.471Z"}
{"shopperId":"shopper-9","productId":"P9", "discountPercent":0.0, "reason":"cart-abandoned","generatedAt":"2026-08-30T03:40:06.071Z"}
```

**Claim 1:** `37.0` appears, a value the generator cannot produce. The rule reached
the running job.

**Claim 2, the discriminating one:** `"cart-abandoned"` appears at **both** `37.0`
and `0.0`. The operator's own output shows why, two consecutive records:

```
RecommendationRequest[shopper-2, P10, priceDropMatched=false, cartAbandoned=true,  discountPercent=0.0,  reason=cart-abandoned]
RecommendationRequest[shopper-5, P1,  priceDropMatched=true,  cartAbandoned=false, discountPercent=37.0, reason=price-drop]
```

The discount tracks `priceDropMatched`, not `reason`. Had every record read `37.0`
the condition would be universal and the Product-keyed branch decorative, which is
what ADR 0008 exists to prevent.

### Claim 3: no restart

```
$ grep -ciE "switched from RUNNING to (FAILED|RESTARTING)|Restoring job" pipeline.log
0
```

Six checkpoints completed during the run. Recommendations kept flowing across the
change with no gap.

### Claim 4: topics healthy

```
$ kubectl get kafkatopic -n kafka
clickstream      True
product-change   True
promo-rule       True
recommendation   True
```

### Timing observed

About **55 seconds** from injection to the first `37.0` on the topic. Longer than
the 10 second checkpoint interval, and not a fault: a Browsing Session must close
*and* have a price-drop match before any record can carry the new rule. Requests
arrive roughly every 20 seconds, and only some of them are price-drop matched.

Counts over the run: 27 session closes produced 7 Recommendations, 17 `UNMATCHED`
and 3 `OUT_OF_STOCK`, which is every close accounted for.
