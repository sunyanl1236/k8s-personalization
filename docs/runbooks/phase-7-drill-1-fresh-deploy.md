# Phase 7 Drill 1: a fresh deploy, from nothing

Date: written 2026-09-12, ahead of the run
Runtime: Flink 2.2.0 on `kind`, Flink Kubernetes Operator 1.15.0
Deployment: `personalization-blue` / `personalization-green`, `mode: native`,
image `lab/personalization-pipeline:0.1-0902abb`
Start state: both sides `suspended`, no Flink pods, **all four Kafka topics
truncated**
End state: one side `RUNNING`, started with **no** `initialSavepointPath`, on a
topic set containing only Phase 7 data

Per [CONTEXT.md](../../CONTEXT.md):

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

What is broken here is nothing. This Drill establishes the floor the other five
are measured against, and it is the only path in `scripts/promote.sh` that starts
a job with no prior state.

## What this Drill checks

Three things, and only the third is about the job.

1. **Discovery picks the fresh-deploy branch.** With neither side Active,
   `promote.sh` must print `FRESH DEPLOY`, not `PROMOTION`. A script that tries
   to promote here would look for a savepoint that does not exist.
2. **A start with no savepoint really is clean.** `spec.job.initialSavepointPath`
   must be empty. An empty path is a *legal* way to say "fresh start", which is
   exactly why it also hides a bug: if a promotion ever wrote `""` there, every
   Shopper's state would be discarded with no error anywhere.
3. **That the topics are genuinely empty**, so every later Drill's duplicate
   count means something.

## Why the topics are truncated first

`--start-from-earliest` defaults to `true`, at
`apps/pipeline/src/main/java/lab/personalization/pipeline/PipelineConfig.java:54`.
A start with no savepoint therefore replays the **whole `clickstream` topic**.

Measured on 2026-09-12, before truncation:

```
clickstream      10,041,754
product-change    1,892,833
promo-rule          336,326
recommendation      166,533
```

Ten million Clicks spanning Phases 3 to 6 plus at least one full source replay.
Two problems come out of that, and the second is worse.

**The replay is slow**, and the number it would produce is a curiosity rather
than a finding. Nothing in this phase is decided by how long it takes to chew
through four phases of history.

**The output is already polluted.** A whole-topic snapshot on 2026-09-10 read
140,598 records carrying only 134,754 identities: **5,844 duplicates already
present before any Phase 7 Drill ran.** Worse, a replay re-emits identities that
already exist, and those records carry **old event times**, because `generatedAt`
is a Browsing Session's window end rather than a wall clock. A snapshot window
opened at the moment you start does not contain them.

Truncating removes both problems at once. From Drill 2 onward, a duplicate in a
comparison is a **real Phase 7 duplicate** rather than history leaking in.

**What this Drill therefore no longer measures.** An earlier draft of this
runbook timed the catch-up. With empty topics there is nothing to catch up on,
and that step is gone.

## Why truncate and not delete

`manifests/strimzi/kafka-topics.yaml` is owned by the `strimzi-kafka-cluster`
ArgoCD Application, so deleting the `KafkaTopic` resources fights ArgoCD, and
Strimzi's Topic Operator adds finalizers that make deletion fiddly.

`kafka-delete-records.sh` moves each partition's **log start offset** forward
instead. It touches no topic config and no custom resource, so there is no drift
and nothing to revert.

Emptying `promo-rule` is safe: `GeneratorConfig.promoRuleInterval` defaults to 30
seconds, so new Promo Rules arrive within half a minute of starting the
generator. That also avoids Phase 4's trap, where a job that misses rules
published before it subscribed leaves every discount at `0.0`.

## Procedure

**1. Preconditions.**

```bash
kubectl get nodes -o wide | grep -i kwok || echo "clear"
git status --porcelain -- manifests/flink/
kubectl get flinkdeployment -A
```

A kwok node crashes `kindnet` on the real nodes (Phase 6 Drill H) and a promotion
needs pod networking across two namespaces. The script refuses a dirty tree,
because it commits on your behalf.

**2. Take both sides down, and stop the generator.**

`promote.sh` has **no "suspend everything" mode**, by design: every path it has
ends with exactly one side Active. So this step is by hand.

```bash
SIDE=blue    # whichever `kubectl get flinkdeployment -A` reports RUNNING
sed -i '0,/^    state: .*/s//    state: suspended/' manifests/flink/${SIDE}/flinkdeployment.yaml
git commit -am "Suspend ${SIDE} for Drill 1" && git push
argocd app sync flink-job-${SIDE}
kubectl get pods -A | grep personalization- || echo "no Flink pods, as expected"
```

`^    state: ` is anchored to four spaces because `state.backend.type` sits at the
same indent under `flinkConfiguration`.

Take the jobs down **before** truncating. A running job holds offsets and window
state referring to data you are about to remove.

**3. Record the sizes, so the before-and-after is on the record.**

```bash
for t in clickstream product-change promo-rule recommendation; do
  echo -n "$t: "
  kubectl exec -n kafka personalization-brokers-3 -- bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic $t 2>/dev/null | awk -F: '{s+=$3} END {print s}'
done
```

**4. Build the truncation spec.**

`offset: -1` means "delete everything up to the high watermark". Three partitions
per topic, four topics, twelve entries.

```bash
python3 - > /tmp/truncate.json <<'PY'
import json
topics = ["clickstream", "product-change", "promo-rule", "recommendation"]
print(json.dumps({
    "version": 1,
    "partitions": [{"topic": t, "partition": p, "offset": -1}
                   for t in topics for p in range(3)]
}))
PY
cat /tmp/truncate.json
```

**5. Truncate.**

```bash
kubectl exec -i -n kafka personalization-brokers-3 -- \
  sh -c 'cat > /tmp/truncate.json' < /tmp/truncate.json
kubectl exec -n kafka personalization-brokers-3 -- \
  bin/kafka-delete-records.sh --bootstrap-server localhost:9092 \
  --offset-json-file /tmp/truncate.json
```

**6. Verify the topics are empty.**

```bash
for t in clickstream product-change promo-rule recommendation; do
  echo -n "$t earliest: "
  kubectl exec -n kafka personalization-brokers-3 -- bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic $t --time earliest 2>/dev/null \
    | awk -F: '{s+=$3} END {print s}'
done
```

Each topic's **earliest** must now equal the **latest** recorded in step 3.

**The offsets do not reset to zero, and that is correct.** Kafka never rewinds an
offset. It moves the log start forward, so the topic is empty while the numbering
continues from where it was. A topic reporting earliest 10,041,754 and latest
10,041,754 holds no records.

**7. Start the generator**, so there is something to consume.

```bash
apps/gradlew -p apps :generator:run \
  --args="--click-rate=800 --shopper-count=2000 --product-change-rate=400"
```

Hold `shopperCount ~ 2.5 x clickRate`. At the old `--click-rate=5` default with
2000 Shoppers every Browsing Session is one Click and the window branch emits
almost nothing, which makes a healthy pipeline look broken.

**8. Run the fresh deploy.**

```bash
./scripts/promote.sh
```

**9. Watch it reach RUNNING.**

```bash
kubectl get flinkdeployment -A -w
```

There is no backlog now, so this should be quick. What takes time is the first
Browsing Session closing, which needs `sessionGap` of event time to pass before
any Recommendation is produced at all.

**10. Confirm the start was clean.**

```bash
kubectl get flinkdeployment personalization-blue -n personalization-blue \
  -o jsonpath='initialSavepointPath=[{.spec.job.initialSavepointPath}]{"\n"}upgradeSavepointPath=[{.status.jobStatus.upgradeSavepointPath}]{"\n"}'
```

**11. Baseline, once Recommendations are flowing.**

```bash
NOW=$(( $(date +%s) * 1000 ))
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-1-after.txt "${NOW}"
```

Keep `${NOW}`. Drill 2 opens its own window, but this number is the record of
when the phase's data actually starts.

## Gate

- All four topics report **earliest equal to latest** after truncation.
- `promote.sh` printed **`FRESH DEPLOY`**, not `PROMOTION`.
- `spec.job.initialSavepointPath` is **empty**.
- The job reaches `RUNNING` and Recommendations appear on the topic.
- The first windowed snapshot reports **records equal to identities**, meaning
  zero duplicates in a clean topic.

A populated `initialSavepointPath` means the wrong branch ran and the Drill did
not test what it claims. Duplicates in the first snapshot mean something is wrong
now, not inherited, which is the whole benefit of having truncated.

## Observed result

*To be filled in from the real run. Paste the actual output, not a summary.*

## Notes

*Anything the run taught that this runbook did not predict.*
