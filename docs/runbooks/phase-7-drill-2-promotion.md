# Phase 7 Drill 2: promote under a Load Ramp

Date: written 2026-09-12, ahead of the run
Runtime: Flink 2.2.0 on `kind`, Flink Kubernetes Operator 1.15.0
Deployment: `personalization-blue` / `personalization-green`, `mode: native`,
image `lab/personalization-pipeline:0.1-0902abb`
Start state: one side `RUNNING` under load, the other `suspended`
End state: the sides swapped, with a bounded pause and no duplicate
Recommendations

Per [CONTEXT.md](../../CONTEXT.md):

> **Promotion**: Moving Active Side status from one namespace to the other by
> suspending with a savepoint and restoring the other side from it.
> Directionless: `blue` and `green` are namespace names, not roles.

This is the phase's headline Drill. Everything else is a variation on it.

## What this Drill checks

**The pause, measured.** The design spec budgeted 60 to 140 seconds by
arithmetic, not observation. No target was set before measuring, deliberately.

**No Gap**, per CONTEXT.md: every Recommendation identity present before is still
present after, **and none appears twice**.

## What the pause is, and what it is not

**No Recommendation is lost.** Kafka holds the input, and the incoming side
resumes from the outgoing side's exact offsets, which ride inside the savepoint
as operator state. The `recommendation` topic goes quiet, then produces a burst
as the new Active Side works through the backlog.

So the pause is a **latency spike, not a loss**. Measure it from the topic, not
from pod states.

## Two things that will look like faults and are not

**Parallelism drops to 2.** A promotion restarts the job, so it starts at
`spec.job.parallelism`, and the autoscaler climbs again from the
`autoscaler-personalization` ConfigMap. That climb is part of the pause.

**The Standby Side's PodDisruptionBudget reports `ALLOWED DISRUPTIONS 0`.** It
has no pods to match. A PDB only carries meaning on the Active Side.

## Snapshots cover the whole topic

**Use a whole-topic snapshot, not a window.** Drill 1 truncated every topic, so
`recommendation` holds only Phase 7 data and there is no history to exclude.
A window opened at the moment you snapshot captures **zero records**, because the
snapshot reads from that offset to the end of the topic. The BEFORE file would be
empty, `compare` would report no gap and no duplicates against an empty set, and
the Drill would pass while checking nothing. Measured 2026-09-12: window-from-now
gave 0 records where the whole topic gave 3,262.

The `since-epoch-ms` argument earns its place only once the topic again carries
data from outside the Drill.

## Procedure

**1. Preconditions.** As Drill 1 step 1.

**2. Start the Load Ramp.**

```bash
apps/gradlew -p apps :generator:run \
  --args="--click-rate=800 --shopper-count=2000 --product-change-rate=400"
```

Session length is `e^(6 x clickRate / shopperCount)`. Hold
`shopperCount ~ 2.5 x clickRate` or the window state exhausts the host. Phase 6
measured the generator's real ceiling at about 2650 Clicks/sec, so **measure the
achieved rate from partition offsets** rather than trusting the flag.

**3. Snapshot before, and keep the window value.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-2-before.txt
```

**4. Promote, recording six timestamps as they pass.**

```bash
./scripts/promote.sh
```

Record: suspend committed, `SUSPENDED` observed, savepoint path non-empty, the
incoming side committed, its first pod, its `RUNNING`. Those six turn one number
into a per-stage breakdown, which is what tells you where the time actually goes.

**5. Measure the pause from the topic.**

The last Recommendation before the promotion to the first one after. Pod states
lie about this: the job is `RUNNING` before it has produced anything.

**6. Snapshot after, and compare.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-2-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-2-before.txt /tmp/drill-2-after.txt
```

Both snapshots must cover the same range, and `compare` refuses a mismatched
pair outright. With a truncated topic that range is simply the whole topic.

**7. Confirm no transaction was left dangling.**

```bash
kubectl exec -n kafka personalization-brokers-3 -- \
  bin/kafka-transactions.sh --bootstrap-server localhost:9092 list
```

Both sides share `--transactional-id-prefix=personalization-phase-7`, which is
what lets the incoming side finalise the outgoing side's in-flight transactions
by reconstructing the same ids. A stale entry means that did not happen.

## Gate

- `compare` reports **`no gap`**, **`no duplicates`**, **`recovery is clean`**.
- The pause is recorded as a number, with the six-stage breakdown.
- `kafka-transactions.sh list` shows only the new Active Side's transactions.

A duplicate here points at the transactional id prefix, not at the savepoint.

## Observed result

*To be filled in from the real run. Paste the actual output, not a summary.*

## Notes

*Anything the run taught that this runbook did not predict.*
