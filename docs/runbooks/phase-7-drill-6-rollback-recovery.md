# Phase 7 Drill 6: recover from a crash-looping deployment

Date: written 2026-09-12, ahead of the run
Runtime: Flink 2.2.0 on `kind`, Flink Kubernetes Operator 1.15.0
Deployment: `personalization-blue` / `personalization-green`, `mode: native`
Start state: one side Active on an image that starts, checkpoints, then fails
End state: the other side Active on the previous image, restored from the failed
side's newest retained checkpoint

**Blocked on Task 10.** `scripts/promote.sh --recover` does not exist yet.

## What this Drill checks

**The Drill that justifies the whole recovery design.** High availability makes a
deployment failure *worse*, not better: it faithfully restores the broken thing,
and it does so forever. No amount of checkpointing fixes a bad jar.

This is the one failure class where promotion is the **only** mechanism, and it
is the case the design spec reversed the original exclusion for.

## Why the newest checkpoint is usable at all

The job never reaches a state where it can take a savepoint, so
`status.jobStatus.savepointInfo.lastSavepoint.location` stays empty. Two settings
already in the manifest make recovery possible anyway:

```yaml
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
execution.checkpointing.num-retained: "3"
```

Checkpoints survive the job's death, and `spec.job.initialSavepointPath` accepts
a retained **checkpoint** directory, not only a savepoint.

## Why there will be no duplicates, and it is mechanism not luck

Exactly-once ties committed output to completed checkpoints.

```
checkpoint N completes -> transaction N commits -> output up to offset O_N visible
records written after O_N sit in transaction N+1: pre-committed, NEVER committed
```

Restoring from checkpoint N resumes reading at exactly `O_N`. Everything past it
was never visible to a `read_committed` consumer, so reprocessing emits it for
the **first** time.

## The thing that will make this Drill fail

**Being too slow.** `num-retained` from Drill 5 bounds how long you have: three
checkpoints at a 10 second interval is thirty seconds of history.

**Draft the recovery command before breaking anything.**

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

**1. Preconditions.** As Drill 1 step 1. Confirm `promote.sh --recover` exists.

**2. Build an image that starts, checkpoints, then fails.**

It must reach `RUNNING` and complete **at least three checkpoints** before
failing. A deliberate throw inside an operator, after a delay, is the cleanest
way.

If it fails before the first checkpoint there is nothing to recover from, and you
are running Drill 4 again with extra steps.

**3. Write the recovery command down now, before deploying the broken image.**

```bash
# fill in the previous good tag; leave this in the terminal, unrun
./scripts/promote.sh --recover --image <previous good tag>
```

**4. Promote the broken image and watch the loop.**

```bash
./scripts/promote.sh
kubectl get pods -n personalization-<side> -w
kubectl get flinkdeployment -A
```

Record the restart count. Confirm it is genuinely **looping** rather than having
failed once; a single failure is a different thing.

**5. Confirm the checkpoints survived.**

```bash
source ./scripts/minio-env.sh
kubectl exec -n minio-tenant personalization-pool-0-0 -- sh -c "
  mc alias set local http://localhost:9000 '$MINIO_ACCESS_KEY' '$MINIO_SECRET_KEY' >/dev/null 2>&1
  mc ls --recursive local/checkpoints/phase-7/<side>/checkpoints/"
```

`RETAIN_ON_CANCELLATION` is what makes this non-empty. A `chk-N` directory
**without** a `_metadata` file is an incomplete checkpoint and restoring from it
fails; the discovery in `--recover` skips those.

**6. Recover.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-6-before.txt
./scripts/promote.sh --recover --image <previous good tag>
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-6-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-6-before.txt /tmp/drill-6-after.txt
```

**7. Record the reprocessing cost.**

The gap between the recovery checkpoint and the failure, in seconds. It should be
under `execution.checkpointing.interval`.

## Gate

- The broken image genuinely crash-looped, with a restart count to prove it.
- `--recover` discovered the newest `chk-N` carrying a `_metadata` file.
- `compare` reports **no duplicates** and **no gap**.
- The reprocessing cost is recorded, and is under the checkpoint interval.

## Observed result

*To be filled in from the real run. Paste the actual output, not a summary.*

## Notes

*Anything the run taught that this runbook did not predict, especially whether
thirty seconds of retained history was enough to notice and act.*
