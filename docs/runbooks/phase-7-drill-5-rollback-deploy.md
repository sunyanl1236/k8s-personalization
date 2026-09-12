# Phase 7 Drill 5: rollback as a deployment

Date: written 2026-09-12, ahead of the run
Runtime: Flink 2.2.0 on `kind`, Flink Kubernetes Operator 1.15.0
Deployment: `personalization-blue` / `personalization-green`, `mode: native`
Start state: one side Active on a new image, both sides healthy
End state: the other side Active on the **previous** image, carrying the state
the new one had accumulated

## What this Drill checks

**That a rollback moves the image backwards and the state forwards.** Those two
move independently, and confusing them is the mistake this Drill exists to
prevent.

```
image:  new (unwanted)  ->  previous
state:  keep the NEWEST snapshot, taken fresh from the Active Side
```

The state is not what is wrong. The code is. So you restore the newest state
**into** the old image.

**That `git revert` is the whole mechanism.** No special flag, no rollback mode.
The Standby Side's manifest, left untouched during the promotion, already records
what was running before.

## The trap

Reaching for the Standby Side's stale `initialSavepointPath` instead of taking a
fresh savepoint. That path is as old as the Active Side's uptime, and restoring
it replays everything since. At Drill 2's rates, six hours of uptime is roughly
1.1 million duplicate Recommendations against zero for a fresh savepoint.

`promote.sh` never does this. The Drill confirms it.

## A decision this Drill needs first

`execution.checkpointing.num-retained: "3"` at
`execution.checkpointing.interval: 10s` keeps **thirty seconds** of recovery
history, and Drill 6 depends entirely on that window. Raising it to 10 gives 100
seconds.

Measure the cost side before deciding:

```bash
source ./scripts/minio-env.sh
kubectl exec -n minio-tenant personalization-pool-0-0 -- sh -c "
  mc alias set local http://localhost:9000 '$MINIO_ACCESS_KEY' '$MINIO_SECRET_KEY' >/dev/null 2>&1
  mc ls --recursive local/checkpoints/phase-7/" | wc -l
```

Whichever you choose lands in **both** manifests, not one. Record the reasoning.

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

**2. Build a second image with a visible, harmless change.**

Something observable in the output, so the two versions are distinguishable
without reading tags.

**Do not change the job graph.** The phase's global constraints forbid it, and
the uids added in Task 2 are what would make such a change survivable *later*,
not now.

```bash
./scripts/build-image.sh
```

Commit `apps/` first if the tag comes back `-dirty`.

**3. Point the Standby Side at the new image and promote.**

```bash
SIDE=green   # whichever is currently Standby
sed -i "s|^  image: .*|  image: <the new tag>|" manifests/flink/${SIDE}/flinkdeployment.yaml
git commit -am "Deploy <tag> to ${SIDE}" && git push
./scripts/promote.sh
```

Confirm the new behaviour is visible in the output before continuing. A rollback
of something you never saw deployed proves nothing.

**4. Revert, and note what the Standby Side's manifest still says.**

```bash
git log --oneline -3 -- manifests/flink/
grep '^  image:' manifests/flink/*/flinkdeployment.yaml
```

The two sides **legitimately differ** on `spec.image` and
`initialSavepointPath`. The now-Standby Side's manifest is the record of the
previous version. That record is the rollback artifact.

```bash
git revert --no-edit <the promote commit>
git push
```

**5. Promote back onto the previous image.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-5-before.txt
./scripts/promote.sh
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-5-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-5-before.txt /tmp/drill-5-after.txt
```

## Gate

Both of these, and either alone is not the result:

- The output returns to its **pre-change** form, proving the image rolled back.
- `compare` reports **no duplicates**, proving the state did not.

## What a rollback does not do

It stops the bleeding; it does not un-publish. Every Recommendation the unwanted
image already committed stays in the topic forever. Nothing in this phase
retracts a committed record.

## Observed result

*To be filled in from the real run. Paste the actual output, not a summary.*

## Notes

*Anything the run taught that this runbook did not predict, including the
`num-retained` decision and why.*
