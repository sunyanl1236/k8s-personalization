# Phase 7 Drill 3: promote back the other way

Date: written 2026-09-12, ahead of the run
Runtime: Flink 2.2.0 on `kind`, Flink Kubernetes Operator 1.15.0
Deployment: `personalization-blue` / `personalization-green`, `mode: native`
Start state: the side Drill 2 promoted is Active, still under load
End state: the sides swapped back, same criteria as Drill 2

Per [CONTEXT.md](../../CONTEXT.md), Promotion is **directionless**: `blue` and
`green` are namespace names, not roles.

## What this Drill checks

**That the direction was discovered and not assumed.** A script that works once
and fails on the return trip almost always has a hardcoded side, or a discovery
test that quietly treats blue as the default. Running it a second time with the
same command and no flags is the only way to find out.

**That the storage split is real.** After this Drill both sides have written a
savepoint into their own prefix. That listing is the visible proof.

## Why this is a separate Drill and not a repeat

Drill 2 proved a promotion works. It did not prove the *mechanism* is symmetric,
because blue happened to be Active and blue is the name that appears first in
`SIDES=(blue green)` inside `scripts/promote.sh`. An inverted or defaulted
discovery passes Drill 2 and fails here.

## Procedure

**1. Preconditions.** As Drill 1 step 1. The generator from Drill 2 should still
be running; if it is not, restart it with the same arguments so the two pauses
are comparable.

**2. Snapshot before.**

```bash
BEFORE=$(( $(date +%s) * 1000 ))
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-3-before.txt "${BEFORE}"
```

**3. Promote, with no flags.**

```bash
./scripts/promote.sh
```

The command is byte-identical to Drill 2's. That is the test.

**4. Snapshot after and compare, same window value.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-3-after.txt "${BEFORE}"
./scripts/recommendation-snapshot.sh compare /tmp/drill-3-before.txt /tmp/drill-3-after.txt
```

**5. Confirm both storage prefixes are now populated.**

```bash
source ./scripts/minio-env.sh
kubectl exec -n minio-tenant personalization-pool-0-0 -- sh -c "
  mc alias set local http://localhost:9000 '$MINIO_ACCESS_KEY' '$MINIO_SECRET_KEY' >/dev/null 2>&1
  mc ls local/checkpoints/phase-7/blue/savepoints/
  mc ls local/checkpoints/phase-7/green/savepoints/"
```

`mc` is not on the host PATH; read the bucket from inside the tenant pod.

## Gate

- The script discovered the **opposite** direction to Drill 2, with no flags
  given.
- `compare` reports `no gap`, `no duplicates`, `recovery is clean`.
- Both `phase-7/blue/savepoints/` and `phase-7/green/savepoints/` hold a
  savepoint.

Compare this pause to Drill 2's. They should be close. A large difference points
at state size, because the side that has been running longest has accumulated
more open Browsing Sessions.

## Observed result

*To be filled in from the real run. Paste the actual output, not a summary.*

## Notes

*Anything the run taught that this runbook did not predict.*
