# Phase 5 Drill B: kill the leader JobManager

Date: 2026-09-07
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Deployment: `FlinkDeployment personalization` in `personalization-blue`,
`mode: native`, image `lab/personalization-pipeline:0.1-0bd7f52`
Topology: 2 JobManagers, 3 TaskManagers, `parallelism: 6`
HA: `high-availability.type: kubernetes`,
`high-availability.storageDir: s3://checkpoints/phase-5-ha`

Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

What is broken here is the **coordinator**, not the state.

## What is actually being exercised

With a single JobManager, killing it means the job is gone. Nothing knows the
execution graph or which checkpoint was last completed, until some new process
reads that back from somewhere.

`high-availability.type: kubernetes` provides that somewhere, and it is **two
places, not one**:

| | Holds | Where |
|---|---|---|
| `personalization-cluster-config-map` | the leader lease, and a pointer | Kubernetes |
| `s3://checkpoints/phase-5-ha/` | the JobGraph and checkpoint metadata | MinIO |

A ConfigMap is small and a JobGraph is not, so Flink persists the metadata to
`storageDir` and stores only a pointer in Kubernetes. Task 5 Step 12 verified
both are non-empty. This Drill is what makes that verification worth having.

### Why this is not Drill A

```
Drill A   destroyed STATE          evidence = a checkpoint restore
Drill B   destroys the COORDINATOR evidence = a leadership change
```

The standby does not start the job over. It acquires the lease, reads the
pointer, and resumes the **same job**.

### The failure mode to watch for

If the surviving JobManager starts a **fresh** job instead of resuming,
`phase-5-ha` was not really in use. Two healthy JobManager pods would still be
listed and the Drill would have proved nothing.

**The tell is the job id changing.** Watch that, not the pod count.

## Identifying the leader, which is harder than it looks

The plan originally said to read the lease annotation and match `holderIdentity`
to a pod name. **You cannot.** Observed on 2026-09-07, before the kill:

```json
{"holderIdentity":"11215c96-0501-4d09-bf7e-e0fc203fd962",
 "leaseDuration":"PT15S","acquireTime":"2026-09-07T16:35:32Z",
 "renewTime":"2026-09-07T18:57:47Z","leaderTransitions":1}
```

```
pod hw6ct   uid 1316540a-81eb-4cac-9810-ac532f673b03
pod zctrk   uid f4c2a46c-4234-4e33-8706-39a942e09cbc
```

`holderIdentity` matches neither. It is a UUID Flink generates per JobManager
**process**, unrelated to the pod's identity.

Grepping the logs for that UUID also fails. It appears in **both** pods, because
the standby observes the election and logs the winner too. Measured: 1 line
against 3.

### What does work

The leader is the only JobManager running the `CheckpointCoordinator`:

```bash
for p in $(kubectl get pods -n personalization-blue -l component=jobmanager -o name); do
  echo -n "$p  completed-checkpoint lines: "
  kubectl logs -n personalization-blue "$p" --tail=-1 2>/dev/null | grep -c "Completed checkpoint"
done
```

```
pod/personalization-6bdd49bc9-hw6ct     0
pod/personalization-6bdd49bc9-zctrk   848      <- leader
```

Unambiguous, and it stays correct as the numbers grow.

**In k9s:** `:po`, highlight a JobManager, press `l` for logs, press `/` and
filter on `Completed checkpoint`. The leader shows lines, the standby shows none.
Filter on that phrase rather than `was granted leadership`: the leader writes a
completed-checkpoint line every 10 seconds so it appears in any tail, while the
leadership line is written once at election time and is usually out of range.

The pods view itself is no help. Both JobManagers carry identical labels,
identical images, and no leader marker.

## Procedure

**1. `apps/gradlew -p apps :generator:run`** (terminal 1)

Rationale: with nothing in flight, a killed JobManager loses nothing and a clean
recovery proves nothing.

**2. Record the leader and the transition count.**

```bash
kubectl get configmap personalization-cluster-config-map -n personalization-blue \
  -o jsonpath='{.metadata.annotations.control-plane\.alpha\.kubernetes\.io/leader}'
```

Write down `holderIdentity` and `leaderTransitions`. Then find the pod with the
loop above.

Rationale: `leaderTransitions` is the cleanest single proof that leadership
moved. Without the before value there is nothing to compare against.

**3. Record the job id.**

```bash
curl -s http://localhost:30011/jobs
```

Rationale: this is the primary evidence for step 6. A changed job id is the
failure mode.

**4. `./scripts/recommendation-snapshot.sh snapshot /tmp/drill-b-before.txt`**

**5. Kill the leader by name.**

```bash
kubectl delete pod <the leader pod> -n personalization-blue
```

Name the pod. Killing the standby tests nothing, and both pods look identical in
`kubectl get pods`.

**6. Watch leadership move.**

Re-read the lease annotation. Expect a **different** `holderIdentity` and
`leaderTransitions` incremented by one.

Allow up to 15 seconds. `leaseDuration: PT15S` means the standby cannot act until
the dead leader stops renewing and the lease expires.

**7. Confirm the job resumed rather than restarted.**

```bash
JID=$(curl -s http://localhost:30011/jobs \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
echo "$JID"
curl -s "http://localhost:30011/jobs/$JID/checkpoints" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['latest']['restored'])"
```

**The job id must be unchanged.** A new id means a fresh job.

**8. Confirm a replacement standby appeared, on a different worker.**

```bash
kubectl get pods -n personalization-blue -l component=jobmanager \
  -o custom-columns='NAME:.metadata.name,NODE:.spec.nodeName'
```

If both land on one worker, the `DoNotSchedule` constraint is not matching, and
Drill C will not test what it claims to.

**9. Snapshot after, and compare.**

```bash
sleep 60
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-b-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-b-before.txt /tmp/drill-b-after.txt
```

## Observed result

Killed `personalization-6bdd49bc9-zctrk`, the leader, at about 19:05:05.

### Leadership moved

```json
before  {"holderIdentity":"11215c96-0501-4d09-bf7e-e0fc203fd962",
         "acquireTime":"2026-09-07T16:35:32Z","leaderTransitions":1}

after   {"holderIdentity":"6ef780d2-c1de-479c-b7bc-d52b10898b9f",
         "acquireTime":"2026-09-07T19:05:12Z","leaderTransitions":2}
```

Different holder, and `leaderTransitions` went 1 to 2.

The previous **standby** is now the leader, confirmed by the discriminator:

```
pod/personalization-6bdd49bc9-hw6ct   21      <- was 0 before the kill, now leader
pod/personalization-6bdd49bc9-p62r4    0      <- new standby
```

### The job resumed, it did not restart

```
job id before   ccf3abb44f948e42142eaac8a5edd1a4
job id after    ccf3abb44f948e42142eaac8a5edd1a4      unchanged
```

```json
{"id": 920, "restore_timestamp": 1788807915053, "is_savepoint": false,
 "external_path": "s3://checkpoints/phase-5/ccf3abb44f948e42142eaac8a5edd1a4/chk-920"}
```

The new leader's log, in order:

```
19:05:12,935  ActiveResourceManager - Recovered worker personalization-taskmanager-2-8 ... registered
19:05:12,935  ActiveResourceManager - Recovered worker personalization-taskmanager-2-9 ... registered
19:05:13,343  JobMasterServiceLeadershipRunner for job ccf3abb44f948e42142eaac8a5edd1a4
                was granted leadership with leader id d24ccb5b-e06e-4eea-a618-0c6d2875c8d1
19:05:13,643  EmbeddedExecutor - Job ccf3abb44f948e42142eaac8a5edd1a4 was recovered successfully.
19:05:15,054  CheckpointCoordinator - Restoring job ccf3abb44f948e42142eaac8a5edd1a4
                from Checkpoint 920 @ 1788807901124
                located at s3://checkpoints/phase-5/ccf3abb44f948e42142eaac8a5edd1a4/chk-920
```

`Job ... was recovered successfully` is Drill B's signature line. Drill A never
produces it, because Drill A never lost the coordinator.

`KubernetesCheckpointRecoveryFactory.createRecoveredCompletedCheckpointStore`
also appears in that stack, which is the HA store being rebuilt from
`phase-5-ha`.

### Timing

```
Killing            pod/personalization-6bdd49bc9-zctrk
Scheduled          pod/personalization-6bdd49bc9-p62r4  -> personalization-lab-worker2
JobStatusChanged   RUNNING -> CREATED        10s after the kill
JobStatusChanged   CREATED -> RUNNING        15s later
```

**15 seconds from job failure to running**, the same figure Drill A produced
twice. The extra 10 seconds before the job noticed is the lease expiring.

### The TaskManagers survived

```
Recovered worker personalization-taskmanager-2-8 ... registered
Recovered worker personalization-taskmanager-2-9 ... registered
```

The new leader **re-adopted the existing TaskManagers** rather than creating new
ones. No TaskManager pod was killed or rescheduled during this Drill. That is the
sharpest contrast with Drill A, where every TaskManager slot was rebuilt.

### End state

```
NAME                              NODE
personalization-6bdd49bc9-hw6ct   personalization-lab-worker3     leader
personalization-6bdd49bc9-p62r4   personalization-lab-worker2     standby
```

Two workers, so two Zones. `DoNotSchedule` on the JobManager spread constraint
held: the replacement did not land on `worker3` beside the new leader.

### Gap check

```
==> before: 602 identities
==> after:  626 identities
 ok no gap
 ok no duplicates
 ok recovery is clean
exit=0
```

## Notes

**The gap check is weak evidence here, as in Drill A.** Committed Kafka records
do not disappear, so `comm -23 BEFORE AFTER` is structurally near-empty whatever
happens. The **unchanged job id** and the **`leaderTransitions` increment** are
what actually prove this Drill. The duplicate check is the meaningful half of the
comparison.

**Why 15 seconds appears three times.** Drill A twice and Drill B once. In all
three the wall clock is dominated by TaskManager registration and job
re-scheduling, not by replaying state. The 10 second checkpoint interval bounds
the replay to at most 10 seconds of Clicks.

**`leaderTransitions` is cumulative and survives.** It read 1 before this Drill,
from a leadership change earlier in the day during the image rollouts. Always
record the before value rather than assuming it starts at 0.

**Next Drill.** Drill C drains a Zone. That is a **voluntary** disruption, so it
goes through the eviction API and the `personalization-pdb` finally matters.
Drills A and B deleted pods, which no budget can refuse.
