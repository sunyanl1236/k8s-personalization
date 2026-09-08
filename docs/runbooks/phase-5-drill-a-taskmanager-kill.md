# Phase 5 Drill A: kill a TaskManager

Date: 2026-09-07
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Deployment: `FlinkDeployment personalization` in `personalization-blue`,
`mode: native`, image `lab/personalization-pipeline:0.1-0bd7f52`
Topology: 2 JobManagers, 3 TaskManagers, `parallelism: 6`,
`taskmanager.numberOfTaskSlots: 2`
Checkpointing: every 10s, `EXACTLY_ONCE`, RocksDB, to
`s3://checkpoints/phase-5`

Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

What is broken here is the assumption that a TaskManager's state can be rebuilt
in place. It cannot.

## What is actually being exercised

A TaskManager holds a slice of the keyed RocksDB state, in memory and on local
disk. Killing the pod destroys that slice.

Recovery is not "restart the pod and carry on". The two surviving TaskManagers
hold state from a **later** point in time than a fresh one would, so resuming
from mixed vintages would silently corrupt every windowed aggregate.

So Flink does the only consistent thing available: it fails the **whole job**,
then restarts every task from the last completed checkpoint. The 10 second
checkpoint interval is what bounds how much replay that costs.

This Drill has to prove two separate things, and only both together are proof:

1. **The job resumed from a checkpoint**, not from empty state.
2. **The `recommendation` topic gained no duplicates** across the restart.

Point 1 alone is satisfied by a job that restored and then produced garbage.
Point 2 alone is satisfied by a job that restarted empty and produced nothing.

### The PodDisruptionBudget is not involved

`personalization-pdb` exists with `minAvailable: 1` and reports
`ALLOWED DISRUPTIONS: 1` throughout this Drill. It does nothing here, and that is
correct.

A PDB constrains **voluntary** disruption, which means the eviction API.
`kubectl drain` goes through that API. `kubectl delete pod` does not. Deleting a
pod is an involuntary disruption and no budget can refuse it.

That distinction is what Drill C exercises. Do not read "the PDB did nothing" as
a fault here.

### One honest limitation of the gap check

`scripts/recommendation-snapshot.sh` reports a gap when an identity present in
BEFORE is missing from AFTER. **For this pipeline that is structurally very hard
to trigger**, because committed Kafka records do not disappear. The BEFORE
snapshot reads records that are already committed, and nothing a Drill does
removes them.

So the meaningful signals are the **restore record** and the **duplicate check**.
A clean `no gap` line is a regression guard against retention or topic loss, not
proof of recovery. Do not over-read it.

The mechanism is in
[the Phase 5 knowledge doc](../knowledge/phase-5-operator-and-ha.md).

## Procedure

**1. `apps/gradlew -p apps :generator:run`** (terminal 1)

Expected: the synthetic producer writes Clicks to `clickstream` at 5 per second
across the 10 Shoppers in `Catalog.SHOPPER_IDS`.

Rationale: with no input in flight, a killed TaskManager loses nothing. The job
restarts, processes nothing, and reports a clean recovery that proves nothing.
The generator must be running for the whole Drill.

**2. `./scripts/recommendation-snapshot.sh snapshot /tmp/drill-a-before.txt`**

Expected: `ok /tmp/drill-a-before.txt: N records, N identities`.

Rationale: the baseline. Records must equal identities. If they differ, the topic
already held duplicates before the Drill and the AFTER comparison is meaningless.

**3. Record the current checkpoint, from the REST API.**

```bash
JID=$(curl -s http://localhost:30011/jobs \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -s "http://localhost:30011/jobs/$JID/checkpoints" | python3 -m json.tool | head -30
```

Rationale: "it recovered" is weaker than "it restored from `chk-N`". You need the
number from before the kill to know the restore used a real checkpoint.

**4. Kill exactly one TaskManager.**

```bash
POD=$(kubectl get pods -n personalization-blue -l component=taskmanager \
        --field-selector status.phase=Running -o name | head -1)
echo "killing $POD"
kubectl delete -n personalization-blue "$POD" --wait=false
```

Rationale: `head -1` must select the pod **before** anything is deleted.

**This is a correction.** The obvious form is wrong:

```bash
# WRONG: deletes every matching pod
kubectl delete pod -n personalization-blue -l component=taskmanager \
  --field-selector status.phase=Running --wait=false | head -1
```

`kubectl delete -l` issues a delete for every pod matching the label. `head -1`
truncates kubectl's **output**, not the deletion, and the API calls have already
been made by the time the pipe runs. `head` closing the pipe early can send
`SIGPIPE` and stop kubectl partway, which makes the damage look bounded when it
is only mistimed. Both runs recorded below killed **two** pods this way.

**5. `kubectl get pods -n personalization-blue -L component -w`** (terminal 2)

Start this **before** step 4. Once the pod is deleted you want to be watching,
not typing.

Expected: the killed pod terminates, a replacement is Scheduled within a second,
and the job cycles `RUNNING` to `CREATED` to `RUNNING`.

**6. Confirm the restore, from the REST API.**

```bash
curl -s "http://localhost:30011/jobs/$JID/checkpoints" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['latest']['restored'])"
```

Expected: a dict with `id`, `restore_timestamp`, `is_savepoint: False`, and an
`external_path` under `s3://checkpoints/phase-5`.

**`restored: None` is the failure this step exists to catch.** It means the job
started from empty state.

**Use the REST API, not the log.** Three reasons, all learned the hard way:

- `--tail=300` does not reach back far enough. This job logs a checkpoint every
  10 seconds, so the restore line scrolls out of a short tail within minutes.
  `--tail=-1` removes the limit.
- Only the **leader** JobManager logs the restore. `-l component=jobmanager`
  fetches from both pods and applies `--tail` to each, so half the output is
  guaranteed to be standby noise.
- A restarted JobManager loses its previous container's log. `--previous` gets
  you one back and nothing gets you two. The REST record lives in the running
  job's state and has no such limit.

The log line, when you have it, reads:

```
CheckpointCoordinator - Restoring job <jobid> from Checkpoint N @ <ts>
  located at s3://checkpoints/phase-5/<jobid>/chk-N
```

**7. Snapshot after, and compare.**

```bash
sleep 60
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-a-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-a-before.txt /tmp/drill-a-after.txt
```

Expected: zero gaps, zero duplicates, exit 0, and **more identities in AFTER than
in BEFORE**.

Rationale for the `sleep 60`: at roughly 2 Recommendations per minute this yields
about 2 new identities, which is what makes the comparison non-trivial. A pause
in output during the restart is expected and is not a gap.

## Observed result

Two runs on 2026-09-07. Both killed two of three TaskManagers, because step 4's
correction had not been made yet. The recovery is still valid evidence, and is a
harder failure than the intended one.

### Run 1

```
Killing            taskmanager-2-1   Stopping container flink-main-container
Killing            taskmanager-2-2   Stopping container flink-main-container
Scheduled          taskmanager-2-4   -> personalization-lab-worker3
Scheduled          taskmanager-2-5   -> personalization-lab-worker2
JobException       FlinkExpectedException: The TaskExecutor is shutting down
JobStatusChanged   RUNNING -> CREATED
JobStatusChanged   CREATED -> RUNNING
```

Restore record:

```json
{"id": 784, "restore_timestamp": 1788806526892, "is_savepoint": false,
 "external_path": "s3://checkpoints/phase-5/ccf3abb44f948e42142eaac8a5edd1a4/chk-784"}
```

**Recovery: 15 seconds**, from `RUNNING -> CREATED` to `CREATED -> RUNNING`.

### Run 2

```
Killing            taskmanager-2-3   Stopping container flink-main-container
Killing            taskmanager-2-4   Stopping container flink-main-container
Scheduled          taskmanager-2-6   -> personalization-lab-worker
Scheduled          taskmanager-2-7   -> personalization-lab-worker3
JobException       FlinkExpectedException: The TaskExecutor is shutting down
JobStatusChanged   RUNNING -> CREATED
JobStatusChanged   CREATED -> RUNNING
```

Restore record:

```json
{"id": 809, "restore_timestamp": 1788806792621, "is_savepoint": false,
 "external_path": "s3://checkpoints/phase-5/ccf3abb44f948e42142eaac8a5edd1a4/chk-809"}
```

**Recovery: 15 seconds.** The same number twice, which is worth more than either
measurement alone.

### Gap check, run 2

```
==> before: 506 identities
==> after:  530 identities
 ok no gap
 ok no duplicates
 ok recovery is clean
exit=0
```

24 new Recommendations were produced across the Drill, so the comparison had real
material rather than two identical sets.

### End state

```
NAME                              COMPONENT     NODE
personalization-6bdd49bc9-hw6ct   jobmanager    personalization-lab-worker3
personalization-6bdd49bc9-zctrk   jobmanager    personalization-lab-worker
personalization-taskmanager-2-7   taskmanager   personalization-lab-worker3
personalization-taskmanager-2-8   taskmanager   personalization-lab-worker
personalization-taskmanager-2-9   taskmanager   personalization-lab-worker2

personalization-pdb   minAvailable 1   ALLOWED DISRUPTIONS 1
checkpoints: 820 completed, 1 failed
```

**Zone spread survived the restart.** The three replacement TaskManagers landed
one per worker, so one per Zone. `whenUnsatisfiable: ScheduleAnyway` on the
TaskManager spread constraint permitted any placement, and the scheduler chose an
even one anyway.

The single `failed` checkpoint dates from job startup, not from either Drill.

## Notes

**The exception is expected.**
`FlinkExpectedException: The TaskExecutor is shutting down` is Flink observing
that a TaskManager left. It is not a fault, and the word `Expected` in the class
name says so.

**`ScheduleAnyway` earned its place.** Had the TaskManager spread constraint used
`DoNotSchedule`, a replacement pod could have been left `Pending` when a Zone was
already at its share, blocking the recovery this Drill exists to observe. That
was the stated reason for the choice in Task 5 Step 4, and this is the run that
tests it.

**Recovery cost is bounded by the checkpoint interval, not by the Drill.** At 10
seconds between checkpoints, the job replays at most 10 seconds of Clicks. The
15 second wall-clock recovery is dominated by pod scheduling and TaskManager
registration, not by replay.

**Next Drill.** Drill B kills the leader JobManager, which exercises the HA
metadata under `s3://checkpoints/phase-5-ha` rather than the checkpoints under
`s3://checkpoints/phase-5`. Those are different prefixes and different failure
modes.
