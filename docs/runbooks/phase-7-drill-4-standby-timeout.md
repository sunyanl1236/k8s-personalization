# Phase 7 Drill 4: break the Standby Side on purpose

Date: written 2026-09-12, ahead of the run
Runtime: Flink 2.2.0 on `kind`, Flink Kubernetes Operator 1.15.0
Deployment: `personalization-blue` / `personalization-green`, `mode: native`
Start state: one side Active, the other `suspended` and about to be sabotaged
End state: the Active Side suspended, the Standby Side refusing to start, and a
human recovering it by hand

## What this Drill checks

**The one failure in the whole phase with no automatic safe path.** Once the
Active Side is suspended and the Standby will not come up, nothing safe remains
to do automatically. Resuming the suspended side is a decision to accept that the
promotion never happened, and that is a human's call.

The point of running it is to **write the fallback procedure from having done
it**, rather than from imagining it.

**A second thing, easy to miss:** that the state it leaves behind is *legible*.
This is where the commit-then-sync decision pays off. Git and the cluster agree
about every side, even mid-failure.

## Why a bad image tag and not a bad jar

Use a tag that does not exist. That produces `ImagePullBackOff`: unambiguous,
immediate, and it leaves no half-started job.

A bad jar is **Drill 6's** failure. Running both at once leaves you unable to
tell which failure you are watching, and Drill 6's whole point is that a job
which *starts* and then crashes is recoverable from its own checkpoints.

## The resource risk

A failed promotion can leave **both** sides holding pods. Phase 6 measured host
headroom at 2.5 GiB; it read 6 GiB on 2026-09-10. Watch it throughout, and be
ready to suspend the broken side through Git if the host starts swapping.

## Procedure

**1. Preconditions.** As Drill 1 step 1. Note which side is currently Active; the
Standby Side is the one you are about to break.

**2. Break the Standby Side, in Git.**

```bash
SIDE=green   # whichever is currently Standby
sed -i "s|^  image: .*|  image: lab/personalization-pipeline:0.1-doesnotexist|" \
  manifests/flink/${SIDE}/flinkdeployment.yaml
git commit -am "Drill 4: break ${SIDE} deliberately" && git push
```

In Git, not with `kubectl patch`. The Drill is about the failure, not about
introducing drift on top of it.

**3. Promote into it, and let the poll time out.**

```bash
free -g
./scripts/promote.sh
```

`RUNNING_TIMEOUT` is 600 seconds. Record how long it actually took and whether
the message named the fallback.

**4. Read the state the timeout leaves behind.**

```bash
kubectl get flinkdeployment -A
kubectl get pods -A | grep personalization-
git log --oneline -3 -- manifests/flink/
free -g
```

Expect the Active Side `suspended` in **both** Git and the cluster, and the
Standby `running` in Git and not up. **They agree.** Note how much easier that is
to reason about than the patched-live equivalent, where Git would still claim the
suspended side was running.

**5. Perform the manual fallback, and time it.**

The script printed it. It resumes the previously Active Side from its own
pre-promotion savepoint, whose path is in the commit history from step 4.

**6. Repair the Standby Side and confirm the pair is healthy.**

```bash
git revert --no-edit <the break commit>
git push
argocd app sync flink-job-${SIDE}
kubectl get flinkdeployment -A
```

## Gate

- The script timed out, exited non-zero, and **printed the fallback rather than
  attempting it**.
- Git and the cluster agreed on both sides throughout.
- The fallback was performed by hand and timed.
- The pair is healthy again, one side Active.

## Observed result

*To be filled in from the real run. Paste the actual output, not a summary.
This is the runbook someone reads at 2am; write step 5 for that reader.*

## Notes

*Anything the run taught that this runbook did not predict.*
