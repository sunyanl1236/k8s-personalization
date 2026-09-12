# Phase 7 design: Blue/green and the deployment mechanism

Date: 2026-09-09
Status: draft, pending the verification gates in the last section
Supersedes: the Phase 7 section of
[the implementation phases](../plans/2026-08-10-implementation-phases.md), and
the "Zero-downtime deployment" section of
[the design spec](2026-07-25-flink-k8s-personalization-design.md)

Companion knowledge doc:
[Phase 7 knowledge](../../knowledge/phase-7-blue-green.md).
Failure classification and recovery mechanics live in
[Phase 5 knowledge](../../knowledge/phase-5-operator-and-ha.md), in the sections
"Nobody is told a node died" and "Every failure this project can have".

## What changed from the phase plan

Three scope decisions, all made during design.

**OTel moved wholesale to Phase 8.** The phase plan put the Collector here, but
its Drill needs Prometheus, node-exporter, and kube-state-metrics, and none of
the three is installed. A Drill that demonstrates a blast-radius boundary cannot
run when neither side of the boundary is observable. Moving it lets the
Collector, the scrapers, the Drill, and the dashboards land in one phase where
each has something to talk to. Phase 7 is renamed **Blue/green and the
deployment mechanism**.

**The recovery exclusion is partially reversed.** The design spec scoped
blue/green for zero-downtime deployment only and assigned all recovery to the
Phase 5 Drills. That holds for infrastructure failures and does not hold for
deployment failures. See "Two families of failure" below.

**No CI.** The phase gained a deployment and rollback mechanism, and lost the
merge-request pipeline that was briefly considered for it. Reasoning in
"Rejected: a CI pipeline".

Budget: the phase plan allowed 9 hours. This design is roughly 12, driven by the
operator uid work and the six Drills. Stated openly rather than absorbed
silently.

## The namespace pair

### Naming

Each side gets its own `metadata.name`, so each is a distinct Flink cluster by
identity as well as by placement.

```
manifests/flink/blue/flinkdeployment.yaml    metadata.name: personalization-blue
manifests/flink/green/flinkdeployment.yaml   metadata.name: personalization-green
```

`metadata.name` becomes `kubernetes.cluster-id`, which Flink stamps on every
object it creates for that cluster: the JobManager Deployment, the Services, the
ConfigMaps, and the `app` label on the pods.

**Tradeoff.** The alternative was keeping `personalization` on both sides and
separating only the storage paths. That would have let `promote.sh` address
either side with one name and a varying `-n` flag, and kept the two directories
byte-identical apart from the namespace, which makes `diff -r` a free drift
check. The rename costs those two things and buys structural distinctness: the
isolation survives even if someone later collapses the storage paths back
together.

**Consequence that is easy to miss.** The `app` label changes per side, so
**three** selectors need per-side values, not one. This spec originally listed
only the first, and the omission was caught in execution on 2026-09-12 after both
JobManagers landed in one Zone:

- `manifests/flink/<side>/pdb.yaml`, the PodDisruptionBudget selector
- `manifests/flink/<side>/flinkdeployment.yaml`, the **jobManager**
  `topologySpreadConstraints.labelSelector`
- `manifests/flink/<side>/flinkdeployment.yaml`, the **taskManager**
  `topologySpreadConstraints.labelSelector`

A `PodDisruptionBudget` whose selector matches nothing is **not an error**. It
reports `ALLOWED DISRUPTIONS` against an expected count of zero and permits every
eviction. Phase 5's Drill C would still appear to pass while proving nothing.

A `topologySpreadConstraint` whose `labelSelector` matches nothing is **also not
an error**, and it is worse, because `whenUnsatisfiable: DoNotSchedule` reads
like a hard guarantee. With no matching pods there is no skew to violate, so the
scheduler places the pods anywhere and reports success. **Observed on
2026-09-12**: both JobManagers on `personalization-lab-worker2`, zone-b, giving a
skew of 2 against `maxSkew: 1`. Phase 5 had recorded one per Zone.

This class of silent selector breakage is the single most dangerous consequence
of the rename. Every selector that names `app` must be checked, not just the
ones in separate files.

### Storage layout

One MinIO bucket, three disjoint prefixes per side.

```
s3://checkpoints/phase-7/blue/checkpoints/<job-id>/
s3://checkpoints/phase-7/blue/ha/personalization-blue/
s3://checkpoints/phase-7/blue/savepoints/

s3://checkpoints/phase-7/green/checkpoints/<job-id>/
s3://checkpoints/phase-7/green/ha/personalization-green/
s3://checkpoints/phase-7/green/savepoints/
```

The three paths behave differently and only one of them is separated by the
rename, which is why both changes are made:

- `high-availability.storageDir` appends `<cluster-id>`, so the rename alone
  would separate it.
- `execution.checkpointing.dir` appends `<job-id>`, **not** cluster-id, so the
  rename does nothing here. Whether two application-mode HA clusters derive the
  same job id is unverified. Splitting the directory makes the question stop
  mattering.
- `execution.checkpointing.savepoint-dir` is flat, with generated file names.
  Collision is not the risk; mixing both sides' savepoints in one listing is.

**Tradeoff.** Doing both is redundant: either change alone makes the HA blob
paths disjoint. The redundancy costs a longer path string and buys independence
from an unverified fact about job-id derivation.

**The failure this prevents.** With shared paths and a shared cluster-id, the
operator's HA data cleanup on deployment deletion wipes
`<storageDir>/<cluster-id>/`, so deleting green would take blue's blobs with it.
And `execution.checkpointing.num-retained` runs a delete over the checkpoint
directory, so whichever side is running would be deleting the other side's
checkpoints.

**Correction to ADR 0006.** That ADR says the standby's JobManager "can attempt
leader election against the active side's HA metadata". That is not the
mechanism. Leader election happens in a ConfigMap, and ConfigMaps are namespaced,
so blue's and green's are separate objects that never meet. The real risk is the
blob store and the cleanup that runs over it. ADR 0006 needs this correction.

### The dashboard

`rest-nodeport.yaml` is **deleted from both sides**. Access is by port-forward:

```bash
kubectl port-forward -n personalization-blue svc/personalization-blue-rest 8081:8081
```

**Why.** A `nodePort` number is allocated cluster-wide, not per namespace, so two
Services cannot both claim 30011. `clusters/kind/` maps host ports 30010 to
30019, of which only 30012 and 30013 are free, and Phase 8 wants both for
Prometheus and Grafana. `extraPortMappings` are fixed when the `kind` container
is created, so adding one means recreating the cluster and losing Kafka's data,
MinIO's buckets, and the pinned `.node-ips`.

**Tradeoff.** Losing the bookmark at `localhost:30011`, and edits to the README
and any Phase 5 or 6 runbook step that cites it. It buys symmetry: giving the
NodePort to blue alone would make blue permanently special, which contradicts
CONTEXT.md's definition of Promotion as directionless, and would leave the Active
Side unreachable after the first promotion.

`kubernetes.rest-service.exposed.type: ClusterIP` stays. Its comment currently
points at `rest-nodeport.yaml` and needs rewriting.

## Code changes

Phase 7 touches `apps/pipeline/`, which the phase plan did not anticipate. A new
image build is required.

### Explicit operator uids

Add `.uid("...")` to all nine stateful operators: the three Kafka sources,
`SessionAggregator`, `CartAbandonmentMatcher`, `ProductChangeJoiner`,
`PromoRuleApplier`, `SignalMerger`, `AsyncRecommendationLookup`, and the Kafka
sink. There are currently zero `.uid()` calls in the pipeline.

**The problem.** Flink matches savepoint state to operators by operator id, and
without an explicit uid that id is derived from the operator's **position in the
job graph**. Verified from the Flink docs:

> Each operator has a default ID derived from its position in the application's
> operator topology. Consequently, an unmodified application can always be
> restarted from its own savepoints. However, the default IDs of operators are
> prone to change if an application is modified.

So without uids, promoting a **changed** job cannot restore. Inserting one
stateless operator shifts every id below it, and the savepoint then carries state
nothing claims.

**Timing constraint.** Adding uids is itself a graph change, so a savepoint taken
before they exist cannot be restored after. They must land at the fresh start,
where there is no state worth keeping. Adding them in Phase 8 would cost a
stateless restart, plus a second one for whatever change motivated it.

**Tradeoff.** Nine lines of code in an otherwise infrastructure phase. It buys
the only thing that makes a topology change promotable, which is the capability
blue/green exists for.

### Explicit operator names

Add `.name("...")` at the same nine call sites. `.uid()` is the stable state key;
`.name()` is the display label. They are different fields.

This is not cosmetic. `status.md` records the cost of its absence in Phase 6
Task 4:

> The scaling report names vertices by hex ID only, and the mapping back to names
> was never captured. Read the mapping before the job restarts; vertex IDs do not
> survive a job graph change.

`.name()` makes the autoscaler's `SCALINGREPORT` and the Flink UI readable
permanently, instead of a mapping that has to be recaptured after every graph
change.

### Constraints that must not drift

`pipeline.max-parallelism: "120"` is baked into the savepoint's key-group layout
and must be identical on both sides. Green is a copy of blue's manifest, so it
already is.

## Transaction identity

Both sides carry the **same** prefix:

```
--transactional-id-prefix=personalization-phase-7
```

**Why shared.** Flink builds transactional ids as
`<prefix>-<subtask>-<counter>`. On restore, a job cleans up the previous run's
open transactions by opening a producer with the **same** id, which bumps
Kafka's producer epoch and fences the old one. Green can only finalise blue's
in-flight transactions if it constructs the same ids. With a different prefix it
commits nothing and aborts nothing, and blue's open transactions block every
`read_committed` consumer at the last stable offset until
`transaction.timeout.ms` expires.

**Tradeoff, and it is the good direction.** A shared prefix means that if both
sides ever run at once, their ids collide and Kafka fences one, which crash-loops
that job. A per-side prefix would avoid the collision and let both commit, so
every Recommendation would be written twice, silently. Shared turns a silent
correctness failure into a loud crash. It does **not** prevent double-processing;
both sources still read every partition. It only means you find out.

**Do not leave it at `phase-6`.** `status.md` records what a stale prefix costs:
a job inheriting a cancelled run's open transactions "spins its abort loop at
100% CPU without ever converging". The abort scan walks `prefix-subtask-counter`
past the last known counter, and a large prior id space makes that scan very
long.

`transaction.timeout.ms` stays at 300000, five minutes. That is the ceiling on
how long a botched promotion blocks consumers, and it must stay below the
broker's `transaction.max.timeout.ms`, which Strimzi leaves at Kafka's default of
fifteen minutes.

## Configuration changes, field by field

Every field this phase adds or changes, with the source for each. Per-side values
are written as `<side>`.

| Field | Where | From | To | Reference |
|---|---|---|---|---|
| `metadata.name` | FlinkDeployment | `personalization` | `personalization-<side>` | [FlinkDeployment reference](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/reference/) |
| `spec.selector.matchLabels.app` | pdb.yaml | `personalization` | `personalization-<side>` | [PodDisruptionBudget](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) |
| `execution.checkpointing.dir` | flinkConfiguration | `s3://checkpoints/phase-6` | `s3://checkpoints/phase-7/<side>/checkpoints` | [Checkpoint config](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/#execution-checkpointing-dir) |
| `high-availability.storageDir` | flinkConfiguration | `s3://checkpoints/phase-6-ha` | `s3://checkpoints/phase-7/<side>/ha` | [Kubernetes HA](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/ha/kubernetes_ha/) |
| `execution.checkpointing.savepoint-dir` | flinkConfiguration | `s3://checkpoints/phase-6-savepoints` | `s3://checkpoints/phase-7/<side>/savepoints` | [Savepoints](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/) |
| `kubernetes.operator.savepoint.format.type` | flinkConfiguration | unset, defaults `CANONICAL` | `NATIVE` | [Operator configuration](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/operations/configuration/) |
| `execution.checkpointing.num-retained` | flinkConfiguration | `"3"` | open decision | [Checkpoint config](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/#execution-checkpointing-num-retained) |
| `--transactional-id-prefix` | spec.job.args | `personalization-phase-6` | `personalization-phase-7`, identical on both sides | [KafkaSink](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/#kafka-sink) |
| `spec.job.initialSavepointPath` | FlinkDeployment | absent | written by `promote.sh` per promotion | [Job management](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/job-management/) |
| `spec.job.upgradeMode` | FlinkDeployment | `savepoint` | unchanged | [Job management](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/job-management/) |
| `.uid(...)`, `.name(...)` | PersonalizationJob | absent on all nine operators | explicit on all nine | [Savepoints, assigning operator IDs](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/#assigning-operator-ids) |

Deleted outright: `manifests/flink/<side>/rest-nodeport.yaml`.

Unchanged and must not drift between sides: `pipeline.max-parallelism: "120"`,
`state.backend.type: rocksdb`, `execution.checkpointing.interval: 10s`,
`jobmanager.scheduler: Adaptive`, and every autoscaler key.

## The promotion mechanism

### Git is the control surface, never the live object

`scripts/promote.sh` makes **no writes** to the cluster. Its only `kubectl` calls
are reads. Every state change goes: edit the file, commit, push,
`argocd app sync`, poll.

**The problem this solves.** The design spec's runbook opens with
`kubectl patch flinkdeployment/blue ... state: suspended`. That changes the live
object while Git still says `running`, so ArgoCD reports the Application
OutOfSync for the entire time green is Active. `selfHeal: false` prevents an
automatic revert, but any manual Sync in that window sets blue back to `running`
and both sides consume the stream. Phase 5's drift Drill deliberately does this
exact thing, so a standing OutOfSync is indistinguishable from that Drill's
setup.

**Tradeoff.** Two commits and two ArgoCD syncs per promotion instead of one
patch, and the script needs push rights plus a logged-in `argocd` CLI. It buys
the elimination of the drift window entirely, and a promotion history: `git log
manifests/flink/` shows every promotion with its timestamp and savepoint path in
the diff.

### The savepoint path is the one thing read from the cluster

`spec` is what you want, and ArgoCD syncs it. `status` is what is true, written
by the controller, and ArgoCD deliberately ignores it. The savepoint location
does not exist until the operator has taken the savepoint, so it can only be
read:

```bash
kubectl get flinkdeployment personalization-blue -n personalization-blue \
  -o jsonpath='{.status.jobStatus.savepointInfo.lastSavepoint.location}'
```

That is a read. It creates no drift. The value then goes into the other side's
file in Git.

**The gate must check two conditions**, not one:

- `status.jobStatus.state == "SUSPENDED"`
- the location string is **non-empty**

Checking only the first is the bug. A job can report SUSPENDED while the location
is still empty, and green would then be committed with
`initialSavepointPath: ""`, which is a legal way to say "fresh start". Every
Shopper's state would be silently discarded.

### Direction is discovered, never declared

CONTEXT.md defines Promotion as directionless, so the script cannot contain
"suspend blue, start green". For each side it reads both sources:

```
want = yq '.spec.job.state' manifests/flink/<side>/flinkdeployment.yaml
have = kubectl get flinkdeployment personalization-<side> -n personalization-<side> \
         -o jsonpath='{.status.jobStatus.state}'
```

Exactly three arrangements are legal:

- **neither side Active** and both `suspended` in Git: a fresh deploy. Pick a
  side, start it with no `initialSavepointPath`.
- **exactly one side** `running` in Git and `RUNNING` live, the other
  `suspended` and not `RUNNING`: a promotion.
- anything else: abort, printing all four cells.

**Why both sides are checked.** The design spec's precondition only verifies that
blue is RUNNING. If both sides were somehow RUNNING already, that check passes,
the script suspends blue, starts green, and reports success. One side is now
running, which looks correct, and nothing signals that the disaster ever
happened.

**Vocabulary trap in the shell.** `spec.job.state` is the operator's lowercase
enum, `running` and `suspended`. `status.jobStatus.state` is Flink's uppercase
job status, `RUNNING`, `SUSPENDED`, `FINISHED`, `RECONCILING`, `FAILED`. They are
never equal as strings. Compare each against its own vocabulary.

**Tradeoff.** Discovery is more code than accepting `--from blue --to green` from
a human, and it needs cluster access before it can decide anything. It buys the
removal of a whole class of mistake: a declared direction can contradict reality
after a rollback, and the check that catches it would be accidental.

**Consequential rename.** `scripts/promote-green.sh` names a direction the script
does not have, and CONTEXT.md's `_Avoid_` list for Promotion already rules out
"going green". The script is `scripts/promote.sh`. The phase plan and the design
spec both name the old file in prose and need editing.

### The image lives per side, and that is the rollback artifact

During a promotion the two manifests **legitimately diverge** on `spec.image` and
`initialSavepointPath`:

```
manifests/flink/blue/   image: 0.1-abc1234   <- the previous version
manifests/flink/green/  image: 0.1-def5678   <- the new version
```

Do not converge them. The Standby Side's manifest, left alone, is a complete
record of what was running before: its image tag and the savepoint it started
from. A rollback is `git revert` of the promotion commit.

**Tradeoff.** `diff -r manifests/flink/blue manifests/flink/green` is no longer
expected to be near-empty. The drift check becomes "the diff should show exactly
these two fields plus the namespace, name, label, and storage paths", which is
weaker but still useful.

## Two families of failure

Telling them apart is the whole skill, and reaching for the wrong one is the
mistake this section prevents.

An **infrastructure failure** means the cluster broke and the deployment is
correct. Flink's own high availability handles every one of them in under a
minute. Promotion is slower, needs a healthy side to snapshot, and fixes nothing
that broke. These stay with the Phase 5 Drills: TaskManager loss, JobManager
loss, node drain, node death, ArgoCD drift.

A **deployment failure** means the deployment itself is the problem. High
availability makes this **worse**, because it faithfully restores the broken
thing and does so forever. No amount of checkpointing fixes a bad jar. Promotion
is the only mechanism.

Full classification, per-case recovery, and the rejected alternatives are in the
Phase 5 knowledge doc, section "Every failure this project can have, and how each
one is recovered". The spec-level decision is:

**Promotion is the mechanism for** deploying a new version, deploying an earlier
version deliberately, and recovering from a bad deployment.

**Promotion is not the mechanism for** any infrastructure failure.

That resolves the phase plan's deferred reconsideration as **partially adopted**.
It needs recording as an amendment to ADR 0006 or as a new ADR. **Open decision,
see the last section.**

### The rule that decides every recovery

Exactly-once ties committed output to completed snapshots. Records written after
a snapshot sit in a transaction that was pre-committed and never committed, so a
`read_committed` consumer never saw them. Reprocessing emits them for the first
time.

**Restoring the newest snapshot of the failed job produces no duplicates and no
gap. Restoring any older snapshot duplicates everything in between.**

Those duplicates are indistinguishable from the originals. A Recommendation
Identity is `(shopperId, generatedAt)`, and `generatedAt` is a Browsing Session's
window end, an event-time value computed from the Clicks themselves. A replay
reproduces it exactly. The `recommendation` topic has no `cleanup.policy`, so it
uses the default `delete` and nothing collapses them.

### Three snapshot sources

`promote.sh` supports three, and the default is always the newest usable one.

**A savepoint taken during this run.** The normal promotion, and the case where
the failed side is healthy enough to snapshot.

**The newest retained checkpoint of the failed side**, discovered under its
checkpoint directory. Used when the job cannot take a savepoint: a crash loop, or
corrupt HA metadata. `externalized-checkpoint-retention: RETAIN_ON_CANCELLATION`
keeps checkpoints after the job dies, and `initialSavepointPath` accepts a
retained checkpoint directory, not only a savepoint.

**An explicit path.** Used when nothing newer survived, or when the newest
snapshot is itself poisoned by a state-corrupting bug. Duplicates for the
interval, accepted and counted.

The savepoint recorded in the Standby Side's manifest from the last promotion
stays as the **floor**, never the default. It is as old as the Active Side's
uptime, so falling back to it replays everything since.

**Tradeoff, quantified.** Always using that floor would be simpler: one code
path, and the value is already in Git needing no discovery. At the Drill rates in
`status.md`, six hours of uptime works out to roughly 1.1 million duplicate
Recommendations, against zero for the newest checkpoint. Three snapshot sources
instead of one is the price of not doing that.

### The retention window is thirty seconds

`execution.checkpointing.num-retained: "3"` at
`execution.checkpointing.interval: 10s` keeps thirty seconds of history. The
newest-checkpoint recovery path depends entirely on that window. Notice a crash
loop later than thirty seconds and the path is gone.

**Open decision, see the last section.** Raising it to 10 gives 100 seconds at
the cost of more objects in MinIO.

## Minimizing the promotion pause

### What "downtime" means here

No Recommendation is lost. Kafka holds the input and green resumes from blue's
exact offsets. The pause is a **latency spike**, not a loss: the `recommendation`
topic goes quiet, then produces a burst as green works through the backlog.

### Where the time goes

```
t0  blue's job stops processing          <- the pause starts here
t1  savepoint fully written
t2  script reads the path, commits, pushes
t3  ArgoCD fetches from GitHub and applies green
t4  green's JobManager pod scheduled, JVM up
t5  green's TaskManagers created, scheduled, JVM up, registered
t6  state restored from the savepoint
t7  green RUNNING                        <- the pause ends here
```

Rough budget, to be replaced by Drill 2's measurement: savepoint write 5 to 30s,
script poll and push 2 to 5s, ArgoCD fetch and apply 5 to 20s, JobManager pod and
JVM 15 to 25s, TaskManagers 20 to 40s, state restore 5 to 20s. Total 60 to 140s.

### The one config lever taken

```yaml
kubernetes.operator.savepoint.format.type: NATIVE
```

Documented as: "Type: Enum. Default: `CANONICAL`. Type of the binary format for
savepoints (CANONICAL or NATIVE)."
[Operator configuration reference](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/operations/configuration/)

CANONICAL reads all state and rewrites it into a backend-independent format so a
savepoint can move between state backends and Flink versions. That rewrite is the
cost, and it is paid twice: once writing at t0 to t1, once restoring at t6.
NATIVE writes RocksDB's own format with no translation.

**Tradeoff.** A NATIVE savepoint is tied to the state backend that wrote it. Both
sides run `state.backend.type: rocksdb` from the same image, so nothing here
needs that portability. The option given up was never going to be used.

### Two smaller levers

The script polls at 1 second rather than 5. And it calls `argocd app sync`
rather than waiting out ArgoCD's three-minute reconcile poll, which was already
decided for correctness reasons and happens to save up to three minutes.

### No target is set before measuring

Drill 2 measures the real pause on this cluster and records it. A target invented
now would be a guess, and the budget above is arithmetic, not observation.

## Alternatives considered and rejected

### Rejected: pre-warming the Standby Side

Starting green's JobManager and TaskManagers before blue suspends would remove
the two most expensive stages, about 40 to 65 seconds, roughly halving the pause.

**It is not available in application mode.** Verified from the operator docs, on
session clusters:

> Session clusters allow for the creation of a Flink deployment without an
> initial job. This approach separates the lifecycle of the cluster
> infrastructure from the submission of individual Flink jobs.

And on `FlinkSessionJobSpec.job`: "Null for session clusters."

In application mode, which `spec.job` makes it, the cluster and the job are one
object with one lifecycle. The JobManager pod's purpose is to run `main()` and
submit the job. There is no state where the pod exists and the job does not. So
"create green's pods early" means "start green's job early", and green would
immediately begin consuming Kafka alongside blue. That is the double-processing
ADR 0006 exists to prevent.

**What session mode would actually buy, by variant.** Native session gets a warm
JobManager only, because TaskManagers are created on demand by the JobManager's
ResourceManager in response to slot demand, and no job means no demand. That is
ADR 0005's own finding that `spec.taskManager.replicas` is meaningless in
`mode: native`. Saving: 15 to 25 seconds. Standalone session gets both warm,
because `spec.taskManager.replicas` does create an independent TaskManager
Deployment. Saving: 40 to 65 seconds.

**Rejected because** the standalone variant reverses ADR 0006 and requires
re-validating all of Phase 5's HA work, zone spread, and PodDisruptionBudget
under a deployment model that ADR explicitly excluded, and splits each side into
two custom resources so the script manipulates a `FlinkSessionJob` rather than a
`FlinkDeployment`. Halving the pause does not justify undoing the phase the lab
was built toward. The native variant saves 15 to 25 seconds for the same
structural change, which is worse value still.

### Rejected: true zero downtime

For no gap at all, green would have to be producing before blue stops. That means
both running, both consuming every partition, and every Recommendation twice.
The shared transactional id prefix would fence one of them anyway, so the attempt
fails loudly rather than working.

The only escape is green writing to a **different topic** while it catches up,
then switching consumers. That is rejected separately below, for the same reason
it is rejected as a duplicate remedy: it moves the problem onto every consumer.

**So a bounded, measured pause is the honest position**, and the phase plan's own
wording already says "bounded pause" rather than zero.

### Rejected: a CI pipeline

A hosted runner cannot do the work. `scripts/build-image.sh` ends with
`kind load docker-image`, which copies an image into `kind` node containers on
the local docker daemon. A datacentre machine has a different daemon and no
cluster. The same holds after the build: `kubectl` needs an API server the
kubeconfig points at on loopback, `argocd app sync` needs `localhost:30010`
through this cluster's `extraPortMappings`, and node IPs are pinned per host into
the gitignored `.node-ips`.

So the runner would have to be self-hosted on this machine, whichever forge is
used. GitLab's shared runners hit the same wall; a locally registered GitLab
Runner works, but that is running a runner yourself, and it needs the `shell`
executor or each job gets a fresh container with no cluster access.

**Given the runner is on this machine either way**, CI adds a trigger you did not
type, a re-readable log, and step ordering. A script gives the last two, and
`tee` gives the log. The first matters when a team shares a deploy and not when
one person on one laptop does it.

**What it would cost**: runner registration and tokens, workflow YAML, a
commit-loop guard because the script itself pushes commits, and path filters so a
doc-only merge does not promote. On GitLab it additionally costs a new remote,
`repoURL` rewritten in seven ArgoCD Application files, and ArgoCD credentials.
None of that teaches Kubernetes or Flink.

**And a queued job is worse than no job.** A self-hosted runner only picks up
work while the laptop is awake.

### Rejected: replaying into a fresh topic

Restore the older savepoint writing to `recommendation-v2`, catch up, switch
consumers, delete the old topic. It gives one clean topic and no duplicates in
it.

**Rejected because it only fixes the stored log.** Any consumer that already read
the duplicated range has been served both copies. It solves the smaller half of
the problem and adds a topic per replay.

### Rejected: compacting `recommendation` to collapse duplicates

Three separate reasons. The record key today is `shopperId`, and compacting on it
would keep only the newest Recommendation per Shopper and delete the history,
which is the opposite of the goal. Making it work needs the key to become
`shopperId` plus `generatedAt`, which scatters one Shopper's Recommendations
across all three partitions and destroys per-Shopper ordering, and breaks
CONTEXT.md's "read directly off the Kafka record as the key". And compaction is
asynchronous garbage collection rather than deduplication: it runs on closed
segments subject to `min.cleanable.dirty.ratio`, so duplicates stay visible for
an unbounded window and live consumers never benefit at all.

**Accepted instead: accept and count.** Duplicates from a deliberate replay are
permitted and counted, using the Recommendation Identity to detect them.

### Rejected: the Standby Side retaining a "last known good" savepoint

Having the script track each side's outgoing savepoint path in a tracked file, so
a future recovery could reach for it.

**Rejected because** it makes the Standby Side hold state the next recovery
depends on, contradicting its definition in CONTEXT.md, and because it adds a
stale snapshot that is attractive to reach for and almost always the wrong
choice. The newest snapshot of the failed side is nearly always available and
nearly always better.

Consequently **CONTEXT.md's Standby Side entry needs no edit.** Green's old
savepoint files still sit in its savepoint directory, but nothing references
them.

### Rejected: `--allowNonRestoredState` as a general escape

**Rejected rolling forward.** When a topology change shifts operator ids, the flag
makes the job start by silently discarding every piece of state it could not
place: open Browsing Sessions, partial CEP matches, join buffers, and the Kafka
offsets. The job then reports `RUNNING` and looks healthy. That is a stateless
restart wearing a costume, and worse than an honest failure.

**Accepted rolling back**, narrowly. When rolling back to an image whose job graph
lacks an operator the newer one added, the unplaceable state genuinely belongs to
an operator that does not exist in the target graph. Nothing is lost that
anything wanted.

### Rejected: blue/green as failover for infrastructure failures

Every failure in that family is already handled in under a minute by mechanisms
that need no human and no second namespace. Promotion is slower, needs a healthy
side to snapshot, and fixes a category of problem that has not occurred.

### Rejected: keeping `metadata.name` identical on both sides

Covered under "Naming" above. It would have kept the script simpler and the
directory diff tighter. Rejected in favour of structural distinctness.

### Rejected: a NodePort dashboard on the Active Side only

Covered under "The dashboard" above. Rejected because it makes blue permanently
special, contradicting directionless Promotion.

## `scripts/promote.sh`

One entry point. Discovery decides the outcome.

```
promote.sh                              fresh deploy or promotion, decided by discovery
promote.sh --recover                    snapshot from the newest chk-N of the failed side
promote.sh --recover --from-snapshot P  snapshot from the explicit path P
promote.sh --recover --image T          roll the image back to tag T
```

Normal mode requires exactly one side Active and aborts otherwise. Recovery mode
asserts the **opposite** precondition and refuses to run when both sides look
healthy, because the normal gate would reject the very situation recovery exists
for.

`--recover` is deliberately **not** reachable from any automated trigger. It runs
by hand, on this machine, with a path a human found in MinIO.

## The Drills

Six, each with a runbook in `docs/runbooks/`, same shape as the seven that exist:
procedure, rationale per command, and an **Observed result** section carrying the
real transcript.

1. **Fresh deploy.** Both sides down, one becomes Active. Proves the discovery
   branch and the no-savepoint start.
2. **Promotion under a Load Ramp.** Measure the pause. Check **No Gap** against
   the `recommendation` topic across the promotion.
3. **Promotion back the other way.** Proves the script is directionless in
   practice and not only in intent.
4. **Break the Standby Side on purpose.** Exercises the step-5 timeout, the one
   failure mode with no automatic safe path: the Active Side is already suspended
   and the Standby will not come up.
5. **Rollback as deployment.** `git revert` the image change, promote back. Both
   sides healthy throughout.
6. **Rollback as recovery.** Deploy a deliberately broken image, watch the crash
   loop, recover from the newest retained checkpoint. This is the Drill that
   proves the whole recovery design, and no CI would have added anything to it.

## Vocabulary

CONTEXT.md gains one entry beside **No Gap**:

> **No Loss**: every Recommendation identity present before a deliberate replay
> is still present after it. Duplicates are expected and are counted, not treated
> as failures. Distinct from **No Gap**, which additionally forbids duplicates
> and is the criterion for Drills.

A Drill is judged by No Gap. A deliberate replay is judged by No Loss. Neither
definition has to be weakened to accommodate the other.

## Documents to amend

- **ADR 0006**: correct the leader-election claim, and record the partial
  reversal of the recovery exclusion.
- **ADR 0005**: its Decision still needs the Phase 6 amendment for the dropped
  KEDA scope, carried over and still outstanding.
- **The phase plan**: rename Phase 7, move OTel to Phase 8, replace
  `promote-green.sh` with `promote.sh`, restate the budget.
- **The design spec**: its "Zero-downtime deployment" runbook is superseded here,
  and its concept map line about "savepoint-based redeploys when rule logic
  changes" now depends on the uid work landing in this phase.
- **CONTEXT.md**: add **No Loss**.
- **README**: replace the `localhost:30011` dashboard entry with the port-forward
  command, and note 30011 is free.
- **Phase 5 and 6 runbooks**: any step citing `localhost:30011`.

## Facts to establish before this spec is final

Phase 5 set the standard that every design decision is settled with evidence
before the plan is written, so no task carries a verification gate. Five facts
here are not yet settled, and each has one command.

**That `kubernetes.operator.savepoint.format.type` is honoured per resource.**
The operator's own settings normally live in its Helm values, and the operator
docs note that FlinkDeployment-level entries drop the `kubernetes.operator.`
prefix for some keys. Whether this one is accepted inside a FlinkDeployment's
`spec.flinkConfiguration` as a per-resource override needs confirming before the
pause budget depends on it.

```bash
kubectl get flinkdeployment personalization-blue -n personalization-blue \
  -o jsonpath='{.status.clusterInfo}'; echo
# and, after a promotion, whether the savepoint directory contains a
# _metadata file in native rather than canonical layout
```

**SETTLED 2026-09-10. What `status.jobStatus.state` reads on a FlinkDeployment
that has never run:** nothing. `.status` is `{}` entirely, so the field is absent
and the jsonpath yields an empty string. Discovery therefore tests "is it
RUNNING" and never compares against the literal `SUSPENDED`.

```bash
kubectl get flinkdeployment personalization-green -n personalization-green \
  -o jsonpath='{.status.jobStatus.state}'; echo
```

**That the `app` label carries the cluster-id.** The per-side PDB selector
depends on it, and a wrong selector fails silently.

```bash
kubectl get pods -n personalization-blue --show-labels
```

**SETTLED 2026-09-10. The REST Service name** follows the operator's
`<cluster-id>-rest` convention: the live Service is `personalization-rest`,
ClusterIP on 8081, beside the NodePort this phase deletes. After the rename it
becomes `personalization-blue-rest`.

```bash
kubectl get svc -n personalization-blue
```

**Whether `execution.checkpointing.num-retained` should rise from 3.** Thirty
seconds of recovery history is the current window, and Drill 6 will show whether
that is enough to notice a crash loop and act.

Two decisions are open and need an answer rather than a command:

- ADR 0006 amendment, or a new ADR 0010 for promotion-as-recovery scope.
- `num-retained`, once Drill 6 has been thought through.
