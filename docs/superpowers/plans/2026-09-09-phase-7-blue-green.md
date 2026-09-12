# Phase 7 Blue/green implementation plan

**Goal:** Stand up a second namespace, build one script that moves the Active
Side from either namespace to the other through Git, and prove with six Drills
that a deployment, a rollback, and a recovery from a bad deployment all work.

**Architecture:** Two `FlinkDeployment` resources with distinct cluster ids and
disjoint MinIO prefixes, one Active at a time. `scripts/promote.sh` reads both
sides to discover direction, commits the state change to Git, asks ArgoCD to
sync, and polls. It never patches a live object, so the cluster and the
repository never disagree. State moves between sides as an absolute
`initialSavepointPath`, which is a file the incoming side reads out of MinIO;
the two clusters never talk to each other.

**Tech Stack:** Flink 2.2.0, Flink Kubernetes Operator 1.15.0, `kind`, Strimzi,
MinIO, ArgoCD, Gradle 9.7.0, `yq`, `argocd` CLI, `mc`.

**Spec:** [Blue/green and the deployment mechanism](../specs/2026-09-09-blue-green-and-deployment-design.md)

**Knowledge:** [Phase 7 knowledge](../../knowledge/phase-7-blue-green.md) for the
two tradeoffs, and
[Phase 5 knowledge](../../knowledge/phase-5-operator-and-ha.md) for the failure
classification and the four clocks. This plan does not re-explain mechanism.

## How to use this plan

This project's [CLAUDE.md](../../../CLAUDE.md) working agreement governs, and it
overrides the default shape of a plan document.

**You write every file and run every command.** Each task gives the goal, the
concept behind it, the failure mode to watch for, a skeleton or a key fragment,
and the command that proves it worked. It deliberately does not give finished
files.

**A task is done when its verification command produces real output you have
read.** Not when a manifest applies. Not when ArgoCD says Synced. Three of this
phase's failure modes are silent: a PodDisruptionBudget selecting nothing, an
empty `initialSavepointPath`, and a savepoint restore that dropped state under
`--allowNonRestoredState`. All three report success.

**This plan prescribes no commits.** Git is yours to drive.

**Five facts were not settled at design time**, because they need a running
cluster. Each is a gate inside the task that needs it, with both branches
written out: Task 3 Step 8, Task 4 Step 5, Task 4 Step 6, Task 7 Step 7, and
Task 11 Step 1. If you find any other undecided question in this plan, it is a
defect.

## Prerequisite

Phase 6 closed on 2026-09-09, all ten tasks with four Drill transcripts. What
this phase inherits is listed in
[status.md](status.md#what-phase-7-inherits-from-phase-6). Six of those items
change what a task here may assume, and they are repeated in the global
constraints below rather than left as a cross-reference.

## Progress

| # | Task | Status |
|---|---|---|
| 0 | Baseline capture | ✅ done, 2026-09-10 |
| 1 | `recommendation-snapshot.sh` gains a time window | ✅ done, 2026-09-10 |
| 2 | Operator uids and names, and a new image | ✅ done, 2026-09-10 |
| 3 | Blue's manifest: rename, paths, savepoint format | 🟡 files done, sync pending a push |
| 4 | Green's directory and Application | 🟡 files done, sync pending a push |
| 5 | `scripts/promote.sh`: discovery and promotion | 🟡 written, dry run pending Task 3's sync |
| 6 | Drill 1: fresh deploy | ⬜ |
| 7 | Drill 2: promotion under a Load Ramp | ⬜ |
| 8 | Drill 3: promotion back | ⬜ |
| 9 | Drill 4: break the Standby Side | ⬜ |
| 10 | `scripts/promote.sh --recover` | ⬜ |
| 11 | Drill 5: rollback as deployment | ⬜ |
| 12 | Drill 6: rollback as recovery | ⬜ |
| 13 | Documents | ⬜ |

Tasks 1 and 2 are independent of each other and of everything else; both can be
done first. Task 3 needs Task 2's image tag. Task 4 needs Task 3. Task 5 needs
Task 4. Drills 1 to 4 are strictly sequential, because each leaves the cluster in
the state the next one starts from. Task 10 needs Task 5 and can be written while
Drills 1 to 4 run. Task 12 needs Task 10.

**Per-task measurements and the traps they expose go in
[status.md](status.md), not here.** This table carries position only.

## Global constraints

Every task's requirements implicitly include this section.

- **Read the live parallelism. Never assume a number.** Phase 6 ran the job at 1,
  2, 3 and 6 with 1 to 3 TaskManagers, all without a restart. No runbook step,
  no assertion, and no snapshot window may hardcode a pod count or a parallelism.
- **A promotion restarts the job, so parallelism drops to
  `spec.job.parallelism`, which is `2`.** The autoscaler then climbs again from
  the `autoscaler-personalization` ConfigMap. This is by design and it is part of
  the pause Drill 2 measures. Do not read it as a regression.
- **Never run a Karpenter Drill and a promotion at the same time.** Phase 6's
  Drill H found that a kwok node crashes `kindnet` on the real nodes, and a
  promotion depends on pod networking across two namespaces. Check for kwok nodes
  before every Drill in this phase.
- **`--transactional-id-prefix` is `personalization-phase-7`, identical on both
  sides.** Not per-side. Leaving it at `phase-6` inherits that run's open
  transactions and spins the sink's abort loop at 100% CPU.
- **`pipeline.max-parallelism: "120"` must be identical on both sides.** It is
  baked into the savepoint's key-group layout, and a mismatch fails every
  restore.
- **Host headroom is 2.5 GiB and is the binding constraint.** Two namespaces do
  not double the load, because only one side has pods at a time, but a failed
  promotion can leave both sides with pods. Watch memory during Drill 4.
- **`--start-from-earliest` defaults to `true`**, at
  `apps/pipeline/src/main/java/lab/personalization/pipeline/PipelineConfig.java:54`.
  Any start without an `initialSavepointPath` replays the whole `clickstream`
  topic, which spans Phases 3 to 6. That is why Task 1 exists and why Drill 1
  takes its baseline after catch-up, not before.
- **`:pipeline` changes are limited to Task 2.** Task 2 adds `.uid()` and
  `.name()` and nothing else. No operator may be added, removed, or reordered in
  this phase, because that is the change the uids are being added to make
  possible *later*.
- **`apps/pipeline/conf/config.yaml` does not exist.** Every configuration key
  lives in `spec.flinkConfiguration` alone. Do not recreate it.

## Files this phase touches

```
apps/pipeline/src/main/java/lab/personalization/pipeline/
  PersonalizationJob.java             Task 2, .uid() and .name() at ELEVEN call sites

manifests/flink/blue/
  flinkdeployment.yaml                Task 3, rename + paths + savepoint format
  pdb.yaml                            Task 3, per-side selector
  rest-nodeport.yaml                  Task 3, DELETED

manifests/flink/green/                Task 4, new directory
  flinkdeployment.yaml                a copy of blue's with seven fields changed
  pdb.yaml                            a copy of blue's with two fields changed

manifests/argocd-apps/
  flink-job-green.yaml                Task 4, new Application

scripts/
  recommendation-snapshot.sh          Task 1, a time window and a SIGPIPE fix
  promote.sh                          Task 5 creates it, Task 10 adds --recover
  build-image.sh                      Task 3, its final-line hint names blue only

clusters/kind/kind-cluster.yaml       Task 13, comment only: 30011 is free

docs/runbooks/                        Tasks 6 to 12
  phase-7-drill-1-fresh-deploy.md
  phase-7-drill-2-promotion.md
  phase-7-drill-3-promotion-back.md
  phase-7-drill-4-standby-timeout.md
  phase-7-drill-5-rollback-deploy.md
  phase-7-drill-6-rollback-recovery.md

docs/adr/0006-blue-green-native-mode.md          Task 13, two corrections
CONTEXT.md                                       Task 13, No Loss entry
docs/superpowers/plans/2026-08-10-implementation-phases.md   Task 13, Phases 7 and 8
docs/superpowers/specs/2026-07-25-flink-k8s-personalization-design.md  Task 13
docs/knowledge/phase-7-blue-green.md             Task 13, already drafted
README.md                                        Task 13, dashboard access
docs/superpowers/plans/status.md                 Task 13
```

Unchanged, and worth stating: `manifests/flink/namespaces.yaml` already declares
both namespaces, and `scripts/bootstrap.sh` already carries
`FLINK_NAMESPACES=(personalization-blue personalization-green)` at line 218, so
the `minio-credentials` Secret lands in green with no change. Verify it rather
than assume it, in Task 4 Step 4.

`manifests/argocd-apps/flink-job-blue.yaml` needs **no** `ignoreDifferences`.
Phase 6 Task 5 Step 6 probed for autoscaler writes to the CR and came back empty.

---

## Task 0: Baseline capture

**Files:** none. Read-only.

**Goal.** Record what is running before anything changes, so every later gate has
something to compare against.

**Why this is a task.** Phase 6's Task 0 found two facts the design did not have,
including that host headroom had halved. Capture first, then change.

**The failure mode to watch for.** Taking the baseline after starting the
generator. The Load Ramp changes parallelism, so a baseline taken under load is
not a baseline.

- [ ] **Step 1: Confirm no kwok nodes are present.**

```bash
kubectl get nodes -o wide | grep -i kwok || echo "no kwok nodes, safe to proceed"
```

A kwok node crashes `kindnet`. If any are present, scale the Decoy Workload to
zero and let Karpenter consolidate before continuing.

- [ ] **Step 2: Record the live job.**

```bash
kubectl get flinkdeployment -A
kubectl get pods -n personalization-blue -o wide
kubectl get pdb -n personalization-blue
kubectl get svc -n personalization-blue
```

Record the parallelism, the TaskManager count, the pod-to-node placement, the
PDB's `ALLOWED DISRUPTIONS`, and the Service names. The last two are the
before-picture for Task 3's rename.

- [ ] **Step 3: Record the current MinIO layout.**

```bash
source ./scripts/minio-env.sh
mc ls --recursive s3/checkpoints/ | awk '{print $NF}' | cut -d/ -f1-2 | sort -u
```

Expect `phase-6/...`, `phase-6-ha/...` and `phase-6-savepoints/...`. Phase 7 uses
`phase-7/<side>/...`, so nothing here is reused, and this listing is what tells
you the old prefixes can be deleted later if space matters.

- [ ] **Step 4: Record host headroom.**

```bash
free -g
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}'
```

Phase 6 Task 0 measured 2.5 GiB free. If it has fallen further, Drill 4 is the
task at risk, because a failed promotion can leave both sides with pods.

- [ ] **Step 5: Record the current image tag and the topic tail.**

```bash
kubectl get flinkdeployment -n personalization-blue -o jsonpath='{.items[0].spec.image}'; echo
./scripts/recommendation-snapshot.sh snapshot /tmp/phase-7-baseline.txt
```

The snapshot will read the whole topic and will be slow. That is the problem
Task 1 fixes. Run it once here anyway, because the record count is the number
Task 1's time window is validated against.

---

## Task 1: `recommendation-snapshot.sh` gains a time window

**Files:**
- Modify `scripts/recommendation-snapshot.sh`

**Goal.** Make the duplicate count mean something, by letting a snapshot cover
only the window a Drill spans.

**The concept.** `kcat -C -e` reads a topic from the beginning to the end. The
`recommendation` topic now spans Phases 3 to 6 and at least one full source
replay, so `snapshot` returns records from runs that have nothing to do with the
Drill you are running. A duplicate count over that range is dominated by history.

`kcat` takes `-o s@<milliseconds>` to start at the first offset whose timestamp
is at or after a wall-clock time, and `-o e@<milliseconds>` to stop. That is the
window a Drill needs.

**The failure mode to watch for.** Confusing the record **timestamp** with
`generatedAt`. The script's header already records that `generatedAt` is carried
in the record timestamp, so `-o s@` filters on the same field the identity uses.
That is correct here, and it means a replay of old Clicks produces records whose
timestamps are old, so a window keyed on wall-clock start time will **not** catch
them. Say so in the script's comments rather than discovering it in Drill 6.

- [ ] **Step 1: Add an optional start-time argument to `snapshot`.**

Skeleton, not a finished file:

```bash
# usage: snapshot <outfile> [since-epoch-ms]
SINCE="${2:-}"
OFFSET_ARGS=()
[[ -n "${SINCE}" ]] && OFFSET_ARGS=(-o "s@${SINCE}")

kcat -b "${BROKER}" -t "${TOPIC}" -C -e "${OFFSET_ARGS[@]}" ...
```

- [ ] **Step 2: Fix the SIGPIPE exit.**

Phase 6 recorded that the script exits 141 when it lists duplicates. That is a
downstream `head` or `sort` closing the pipe under `set -o pipefail`. Decide
deliberately whether to drop `pipefail` for that one pipeline or to buffer the
list first. Do not leave a script that reports failure when it succeeded.

- [ ] **Step 3: Print the window in the snapshot header.**

A snapshot file that does not say which window it covers cannot be compared to
another one safely. Write the `since` value and the record count into the file's
first line.

- [ ] **Step 4: Verify against Task 0's full-topic count.**

```bash
NOW=$(( $(date +%s) * 1000 ))
./scripts/recommendation-snapshot.sh snapshot /tmp/win-test.txt "$(( NOW - 60000 ))"
wc -l /tmp/win-test.txt /tmp/phase-7-baseline.txt
```

Expected: the windowed file is dramatically smaller, and its count is plausible
against 60 seconds at the current rate. If they are the same size, `-o s@` was
not applied.

- [ ] **Step 5: Verify the exit code.**

```bash
./scripts/recommendation-snapshot.sh compare /tmp/win-test.txt /tmp/win-test.txt; echo "exit=$?"
```

Expected: `exit=0`. Comparing a file to itself is the degenerate case, and it
must not exit 141.

---

## Task 2: Operator uids and names, and a new image

**Files:**
- Modify `apps/pipeline/src/main/java/lab/personalization/pipeline/PersonalizationJob.java`

**Goal.** Give every stateful operator a stable identity, so a later topology
change can still restore from a savepoint.

**The concept.** Without `.uid()`, Flink derives an operator's id from its
position in the job graph. The Flink docs put it plainly: "the default IDs of
operators are prone to change if an application is modified." So today a
promotion can only carry state when both sides run a byte-identical graph.

`.name()` is a separate field. It is the display label, and its absence is
already costing you: Phase 6 Task 4 could only identify vertices by hex id and
had to note "read the mapping before the job restarts".

**The failure mode to watch for.** This change **is itself** a graph identity
change. Every savepoint taken before it cannot be restored after it. That is
fine, because Phase 7 starts fresh by decision, but it means Task 2 must land
before Task 3 and not after.

**Why all of them at once.** A partial set is worse than none: you would have a
graph where some operators survive a change and others do not, and no way to
predict which restores will fail.

**Correction, found while executing on 2026-09-10: there are ELEVEN call sites,
not nine.** The count this plan first gave listed ten items and called them nine.
The eleventh is the stateless `ShopperSignal` map at `PersonalizationJob.java`,
which holds no state and needs no uid for its own sake. Pin it anyway, because
`--debug-prints` conditionally adds print sinks to the graph; with every other id
pinned, flipping that flag adds and removes nodes without moving anyone else's
id.

- [ ] **Step 1: Add `.uid()` and `.name()` to the three sources.**

```java
DataStream<Click> clicks = env.fromSource(
        clickSource(config), watermarks, "clickstream")
    .uid("source-clickstream")
    .name("source-clickstream");
```

`env.fromSource(...)` returns a `SingleOutputStreamOperator`, so both methods
chain. The third argument to `fromSource` is a source name and is **not** the
same thing as `.name()`; set both.

- [x] **Step 2: Add them to the six stateful operators and the map.**

`SessionAggregator`, `CartAbandonmentMatcher`, `ProductChangeJoiner`,
`PromoRuleApplier`, `SignalMerger`, `AsyncRecommendationLookup`, and the
`ShopperSignal` map.

Pick uid strings that describe the operator's job, not its position. Never
include a number that would tempt someone to renumber.

- [ ] **Step 3: Add them to the sink.**

```java
recommendations.sinkTo(recommendationSink(config))
    .uid("sink-recommendation")
    .name("sink-recommendation");
```

`sinkTo` returns a `DataStreamSink`, which carries `.uid()` and `.name()` too.

- [ ] **Step 4: Run the pipeline tests.**

```bash
apps/gradlew -p apps :pipeline:test
```

Expected: the 27 tests that passed in Phase 6 still pass. Read the count out of
the XML report, not the console summary.

- [ ] **Step 5: Build the image and record the tag.**

```bash
./scripts/build-image.sh
```

The last line is the tag. Task 3 needs it. If the tag ends in `-dirty`, commit
`apps/` first and rebuild, because a `-dirty` tag does not identify what is in
the image.

---

## Task 3: Blue's manifest, rename, paths, savepoint format

**Files:**
- Modify `manifests/flink/blue/flinkdeployment.yaml`
- Modify `manifests/flink/blue/pdb.yaml`
- Delete `manifests/flink/blue/rest-nodeport.yaml`
- Modify `scripts/build-image.sh`, the final hint line only

**Goal.** Turn blue into one of a symmetric pair, and start it clean on the new
image.

**Why this is one task and not five.** Changing `metadata.name` is a delete and a
create as far as ArgoCD is concerned, with `prune: true` on the Application. The
running job dies either way. Landing the storage paths, the prefix, and the image
separately would put the job through several needless restarts, each of which is
a spec change under `upgradeMode: savepoint` and each of which attempts a restore
that cannot succeed.

**The concept.** `metadata.name` becomes `kubernetes.cluster-id`, and Flink
stamps that on every object it creates, including the `app` label on the pods.
That is why `pdb.yaml` moves in the same commit.

**The failure mode to watch for, and it is silent.** A `PodDisruptionBudget`
whose selector matches no pods is not an error. It reports `ALLOWED DISRUPTIONS`
against an expected count of zero and permits every eviction. Phase 5's Drill C
would still appear to pass while proving nothing. Step 8 is the gate for this.

- [ ] **Step 1: Rename the deployment.**

```yaml
metadata:
  name: personalization-blue
  namespace: personalization-blue
```

- [ ] **Step 2: Split the three storage paths.**

```yaml
    execution.checkpointing.dir: s3://checkpoints/phase-7/blue/checkpoints
    high-availability.storageDir: s3://checkpoints/phase-7/blue/ha
    execution.checkpointing.savepoint-dir: s3://checkpoints/phase-7/blue/savepoints
```

The rename alone would separate only the second of these, because
`high-availability.storageDir` appends the cluster id while
`execution.checkpointing.dir` appends the **job id**. Both changes are made so
the job-id question stops mattering.

- [ ] **Step 3: Set the savepoint format.**

```yaml
    kubernetes.operator.savepoint.format.type: NATIVE
```

`CANONICAL` is the default and rewrites all state into a backend-independent
format, paying that cost twice, once writing and once restoring. `NATIVE` writes
RocksDB's own format. Portability across state backends is given up, and both
sides run `rocksdb` from the same image. Whether this key is honoured inside
`spec.flinkConfiguration` is the gate in Task 7 Step 7.

- [ ] **Step 4: Move the transactional id prefix to phase 7.**

```yaml
      - "--transactional-id-prefix=personalization-phase-7"
```

- [ ] **Step 5: Point `spec.image` at Task 2's tag.**

- [ ] **Step 6: Fix the PDB selector.**

```yaml
  selector:
    matchLabels:
      app: personalization-blue
      component: jobmanager
```

- [ ] **Step 7: Delete `rest-nodeport.yaml`, and check `build-image.sh`.**

The Service is pruned on the next sync. `build-image.sh`'s closing comment tells
the reader to paste the tag into `manifests/flink/blue/flinkdeployment.yaml`;
there are two sides now, so it should name whichever side is Standby.

`kubernetes.rest-service.exposed.type: ClusterIP` **stays**. Its inline comment
points at the file you just deleted and needs rewriting to name port-forward.

- [ ] **Step 8: Sync, then run the gate on the `app` label.**

```bash
argocd app sync flink-job-blue
kubectl get pods -n personalization-blue --show-labels
kubectl get pdb -n personalization-blue
kubectl get svc -n personalization-blue
```

**Gate, both branches.**

**Half-settled on 2026-09-10, before the rename.** The live pods carry
`app=personalization`, matching the then-current `metadata.name`, and the PDB
reported `ALLOWED DISRUPTIONS 1` against 2 JobManagers. That is consistent with
the label carrying the cluster id but does not prove it, because both strings
were `personalization`. The rename is what makes the test conclusive.

The REST Service is **`personalization-rest`**, ClusterIP on 8081, confirming the
operator's `<cluster-id>-rest` convention. After the rename expect
`personalization-blue-rest`, and that is the name the README's port-forward
command should use.

- If the pod labels show `app=personalization-blue` and the PDB reports
  `ALLOWED DISRUPTIONS 1` against 2 expected pods, the selector is correct.
- If the PDB shows an expected count of **0**, the selector does not match. Read
  the actual `app` value off the pods and use it. Do not guess a second time.

- [ ] **Step 9: Confirm the clean start.**

```bash
kubectl get flinkdeployment personalization-blue -n personalization-blue \
  -o jsonpath='{.status.jobStatus.state}{"\n"}{.spec.job.upgradeSavepointPath}{"\n"}'
source ./scripts/minio-env.sh
mc ls s3/checkpoints/phase-7/blue/
```

Expected: `RUNNING`, an empty savepoint path, and `checkpoints/` and `ha/`
appearing under `phase-7/blue/`. An empty savepoint path beside `RUNNING` is what
proves it started clean rather than silently restoring something.

---

## Task 4: Green's directory and Application

**Files:**
- Create `manifests/flink/green/flinkdeployment.yaml`
- Create `manifests/flink/green/pdb.yaml`
- Create `manifests/argocd-apps/flink-job-green.yaml`

**Goal.** Give green everything blue has, suspended, so the pair is symmetric and
`promote.sh` has two sides to discover.

**The concept.** Green is a copy of blue with a fixed, short list of differences.
Keeping that list short is what makes `diff -r manifests/flink/blue
manifests/flink/green` a usable drift check for the rest of the project's life.

**The failure mode to watch for.** Copying blue's `spec.image` and forgetting that
green must start **suspended**. A green that comes up `running` while blue is also
running means both sides consume every partition, and the shared transactional id
prefix will fence one sink into a crash loop.

- [ ] **Step 1: Copy blue's two files and change exactly these fields.**

```
flinkdeployment.yaml
  metadata.name                            personalization-green
  metadata.namespace                       personalization-green
  execution.checkpointing.dir              s3://checkpoints/phase-7/green/checkpoints
  high-availability.storageDir             s3://checkpoints/phase-7/green/ha
  execution.checkpointing.savepoint-dir    s3://checkpoints/phase-7/green/savepoints
  spec.job.state                           suspended
pdb.yaml
  metadata.namespace                       personalization-green
  selector.matchLabels.app                 personalization-green
```

Everything else is identical, `--transactional-id-prefix` included. That is
deliberate, not an oversight: a shared prefix is what lets green finalise blue's
in-flight transactions.

- [ ] **Step 2: Confirm the difference list is exactly that.**

```bash
diff -r manifests/flink/blue manifests/flink/green
```

Read every line of the output. Anything beyond the seven fields above is a
mistake, and this is the cheapest moment to catch it.

- [ ] **Step 3: Write the Application.**

Copy `manifests/argocd-apps/flink-job-blue.yaml`, changing `metadata.name` to
`flink-job-green`, `spec.source.path` to `manifests/flink/green`, and
`spec.destination.namespace` to `personalization-green`. Keep
`automated.selfHeal: false` and `prune: true`; the first is load-bearing for the
Phase 5 drift Drill and the second for Task 3's rename.

- [ ] **Step 4: Sync, and confirm the Secret arrived.**

```bash
argocd app sync flink-job-green
kubectl get secret minio-credentials -n personalization-green
```

**Confirmed on 2026-09-10**, before any sync: `minio-credentials` already exists
in `personalization-green`, 2 keys, 9 days old. `scripts/bootstrap.sh` line 218
lists both namespaces, so nothing is needed here.

**A finding this plan did not anticipate:** `manifests/flink/namespaces.yaml` is
owned by **no** Application. `grep -rn "manifests/flink" manifests/argocd-apps/`
returns only `flink-job-blue.yaml`, pointing at `manifests/flink/blue`. Both
namespaces carry `kubectl.kubernetes.io/last-applied-configuration` and no ArgoCD
tracking id, so they were applied by hand. They exist and are 9 days old, so
nothing blocks this phase, but a cluster rebuild must apply that file by hand,
the same way the Karpenter controller must. This belongs in Task 13's status.md
entry and in the README's rebuild notes.

- [ ] **Step 5: Gate, what a never-run deployment reports.**

```bash
kubectl get flinkdeployment personalization-green -n personalization-green \
  -o jsonpath='{.status.jobStatus.state}'; echo "<-- end"
kubectl get pods -n personalization-green
```

**Gate settled on 2026-09-10, first branch.** The deployment was applied by hand,
observed for 60 seconds, and removed again. `.status` is `{}` entirely: not an
empty `jobStatus.state` inside a populated status, but no status object at all.
`kubectl get flinkdeployment -A` showed both the JOB STATUS and LIFECYCLE STATE
columns blank for green.

So `promote.sh` tests **"is it RUNNING"** everywhere and never compares against
the literal `SUSPENDED`. That is what Task 5's `discover` does.

Zero pods, as expected: in application mode a suspended deployment has none,
which is exactly why pre-warming is impossible.

- [ ] **Step 6: Gate, the two job ids.**

```bash
kubectl get flinkdeployment personalization-blue -n personalization-blue \
  -o jsonpath='{.status.jobStatus.jobId}'; echo
```

Green has none yet, so record blue's now and compare after Drill 1. The storage
split already makes a collision harmless, so this is recorded for the knowledge
doc rather than as a blocker.

---

## Task 5: `scripts/promote.sh`, discovery and promotion

**Files:**
- Create `scripts/promote.sh`

**Goal.** One command that moves the Active Side, in either direction, through
Git, and refuses to run when the cluster is not in a state it understands.

**The concept.** Two sources of truth answer different questions. Git says what
you want; the live cluster says what is true. A promotion needs both to agree
before it touches anything, because promoting under existing drift is how you get
two Active Sides.

**The failure mode to watch for.** The design spec's original runbook checks only
that blue is RUNNING. If both sides were somehow RUNNING, that check passes, the
script suspends blue, starts green, and reports success. One side is running,
which looks correct, and nothing ever signals the disaster.

**The vocabulary trap.** `spec.job.state` is the operator's lowercase enum,
`running` and `suspended`. `status.jobStatus.state` is Flink's uppercase job
status. They are never equal as strings. Compare each against its own vocabulary.

- [ ] **Step 1: Require the tools, and refuse to run dirty.**

```bash
require yq; require kubectl; require argocd; require git
[[ -z "$(git status --porcelain -- manifests/flink/)" ]] \
  || die "manifests/flink/ has uncommitted changes; commit or stash first"
```

The script commits on your behalf. Running it over a dirty tree would sweep
unrelated edits into a promotion commit.

- [ ] **Step 2: Discover both sides.**

```bash
for side in blue green; do
  want[$side]=$(yq '.spec.job.state' "manifests/flink/${side}/flinkdeployment.yaml")
  have[$side]=$(kubectl get flinkdeployment "personalization-${side}" \
                  -n "personalization-${side}" \
                  -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)
done
```

Use whichever "not RUNNING" test Task 4 Step 5 established.

- [ ] **Step 3: Branch on exactly three legal arrangements.**

```
neither side RUNNING, both `suspended` in Git   -> FRESH DEPLOY
exactly one RUNNING and `running` in Git,
  the other not RUNNING and `suspended`         -> PROMOTION
anything else                                   -> abort
```

The abort path must print **all four cells** as a table, not a one-line error.
You need to see which cell is wrong.

- [ ] **Step 4: Fresh deploy path.**

Set the chosen side's `spec.job.state` to `running`, leave
`initialSavepointPath` absent, commit, push, `argocd app sync`, poll for
`RUNNING`.

- [ ] **Step 5: Promotion path, suspend through Git.**

```bash
yq -i '.spec.job.state = "suspended"' "manifests/flink/${from}/flinkdeployment.yaml"
git commit -am "Suspend ${from} for promotion"
git push
argocd app sync "flink-job-${from}"
```

No `kubectl patch`. If you patch, Git and the cluster disagree for the whole time
the other side is Active, and any manual Sync in that window restarts the side
you just suspended.

- [ ] **Step 6: Poll for two conditions, not one.**

```bash
# poll every 1s
state=$(kubectl get flinkdeployment ... -o jsonpath='{.status.jobStatus.state}')
path=$(kubectl  get flinkdeployment ... \
        -o jsonpath='{.status.jobStatus.savepointInfo.lastSavepoint.location}')
[[ "${state}" == "SUSPENDED" && -n "${path}" ]] && break
```

**Checking only the state is the bug.** A job can report `SUSPENDED` while the
location is still empty. An empty `initialSavepointPath` is a legal way to say
"fresh start", so the Standby Side would come up with every Shopper's state
silently discarded and no error anywhere.

Poll at 1 second, not 5. It is part of the pause Drill 2 measures.

- [ ] **Step 7: On timeout, roll the suspend back.**

```bash
yq -i '.spec.job.state = "running"' "manifests/flink/${from}/flinkdeployment.yaml"
git commit -am "Abort promotion, resume ${from}"
git push && argocd app sync "flink-job-${from}"
```

Nothing was lost. The other side was never touched. This is the only failure
point in the whole script with a clean automatic path, which is why it has one.

- [ ] **Step 8: Write the path into the other side and start it.**

```bash
yq -i ".spec.job.initialSavepointPath = \"${path}\"" \
      "manifests/flink/${to}/flinkdeployment.yaml"
yq -i '.spec.job.state = "running"' "manifests/flink/${to}/flinkdeployment.yaml"
git commit -am "Promote ${to} from ${path}"
git push
argocd app sync "flink-job-${to}"
```

Do **not** also update the now-Standby side's `spec.image`. Its manifest, left
alone, is the record of the previous version, and that record is what makes a
rollback a `git revert`.

- [ ] **Step 9: Poll for RUNNING, and say plainly there is no safe path.**

If this poll times out, the Active Side is already suspended and the Standby is
not up. Print the fallback rather than attempting it: a human resumes the
previous side from its own pre-promotion savepoint, then debugs separately.
Drill 4 exercises exactly this.

- [ ] **Step 10: Dry-run the script against the current state.**

```bash
./scripts/promote.sh --dry-run
```

**This cannot pass until Task 3 Step 8 has synced**, an ordering dependency this
plan did not state. Discovery reads the live CR by its new name,
`personalization-blue`, and until the rename is pushed and synced that object
does not exist, so discovery correctly refuses to act. Run this after the sync.

Expected once synced: the four-cell table shows blue `running`/`RUNNING` and
green `suspended`/empty, and the plan names `blue` as the side to suspend. If it
names green, the discovery is inverted.

**`argocd` was not installed on 2026-09-10.** Install it and log in to
`localhost:30010` before this step: the CLI installation page is at
<https://argo-cd.readthedocs.io/en/stable/cli_installation/>.

**`yq` is deliberately not a dependency**, against what this plan's Task 5 Step 2
sketch assumed. `yq -i` rewrites and reflows the whole document, and the
manifests under `manifests/flink/` carry load-bearing comments. The script uses
anchored `sed` on the two fields that ever change. Verified on 2026-09-10: after
a suspend, a savepoint write, and a start, the whole-file diff showed exactly two
changed lines and every comment intact.

---

## Task 6: Drill 1, fresh deploy

**Files:**
- Create `docs/runbooks/phase-7-drill-1-fresh-deploy.md`

**Goal.** Prove the fresh-deploy branch, and establish that a start with no
savepoint really does start clean.

**The concept.** A fresh deploy is the only path with no `initialSavepointPath`.
Everything else in this phase carries state forward.

**The failure mode to watch for.** `--start-from-earliest` defaults to `true`, so
this start replays the whole `clickstream` topic, spanning Phases 3 to 6. Catch-up
will take a long time and will re-emit Recommendation identities that already
exist in the topic. That is expected, and it is why the baseline snapshot is
taken **after** catch-up, using Task 1's time window.

- [ ] **Step 1: Suspend blue through Git so both sides are down.**

Use the script, not a patch.

- [ ] **Step 2: Run `promote.sh` and read which branch it took.**

Expected: FRESH DEPLOY, because neither side is Active.

- [ ] **Step 3: Watch the catch-up, and record how long it takes.**

```bash
kubectl get flinkdeployment -A -w
```

Record the wall-clock time from `RUNNING` to the point where the source vertex's
lag stops falling. That number is the cost of a stateless start and belongs in
status.md.

- [ ] **Step 4: Confirm the start was clean.**

```bash
kubectl get flinkdeployment ... -o jsonpath='{.spec.job.initialSavepointPath}'; echo "<-- end"
```

Expected: empty. A populated path here means the discovery branch was wrong.

- [ ] **Step 5: Take the post-catch-up baseline with a window.**

```bash
NOW=$(( $(date +%s) * 1000 ))
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-1-after.txt "${NOW}"
```

- [ ] **Step 6: Write the runbook, including the Observed result.**

Match the shape of the seven existing runbooks: the procedure, the rationale per
command, and the real transcript.

---

## Task 7: Drill 2, promotion under a Load Ramp

**Files:**
- Create `docs/runbooks/phase-7-drill-2-promotion.md`

**Goal.** Measure the real pause, and prove **No Gap** across a promotion.

**The concept.** The pause is a latency spike, not a loss. Kafka holds the input
and the incoming side resumes from the outgoing side's exact offsets.

**The failure mode to watch for.** Reading the parallelism drop as a regression.
A promotion restarts the job, so it starts at `spec.job.parallelism`, which is 2,
and the autoscaler climbs again. That climb is part of the pause.

- [ ] **Step 1: Check for kwok nodes and refuse to proceed if any exist.**

- [ ] **Step 2: Start the Load Ramp, with the Shopper count raised to match.**

```bash
apps/gradlew -p apps :generator:run --args="--click-rate=800 --shopper-count=2000 --product-change-rate=400"
```

Session length is `e^(6 × clickRate ÷ shopperCount)`. Hold
`shopperCount ≈ 2.5 × clickRate`. Phase 6 measured the generator's real ceiling
at about 2650 Clicks/sec, so measure the achieved rate from partition offsets
rather than trusting the flag.

- [ ] **Step 3: Snapshot before, with a window opening now.**

- [ ] **Step 4: Run `promote.sh`, timestamping each phase.**

Record six timestamps: suspend committed, `SUSPENDED` observed, savepoint path
non-empty, green committed, green's first pod, green `RUNNING`. Those six give
you the per-stage breakdown the spec's budget guessed at.

- [ ] **Step 5: Record the pause.**

The pause is from the last Recommendation before the promotion to the first one
after. Read it off the topic, not off pod states.

- [ ] **Step 6: Snapshot after, and compare for No Gap.**

```bash
./scripts/recommendation-snapshot.sh compare /tmp/drill-2-before.txt /tmp/drill-2-after.txt
```

Expected: every identity present before is still present, and none appears twice.
A duplicate here means the savepoint handoff did not carry the sink's transaction
state, which points at the transactional id prefix.

- [ ] **Step 7: Gate, was `NATIVE` honoured?**

```bash
source ./scripts/minio-env.sh
mc ls --recursive s3/checkpoints/phase-7/blue/savepoints/
```

**Both branches.**

- If the savepoint directory contains a `_metadata` file alongside RocksDB
  `.sst` files, the native format was used and the spec's pause reasoning holds.
- If it contains only `_metadata` and canonical state files, the key was ignored
  inside `spec.flinkConfiguration`. Move it to the operator's Helm values instead
  and note that the operator config is cluster-wide, not per-resource.

- [ ] **Step 8: Confirm no dangling Kafka transactions.**

```bash
kubectl exec -n kafka personalization-brokers-3 -- \
  bin/kafka-transactions.sh --bootstrap-server localhost:9092 list
```

Expected: only green's active transactions carrying the `personalization-phase-7`
prefix. A stale entry means the outgoing side's transactions were not finalised.

- [ ] **Step 9: Write the runbook.**

---

## Task 8: Drill 3, promotion back

**Files:**
- Create `docs/runbooks/phase-7-drill-3-promotion-back.md`

**Goal.** Prove the script is directionless in practice, not only in intent.

**The concept.** CONTEXT.md defines Promotion as directionless. Nothing in
`promote.sh` names a side; direction comes entirely from discovery.

**The failure mode to watch for.** A script that works once and fails on the
return trip almost always has a hardcoded side somewhere, or a discovery test
that assumes blue is the default.

- [ ] **Step 1: With the Load Ramp still running, run `promote.sh` again.**

Expected: it discovers green Active and blue not, and promotes blue.

- [ ] **Step 2: Compare snapshots for No Gap, same as Drill 2.**

- [ ] **Step 3: Compare the pause to Drill 2's.**

They should be close. A large difference points at state size, because green has
been running and accumulating Browsing Sessions.

- [ ] **Step 4: Confirm both savepoint directories are now populated.**

```bash
mc ls s3/checkpoints/phase-7/blue/savepoints/ s3/checkpoints/phase-7/green/savepoints/
```

Each side has now written one. This is the visible proof that the storage split
worked.

- [ ] **Step 5: Write the runbook.**

---

## Task 9: Drill 4, break the Standby Side

**Files:**
- Create `docs/runbooks/phase-7-drill-4-standby-timeout.md`

**Goal.** Exercise the one failure mode with no automatic safe path, and document
the manual fallback from having actually run it.

**The concept.** Once the Active Side is suspended and the Standby will not come
up, there is nothing safe left to do automatically. Resuming the suspended side
from its own pre-promotion savepoint is a human decision, because it is the
choice to accept the promotion never happened.

**The failure mode to watch for.** Host memory. A failed promotion can leave both
sides with pods. Phase 6 measured 2.5 GiB headroom. Watch `free -g` throughout,
and be ready to suspend green through Git if the host starts swapping.

- [ ] **Step 1: Break green deliberately, in Git.**

Point green's `spec.image` at a tag that does not exist. That produces
`ImagePullBackOff`, which is unambiguous and leaves no half-started job.

Do **not** break it with a bad jar for this Drill. That is Drill 6's failure, and
running both at once would leave you unable to tell which one you were watching.

- [ ] **Step 2: Run `promote.sh` and let Step 9's poll time out.**

Record how long the timeout is and whether the message names the fallback.

- [ ] **Step 3: Confirm the state the timeout leaves behind.**

```bash
kubectl get flinkdeployment -A
git log --oneline -3 -- manifests/flink/
```

Expected: blue `suspended` in both Git and the cluster, green `running` in Git
and not up. Git and the cluster **agree**, which is the whole benefit of the
commit-then-sync decision. Note how much easier this state is to reason about
than the patched-live equivalent.

- [ ] **Step 4: Perform the manual fallback and time it.**

Resume blue from its own pre-promotion savepoint, which is in the commit history
from Step 3.

- [ ] **Step 5: Revert green's image and confirm the pair is healthy.**

- [ ] **Step 6: Write the runbook, with the fallback as a numbered procedure.**

This is the runbook someone reads at 2am. Write it for that reader.

---

## Task 10: `scripts/promote.sh --recover`

**Files:**
- Modify `scripts/promote.sh`

**Goal.** Recover from a deployment that cannot take a savepoint.

**The concept.** `externalized-checkpoint-retention: RETAIN_ON_CANCELLATION`
keeps checkpoints after the job dies, and `initialSavepointPath` accepts a
retained **checkpoint** directory, not only a savepoint. So a crash-looping job
still has a usable snapshot, and it is at most
`execution.checkpointing.interval` old.

**The failure mode to watch for.** Recovery mode must assert the **opposite**
precondition to normal mode. The normal gate demands exactly one side RUNNING,
which is precisely the situation recovery does not have. A recovery mode that
reuses the normal gate refuses to run every time it is needed.

- [ ] **Step 1: Add the inverted precondition.**

`--recover` refuses to run when both sides look healthy, and requires that the
side being recovered from is **not** RUNNING.

- [ ] **Step 2: Discover the newest retained checkpoint.**

```bash
mc ls --recursive "s3/checkpoints/phase-7/${from}/checkpoints/" \
  | awk '/chk-[0-9]+\/_metadata$/ {print $NF}' \
  | sed 's|/_metadata$||' \
  | sort -t- -k2 -n | tail -1
```

The `_metadata` file is the marker. A `chk-N` directory without one is an
incomplete checkpoint and restoring from it fails.

- [ ] **Step 3: Add `--from-snapshot <path>`.**

An explicit path, for when nothing newer survived or when the newest snapshot is
itself poisoned. This is the only mode that knowingly produces duplicates.

- [ ] **Step 4: Add `--image <tag>`.**

A recovery almost always rolls the image back too. Without this the script would
restore good state into the broken code.

- [ ] **Step 5: Refuse to be automated.**

Print a line saying `--recover` is a hand-run mode. It needs a path a human found
in MinIO, and the normal discovery gate would misread the situation as a fresh
deploy.

- [ ] **Step 6: Dry-run both recovery modes against the healthy cluster.**

Expected: both refuse, naming the healthy pair as the reason. A recovery mode
that runs against a healthy cluster is the dangerous defect here.

---

## Task 11: Drill 5, rollback as deployment

**Files:**
- Create `docs/runbooks/phase-7-drill-5-rollback-deploy.md`

**Goal.** Prove that `git revert` plus a promotion returns the previous version
with no duplicates.

**The concept.** A rollback moves two things independently. The image goes back
to the previous tag. The state does **not** go back; it comes from a fresh
savepoint of the currently Active Side. Old code, new state.

**The failure mode to watch for.** Reaching for the Standby Side's stale
`initialSavepointPath` instead of taking a fresh savepoint. That path is as old
as the Active Side's uptime, and using it replays everything since.

- [ ] **Step 1: Decide `num-retained`, and record the reasoning.**

**Gate, both branches.** `execution.checkpointing.num-retained: "3"` at a 10
second interval is thirty seconds of recovery history, and Drill 6 depends
entirely on that window.

- Keep 3 if you accept that a crash loop must be noticed within thirty seconds.
- Raise it, 10 gives 100 seconds, at the cost of more objects in MinIO. Measure
  the object count first with
  `mc ls --recursive s3/checkpoints/phase-7/ | wc -l`.

Either way this lands in both manifests, not one.

- [ ] **Step 2: Build and deploy a second image with a visible, harmless change.**

Something observable in the output, so you can tell the two versions apart
without reading tags. Do **not** change the job graph; the global constraints
forbid it in this phase.

- [ ] **Step 3: Promote it, then `git revert` the promotion commit.**

- [ ] **Step 4: Run `promote.sh` on the reverted tree.**

Expected: it discovers the current Active Side, takes a **fresh** savepoint,
and starts the other side on the previous image from that savepoint.

- [ ] **Step 5: Confirm No Gap and the previous behaviour.**

The output should return to the pre-change form, and the snapshot comparison
should show no duplicates. Both must hold. Either alone is not the result.

- [ ] **Step 6: Write the runbook.**

---

## Task 12: Drill 6, rollback as recovery

**Files:**
- Create `docs/runbooks/phase-7-drill-6-rollback-recovery.md`

**Goal.** Prove that a crash-looping deployment recovers from the newest retained
checkpoint with no duplicates, using `--recover`.

**The concept.** This is the Drill that justifies the whole recovery design. High
availability makes a deployment failure **worse**, because it faithfully restores
the broken thing and does so forever. No amount of checkpointing fixes a bad jar.

**The failure mode to watch for.** Being too slow. `num-retained` from Task 11
Step 1 bounds how long you have. Have the recovery command drafted before you
break anything.

- [ ] **Step 1: Build an image that starts, checkpoints, then fails.**

It must reach `RUNNING` and complete at least three checkpoints before failing,
otherwise there is nothing to recover from and you are testing Drill 4 again.
A deliberate throw in an operator, after a delay, is the cleanest way.

- [ ] **Step 2: Promote it and watch the crash loop.**

Record the restart count and confirm the job is genuinely looping rather than
failed once.

- [ ] **Step 3: Confirm the checkpoints survived the failure.**

```bash
mc ls --recursive "s3/checkpoints/phase-7/${broken}/checkpoints/" | grep _metadata
```

`RETAIN_ON_CANCELLATION` is what makes this list non-empty. If it is empty, the
job never completed a checkpoint and Step 1 needs a longer delay.

- [ ] **Step 4: Recover.**

```bash
./scripts/promote.sh --recover --image <the previous good tag>
```

- [ ] **Step 5: Confirm no duplicates, and explain why there are none.**

Exactly-once ties committed output to completed checkpoints. Records written
after the last completed checkpoint sat in a transaction that was pre-committed
and never committed, so no `read_committed` consumer ever saw them.
Reprocessing emits them for the first time. The snapshot comparison should show
zero duplicates, and that is the mechanism, not luck.

- [ ] **Step 6: Record how much reprocessing it cost.**

The gap between the recovery checkpoint and the failure, in seconds. It should be
under the checkpoint interval.

- [ ] **Step 7: Write the runbook.**

---

## Task 13: Documents

**Files:** as listed below.

**Goal.** Leave the repository consistent with what was built.

- [ ] **Step 1: ADR 0006, two corrections.**

Correct the leader-election claim: leader election happens in a ConfigMap, and
ConfigMaps are namespaced, so the two sides' HA ConfigMaps never meet. The real
risk was the shared blob store and the cleanup that runs over it.

Record the partial reversal of the recovery exclusion: promotion **is** the
recovery path for a deployment failure and is **not** a failover mechanism for an
infrastructure failure.

**Open decision.** Amend 0006, or write ADR 0010 and have 0006 point at it.
0006's Context and Alternatives remain accurate, so an amendment keeps one
document; a new ADR keeps the dated record of when the reversal happened. Pick
one and say which in the commit message.

- [ ] **Step 2: ADR 0005, the amendment Phase 6 left outstanding.**

Its Decision still describes two autoscaling variants. Phase 6 dropped the
Standalone Variant and KEDA by decision, so the lab has no
external-metric-driven autoscaling, which was 0005's stated reason for keeping
KEDA. Its Context and analysis stay correct and are load-bearing: this plan cites
its `spec.taskManager.replicas` finding twice.

Amend the Decision only. Do not touch the analysis.

- [ ] **Step 3: CONTEXT.md, add No Loss.**

Beside **No Gap**, with the distinction explicit: a Drill is judged by No Gap, a
deliberate replay by No Loss. Neither definition is weakened to fit the other.

The **Standby Side** entry needs **no** edit. Nothing was added that makes it
hold state between promotions.

- [ ] **Step 4: The phase plan.**

Rename Phase 7 to "Blue/green and the deployment mechanism". Move the OTel work,
the Collector-kill Drill, and the Grafana grouping criterion into Phase 8. Replace
`promote-green.sh` with `promote.sh`. Restate the budget honestly against what
this actually took.

- [ ] **Step 5: The design spec.**

Its "Zero-downtime deployment" runbook is superseded. Its concept map line about
"savepoint-based redeploys when rule logic changes" is now true only because
Task 2 landed the uids, and should say so.

- [ ] **Step 6: README and `clusters/kind/`.**

Replace the `localhost:30011` dashboard entry with the port-forward command,
using the real Service name recorded in Task 3 Step 8. Note in
`clusters/kind/kind-cluster.yaml` that 30011 is now free, so Phase 8 can take it.

**Do not rewrite the Phase 5 and Phase 6 runbooks.** Their `curl localhost:30011`
lines are part of an **Observed result** transcript, a record of what was really
run on the day. Rewriting them would falsify the record. `grep -rn 30011 docs/`
returns roughly 50 hits and nearly all are of this kind.

Add one banner line at the top of each affected runbook instead, saying that
30011 was retired in Phase 7 and that re-running the Drill needs
`kubectl port-forward` per the README. Affected:
`phase-5-drill-a-taskmanager-kill.md`, `phase-5-drill-b-jobmanager-kill.md`,
`phase-6-drill-e-dry-run.md`, `phase-6-drill-f-scale-up.md`,
`phase-6-drill-g-scale-down.md`.

**Done on 2026-09-11**, ahead of this task: the README's "Flink UI" section was
rewritten around port-forward and the per-side Service name, and
`clusters/kind/kind-cluster.yaml` now records 30011 as free with the reason.

- [ ] **Step 7: The knowledge doc.**

`docs/knowledge/phase-7-blue-green.md` was drafted during design and says so at
the top. Replace that note with what actually happened, and add anything the six
Drills taught that the design did not know.

- [ ] **Step 8: status.md.**

Per-task measurements, every trap found, and a "what Phase 8 inherits from Phase
7" section. Phase 6 wrote one and this plan depended on it heavily; return the
favour.
