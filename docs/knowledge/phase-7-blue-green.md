# Phase 7 knowledge: Blue/green and the deployment mechanism

**Written during design, not during the phase.** The
[README](README.md) says a knowledge doc is written while the phase runs, so
that it records the explanations that were actually needed. This one starts
earlier, the same way Phase 6's did. Everything here is design reasoning until
the phase runs and replaces it with what really happened.

Scope note: OTel moved out of this phase and into Phase 8, so that the
Collector, Prometheus, node-exporter, kube-state-metrics, the Collector-kill
Drill, and the dashboards all land together where each has something to talk to.
Phase 7 is blue/green and the deployment mechanism, which is a script.

## Three things called "cluster", and how many of each there are

This is the first thing to get straight, because the word does three jobs and
Phase 7 touches all three.

**The Kubernetes cluster.** One. Always one. `clusters/kind/kind-cluster.yaml`
defines it: `name: personalization-lab`, six nodes, three control-plane and three
workers. Nothing in Phase 7 creates a second one, and nothing could.

**The namespaces.** Two, both inside that one cluster, both declared in
`manifests/flink/namespaces.yaml`: `personalization-blue` and
`personalization-green`.

**The Flink clusters.** Two. A Flink cluster means one JobManager plus its
TaskManagers, which is a set of pods, not a machine. You get one per
`FlinkDeployment`, so having two sides means having two, and only one ever has
pods at a time.

```
kind cluster "personalization-lab"          ONE. 6 nodes.
  |
  +-- namespace personalization-blue
  |     FlinkDeployment personalization-blue     a Flink cluster
  |
  +-- namespace personalization-green
        FlinkDeployment personalization-green    a Flink cluster
```

`metadata.name` on a `FlinkDeployment` names the **third** row. Phase 7 renamed
it from `personalization` to `personalization-<side>`, and that renamed a Flink
cluster. It did nothing to the Kubernetes cluster, and it did not create one.

## What a `FlinkDeployment` creates, and what it does not

### It does not only create a cluster

That depends entirely on whether `spec.job` is present.

**With `spec.job`, which is what this project has, it is application mode**, and
the deployment creates a Flink cluster **and** one job as a single inseparable
thing. There is no way to submit a job to it later, and no way to have it running
with no job.

**But `suspended` is not one state, it is two, and the difference matters.**
Measured on 2026-09-12:

| | never run | suspended after running |
|---|---|---|
| JobManager Deployment | absent | **2/2, still up** |
| `<name>-rest` Service | absent | **still there** |
| ConfigMaps | absent | **all five still there** |
| TaskManagers | absent | gone, released with the job's slots |
| `status` | `{}` entirely | `lifecycleState: SUSPENDED`, `jobStatus.state: FINISHED` |

Suspending stops the **job**. It does not tear down the cluster. The TaskManagers
go because the job released its slots, and the JobManager pods carry on with a
job that has finished.

**Without `spec.job` it is a session cluster**, and the deployment creates only
the cluster. Jobs arrive later as separate `FlinkSessionJob` resources. The
operator's reference says so directly, describing the `job` field as "Null for
session clusters".

### The creation chain

"A Flink cluster" is not one object, and two different things build it.

```
you commit             FlinkDeployment            a custom resource. Just YAML
ArgoCD applies               |                    in etcd. Nothing runs yet.
                             v
the Flink OPERATOR creates:
        Deployment  personalization-blue                 the JobManager, 2 pods
        ConfigMap   personalization-blue-config-map      the Flink config
                             |
                             v
the JOBMANAGER pod starts. In mode: native it is itself a Kubernetes API
client, and it creates:
        Service     personalization-blue-rest            REST API and Web UI, 8081
        ConfigMap   personalization-blue-cluster-config-map   HA lease and pointers
                             |
it runs main(), submits the job, and its ResourceManager asks Kubernetes
for slots:
        Pods        personalization-blue-taskmanager-N-M
                    count = ceil(slots needed / taskmanager.numberOfTaskSlots)
```

**Two different things create this cluster, and the split is easy to get wrong.**
The operator creates the JobManager Deployment and stops there. Everything below
it, the Services, the HA ConfigMap, and the TaskManager pods, is created by the
**JobManager itself**, because `mode: native` makes it a Kubernetes API client.
See [Phase 5 knowledge](phase-5-operator-and-ha.md), "What Flink creates on its
own in `mode: native`" and "Three Services in front of one JobManager".

That is also [ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md)'s
finding: `spec.taskManager.replicas` is meaningless in `mode: native` precisely
because the operator is not the thing creating those pods. It is why the Phase 6
autoscaler can change the pod count with no restart.

**So `personalization-blue-rest` is created by the JobManager.** It is not in any
manifest in this repository. A side that has **never run** has no Service,
because it has no JobManager to create one. A side **suspended after running**
keeps both, as the table above shows.

**There is no internal `personalization-blue` Service.** Phase 5's doc left this
as an open question: with `high-availability.type: kubernetes` the TaskManagers
find the leader through the HA ConfigMap rather than through a Service, and
whether Flink then skips creating the internal headless Service was unverified
for 2.2.0. **Measured on 2026-09-10**: `kubectl get svc -n personalization-blue`
returned exactly two rows, the hand-written NodePort and `personalization-rest`.
Flink does skip it.

The `-config-map` attribution above is the one line here not confirmed against
this cluster. Read `kubectl get cm -n personalization-<side>` after the first
sync and correct it if the operator turns out not to own that one.

Every name above is prefixed with `kubernetes.cluster-id`, which the operator
derives from `metadata.name`. The `app` label on the pods carries it too, which is
why renaming forced a matching change in `pdb.yaml`.

Observed on 2026-09-10, before the rename: two JobManager pods on `worker3` and
`worker2`, one TaskManager pod, and both Services, with
`personalization-rest` confirming the `<cluster-id>-rest` convention.

### What it does not create

Worth knowing the boundary, because several objects in each side's directory are
yours rather than the operator's:

- `manifests/flink/<side>/pdb.yaml`, the PodDisruptionBudget. It selects the
  operator's pods by the `app` label, which is the coupling that made the rename
  a two-file change.
- `manifests/flink/namespaces.yaml`, the namespaces themselves. **Owned by no
  ArgoCD Application**: both namespaces carry
  `kubectl.kubernetes.io/last-applied-configuration` and no ArgoCD tracking id,
  so they were applied by hand. A cluster rebuild must apply that file by hand,
  the same way the Karpenter controller must.
- The `minio-credentials` Secret, from `scripts/bootstrap.sh`, whose
  `FLINK_NAMESPACES` array already lists both sides.
- `rest-nodeport.yaml`, which Phase 7 deleted from both sides. See
  [Tradeoff: port-forward rather than a NodePort dashboard](#tradeoff-port-forward-rather-than-a-nodeport-dashboard).

### The consequence that matters most

Because application mode ties the cluster to the job, "create green's pods early
so they are warm" means "start green's job early", and green would immediately
begin consuming Kafka alongside blue. Every Click processed twice.

That is the whole of
[Tradeoff: warming the Standby Side before the promotion](#tradeoff-warming-the-standby-side-before-the-promotion)
below, and it is the reason a promotion has a bounded pause rather than none.

## Tradeoff: two namespaces rather than one

Both sides could live in a single namespace, as two `FlinkDeployment` objects
with different names. Promotion would be byte-for-byte identical, because it only
ever touches `spec.job.state` and `spec.job.initialSavepointPath`, neither of
which knows anything about namespaces.

What one namespace would cost:

**Kubernetes' own isolation boundary.** ResourceQuota, LimitRange,
NetworkPolicy, and RBAC are all per namespace. Sharing one leaves no way to cap
or fence one side separately from the other, which matters on a host whose
headroom is the binding constraint.

**Phase 8's namespace tag.** The OTel Collector's `k8sattributes` processor tags
each metric with `k8s.namespace.name`, and that tag is the only thing that
distinguishes blue's metrics from green's, because both sides emit identical
Flink metric names. One namespace means the tag is identical on both and Phase 8
needs a different discriminator invented for it.

**Blast radius.** `kubectl delete -n personalization-green --all` is survivable
when green is the Standby Side. The same command against a shared namespace is
not.

What it would buy: one fewer namespace, and `scripts/bootstrap.sh` copying the
Secret once instead of twice.

**Rejected.** The second reason decides it: Phase 8 already depends on the
namespace tag, and inventing a substitute discriminator costs more than the
namespace does.

## Tradeoff: port-forward rather than a NodePort dashboard

### The constraint

A `Service` object is namespaced. **A `nodePort` number is not.** It is allocated
from one cluster-wide range and opened on every node.

So copying Phase 5's `rest-nodeport.yaml` into the green directory unchanged
fails at sync time:

```
Service "personalization-flink-dashboard" is invalid:
spec.ports[0].nodePort: Invalid value: 30011: provided port is already allocated
```

The Service **name** duplicating across namespaces is fine. The port number is
not. And `metadata.name` on the FlinkDeployment has nothing to do with it, so the
Phase 7 rename does not help.

### The port budget

`clusters/kind/kind-cluster.yaml` maps host ports 30010 to 30019. ArgoCD holds
30010, Flink held 30011, MinIO holds 30014 and 30015, and Kafka holds 30016 to
30019 because a nodeport listener needs one port for bootstrap plus one per
broker. That leaves **30012 and 30013 free, and Phase 8 wants both** for
Prometheus and Grafana.

`extraPortMappings` are fixed when the `kind` node container is created. Adding
one means **recreating the cluster**, which takes Kafka's data, MinIO's buckets,
and the pinned `.node-ips` with it. That is far heavier than anything this phase
otherwise does.

### Three shapes, and why the tempting one is wrong

**Recreate `kind` with more ports.** Both sides keep a dashboard on a stable
bookmark. Costs a cluster rebuild, which is out of proportion to a convenience.

**Blue keeps 30011, green uses port-forward.** Tempting, because it changes
almost nothing and the Standby Side is rarely looked at.

**Rejected, and this is the interesting one.** CONTEXT.md defines Promotion as
directionless and `blue`/`green` as namespace names rather than roles. Giving
only blue a dashboard makes blue permanently special, and after the first
promotion the Active Side is the one you **cannot** look at. The asymmetry is
invisible while blue happens to be Active, which is exactly when nobody would
notice it was wrong.

**Neither side gets a NodePort.** Chosen.

### What it costs, and what it buys

The cost is the bookmark at `localhost:30011`, plus a line in the README and a
banner on the five Phase 5 and 6 runbooks whose transcripts cite that port.
Those transcripts are **not** rewritten: they record what was really run on the
day, and editing them would falsify the record.

What it buys, beyond symmetry: `manifests/flink/<side>/` drops to two files, the
FlinkDeployment and the PDB, identical between sides apart from the fields that
are meant to differ. `diff -r manifests/flink/blue manifests/flink/green` stays a
short, readable drift check. And host port 30011 is freed for Phase 8, which
wanted a third port and only had two.

`kubernetes.rest-service.exposed.type: ClusterIP` stays as it was. Flink cannot
pin a nodePort number itself, which is why the hand-written Service existed at
all; with that Service gone, ClusterIP plus `kubectl port-forward` is the whole
access path.

## Tradeoff: a script rather than a CI pipeline

### What a deploy looks like by hand today

Deploying a code change is five manual steps.

```bash
./scripts/build-image.sh
# it prints, on the last line:
#   lab/personalization-pipeline:0.1-abc1234
```

Then you open `manifests/flink/blue/flinkdeployment.yaml`, paste that tag into
`spec.image`, and:

```bash
git commit -am "Point spec.image at 0.1-abc1234"
git push
```

Then ArgoCD notices the change and applies it.

The git log already carries two commits shaped exactly like this, `478c08e` and
`ed0198d`, both named "Point spec.image at ...". That repetition is what
`scripts/promote.sh` absorbs.

### Why a promotion cannot be ArgoCD alone

A fresh deploy is one commit and one sync, so ArgoCD could carry it unaided.

A promotion cannot be, because it is a **sequence with a wait in the middle**:

```
1. commit state: suspended to the Active Side, sync
2. WAIT until status.jobStatus.state == SUSPENDED
        AND the savepoint location is non-empty
3. READ that location out of live status
4. WRITE it into the Standby Side's manifest, commit, sync
5. WAIT until the Standby Side reports RUNNING
```

ArgoCD reconciles a desired state. It has no notion of "wait, then read a value
the controller produced, then write a second file". Steps 2 to 4 are why
something has to orchestrate above it. In this project that something is a shell
script.

### Why not CI

CI was considered seriously and rejected. The reasoning is worth keeping, because
"we should have a pipeline" is an easy thing to re-propose.

**A hosted runner cannot do the work.** `scripts/build-image.sh` ends with `kind
load docker-image`, which copies an image out of the local docker daemon into the
`kind` node containers, which are also containers on the local docker daemon. A
machine in a datacentre has a different daemon and no `kind` cluster. The same
holds for everything after the build: `kubectl` needs an API server the
kubeconfig points at on loopback, `argocd app sync` needs `localhost:30010`
through this cluster's `extraPortMappings`, and the node IPs are pinned per host
into `.node-ips`, which is gitignored precisely because it is host-specific.

**So the runner would have to be self-hosted**, on this machine, whether the
forge is GitHub or GitLab. That is the same install either way. GitLab's shared
runners are in GitLab's cloud and hit the same wall; a locally registered GitLab
Runner works, but that is you running a runner, not GitLab lending you one. It
also needs the `shell` executor rather than `docker`, or each job gets a fresh
container with no cluster access.

**Given the runner is on this machine either way**, CI adds three things over a
script: a trigger you did not type, a re-readable log, and enforcement that steps
run in order. A script gives the last two, and `tee` gives the log. The first is
worth having when a team shares a deploy and worth nothing when one person on one
laptop does it.

**What it would cost**: runner registration and tokens, workflow YAML, a
commit-loop guard because the script itself pushes commits, and path filters so a
doc-only merge does not promote. On GitLab it additionally costs a new remote,
`repoURL` rewritten in seven ArgoCD Application files, and ArgoCD credentials for
GitLab. None of that teaches Kubernetes or Flink, which is what this project is
for.

**And a queued job is worse than no job.** A self-hosted runner only picks up work
while the laptop is awake. A script was never waiting on anything.

### The division of labour that remains

The script does **not** apply anything to the cluster directly. ArgoCD still
performs every apply, exactly as it does today.

```
promote.sh  ->  builds the image, loads it into kind,
                writes the tag into a manifest, commits, pushes,
                asks ArgoCD to sync, then polls until the state is right

ArgoCD      ->  reads the manifest from GitHub and applies it
```

The script orchestrates. ArgoCD reconciles. The rule that keeps them apart is
that the script **changes Git and never patches a live object**, so the cluster
and the repository never disagree, not even for the minutes a promotion takes.

## Tradeoff: warming the Standby Side before the promotion

### The question, because it is the obvious one

A promotion pauses for 60 to 140 seconds, and the two most expensive stages are
green's JobManager pod starting and green's TaskManagers being created,
scheduled, started, and registered. Together that is roughly 40 to 65 seconds of
the total.

Both happen **after** blue has already stopped. So the obvious idea is: create
green's pods early, while blue is still writing its savepoint, and have them
waiting. That would remove both stages from the pause and roughly halve it.

It cannot be done in this deployment model, and the reason is worth understanding
rather than accepting.

### Application mode ties the cluster and the job together

`manifests/flink/<side>/flinkdeployment.yaml` has a `spec.job` block. That block
is what makes it an **application mode** deployment.

In application mode the JobManager pod's entire purpose is to run the
application's `main()` and submit the job. The pod and the job are one object
with one lifecycle. There is no state in which the pod exists and the job does
not.

The operator's own custom resource reference makes the split explicit, describing
the `job` field as:

> Job specification for application deployments/session job. **Null for session
> clusters.**

And, on what a session cluster is for:

> Session clusters allow for the creation of a Flink deployment **without an
> initial job**. This approach **separates the lifecycle of the cluster
> infrastructure from the submission of individual Flink jobs**.

"Separates the lifecycle" is precisely the property pre-warming needs, and it is
named as the thing session clusters have and application deployments do not.

### So "create the pods early" means "start the job early"

And a started green job immediately begins reading Kafka.

```
blue still RUNNING, writing its savepoint
green started early to warm up  ->  green's KafkaSource enumerator assigns
                                    itself every partition and starts consuming
```

Both sides consuming means every Click processed twice, which is the exact
outcome [ADR 0006](../adr/0006-blue-green-native-mode.md) exists to prevent.
The shared `--transactional-id-prefix` would then fence one of the two sinks, so
the attempt does not even reach the double-write. It crash-loops instead.

Loud rather than silent, but still a failure.

### What session mode would actually buy, by variant

Less than it first appears, and it splits.

**Native session mode warms the JobManager only.** TaskManagers in `mode: native`
are created on demand by the JobManager's ResourceManager in response to slot
demand. This is [ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md)'s own
finding, that `spec.taskManager.replicas` is meaningless in native mode. An idle
session cluster has no job, therefore no slot demand, therefore no TaskManagers.
Saving: about 15 to 25 seconds.

**Standalone session mode warms both**, because `spec.taskManager.replicas` does
create an independent TaskManager Deployment there. Saving: about 40 to 65
seconds, the full amount.

### The cost, and the verdict

Standalone session mode reverses ADR 0006. It requires re-validating all of
Phase 5's high availability work, the Zone spread constraints, and the
PodDisruptionBudget under a deployment model that ADR explicitly excluded. It
also splits each side into two custom resources, a `FlinkDeployment` for the
cluster and a `FlinkSessionJob` for the job, so `scripts/promote.sh` manipulates
the second rather than the first, and `upgradeMode` and savepoint semantics move
with it.

Halving the pause is worth something. It is not worth undoing the deployment
model the whole lab was built toward across Phases 5, 6a, and 7.

The native variant is worse value still: the same structural upheaval for 15 to
25 seconds.

**Rejected, and recorded here so it is not re-proposed blind.** The pause stays
bounded and measured rather than minimised at the cost of the architecture.

### What is taken instead

Three smaller levers, all inside the current model:

- `kubernetes.operator.savepoint.format.type: NATIVE`, which removes the
  translation cost from both the savepoint write and the restore, because
  `CANONICAL` rewrites all state into a backend-independent format and `NATIVE`
  writes RocksDB's own. Portability is given up, and both sides run the same
  state backend from the same image, so it was never going to be used.
- The script polls at 1 second rather than 5.
- The script calls `argocd app sync` rather than waiting out ArgoCD's three
  minute reconcile poll. That was already decided for correctness, and it happens
  to save up to three minutes.

### And true zero downtime is not reachable at all

For no gap, green would have to be producing before blue stops, which is the same
double-consumption above.

The only escape is green writing to a **different topic** while it catches up,
then switching consumers over. That is rejected separately, for the same reason
it is rejected as a remedy for replay duplicates: it moves the problem onto every
consumer, and any consumer that already read the old topic has already been
served the old data.

So a bounded, measured pause is the honest position. The phase plan's own wording
already says "bounded pause" rather than zero, and Drill 2 replaces the estimate
with a number measured on this cluster.
