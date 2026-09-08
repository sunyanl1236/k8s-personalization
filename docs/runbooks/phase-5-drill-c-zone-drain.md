# Phase 5 Drill C: drain a Zone

Date: 2026-09-07
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Deployment: `FlinkDeployment personalization` in `personalization-blue`,
`mode: native`, image `lab/personalization-pipeline:0.1-0bd7f52`
Target: `personalization-lab-worker2`, Zone `zone-b`

Per `CONTEXT.md`:

> **Drill**: A deliberate, repeatable act of breaking something to observe
> recovery.

What is broken here is a whole **Zone**, and for the first time the break goes
through the eviction API.

## What is actually being exercised

`kubectl drain` is a **voluntary** disruption. It goes through the eviction API,
and the eviction API consults every PodDisruptionBudget that selects the pod.

That is why the PDB was inert in Drills A and B and matters here. Both of those
used `kubectl delete pod`, which is **involuntary**, and no budget can refuse it.

Per [CONTEXT.md](../../CONTEXT.md), the Zone label drives real scheduling
decisions but is not a real failure domain, since every node is a container on
one host. This Drill therefore tests the scheduler and the disruption budget
honestly, and does not claim to test datacentre resilience.

## The plan's prediction was wrong, and the arithmetic says why

Task 9 expected `personalization-pdb` to refuse an eviction and the drained
Zone's JobManager to sit `Pending`. **Neither can happen with this topology.**

```
PDB      minAvailable: 1, selecting component=jobmanager
JMs      2, both Ready        ALLOWED DISRUPTIONS = 2 - 1 = 1

evict one JobManager  ->  1 remains  ->  1 >= 1  ->  ALLOWED
```

The budget has slack, so it permits the eviction. Then the replacement needs a
Zone:

```
JM counts after eviction   zone-a: 0   zone-b: 0   zone-c: 1
place in zone-a            zone-a: 1   zone-b: 0   zone-c: 1   skew 1, allowed
```

**Two replicas across three Zones always leaves a spare Zone.** `maxSkew: 1` is
satisfiable, so the replacement schedules rather than going `Pending`.

`personalization-pdb` cannot refuse a single-node drain at this replica count.
That is a fact about the numbers, not a fault.

## Procedure

**1. `apps/gradlew -p apps :generator:run`** (terminal 1)

**2. Snapshot, and record placement and budgets.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-c-before.txt
kubectl get pods -n personalization-blue -o wide
kubectl get pdb -A
```

Record `kubectl get pdb -A`, not just the Flink namespace. Drills A and B needed
only the Flink PDB. This Drill drains a **node**, so every workload on it is
evicted and every PDB in the cluster is consulted.

**3. Cordon, as a separate step.**

```bash
kubectl cordon personalization-lab-worker2
kubectl get nodes                                    # Ready,SchedulingDisabled
kubectl get pods -n personalization-blue -o wide     # unchanged
```

Cordon marks the node unschedulable and evicts nothing. Running it separately
proves that on this cluster rather than taking it on faith, so every movement in
step 4 is attributable to eviction alone.

It also makes the cleanup obligation explicit. An interrupted `drain` leaves the
node cordoned either way, and a cordon you never typed is how a node stays
`SchedulingDisabled` for three days.

**4. Drain it.**

```bash
kubectl drain personalization-lab-worker2 --ignore-daemonsets --delete-emptydir-data
```

**`drain` evicts every pod on the node, not one.** Ten pods lived on `worker2`.

**5. Read the end state.**

```bash
kubectl get pods -n personalization-blue -o wide
kubectl get pods -n kafka -o wide
kubectl get pdb -A
```

**6. Uncordon, and confirm recovery.**

```bash
kubectl uncordon personalization-lab-worker2
kubectl get nodes
kubectl get pods -n kafka -o wide
kubectl get pdb -A
kubectl exec -n kafka personalization-brokers-3 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions
```

**7. Snapshot after, and compare.**

```bash
sleep 60
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-c-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-c-before.txt /tmp/drill-c-after.txt
```

## Observed result

### One drain command, ten evictions

```
node/personalization-lab-worker2 already cordoned
Warning: ignoring DaemonSet-managed Pods: kube-system/kindnet-lms2q, kube-system/kube-proxy-86p5g
evicting pod cert-manager/cert-manager-75c55f7677-jwr2j
evicting pod kafka/personalization-brokers-4
evicting pod argocd/argocd-application-controller-0
evicting pod argocd/argocd-dex-server-6f449df767-bs8xz
evicting pod kafka/personalization-entity-operator-85fb67d454-z9hdr
evicting pod minio-operator/minio-operator-8479f6867d-f9xnq
evicting pod personalization-blue/personalization-6bdd49bc9-p62r4
evicting pod personalization-blue/personalization-taskmanager-2-9
evicting pod argocd/argocd-repo-server-d66864785-jght8
evicting pod kafka/personalization-brokers-5
```

**Both `brokers-4` and `brokers-5` were on `worker2`.** Two of the three Kafka
brokers on one node. See the notes.

### The Flink pods were evicted without objection

```
pod/personalization-6bdd49bc9-p62r4 evicted
pod/personalization-taskmanager-2-9 evicted
```

`personalization-pdb` was consulted and permitted both, exactly as the
arithmetic above predicts.

### The refusal, verbatim, and it came from Strimzi

```
evicting pod kafka/personalization-brokers-5
error when evicting pods/"personalization-brokers-5" -n "kafka" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget.
evicting pod kafka/personalization-brokers-5
error when evicting pods/"personalization-brokers-5" -n "kafka" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget.
```

Repeating every 5 seconds, indefinitely. Nine of ten evictions succeeded. The
tenth never could.

### Why it could never succeed

```
NAME                    MIN AVAILABLE   ALLOWED DISRUPTIONS
personalization-kafka   5               0
personalization-pdb     1               1
```

`personalization-kafka` covers 6 pods, 3 controllers and 3 brokers, with
`minAvailable: 5`.

```
1. before the drain          6 available
2. evict brokers-4           if it leaves -> 5 remain, 5 >= 5   ALLOWED
3. brokers-4 goes Pending    5 available
4. evict brokers-5           if it leaves -> 4 remain, 4 <  5   REFUSED
```

`ALLOWED DISRUPTIONS` is the subtraction `available - minAvailable`, so 5 minus 5
is 0. **A budget can be satisfied and immovable at the same time.** Step 2 spent
the only slack.

**`minAvailable` is a rule about the state after the eviction, not about the
state now.**

### Why `brokers-4` could not come back

```
FailedScheduling: 0/6 nodes are available:
  1 node(s) were unschedulable,                        <- worker2, cordoned by the drain
  2 node(s) didn't match PersistentVolume's node affinity,
  3 node(s) had untolerated taint(s)                   <- the 3 control planes
```

Six nodes, three reasons, one node left standing and it is the cordoned one.

```
PV pvc-018628e2-42e6-48cb-af73-c0aa8c32e36b
   claim data-0-personalization-brokers-4
   node  personalization-lab-worker2
```

`kind` uses a local-path provisioner, so a PersistentVolume is a directory on one
node's disk. It cannot follow the pod. `brokers-4` can only ever run on
`worker2`.

### The deadlock, in full

```
brokers-4 can run ONLY on worker2        its PV is pinned there
worker2 is cordoned                      by the drain itself
5 available, minAvailable 5              ALLOWED DISRUPTIONS = 0
brokers-5 eviction refused               forever
drain never exits
```

**Nothing inside the cluster can break this.** The eviction waits on a pod that
waits on the cordon that the drain applied. The only exit is `uncordon`, or
interrupting the drain.

### Flink placement after the drain

```
JM  hw6ct  personalization-lab-worker3   (zone-c)
JM  z2ppt  personalization-lab-worker    (zone-a)    <- replacement, NOT Pending
TM  2-7    personalization-lab-worker3
TM  3-1    personalization-lab-worker3               <- worker3 now carries TWO
TM  2-8    personalization-lab-worker

JOB      LIFECYCLE
RUNNING  STABLE
```

**`DoNotSchedule` held.** Two JobManagers, two different Zones, no `Pending` pod.

**`ScheduleAnyway` earned its place.** The evicted TaskManager's replacement
landed on `worker3`, which already had one, making the distribution 2/0/1 rather
than 1/1/1. A `DoNotSchedule` TaskManager constraint would have left it `Pending`
and stalled the recovery this Drill exists to observe. That was the stated
reasoning in Task 5 Step 4, and this is the run that tests it.

**The job stayed `RUNNING` throughout.** No restart, no checkpoint restore. Only
one of six slots moved, and Flink redeployed that subtask without failing the
job.

### Recovery after uncordon

```
nodes                    no SchedulingDisabled
brokers-4                Running on worker2, Ready
personalization-kafka    ALLOWED DISRUPTIONS 0 -> 1
under-replicated         none
```

Kafka caught `brokers-4` back up on its own. Nothing to repair by hand.

### Gap check

```
==> before: 725 identities
==> after:  801 identities
 ok no gap
 ok no duplicates
 ok recovery is clean
exit=0
```

76 new Recommendations across the Drill.

## Notes

**Kafka is not Zone-spread, and Flink is.** `brokers-4` and `brokers-5` shared
`worker2`, so draining one node removed two thirds of the Kafka cluster and left
it at exactly `min.insync.replicas: 2` with no margin. One more broker loss and
producers would have failed.

`manifests/flink/blue/flinkdeployment.yaml` carries
`topologySpreadConstraints` on both roles. `manifests/strimzi/kafka-cluster.yaml`
carries none, so the scheduler placed brokers wherever they fit. **This is worth
fixing in the Phase 1 manifests**, not just recording here.

**A strict PDB plus node-local storage makes a node undrainable.** Not slow.
Undrainable, until one of the two conditions is removed. This is the real
production failure mode a `kubectl drain` for a kernel upgrade would hit, and it
is a better finding than the outcome the plan scripted.

**`personalization-pdb` cannot refuse a single-node drain** at 2 replicas across
3 Zones, because a spare Zone always exists and the budget always has slack. To
see it refuse, both other Zones would have to be cordoned first, which drives
`ALLOWED DISRUPTIONS` to 0 by leaving the replacement `Pending`.

**Record `kubectl get pdb -A`, not just the Flink namespace.** Drills A and B
touched one pod. This Drill drained a node, so every workload on it was evicted
and every PDB in the cluster was consulted. The refusal came from a namespace the
plan never mentioned.

**Always uncordon.** The failure mode the plan warns about is real: the node
stays unschedulable, Phase 6's autoscaling then behaves strangely, and the cause
is days behind you. `kubectl get nodes` before walking away.

**Next Drill.** Drill D patches `job.state` to `suspended` through an ArgoCD Lua
action and reads the resulting `OutOfSync` as its signal. No pod is killed and no
node is touched, so neither the PDB nor the spread constraints are involved.
