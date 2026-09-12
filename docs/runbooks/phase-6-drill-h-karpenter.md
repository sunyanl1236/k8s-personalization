# Phase 6 Drill H: Karpenter provisions a node, then takes it away

Date: 2026-09-09
Runtime: Flink 2.2.0 on `kind`, managed by Flink Kubernetes Operator 1.15.0
Karpenter: `kubernetes-sigs/karpenter` at commit `02caf5a` (2026-09-02), kwok
provider, controller image `lab/karpenter-kwok:0.1-02caf5a` built locally
kwok: `v0.8.0`
Installed by: `scripts/bootstrap.sh karpenter`, outside ArgoCD (spec 9.3)
Resources: `manifests/karpenter/nodepool.yaml`, `manifests/karpenter/decoy.yaml`,
synced by `manifests/argocd-apps/karpenter-resources.yaml`
Start state: 6 real nodes, 0 NodeClaims, Decoy at `replicas: 0`

Per `CONTEXT.md`:

> **Decoy Workload**: a workload whose only purpose is to be unschedulable.

## What this Drill proves, and what it does not

**It does not prove "we ran out of room and got more room."** That cannot be
shown on one laptop, with fake nodes or real ones. Every kind node advertises the
whole 23.5 GiB host, so the scheduler will never report `Pending` for memory
here.

**It proves the control loop.** A pod cannot be placed, Karpenter notices,
decides what node would fix it, creates one, the pod lands, and the node is
removed when no longer needed. That loop is identical on EKS with real EC2
instances. Only the provider underneath changes, and the seam it changes at is
three lines of `nodeClassRef`.

## The one thing that makes or breaks it

**Karpenter never creates a node because you asked it to.** It reacts only to
pods the scheduler could not place. No `Pending` pod, no node, ever. There is no
"create three nodes" button in a NodePool.

So the Drill needs something that cannot be scheduled. That is the Decoy's whole
job, and it is why two fields do two different things:

- `nodeSelector: node-role=decoy` makes it **unschedulable now**. No real worker
  carries that label.
- The `workload=flink:NoSchedule` toleration lets it **land later**, once a kwok
  node exists.

**A toleration is permission, not attraction.** A Decoy carrying only the
toleration schedules onto a real worker immediately. Karpenter then sees nothing
pending, creates nothing, and the Drill appears to run while proving nothing.
That is the failure this Drill is built to catch, and the phase plan gets it
wrong.

Mechanism is in [the Phase 6 knowledge doc](../knowledge/phase-6-autoscaling.md).

## Procedure

**1. Record the starting state.**

```bash
kubectl get nodes
kubectl get nodeclaims
```

Expect 6 nodes, all `personalization-lab-*`, all days old. Expect
`No resources found` for NodeClaims.

**2. Start a watch before scaling.** The `Pending` window is about five seconds,
so it closes before a plain `get pods` returns.

```bash
kubectl -n karpenter-decoy get pods -o wide -w
```

**An empty watch looks identical to a hung command.** With `replicas: 0` there
are no pods, so `-w` prints nothing and blocks. That is correct. Run the same
command without `-w` to see `No resources found` and confirm.

**3. Scale the Decoy up.**

```bash
kubectl -n karpenter-decoy scale deploy/decoy --replicas=5
```

**4. Prove the pods were unschedulable.** The durable evidence is Karpenter's own
log, not the watch.

```bash
kubectl -n kube-system logs deploy/karpenter --since=5m | grep -i provisionable
```

`"pending-pods":5` and `found provisionable pod(s)` naming all five is the proof.
Karpenter only ever acts on pods the scheduler could not place, so those lines
mean they were `Pending` whether or not you saw it.

**5. Confirm a node was created, and that it is Karpenter's.**

```bash
kubectl get nodes
kubectl get nodes -l node-role=decoy
kubectl get nodeclaims
kubectl -n karpenter-decoy get pods -o wide
```

The label query is the sharpest of the four: `node-role=decoy` exists only on
nodes this NodePool creates, so anything it returns was provisioned. Filtering
by the name `kwok` is weaker, because a name could coincide.

The `NodeClaim` is the receipt. It links NodePool, instance type, capacity type
and the Node that fulfilled it, in one row.

**6. Confirm the node is fake.**

```bash
N=$(kubectl get nodes -l node-role=decoy -o name | head -1)
kubectl get "$N" -o jsonpath='{.status.nodeInfo}' | python3 -m json.tool
```

This is worth capturing, because it is the reason the Decoy exists at all rather
than reusing a Flink pod.

**7. Confirm Flink was untouched.**

```bash
kubectl -n personalization-blue get pods -o wide
kubectl get pods -A -o wide | grep kwok-decoy
```

Every TaskManager still on a real worker, and no Flink pod on the kwok node at
any point.

**8. Scale to zero and watch consolidation.**

```bash
kubectl -n karpenter-decoy scale deploy/decoy --replicas=0
kubectl get nodes -l node-role=decoy -w
```

## Observed result

Two runs on 2026-09-09. Run 1 covered the full cycle including teardown. Run 2
is the one driven by hand, and it produced better evidence of the `Pending`
state.

### Run 2: provisioning

Karpenter's decision, 5 seconds after the scale:

```
DEBUG  provisioner  computing scheduling decision for provisionable pod(s)
       pending-pods: 5   deleting-pods: 0
INFO   provisioner  found provisionable pod(s)
       Pods: karpenter-decoy/decoy-6d45f5d4bb-p5glj, …-blnpq, …-gzf6l, …-czb8k, …-zrf2r
       duration: 4.189884ms
```

**`pending-pods: 5` is the line that matters.** It is the only direct record that
all five pods were unschedulable, and it survives long after the pods have
started.

Nodes afterwards:

```
NAME                                                  STATUS   ROLES           AGE     VERSION
kwok-decoy-qvhbs-whipindigo-2-1mgnhybdus-3441545326   Ready    <none>          2m12s   kwok-v0.8.0
personalization-lab-control-plane                     Ready    control-plane   28d     v1.34.8
personalization-lab-control-plane2                    Ready    control-plane   28d     v1.34.8
personalization-lab-control-plane3                    Ready    control-plane   28d     v1.34.8
personalization-lab-worker                            Ready    <none>          28d     v1.34.8
personalization-lab-worker2                           Ready    <none>          28d     v1.34.8
personalization-lab-worker3                           Ready    <none>          28d     v1.34.8
```

**The `VERSION` column gives it away before anything else does.** Six nodes at
`v1.34.8`, one at `kwok-v0.8.0`. And `2m12s` against `28d`.

The label query returns exactly one node:

```
kwok-decoy-qvhbs-whipindigo-2-1mgnhybdus-3441545326   Ready   <none>   2m39s   kwok-v0.8.0
```

The NodeClaim:

```
NAME          TYPE               CAPACITY   ZONE          NODE                     READY   AGE
decoy-qvhbs   c-8x-amd64-linux   spot       test-zone-b   kwok-decoy-qvhbs-…       True    3m11s
```

All five Decoy pods on it, one node, not five:

```
decoy-6d45f5d4bb-blnpq   1/1   Running   0   3m42s   10.244.7.1   kwok-decoy-qvhbs-…
decoy-6d45f5d4bb-czb8k   1/1   Running   0   3m42s   10.244.7.3   kwok-decoy-qvhbs-…
decoy-6d45f5d4bb-gzf6l   1/1   Running   0   3m42s   10.244.7.4   kwok-decoy-qvhbs-…
decoy-6d45f5d4bb-p5glj   1/1   Running   0   3m42s   10.244.7.5   kwok-decoy-qvhbs-…
decoy-6d45f5d4bb-zrf2r   1/1   Running   0   3m42s   10.244.7.2   kwok-decoy-qvhbs-…
```

### The node is fake

```json
{
    "architecture": "amd64",
    "bootID": "",
    "containerRuntimeVersion": "kwok-v0.8.0",
    "kernelVersion": "kwok-v0.8.0",
    "kubeProxyVersion": "kwok-v0.8.0",
    "kubeletVersion": "kwok-v0.8.0",
    "machineID": "",
    "operatingSystem": "linux",
    "osImage": "",
    "systemUUID": ""
}
```

**`bootID`, `machineID`, `systemUUID` and `osImage` are all empty strings.** A
real node cannot have those. Nothing booted, so there is no boot id. And the
kubelet, kube-proxy, container runtime and kernel all report `kwok-v0.8.0`,
because one program is answering for all four.

Provenance and the taint the NodePool asked for:

```
nodepool=decoy   instance-type=c-8x-amd64-linux   capacity-type=spot   zone=test-zone-b
taints=[{"effect":"NoSchedule","key":"workload","value":"flink"}]
```

### Flink was untouched

```
personalization-66947d9f4d-9nsxr -> personalization-lab-worker3
personalization-66947d9f4d-xpvr2 -> personalization-lab-worker2
personalization-taskmanager-1-1  -> personalization-lab-worker2
```

Everything on the kwok node:

```
karpenter-decoy/decoy-6d45f5d4bb-blnpq
karpenter-decoy/decoy-6d45f5d4bb-czb8k
karpenter-decoy/decoy-6d45f5d4bb-gzf6l
karpenter-decoy/decoy-6d45f5d4bb-p5glj
karpenter-decoy/decoy-6d45f5d4bb-zrf2r
kube-system/kindnet-dxtzb
kube-system/kube-proxy-hjpzv
```

**Seven pods, not five, and the two extras are expected.** `kindnet` and
`kube-proxy` are DaemonSets that tolerate everything, so they land on any node
that appears. Neither runs, because nothing on a kwok node runs. **Do not read
this as a leak.** The check that matters is that no `personalization-*` pod is in
this list.

### Run 1: consolidation

Scaled from 5 to 0, polled every 5 seconds:

```
t+5s   kwok nodes=1  nodeclaims=1
t+10s  kwok nodes=1  nodeclaims=1
…
t+40s  kwok nodes=1  nodeclaims=1
t+45s  kwok nodes=0  nodeclaims=0
```

**45 seconds, not 10.** See the Notes.

Run 1's provisioning was identical in shape: one node
`kwok-decoy-7gwxj-chestpeppermint-1-cz05lesppx-921336627`, NodeClaim
`decoy-7gwxj`, `c-8x-amd64-linux`, `spot`, `test-zone-a`, all five pods on it.
The controller log gave the full sequence:

```
provisioner          computing scheduling decision for provisionable pod(s)
provisioner          found provisionable pod(s)
provisioner          computed new nodeclaim(s) to fit pod(s)
provisioner          created nodeclaim
                     instance-types: c-16x, c-8x, m-16x, m-8x, s-16x and 1 other
nodeclaim.lifecycle  launched nodeclaim
nodeclaim.lifecycle  registered nodeclaim
nodeclaim.lifecycle  initialized nodeclaim
nodeclaim.disruption marking consolidatable
```

## Gate

**Drill H passes.**

- **Nodes appeared under load.** One kwok node, 5 seconds after the scale, with a
  NodeClaim naming the NodePool that asked for it.
- **The pods were genuinely unschedulable.** `pending-pods: 5` in the controller
  log, which no amount of watching by hand could have proved after the fact.
- **Nodes went away under consolidation.** 5 to 0 in 45 seconds, NodeClaim
  deleted with the node.
- **Flink never touched a fake node.** Every TaskManager on a real worker
  throughout, and nothing named `personalization-*` on the kwok node at any
  point. **Flink pods were untouched; the cluster's CNI was not.** See "The fake
  node breaks the real CNI" below.
- **The runbook says what the Drill does and does not prove.** See the top.

## The fake node breaks the real CNI

**Found on 2026-09-10, after Run 2.** Five of six `kindnet` pods went
`CrashLoopBackOff` on the **real** nodes while the kwok node existed.

From kindnet's own log:

```
Node kwok-decoy-qvhbs-… has CIDR [10.244.7.0/24]
Adding route {Dst: 10.244.7.0/24  Gw: 10.244.3.7}
Failed to reconcile routes, retrying after error: network is unreachable
panic: Maximum retries reconciling node routes: network is unreachable
```

The chain:

1. The kwok node is a real `Node` object, so **kube-controller-manager allocates
   it a pod CIDR** like any other node. Here, `10.244.7.0/24`.
2. kindnet on every real node reads all `Node` objects and adds a route to each
   one's pod CIDR, via that node's internal IP.
3. kwok gave the node the IP `10.244.3.7`, which sits **inside worker's own pod
   CIDR**. That is nonsense for a node and unreachable as a gateway.
4. `ip route add` fails with `ENETUNREACH`, kindnet retries, then panics.

**Impact, measured:**

- 5 of 6 kindnet pods crashing, 14 restarts each.
- **Flink was unaffected.** All three pods `Running` with `RESTARTS 0`, job
  `RUNNING`, `lifecycle=STABLE` throughout. kindnet logs `FailOpen is true,
  skipping nftables cleanup on shutdown`, so existing traffic kept flowing while
  it was down.
- **This is not cosmetic.** The CNI genuinely died on those nodes. It did not
  cause visible damage here because the cluster was quiet and FailOpen held.

**Recovery.** Once the kwok node is deleted, nothing references the stale CIDR
and the next restart succeeds on its own. After 14 restarts the backoff is at its
5-minute maximum, so it is faster to force it:

```bash
kubectl -n kube-system delete pod -l app=kindnet
```

Safe: the pods are already crashlooping, and FailOpen means shutdown does not
tear down the rules.

**A second, quieter cost. Each run permanently consumes a `/24`.** Before the
Drills, the workers held `10.244.3/4/5`. After two runs:

```
worker   10.244.3.0/24
worker2  10.244.5.0/24     <- was .4
worker3  10.244.6.0/24     <- was .5
```

Run 1 took `.4`, Run 2 took `.7`. **The blocks are not reclaimed when the node
goes.** `10.244.0.0/16` holds 256 of them, so this is not urgent, but it
accumulates across runs and a cluster rebuild is the only reset.

**No clean fix was identified.** `--allocate-node-cidrs` is set cluster-wide by
kind and cannot be waived for one node, and the kwok provider chooses the node's
internal IP. The practical mitigations are:

- **Keep the kwok node's lifetime short.** Scale up, capture the evidence, scale
  straight back down.
- **Restart kindnet afterwards**, with the command above.
- Run Karpenter Drills on a throwaway cluster if the CNI churn ever matters.

**This is worth knowing before Phase 7.** Blue/green promotion depends on pod
networking between two namespaces. Do not run a Karpenter Drill and a promotion
at the same time.

## Notes

**Consolidation took 45 seconds, and `consolidateAfter` is `10s`.** These are not
in conflict. `consolidateAfter` is how long a node must sit **idle** before
Karpenter will *consider* disrupting it. It is not a countdown to deletion. On
top of it come the disruption controller's own evaluation interval, draining the
node, and deleting the NodeClaim. **Expect tens of seconds, and do not read the
gap as a stall.**

Run 1's log shows `marking consolidatable` arriving while the node was still
fully loaded, which is the same distinction from the other side: being *eligible*
for consolidation is not being consolidated.

**The `Pending` window is about 5 seconds, so watching by hand does not work.**
kwok creates a node instantly, unlike a real cloud where an instance takes a
minute or more to boot and register. By the time `kubectl get pods` returns, the
pods are already `Running` on a node that did not exist when you pressed enter.
**Read `pending-pods` out of the controller log instead.** It is the only durable
record.

**One node took all five pods, and the node was sized to fit them.** The
NodeClaim asked for exactly:

```
requests: {"cpu":"5100m","memory":"1330Mi","pods":"7"}
```

Five Decoy pods at `cpu: 1` each is 5000m, plus 100m for the two DaemonSet pods,
and `pods: 7` is five plus those two. Karpenter did the bin-packing first and
then asked for one node that fits the answer. It did not create one node per pod.

**The `limits.cpu: "20"` ceiling was never reached**, because 5100m is well under
it. To exercise the ceiling the Decoy would need roughly 20 replicas. Worth
knowing that this run did not test that path.

**The resource requests on the Decoy are load-bearing, and the plan does not
mention them.** `manifests/karpenter/decoy.yaml` sets `cpu: "1"` and
`memory: 256Mi`. Without them Karpenter would size a node for pods that ask for
nothing, and `limits.cpu` would be decorative.

**`instance-type` and `capacity-type` are theatre, and useful theatre.** The node
reports `c-8x-amd64-linux`, `spot`, `test-zone-b`, and Run 1 got `test-zone-a`.
The kwok provider invents a plausible instance catalogue so that Karpenter's real
instance-selection code has something to choose from. Run 1's log shows it
considering six types before picking one. **None of it exists.** It is what makes
the control loop exercised here the same code that runs on EKS.

**`kubectl get nodes` gives the fake away in the `VERSION` column** before any
`jsonpath` is needed: `kwok-v0.8.0` against `v1.34.8`, and an `AGE` in minutes
against `28d`.

**Next task.** Task 9, documents: amend ADR 0005's Decision, add the Decoy
Workload to `CONTEXT.md`, update the Phase 6 entry in the phase plan, and close
out `status.md`.
