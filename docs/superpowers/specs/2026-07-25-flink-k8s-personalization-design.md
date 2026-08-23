# Flink on Kubernetes: Real-Time E-Commerce Personalization Lab

Date: 2026-07-25
Status: approved (design), not yet implemented

## Goal

Learn Kubernetes and Apache Flink together by building one real-time e-commerce
personalization pipeline, deployed on a self-managed, multi-AZ, highly available
Kubernetes cluster. The project should exercise the full depth of Flink's
DataStream API and the core mechanics of running your own highly available
Kubernetes control plane, not a curated subset of either.

## Why this domain

E-commerce personalization was chosen over four other candidates (fraud
detection, network intrusion detection, ride-hailing dispatch, IoT predictive
maintenance) because it has the strongest public evidence of any Flink use
case: Alibaba's Blink platform (later merged upstream into Flink) runs this
exact pattern (real-time recommendation, search ranking, ad serving) at over
one trillion events a day, peaking above 470 million transactions per second
during the Double 11 shopping festival. This project follows that shape at
lab scale, not that volume.

Fraud detection was the strongest alternative (Flink's own team publishes a
fraud detection reference architecture built on the same primitives used
here: keyed state, broadcast state for live rule updates). If personalization
turns out to be too thin on any one concept during implementation, fraud
detection is the fallback domain, not intrusion detection, ride-hailing, or
IoT, which had weaker public evidence.

## Explicit constraints

- **DataStream API only.** No Table API, no Flink SQL. The Table API would
  handle windowing and state internally; the point of this project is to
  expose that machinery directly (ProcessFunction, KeyedProcessFunction,
  explicit state descriptors, explicit watermark strategies).
- **No AWS, no cloud spend.** Originally scoped as EKS-based, then revised:
  the project runs entirely on a local `kind` cluster. The reason for
  dropping EKS was explicitly re-checked against the original motivation for
  pairing Kubernetes with Flink (a job requirement for "Kubernetes" broadly,
  not "EKS" specifically), so nothing is lost by dropping it.
- **Self-managed, multi-AZ, highly available control plane**, not a managed
  one. This is the one requirement carried over unchanged from the original
  EKS framing: even without EKS, the control plane must still be HA and
  zone-aware, not a single-node convenience cluster.

## Non-goals

- No IRSA, no VPC/subnet configuration, no real S3 (MinIO substitutes).
- No real cloud capacity behind infrastructure-level autoscaling. Karpenter
  runs for real, using its official `kwok` provider (part of
  `kubernetes-sigs/karpenter`), which exercises the real Karpenter
  controller and real provisioning decisions (NodePool/NodeClass,
  consolidation, disruption budgets) against simulated nodes. The accepted
  limit: `kwok` nodes have no real kubelet and no real container capacity,
  so Flink TaskManagers cannot actually schedule onto capacity Karpenter
  "provisions." Infra-level autoscaling is therefore learned at the level
  of Karpenter's decision logic, not observed end-to-end with the Flink
  workload actually landing on new nodes. A short, deliberate real-AWS
  validation of the same Karpenter config was considered and explicitly
  deferred, not ruled out; revisit if the kwok-only version leaves this
  concept feeling incomplete.
- No Spot-instance node reclaim simulation (that was an AWS-specific
  resilience story; replaced by manually killing control-plane and
  TaskManager pods/nodes and verifying recovery).
- No IRSA or real EC2 for Karpenter itself. Karpenter's AWS provider does
  support self-managed (non-EKS) clusters, which was checked directly since
  it's a real constraint, but the setup is meaningfully more manual than
  with EKS (hand-written node bootstrap userData, no automatic EKS-tagged
  subnet/security-group discovery). That path was considered and not taken,
  in favor of the `kwok` provider below.
- No manual kubeadm bootstrapping. `kind`'s built-in multi-control-plane mode
  (which runs kubeadm under the hood) is used instead of hand-rolling etcd
  and cert distribution. This was a deliberate choice for depth-per-hour:
  you still observe and can break real API server HA and etcd quorum
  behavior, but bootstrapping itself is templated.
- No personal daily-use pivot. An earlier direction considered turning this
  into a personal shopping/price-tracking tool the author would use daily;
  that was explicitly dropped in favor of keeping the original
  Alibaba-scale synthetic personalization domain.
- No Blue/Green environment duplication *for failure recovery*. That
  recovery story is already covered by the ArgoCD suspend/resume/restart
  drills and the manual pod/node kill drills below. A Blue/Green split is
  still used, but for a different purpose: zero-downtime deployment of job
  changes, described in its own section below. That's a separate concern
  from recovery, not a reversal of this decision.

## Architecture

```
                     Synthetic clickstream generator
                     (many simulated users, out-of-order
                      arrival across simulated regions)
                                |
                                v
                     Kafka (Strimzi, in-cluster)
                                |
                                v
        [Watermark strategy: bounded out-of-orderness]
                                |
                                v
                          [keyBy(userId)]
                                |
        +-----------------------+-----------------------+
        |                       |                        |
        v                       v                        v
[Session window:         [KeyedProcessFunction:   [Broadcast state:
 browsing behavior        per-user running          live promo/discount
 per session]             state, RocksDB,           rules, updated
                          checkpointed]             without redeploy]
        |                       |                        |
        +-----------+-----------+-----------+------------+
                    |                       |
                    v                       v
        [Interval join: clickstream    [CEP: "viewed X 3x,
         x inventory/price-change       checked competitor,
         stream]                        went idle" pattern]
                    |                       |
                    +-----------+-----------+
                                |
                                v
                  [Async I/O: recommendation
                   service lookup, mocked API]
                                |
                                v
                  [Side output: late events /
                   low-confidence signals]
                                |
                                v
                 Personalized recommendation/offer
                 sink (Kafka topic), exactly-once
                 via checkpointing
```

### Kubernetes layer, wrapped around the app, not baked into it

```
kind cluster
  3 control-plane nodes  <-- HAProxy load balancer (kind-managed)
  N worker nodes, labeled topology.kubernetes.io/zone = zone-a / zone-b / zone-c

  Helm installs every component below; nothing is applied as raw manifests

  cert-manager
    Issues the webhook TLS certificate the Flink Kubernetes Operator needs

  Flink Kubernetes Operator (depends on cert-manager)
    FlinkDeployment CRD
    JobManager: HA (standby instances), spread across zones via
                pod topology spread constraints, protected by a
                PodDisruptionBudget (minAvailable: 1)
    TaskManager: spread across zones, RocksDB state backend,
                 checkpoints to MinIO (S3-compatible)

  Strimzi (Kafka, in-cluster)
  MinIO (S3-compatible checkpoint storage)
  Prometheus + Grafana (Flink metrics, cluster health)

  Karpenter (real controller) + kwok provider (simulated nodes)
    NodePool / NodeClass definitions
    NodePool carries a workload=flink NoSchedule taint; FlinkDeployment
    pods tolerate it, keeping other workloads off Karpenter-managed nodes
    Reacts to unschedulable pods generated by load tests
    Provisions/deprovisions simulated Node objects, no real kubelet

  KEDA
    ScaledObject watches Kafka consumer-group lag on the clickstream topic
    Scales TaskManager replica count; Flink reactive mode absorbs the new
    count without a savepoint restart

  ArgoCD
    Syncs everything above from a Git repo of manifests/Helm values
    Custom Lua actions (suspend/resume/restart) patch a live
    FlinkDeployment for operational drills; the next sync reverts the patch

  personalization-blue / personalization-green (namespaces)
    One FlinkDeployment each, sharing the cluster's Kafka topics and MinIO
    bucket. Only one namespace is ever RUNNING at a time; see Zero-downtime
    deployment below

  OTel Collector (scoped to Flink metrics only, not cluster-wide)
    prometheus receiver scrapes Flink JM/TM endpoints in both namespaces
    k8sattributes processor tags each metric with k8s.namespace.name
    prometheus exporter re-exposes the result as one new Prometheus
    scrape target; node-exporter/kube-state-metrics stay on Prometheus's
    existing direct scrape, unchanged
```

Because the app only talks to Kafka and an S3-compatible endpoint, none of
the pipeline code depends on being local vs. cloud. That boundary is what
keeps the Kubernetes layer swappable without touching Flink logic.

### Zero-downtime deployment

Both namespaces run the same job against the same Kafka topics and the
same MinIO checkpoint bucket; nothing is replicated across them since it's
one cluster. A promotion moves the active job from one namespace to the
other via a scripted GitOps runbook, `scripts/promote-green.sh` (works in
either direction; "green" here just means "the standby side"):

```
1. kubectl patch flinkdeployment/blue -n personalization-blue --type merge \
     -p '{"spec":{"job":{"state":"suspended"}}}'
   Requires upgradeMode: savepoint on the FlinkDeployment, so this single
   patch makes the operator take a savepoint AND tear down blue's
   TaskManagers as one atomic step. Precondition: script first verifies
   blue is currently RUNNING, and aborts loudly if it isn't -- this guards
   against two overlapping promotion runs.
2. Poll until status.jobStatus.state == "SUSPENDED" and
   status.jobStatus.savepointInfo.lastSavepoint.location is non-empty.
   Timeout -> abort: patch blue's state back to "running". Nothing lost,
   green was never touched.
3. Edit manifests/green/flinkdeployment.yaml:
     spec.job.initialSavepointPath = <captured path>, spec.job.state = running
4. git commit + git push (the GitOps repo ArgoCD watches)
5. Poll green's FlinkDeployment until status.jobStatus.state == "RUNNING".
   Timeout here is the one failure mode with no automatic safe path: blue
   is already suspended and green isn't up. Fallback: a human resumes blue
   from its own pre-cutover savepoint as immediate mitigation, then debugs
   green separately.
```

Blue and green are never both RUNNING at once by design. Kafka itself
doesn't enforce this. Flink's Kafka source assigns partitions and tracks
offsets itself, entirely bypassing Kafka's consumer-group rebalance
protocol. So if both jobs ran at the same time, each would independently
consume the whole topic and double-process every event. What actually
prevents that is the ordering above: blue is fully torn down in step 2
before green starts in steps 3 through 5, not any Kafka-side coordination.

**Rollback** mirrors the same script in reverse: take a fresh savepoint
from the now-active side before suspending it, then resume the other side
from *that* savepoint, not its original pre-cutover one, so no progress
made during its run is discarded.

## Concept coverage map

### Flink (DataStream API)

| Concept | Where it lives in the app |
|---|---|
| Event-time processing, watermarks, late data | Bounded out-of-orderness watermark strategy on the clickstream; side output for late/dropped events |
| Windowing | Session windows for per-user browsing behavior |
| Keyed state | `KeyedProcessFunction` maintaining per-user running state |
| Operator state | Checkpointed generator/source state |
| Broadcast state | Live promo/discount rule updates without redeploying the job |
| CEP | Multi-step behavior pattern: viewed a product repeatedly, viewed a competitor, went idle, triggers a real-time offer |
| Interval join | Clickstream joined against an inventory/price-change event stream |
| Async I/O | Call to a mocked external recommendation service per relevant event |
| Checkpointing / savepoints | Exactly-once delivery to the recommendation sink; savepoint-based redeploys when rule *logic* (not just broadcast state) changes |

### Kubernetes

| Concept | Where it lives in the app |
|---|---|
| Multi-control-plane HA | 3 control-plane nodes behind kind's load balancer; etcd quorum observed directly by killing a control-plane node |
| Zone-aware scheduling | `topology.kubernetes.io/zone` labels plus pod topology spread constraints for JobManager and TaskManager pods |
| Operator pattern | Flink Kubernetes Operator managing a FlinkDeployment CRD |
| Pod/node failure recovery | Killing a TaskManager pod in a simulated zone, verifying checkpoint-based recovery |
| Workload-level autoscaling | Flink's Job Autoscaler reacting to a synthetic load ramp (parallelism/resource adjustment under backpressure) |
| Infrastructure-level autoscaling | Karpenter (real controller) with the `kwok` provider: real NodePool/NodeClass config, real provisioning/consolidation/disruption decisions, simulated node capacity |
| External-metric-driven autoscaling | KEDA `ScaledObject` scaling TaskManager replica count off Kafka consumer-group lag, paired with Flink's `reactive` mode absorbing the new count without a savepoint restart. A direct contrast with the backpressure-driven Job Autoscaler above |
| Voluntary disruption protection | `PodDisruptionBudget` (`minAvailable: 1`) on JobManager pods, exercised in the same node-kill drills as zone-aware scheduling |
| Declarative delivery / GitOps | Helm installs every in-cluster component; ArgoCD syncs `FlinkDeployment` and supporting manifests from Git, exercising continuous reconciliation instead of one-off `kubectl apply` |
| Live operational actions | ArgoCD custom Lua actions (`suspend`/`resume`/`restart`) patch a running `FlinkDeployment` directly; the next Git sync reverts the patch, exercising GitOps drift-correction alongside the manual pod/node kill drills |
| Operator webhook prerequisites | cert-manager issues the TLS certificate the Flink Kubernetes Operator's admission webhook needs, installed and verified ready before the operator itself |
| Zero-downtime deployment | Blue/green namespaces sharing Kafka and MinIO; a scripted GitOps promotion runbook stops the active job with a savepoint, restores the standby from it, and confirms it's running before suspending the old one |
| Telemetry enrichment | OTel Collector scoped to Flink metrics only, tagging each metric with its namespace (blue/green) via the `k8sattributes` processor before Prometheus's existing scrape picks it up |
| Observability | Prometheus scraping Flink and cluster metrics, Grafana dashboards |

## Build order

1. **Phase 0: local infrastructure.** `kind` multi-control-plane config,
   zone labels on worker nodes, Helm as the install mechanism for every
   component that follows, Strimzi, MinIO, cert-manager. Verify the cluster
   survives a killed control-plane node before writing any Flink code.
2. **Phase 1: core DataStream pipeline, locally.** Synthetic clickstream
   generator, watermark strategy, session windows, keyed state,
   checkpointing. Get this correct before adding complexity.
3. **Phase 2: broadcast state, interval join, CEP, async I/O.** Layer in
   the remaining Flink concepts on top of the working Phase 1 pipeline.
4. **Phase 3: Kubernetes-specific behavior.** Deploy via the Flink
   Kubernetes Operator (Helm install, depends on cert-manager) with
   zone-spread pods and a JobManager PodDisruptionBudget, exercise pod/node
   failure recovery, exercise the Job Autoscaler under a load ramp, install
   Karpenter with the `kwok` provider and exercise its provisioning and
   consolidation decisions under the same load ramp, install KEDA and
   re-run the load ramp scaling TaskManager replicas off Kafka consumer lag
   with Flink reactive mode absorbing the change, install ArgoCD and bring
   all of the above under GitOps sync, exercise its Lua suspend/resume/
   restart actions as an additional operational drill.
5. **Phase 4: zero-downtime deployment.** Stand up the `personalization-blue`
   / `personalization-green` namespaces, build and test the
   `promote-green.sh` runbook (and its rollback direction) against a live
   load ramp, verifying a bounded pause with no duplicate processing.
   Deliberately break the standby side once to exercise the step-5 timeout
   fallback. Install the OTel Collector scoped to Flink metrics, verify
   Grafana can filter/group by blue vs. green, and deliberately kill the
   Collector pod to confirm cluster-infra metrics keep working while Flink
   metrics visibility recovers on its own.
6. **Phase 5: observability and polish.** Prometheus/Grafana dashboards,
   documentation. Optional stretch if time remains: extend the OTel
   Collector with a second pipeline forwarding Flink pod logs, reusing the
   same Collector already installed in Phase 4.

## Timeline

6 to 9 weeks, part time. Rough estimate: 55 to 80 hours, tight but accepted.
This rose from 45 to 65 hours because blue/green zero-downtime deployment
(namespaces, promotion/rollback scripting, testing) and the OTel Collector
metrics pipeline are new concepts layered on top of the prior scope, not
swapped-in replacements for anything else.

## Open risks

- CEP is the concept most likely to run over time budget if the pattern
  library's API turns out to be less ergonomic than expected.
- Killing control-plane nodes inside `kind` to observe etcd quorum behavior
  is not extensively documented; may require experimentation to do safely
  and repeatably.
- Karpenter's `kwok` provider proves the provisioning/consolidation logic,
  not the end-to-end experience of a Flink TaskManager actually landing on
  new capacity. If that gap ends up feeling like it undermines the concept
  rather than just simplifying it, the deferred short real-AWS validation
  (see Non-goals) is the pre-agreed way to close it, not a reason to redesign.
- If four weeks in the full concept list is clearly not going to fit, the
  agreed fallback is to trim CEP to a single pattern before cutting anything
  from the Kubernetes HA side, since HA/multi-AZ was the one requirement
  carried through every revision of this plan.
- KEDA and the Flink Job Autoscaler are two autoscaling paths acting on the
  same TaskManager pods. If running both at once causes them to fight over
  replica count during the load ramp, the agreed fallback is to demonstrate
  them in separate windows (Job Autoscaler first, then KEDA/reactive mode
  with the Job Autoscaler disabled), not to design one combined policy.
- ArgoCD's Lua sandbox has no `string` or `math` library, so the
  suspend/resume/restart custom actions must do whole-value patches (e.g.
  flipping `spec.job.state`) rather than any string parsing. This should be
  budgeted for up front, not discovered mid-implementation.
- The promotion runbook's step-5 timeout (standby side never reaches
  `RUNNING`, active side already suspended) is a real downtime window with
  no automatic safe path, only a documented manual fallback. If this proves
  more common than expected during testing, revisit whether the runbook
  needs a pre-flight validation step on the standby manifest before ever
  touching the active side.
- The OTel Collector is a new single point of failure for Flink metrics
  visibility specifically (Flink itself is unaffected if it goes down).
  This is an accepted trade-off, not an oversight: the Collector was kept
  for this narrow scope as a deliberate exercise in OTel pipeline
  mechanics, not because it's operationally necessary (the same namespace
  labeling is achievable with a one-line Prometheus `relabel_configs` rule
  and zero extra components).
