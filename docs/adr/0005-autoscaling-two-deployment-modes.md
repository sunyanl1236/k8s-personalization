# ADR 0005: The Job Autoscaler and KEDA are two FlinkDeployment variants, not one

Date: 2026-08-10
Status: accepted; the two-variant half superseded 2026-09-08 by the decision
recorded below

> **Superseded in part.** The Decision section maintains two manifests and has
> Phase 6b swap to the Standalone Variant. Phase 6 builds **only** the Native
> Variant. The analysis is unchanged and still load-bearing: `mode: native` and
> `mode: standalone` really are incompatible, and a `ScaledObject` against a
> native `FlinkDeployment` really is a no-op. What changed is the answer to
> "should this lab demonstrate both", and it is now no. See
> [Amendment, 2026-09-08](#amendment-2026-09-08) at the end.

## Context

The design spec lists both autoscaling paths and records the risk as a conflict
over replica count:

> KEDA and the Flink Job Autoscaler are two autoscaling paths acting on the same
> TaskManager pods. If running both at once causes them to fight over replica
> count during the load ramp, the agreed fallback is to demonstrate them in
> separate windows.

That understates the problem. They are not two policies over one mechanism.
They require different values of `spec.mode` on the `FlinkDeployment`.

From `FlinkConfigBuilder.applyTaskManagerSpec` in the operator source:

```java
if (spec.getJob() != null
        && KubernetesDeploymentMode.getDeploymentMode(spec)
                == KubernetesDeploymentMode.STANDALONE) {
    if (!effectiveConfig.contains(
            StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS)) {
        effectiveConfig.set(
                StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS,
                FlinkUtils.getNumTaskManagers(effectiveConfig, getParallelism()));
    }
}
```

`spec.taskManager.replicas` maps to a **standalone-only** internal config
option. In `native` mode the JobManager's Kubernetes ResourceManager allocates
TaskManagers itself from the job's parallelism, and the replicas field has no
effect. The operator's controller-flow documentation confirms the counterpart:
rescaling TaskManagers on a scale operation is described specifically for
standalone mode with reactive scaling enabled.

The consequence is concrete. A KEDA `ScaledObject` whose `scaleTargetRef`
points at a `native` `FlinkDeployment` scales nothing at all. It is not a
conflict. It is a no-op.

| Path | Required `spec.mode` | Required config | Scaling knob |
|---|---|---|---|
| Flink Job Autoscaler | `native` | `jobmanager.scheduler: adaptive`, `job.autoscaler.enabled`, Flink 1.18+ for in-place rescaling | operator rewrites job parallelism |
| KEDA + reactive mode | `standalone` | `scheduler-mode: reactive` | `spec.taskManager.replicas` |

## Decision

Maintain two manifests. Phase 6 splits accordingly.

- `manifests/flinkdeployment-native.yaml`: `mode: native`, adaptive scheduler,
  Job Autoscaler enabled. This is the main line, and it is what Phases 5 and 7
  build on.
- `manifests/flinkdeployment-standalone.yaml`: `mode: standalone`, reactive
  scheduler, KEDA `ScaledObject` watching Kafka consumer-group lag.

Phase 6a demonstrates backpressure-driven scaling on the native variant.
Phase 6b swaps to the standalone variant and demonstrates lag-driven scaling.
They are never running at the same time.

## Consequences

Two manifests to keep in step. Job code, image, and Kafka/MinIO wiring are
shared. Mode, scheduler, and scaling config diverge.

The standalone variant does not inherit Phase 5's validation for free. Pod
templates, HA configuration, and upgrade behavior differ between modes, so
zone spread and the JobManager PodDisruptionBudget hold only for the native
variant unless separately re-verified. Phase 6b explicitly does not re-verify
them. That is an accepted scope limit, not an oversight.

The KEDA path stays a demonstration rather than part of the main line, because
blue/green in Phase 7 runs native. See
[0006](0006-blue-green-native-mode.md).

## Alternatives rejected

- **Cut KEDA, keep the Job Autoscaler.** One manifest, and the Job Autoscaler is
  what a real Flink shop uses. Rejected because it removes external-metric-driven
  autoscaling, which is the more transferable Kubernetes skill.
- **Cut the Job Autoscaler, keep KEDA.** Rejected because reactive mode is a
  comparatively dead-end Flink feature and backpressure-driven scaling is the
  concept worth having.

## Amendment, 2026-09-08

**Phase 6 implements the Native Variant only.** `manifests/flinkdeployment-standalone.yaml`
is not written, KEDA is not installed, and Phase 6b does not run.

### What is unchanged

Everything in Context. The `FlinkConfigBuilder.applyTaskManagerSpec` finding
stands, and Phase 6 has since added two pieces of evidence for it, both read from
the live cluster.

The `FlinkDeployment` CRD **does** declare a scale subresource:

```json
{"scale":{"labelSelectorPath":".status.taskManager.labelSelector",
          "specReplicasPath":".spec.taskManager.replicas",
          "statusReplicasPath":".status.taskManager.replicas"}}
```

So an HPA or a `ScaledObject` can target a `FlinkDeployment`, the API accepts the
write, and `kubectl` then shows the number sitting beside an unchanged pod count.
The original wording, "it is not a conflict, it is a no-op", is right and is
sharper than it first reads: the plumbing is complete and terminates in a field
that no native-mode code path reads.

Flink's own configuration reference forbids the same combination from the other
side. `scheduler-mode: REACTIVE` is "exclusively supported for standalone
application deployments and is not compatible with active resource managers like
YARN or Kubernetes". Two independent sources, the operator's code and Flink's
config docs, reach the same conclusion.

### Why the second variant is dropped

Three reasons, in the order that decided it.

1. **Phase 7 settles it before cost does.**
   [ADR 0006](0006-blue-green-native-mode.md) runs blue/green on the Native
   Variant. The Standalone Variant would be built, demonstrated, and then torn
   down one phase later, having contributed nothing that Phase 7 can stand on.
2. **The scope limit in Consequences is larger than it looked.** That paragraph
   accepts not re-verifying Zone spread and the PodDisruptionBudget. Phase 5 has
   since closed on the Native Variant with Zone spread proven 1/1/1, a PDB
   consulted during a real Zone drain, HA failover with `leaderTransitions`
   observed, and checkpoints landing in MinIO. The Standalone Variant inherits
   none of it, so it would be the only deployment in this project running
   unvalidated.
3. **The two variants are less different than the ADR assumed.** Reactive mode
   is a mode *of* the adaptive scheduler, so a KEDA-driven scale runs the same
   state machine as an autoscaler-driven one: cancel the Executions, rebuild the
   ExecutionGraph, restore from the last checkpoint. No savepoint, no redeploy,
   no JobManager restart, either way. The genuine difference is narrower than
   "two mechanisms":

   ```
   Native      parallelism changes  ->  pods follow
   Standalone  pods change          ->  parallelism follows
   ```

   Which is also why the Standalone Variant can only move the whole job.
   Reactive mode has one instruction, "use everything available", so it cannot
   give one vertex more subtasks than another. This job has 9 vertices, of which
   three carry the full Click rate and six see roughly a twentieth of it.

### What this costs

The reason **Alternatives rejected** gave for keeping KEDA still holds:
external-metric-driven autoscaling is the more transferable Kubernetes skill.
Dropping it means this project demonstrates no external-metric autoscaling
anywhere, and never exercises the `APIService` aggregation pattern by which
`external.metrics.k8s.io` is served.

**That is a deliberate exclusion, not an oversight.** It is the price of reasons
1 and 2. Anyone reading this later should not "fix" it by adding a `ScaledObject`
to the Native Variant, because that is precisely the silent no-op this ADR was
written to document.

### Consequences of the amendment

- The Consequences section's "two manifests to keep in step" no longer applies.
  There is one manifest.
- Phase 6's hour budget falls from 11 to roughly 7.
- No third namespace is needed, and the operator's `watchNamespaces` is
  unchanged.
- The comparison behind this decision, with the trade-offs of all three
  candidates, is in
  [the Phase 6 knowledge doc](../knowledge/phase-6-autoscaling.md).
