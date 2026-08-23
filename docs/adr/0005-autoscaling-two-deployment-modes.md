# ADR 0005: The Job Autoscaler and KEDA are two FlinkDeployment variants, not one

Date: 2026-08-10
Status: accepted

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
