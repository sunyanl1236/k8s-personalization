# ADR 0006: Blue/green runs the `native` variant

Date: 2026-08-10
Status: accepted

## Context

[ADR 0005](0005-autoscaling-two-deployment-modes.md) leaves the project with two
`FlinkDeployment` variants. Phase 7's blue/green promotion has to pick one.

## Decision

Both `personalization-blue` and `personalization-green` run the `native`
variant, matching Phases 5 and 6a.

## Consequences

The promotion runbook operates on the same manifest lineage that the HA work,
zone spread, PodDisruptionBudget, and Job Autoscaler were validated against.
One config path carries through Phases 5, 6a, and 7.

The standalone/KEDA variant is a side branch that is never promoted. Lag-driven
scaling and zero-downtime promotion are demonstrated separately and never
compose. Accepted.

The promotion is a shell script, `scripts/promote-green.sh`, not an ArgoCD Lua
action. The Lua sandbox has no `string` library, so it cannot parse a savepoint
path out of `status.jobStatus.savepointInfo.lastSavepoint.location`. See
[0004](0004-gitops-from-phase-0.md).

## Correctness note carried from the design spec

Blue and green must never both be `RUNNING`. Nothing in Kafka enforces this.
Flink's `KafkaSource` does not participate in Kafka's consumer-group rebalance
protocol. The `KafkaSourceEnumerator` in the JobManager queries the partition
list directly and assigns partitions to its own subtasks. Read offsets live in
Flink operator state and ride inside the checkpoint, not in
`__consumer_offsets`. Any offset commit back to Kafka is for external lag
monitoring only, and Flink never reads it back.

So two concurrently running jobs with the identical `group.id` do not
coordinate. Each independently believes it owns every partition, and every
event is processed twice.

The only thing preventing that is the ordering inside the runbook: blue's
TaskManagers are confirmed torn down (`status.jobStatus.state == "SUSPENDED"`)
before green is started. That is an operational guarantee the script enforces,
not a protocol guarantee Kafka provides.

## Shared-storage note

Blue and green share one MinIO bucket. Checkpoint paths are namespaced per job
so they do not collide. HA metadata is the risk: both deployments must use
distinct `high-availability.cluster-id` values, otherwise the standby side's
JobManager can attempt leader election against the active side's HA metadata.
Verify this before the first promotion, not after.

## Alternatives rejected

- **Standalone for both sides.** Would let KEDA and promotion compose, but
  forces re-validating all of Phase 5's HA work under a different mode.
- **Blue native, green standalone.** Savepoints are mode-portable so it would
  probably work, but it turns Phase 7 into a research task in a timeline the
  spec already calls tight.
