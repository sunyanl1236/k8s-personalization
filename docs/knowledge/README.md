# Knowledge docs

One document per phase. Each captures the concepts needed to *understand* that
phase, as opposed to the steps needed to *execute* it.

Three document types in this repo, and they do different jobs:

| Location | Answers |
|---|---|
| `docs/superpowers/specs/` | What are we building, and why this and not something else |
| `docs/superpowers/plans/` | In what order, and how do we know a phase is done |
| `docs/adr/` | Why did we decide *that*, and what did we reject |
| `docs/knowledge/` | **How does this thing actually work** |
| `CONTEXT.md` | What do we call things |

A knowledge doc is written *during* the phase, not before it. It records the
explanations that were actually needed, including the ones that only became
necessary because something was confusing or went wrong. That makes it honest
about where the real difficulty was.

## Index

| Phase | Doc | Status |
|---|---|---|
| 0 | [Cluster floor](phase-0-cluster-floor.md) | done |
| 1 | [Data platform](phase-1-data-platform.md) | done |
| 2 | Domain model and generator | done |
| 3 | [Core pipeline](phase-3-core-pipeline.md) | done |
| 4 | [Advanced Flink](phase-4-advanced-flink.md) | in progress |
| 5 | [Operator and HA](phase-5-operator-and-ha.md) | done |
| 6 | [Autoscaling](phase-6-autoscaling.md) | done |
| 7 | [Blue/green and the deployment mechanism](phase-7-blue-green.md) | design, not started |
| 8 | Observability and docs | not started |

## Runbooks

Step-by-step Drill procedures, with the rationale per command and an
**Observed result** section carrying the real transcript. They live in
[`docs/runbooks/`](../runbooks/).

| Phase | Drill | What it breaks |
|---|---|---|
| 0 | [Control plane](../runbooks/phase-0-control-plane-drill.md) | a control-plane node |
| 3 | [Late Click](../runbooks/phase-3-late-click-drill.md) | event-time ordering, by injecting a Click behind the watermark |
| 4 | [Promo Rule](../runbooks/phase-4-promo-rule-drill.md) | a live broadcast state update |
| 5 | [A: TaskManager kill](../runbooks/phase-5-drill-a-taskmanager-kill.md) | a slice of RocksDB state |
| 5 | [B: JobManager kill](../runbooks/phase-5-drill-b-jobmanager-kill.md) | the leader, and therefore the coordinator |
| 5 | [C: Zone drain](../runbooks/phase-5-drill-c-zone-drain.md) | a whole Zone, through the eviction API |
| 6 | [E: Autoscaler dry run](../runbooks/phase-6-drill-e-dry-run.md) | nothing. It reads the autoscaler's recommendation before anything may act on it |
| 6 | [F: Scale up](../runbooks/phase-6-drill-f-scale-up.md) | the assumption that parallelism is fixed. Four rescales, no restart |
| 6 | [G: Scale down](../runbooks/phase-6-drill-g-scale-down.md) | the assumption that shrinking mirrors growing. It does not |
| 6 | [H: Karpenter](../runbooks/phase-6-drill-h-karpenter.md) | the scheduler, with a pod that cannot be placed anywhere real |

Phase 5's Drill D, ArgoCD Lua actions and drift, was **removed by decision on
2026-09-07**. See
[status.md](../superpowers/plans/status.md) for what that leaves untested.

Drills E to G break nothing in the usual sense. What they exercise is an
**assumption**, and the design's own warning is that four of its five blockers
produce a plausible but wrong result while every layer reports success. Drill H
does break something, but not on purpose: a kwok node crashes `kindnet` on the
real nodes for as long as it exists. Its runbook records why and how to recover.

Phases are defined in
[the implementation plan](../superpowers/plans/2026-08-10-implementation-phases.md).
