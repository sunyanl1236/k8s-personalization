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
| 6 | [Autoscaling](phase-6-autoscaling.md) | in progress |
| 7 | Blue/green and OTel | not started |
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

Phase 5's Drill D, ArgoCD Lua actions and drift, was **removed by decision on
2026-09-07**. See
[status.md](../superpowers/plans/status.md) for what that leaves untested.

Phases are defined in
[the implementation plan](../superpowers/plans/2026-08-10-implementation-phases.md).
