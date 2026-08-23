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
| 3 | Core pipeline | in progress |
| 4 | Advanced Flink | not started |
| 5 | Operator and HA | not started |
| 6 | Autoscaling | not started |
| 7 | Blue/green and OTel | not started |
| 8 | Observability and docs | not started |

Phases are defined in
[the implementation plan](../superpowers/plans/2026-08-10-implementation-phases.md).
