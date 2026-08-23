# ADR 0001: Build the pipeline against MiniCluster, port to the cluster at Phase 5

Date: 2026-08-10
Status: accepted

## Context

The design spec's build order says "Phase 1: core DataStream pipeline, locally"
without saying what "locally" means. Two readings exist, and they imply
different phase boundaries and different tooling.

1. Flink's `MiniCluster` running inside the IDE or a JUnit test, talking to
   Kafka and MinIO in the `kind` cluster over a forwarded port.
2. A real `FlinkDeployment` on `kind` from the first line of pipeline code.

Option 2 makes every iteration a `docker build`, `kind load docker-image`,
operator reconcile cycle. That is minutes per change. Phases 3 and 4 are the
phases with the most code churn, so the cost lands exactly where it hurts most.

## Decision

Build Phases 3 and 4 against `MiniCluster`. Kafka and MinIO run in `kind` and
are reached over an external listener. The first real `FlinkDeployment` happens
in Phase 5.

## Consequences

The feedback loop for pipeline logic is seconds rather than minutes. Watermark,
window, and state bugs are found with a debugger attached.

Two porting costs move to Phase 5 and must be budgeted there rather than
discovered:

- **S3 filesystem plugin loading.** `MiniCluster` accepts `flink-s3-fs-hadoop`
  as an ordinary compile dependency. A containerized Flink requires it in
  `/opt/flink/plugins/s3-fs-hadoop/`, because Flink loads filesystems through a
  separate plugin classloader. The same code fails on-cluster with a
  `UnsupportedFileSystemSchemeException` if this is missed.
- **HA and JobManager failover are never exercised before Phase 5.**
  `MiniCluster` has one embedded JobManager. Standby JobManagers, HA metadata in
  MinIO, and leader election are all first touched in Phase 5.

This decision creates a hard requirement on Phase 1. See [0002](0002-strimzi-external-listener.md).

## Alternatives rejected

- **On-cluster from day 1.** No porting surprises, but it slows the two
  heaviest code phases by roughly an order of magnitude per iteration.
- **Hybrid (harness tests local, full graph on-cluster).** More up-front setup,
  and it splits Phase 3 into two tracks before the pipeline is even correct once.
