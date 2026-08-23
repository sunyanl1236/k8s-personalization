# ADR 0002: Strimzi exposes an external listener from Phase 1

Date: 2026-08-10
Status: accepted

## Context

[ADR 0001](0001-minicluster-first-dev-loop.md) puts the pipeline in a
`MiniCluster` on the developer's machine while Kafka runs inside `kind`. The
obvious bridge is `kubectl port-forward` to the Strimzi bootstrap service.

That does not work, and the failure is confusing rather than obvious.

Kafka's client protocol is two-step. A client connects to the bootstrap address
and asks for metadata. The brokers answer with the addresses clients should use
for actual produce and fetch traffic. Strimzi's default `internal` listener
advertises in-cluster DNS names such as
`my-cluster-kafka-0.my-cluster-kafka-brokers.kafka.svc.cluster.local`.

So the port-forward succeeds, the metadata request succeeds, and then every
produce and fetch fails to resolve. The symptom looks like a hung producer, not
a networking misconfiguration.

## Decision

The `Kafka` custom resource declares a `nodeport` external listener from the
moment Kafka is first deployed in Phase 1. Per-broker `advertisedHost` and
`advertisedPort` overrides point at an address the host machine can reach.
`kind`'s `extraPortMappings` publish those ports out of the node containers.

## Consequences

Phase 1 is not done when a pod is `Ready`. It is done when a producer running
outside the cluster round-trips a message.

The external listener stays in place permanently. It is also what lets the
Kafka consumer-lag Grafana panels and any local debugging tool work later.

Every Phase 1 deliverable is an ArgoCD `Application` per
[ADR 0004](0004-gitops-from-phase-0.md), so this listener config is in Git from
the start rather than being a local hand-edit that gets reverted by a sync.

## Alternatives rejected

- **`port-forward` to bootstrap only.** Does not work, for the reason above.
- **Run the generator and pipeline in-cluster just to avoid this.** That is
  option 2 of ADR 0001, already rejected there.
