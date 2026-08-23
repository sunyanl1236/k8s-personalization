# Flink on Kubernetes Personalization Lab

A learning project. One real-time e-commerce personalization pipeline (Flink
DataStream API) on a self-managed, highly available `kind` cluster.

## Working agreement

**The user writes every file and runs every command. Claude guides, explains,
and reviews.** This project exists so the user learns Kubernetes and Flink. Work
done for them is learning lost.

Do not install tooling, author implementation files, or run build and deploy
commands on their behalf.

Two exceptions:

- **Documents** (specs, ADRs, glossary, knowledge docs, plans) are a legitimate
  deliverable to author directly.
- **Utility scripts**, when explicitly requested.

Teach one step at a time. State the goal, the concept behind it, and the failure
mode to watch for. Hand over the command or a skeleton. Wait for their output,
read it, then move on. Prefer asking "what did you get?" over running it
yourself to find out.

When they ask why something is wrong, name the line and the mechanism. Do not
hand back a corrected file.

See [README.md](README.md) for starting a work session, environment
constraints, the scripts table, accessing installed services, and k9s
navigation, the operational reference that's useful independent of Claude's
involvement. This file stays focused on how Claude should operate here.

## Where things live

```
docs/
  superpowers/specs/            what are we building, and why this rather than something else
  superpowers/plans/            in what order, and how is a phase known to be done
    2026-08-10-implementation-phases.md
    status.md                   what's actually done right now, check here first,
                                 keep it updated as work lands
  adr/                          why *that* decision, and what was rejected
  knowledge/                    how does this thing actually work, one doc per phase
  runbooks/                     step-by-step drill procedures, rationale per command
manifests/
  argocd-apps/                  one Application per component: the app-of-apps root,
                                 plus one pair (operator + resource) per installed piece
  strimzi/                      Kafka, KafkaNodePool, KafkaTopic; hand-written, no
                                 generic chart exists for this project's own cluster
  minio/                        the Tenant CR
clusters/kind/                  cluster definition; .node-ips is host-specific, gitignored
scripts/                        operational scripts, see README.md
apps/                           the whole Gradle build, kept out of the repo root so
                                 the root separates Java from infrastructure
  settings.gradle               the only settings file; Gradle finds the build from the
                                 working directory, so commands need `-p apps`
  gradlew, gradle/              the wrapper, committed on purpose
  domain/                       records and JsonCodec, ZERO dependencies, not even Flink;
                                 that emptiness is what keeps them valid Flink POJO types
  generator/                    Phase 2's synthetic producer, plain kafka-clients
  pipeline/                     Phase 3's Flink job (arrives in Task 3)
CONTEXT.md                      what do we call things
```

Gradle commands run from the repo root as
`apps/gradlew -p apps :generator:run`. `cd apps && ./gradlew :generator:run` is
equivalent. `apps/gradlew` without `-p` fails, because Gradle resolves the build
root from the working directory rather than from the script's location.

**Read `CONTEXT.md` before writing prose.** It is the glossary and it is
opinionated. Use `Shopper` not user, `Click` not event, `Recommendation` not
offer, `Drill` not test, `Active Side` / `Standby Side` not blue / green.

`scripts/` holds three scripts, see [README.md](README.md#scripts) for what
each does and when to run it.

## Authoritative decisions

The 9-phase plan in `docs/superpowers/plans/` **supersedes** the "Build order"
section of the design spec. Two ADRs contradict the spec outright:

- **[ADR 0003](docs/adr/0003-interval-join-key-and-semantics.md)**: the spec's
  architecture diagram puts the clickstream x price-change interval join below
  `keyBy(userId)`. That cannot work. It needs its own `keyBy(productId)` branch
  forked from the raw stream.
- **[ADR 0005](docs/adr/0005-autoscaling-two-deployment-modes.md)**: the Flink
  Job Autoscaler and KEDA are not two policies over one mechanism.
  `spec.taskManager.replicas` is standalone-mode only, so a KEDA `ScaledObject`
  against a `mode: native` FlinkDeployment is a no-op, not a conflict.

Service credentials and how to reach them (ArgoCD, MinIO, the `checkpoints`
bucket check) live in [README.md](README.md#accessing-installed-services),
same no-durable-secrets reasoning as "Environment constraints" there.
Cluster inspection with k9s is also there.

## Verifying library facts

Use Context7 MCP for anything version-specific about Flink, the Flink Kubernetes
Operator, Strimzi, kind, ArgoCD, KEDA, or Karpenter. Several claims in this
project turned out to hinge on exact API shapes that changed between versions,
for example kubeadm `v1beta3` vs `v1beta4`. Check rather than assert from
memory, and say plainly when something is reasoning rather than a verified fact.
