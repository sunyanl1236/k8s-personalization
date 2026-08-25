# Flink on Kubernetes Personalization Lab

A learning project. One real-time e-commerce personalization pipeline (Flink
DataStream API) on a self-managed, highly available `kind` cluster.

See [CLAUDE.md](CLAUDE.md) for the working agreement this project uses with
Claude, and [CONTEXT.md](CONTEXT.md) for the project's own glossary.

## Starting a work session

Docker's boot-time autostart is **deliberately disabled** on this machine
(`sudo systemctl disable docker.service`). The daemon is only needed while
1. working on this project, so it is started on demand.
```bash
./scripts/session-start.sh
```
2. opening a new terminal, run 
```bash
kind create cluster --config clusters/kind/kind-cluster.yaml --wait 180s
```
3. At the end of the day:
```bash
./scripts/session-start.sh --stop
```

## Environment constraints

Prefer **scoped, reversible** changes over permanent machine-wide state. This is
a temporary lab project and its tooling should not outlive it.

| Prefer | Over |
|---|---|
| `sudo systemctl start docker` | `systemctl enable docker` |
| `sg docker -c "..."` | `usermod -aG docker $USER` |
| per-invocation `sudo` | permanent privilege grants |

If a scoped form is materially more friction than it is worth, say so and
explain the tradeoff. Do not silently choose the permanent form.

Two host settings are in place and intentionally kept:

- `/etc/sysctl.d/99-kind.conf` raises `fs.inotify.max_user_instances` to 1024.
  The default 128 is exhausted by a 6-node kind cluster, and the failure looks
  like `too many open files` or a node going `NotReady`, never like an inotify
  problem.
- `C:\Users\yilul\.wslconfig` gives WSL 24GB. The full stack needs roughly 13Gi.
  Applies on `wsl --shutdown`.

## Scripts

| Script | What it does | When to run it |
|---|---|---|
| `session-start.sh` | Starts the Docker daemon on demand, wakes the `kind` cluster, pins each node container's IP (recorded in `clusters/kind/.node-ips`) and restores it if Docker hands out a different one on a later run. `--stop` stops the daemon for the day; node containers stop, not delete, cluster state survives. | Start of every work session; `--stop` at the end. See "Starting a work session" above. |
| `bootstrap-phase0.sh` | The two GitOps-exception installs from Phase 0, cert-manager and ArgoCD, both via plain `helm upgrade --install`, before GitOps exists to take over anything else. Safe to re-run: does nothing if the release already matches. | Once, standing up the cluster floor. Re-run only when rebuilding the cluster from scratch. |
| `bootstrap-minio-secret.sh` | Creates the `minio-tenant` namespace and the `storage-configuration` Secret the MinIO `Tenant` needs: root user, a randomly generated password, `EC:2` storage class. Deliberately not committed to git. Idempotent, skips if the Secret already exists rather than rotating a running Tenant's password. | Once, before the `minio-tenant` ArgoCD Application can go Healthy. |
| `minio-env.sh` | Exports `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` from the `storage-configuration` Secret, for the Phase 3 pipeline's checkpoint writes. Reads the Secret each time and writes nothing to disk. **Source it, do not execute it**: a child process cannot set its parent's environment, and the script refuses to run if executed. | In each terminal that runs `:pipeline:run`, after `session-start.sh`. |

Both bootstrap scripts only need re-running after the cluster itself is fully recreated (`kind delete` + `kind create`), not after a routine `session-start.sh` stop/start, since `etcd` state persists across the latter but not the former, and neither script's target is GitOps-tracked, so nothing else regenerates it automatically.

## Accessing installed services

Credentials are never written into this file, only how to fetch them. See
"Environment constraints" above for why, the same scoped/no-durable-secrets
reasoning applies here.

**ArgoCD**

- URL: http://localhost:30010
- Username: `admin`
- Password: `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo`

ArgoCD's initial admin secret is meant to be one-time: set a real password after
first login, then delete `argocd-initial-admin-secret`. Leaving it around
indefinitely is against ArgoCD's own recommended practice, not just this
project's.

**MinIO**

- S3 API on **NodePort 30014**, added in Phase 3 Task 2 as the `minio-s3-api`
  Service. The Phase 3 pipeline runs on `MiniCluster` on your host, so it is an
  external client and needs host access; `s3.endpoint: http://localhost:30014`
  in `apps/pipeline/conf/config.yaml`. Verified with
  `curl http://localhost:30014/minio/health/live` returning 200, not from the
  Service looking healthy. At Phase 5 the job moves into the cluster and uses the
  in-cluster Service instead.
- Root credentials: created out of band by `scripts/bootstrap-minio-secret.sh`,
  never committed. Fetch with:
  `kubectl get secret storage-configuration -n minio-tenant -o jsonpath='{.data.config\.env}' | base64 -d`

**Checking the `checkpoints` bucket exists**

Don't trust the `Tenant`'s own `Ready` status alone, same reasoning as the
Kafka external listener drill in Phase 1: confirm with a real client.

```bash
kubectl port-forward svc/personalization-console -n minio-tenant 9090:9090
```

Open `http://localhost:9090`, log in with the root credentials above, and
confirm `checkpoints` is listed. No CLI tool to install, `personalization-console`
is the Console UI Service, already running, same pattern as ArgoCD's own UI
above. `svc/minio`, port 80, is the separate S3 API Service, for an actual
client like `mc` or Flink's S3 plugin, not for browser access.

## Running the generator

Phase 2's synthetic generator, `apps/generator/`, a plain Java program (not a
Flink job, see
[the generator design doc](docs/superpowers/specs/2026-08-16-generator-event-production-design.md)),
producing `Click`, `PriceChange`/`StockChange`, and `PromoRule` events onto
`clickstream`, `product-change`, and `promo-rule`.

```bash
apps/gradlew -p apps :generator:run
```

**Two things changed here in Phase 3.** The Gradle build moved under `apps/`,
so the repo root separates the Java build from `manifests/`, `clusters/`, and
`scripts/`. And the build is now multi-project, so the task is `:generator:run`
rather than a bare `run`.

`-p apps` points Gradle at the build root. Gradle finds its build from the
*working directory*, not from where the `gradlew` script lives, so
`apps/gradlew run` on its own fails with "does not contain a Gradle build".
`cd apps && ./gradlew :generator:run` works equally well; the flag form is used
here so Gradle, `kubectl`, and `kcat` commands can all be run from the repo
root without changing directory between them.

Runs with sensible defaults and no args: `localhost:30016` for bootstrap
servers, 5 clicks/sec with up to 2s of injected skew, 1 product-change/sec
with up to 2s of skew, one promo rule every 30s. Override any of these with
`apps/gradlew -p apps :generator:run --args="--click-rate=10 --click-max-skew-seconds=5"`,
see `GeneratorConfig.parse` for the full list of `--key=value` options.

**Don't trust it's working from the log line alone.** Same standard as the
Kafka external listener drill in Phase 1: verify with a real consumer, in
separate terminals:

```bash
kcat -C -b localhost:30016 -t clickstream
kcat -C -b localhost:30016 -t product-change
kcat -C -b localhost:30016 -t promo-rule
```

What to check: `clickstream` should show a new JSON line roughly every
200ms at the default rate, `product-change` roughly once a second,
`promo-rule` roughly once every 30 seconds. Each `Click` line has
`shopperId`, `productId`, `eventTime`, `actionType`. Each `product-change`
line has a `"type"` field, `"PRICE"` or `"STOCK"`, matching whichever
value fields are actually present, the sealed interface distinction that
doesn't survive serialization on its own.

## Running the pipeline

Phase 3's Flink job, `apps/pipeline/`, running on `MiniCluster` (see
[ADR 0001](docs/adr/0001-minicluster-first-dev-loop.md)) against the Kafka and
MinIO already in the cluster. It reads `Click` events from `clickstream`, groups
them into Browsing Sessions, and publishes a `Recommendation` per closed Session
to `recommendation`, checkpointing RocksDB state to MinIO.

```bash
source scripts/minio-env.sh
apps/gradlew -p apps :pipeline:run
```

**The `source` is not optional.** `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` are
required, and the job fails fast with `MINIO_ACCESS_KEY is not set` rather than
producing a confusing S3 error. Source it in the *same terminal* that runs
Gradle: the forked JVM inherits that shell's environment and no other.

### Where the configuration lives

Two surfaces, split the way production splits them.

**Flink settings are data**, in `apps/pipeline/conf/config.yaml`, loaded with
`GlobalConfiguration.loadConfiguration(...)`. State backend, checkpoint interval
and mode, checkpoint directory, S3 endpoint. Nothing Flink-related is configured
in Java. At Phase 5 that file's contents move verbatim into
`spec.flinkConfiguration` on a `FlinkDeployment`, and the operator renders it
back into a `config.yaml` inside the pod. The jar carries no environment
knowledge either way.

**Job settings are flags**, `--key=value`, same style as `GeneratorConfig`. See
`PipelineConfig.parse` for the full list. Bootstrap servers, topics, consumer
group, watermark bound, session gap, cooldown, transactional id prefix.

Every flag needs its value. `--bounded` alone is rejected with
`Expected --key=value`; pass `--bounded=true`.

### Verifying it, and the ten seconds of silence

Same standard as everywhere else here: not "the log said it started".

```bash
kcat -C -b localhost:30016 -t recommendation -X isolation.level=read_committed
```

**Expect silence for about ten seconds, then a burst, repeating.** That is not a
stall. The sink is exactly-once, so records become visible only when the
checkpoint covering them commits the transaction, and the checkpoint interval is
10s. The sawtooth *is* the mechanism working.

`isolation.level=read_committed` matters. Without it a consumer sees records
from transactions that were never committed, including the orphan a restart is
meant to fence away.

Two other things worth knowing when reading the output:

- Offsets advance by **2** per record. Each committed transaction writes a
  control record into every partition it touched. Those occupy an offset and are
  never delivered to consumers, so a plain record count and the offset counter
  disagree by design.
- Timestamps are ordered **per Shopper**, not globally. Records are keyed by
  `shopperId`, and Kafka orders within a partition only.

To confirm checkpoints are landing, watch the job's own log, which reports the
checkpoint id, size and duration:

```
Completed checkpoint 5 for job 66790bc4... (49554593 bytes, checkpointDuration=383 ms)
```

Or browse `checkpoints/phase-3/<job-id>/chk-N` in the MinIO Console, per
[Accessing installed services](#accessing-installed-services). **A `chk-N`
directory is only restorable if it contains `_metadata`**, which the coordinator
writes last, after every subtask acknowledges. Pick a checkpoint from the log's
`Completed checkpoint N` line, never from the highest number in the bucket.

### Drill mode

`--bounded=true` pins the topic's end offsets at job start and ends the job
there, so two runs read an identical range and their outputs can be compared
line for line. `--restore-from=s3://checkpoints/phase-3/<job-id>/chk-N` resumes
from a checkpoint; offsets in the restored state take priority over
`--start-from-earliest`, and that priority is the recovery mechanism.

Use `--output-topic=` with a throwaway name when repeating a drill. Auto-creation
is on. Do not delete and recreate `recommendation`; ArgoCD manages it.

Observed on 2026-08-25, one bounded run over a backlog of about 3 million
Clicks: **1m 47s**, 125,893 Browsing Sessions closed, 111,988 Recommendations
published, 13,905 suppressed by the cooldown, 0 Late Clicks, 5 checkpoints
completed. A `read_committed` consumer counted **exactly 111,988** records on the
output topic, matching the job's own emitted count.

## Inspecting the cluster with k9s

Install: `sudo dnf install k9s` (in Fedora's official repos, no extra repo
needed).

No connection config required. k9s reads whatever kubectl context is
currently active, same as `kubectl` itself. `kind create cluster` already
sets this automatically. Confirm with `kubectl config current-context`, it
should print `kind-personalization-lab`; if so, just running `k9s` opens
straight into this cluster.

**Namespace scope is sticky across resource views.** If you were looking at
pods scoped to `default` and switch to viewing a different resource type
(`:application`, `:svc`, etc.), it keeps that same `default` scope rather
than resetting, so a real object in another namespace will show as `[0]`
results, not an error, just the wrong scope. This caused real confusion
twice in this project before the pattern was clear.

- `0` — all namespaces at once, the fastest way to just see everything
- `1` — jump to `default`
- `:ns` — open a namespace picker and set one explicitly, sticks across
  future view switches until changed again
- `:` followed by a resource name jumps to that resource type, e.g.
  `:application` for ArgoCD's `Application` objects (shows its
  `SYNC STATUS`/`HEALTH STATUS` columns directly), `:pods`, `:svc`
- `d` describe, `e` edit, `l` logs, `ctrl-d` delete, `ctrl-r` force a
  refresh if a view looks stale
