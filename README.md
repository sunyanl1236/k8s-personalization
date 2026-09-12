# Flink on Kubernetes Personalization Lab

One real-time e-commerce personalization pipeline (Flink DataStream API) on a
self-managed, highly available `kind` cluster.

- [CLAUDE.md](CLAUDE.md), working agreement
- [CONTEXT.md](CONTEXT.md), glossary
- [docs/knowledge/](docs/knowledge/), how each phase works
- [docs/adr/](docs/adr/), decisions and what was rejected

## Starting a work session

1. Start the Docker daemon and wake the cluster:
   ```bash
   ./scripts/session-start.sh
   ```
2. Only if no cluster exists, step 1 also does this:
   ```bash
   kind create cluster --config clusters/kind/kind-cluster.yaml --wait 180s
   ```
3. At the end of the day:
   ```bash
   ./scripts/session-start.sh --stop
   ```

## Environment constraints

Prefer scoped, reversible changes over permanent machine-wide state.

| Prefer | Over |
|---|---|
| `sudo systemctl start docker` | `systemctl enable docker` |
| `sg docker -c "..."` | `usermod -aG docker $USER` |
| per-invocation `sudo` | permanent privilege grants |

Two permanent host settings are kept, see
[phase 0 knowledge](docs/knowledge/phase-0-cluster-floor.md):

| Setting | Value |
|---|---|
| `/etc/sysctl.d/99-kind.conf` | `fs.inotify.max_user_instances = 1024` |
| `C:\Users\yilul\.wslconfig` | WSL 24GB, applied on `wsl --shutdown` |

## Scripts

| Script | When to run it |
|---|---|
| `session-start.sh` | Start of every session. `--stop` at the end. |
| `bootstrap.sh` | Once, after a full `kind delete` + `kind create`. `all` runs every stage in dependency order; `phase0`, `minio-secret`, `flink-secret` and `karpenter` run them one at a time. |
| `build-image.sh` | After any change under `apps/`. Prints the tag to paste into `spec.image`. |
| `recommendation-snapshot.sh` | During a Drill. `snapshot` before, `snapshot` + `compare` after. |
| `minio-env.sh` | In each terminal that runs `:pipeline:run`. **Source it, do not execute it.** |

## Accessing installed services

Credentials are never written into this file, only how to fetch them.

### ArgoCD

1. Open http://localhost:30010
2. Username `admin`, password:
   ```bash
   kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo
   ```
3. Set a real password, then delete the one-time secret:
   ```bash
   kubectl -n argocd delete secret argocd-initial-admin-secret
   ```

### Flink UI

The dashboard is the JobManager's own web server. It ships no authentication,
so there is no credential to fetch.

Reach it with `kubectl port-forward`.

**1. Find the Active Side.** Only one side has pods; the Standby Side is
suspended and has none.

```bash
kubectl get flinkdeployment -A
```

The side reporting `RUNNING` is the Active Side.

**2. Forward that side's REST Service.** Leave it running in its own terminal.

```bash
# blue or green
kubectl port-forward -n personalization-blue svc/personalization-blue-rest 8081:8081
```

The Service is named `<metadata.name>-rest`. Confirm with
`kubectl get svc -n personalization-<side>` rather than guessing.

**3. Open <http://localhost:8081>**, then **Running Jobs**, then the job. Its
name is set in the job code, not in the FlinkDeployment.

To watch a promotion, forward both sides at once on different local ports, for
example `8081:8081` and `8082:8081`.

### MinIO S3 API

| Item | Value |
|---|---|
| Service | `minio-s3-api`, NodePort 30014 |
| Endpoint for a host client | `s3.endpoint: http://localhost:30014` in `apps/pipeline/conf/config.yaml` |
| Health check | `curl http://localhost:30014/minio/health/live` returns 200 |

### Logging in to MinIO to check checkpoints and savepoints

1. Read the root credentials. `MINIO_ROOT_USER` is the username,
   `MINIO_ROOT_PASSWORD` the password:
   ```bash
   kubectl get secret storage-configuration -n minio-tenant -o jsonpath='{.data.config\.env}' | base64 -d
   ```
2. Forward the Console UI Service. Leave it running:
   ```bash
   kubectl port-forward svc/personalization-console -n minio-tenant 9090:9090
   ```
3. Open `http://localhost:9090` and log in with those credentials.
4. Open **Object Browser**, then the `checkpoints` bucket.
5. Browse to the path you need:

   | Path | Holds | Set by |
   |---|---|---|
   | `phase-3/<job-id>/chk-N` | Phase 3 MiniCluster checkpoints | `apps/pipeline/conf/config.yaml` |
   | `phase-5/<job-id>/chk-N` | Phase 5 in-cluster checkpoints | `execution.checkpointing.dir` |
   | `phase-5-savepoints/` | Savepoints, written on `upgradeMode: savepoint` | `execution.checkpointing.savepoint-dir` |
   | `phase-5-ha/` | JobManager HA metadata | `high-availability.storageDir` |

6. Stop the port-forward with Ctrl+C when finished.

**A `chk-N` directory is only restorable if it contains `_metadata`.** Pick one
from the job log's `Completed checkpoint N` line, not the highest number in the
bucket.

## Running the generator

1. Run it:
   ```bash
   apps/gradlew -p apps :generator:run
   ```
2. Override any default, full list in `GeneratorConfig.parse`:
   ```bash
   apps/gradlew -p apps :generator:run --args="--click-rate=10 --click-max-skew-seconds=5"
   ```
3. Verify with a real consumer, in separate terminals:
   ```bash
   kcat -C -b localhost:30016 -t clickstream
   kcat -C -b localhost:30016 -t product-change
   kcat -C -b localhost:30016 -t promo-rule
   ```

Defaults, and what each topic should show:

| Topic | Default rate | A line every |
|---|---|---|
| `clickstream` | 5/sec, up to 2s skew | ~200ms |
| `product-change` | 1/sec, up to 2s skew | ~1s |
| `promo-rule` | 1 per 30s | ~30s |

Bootstrap servers default to `localhost:30016`.

Fields to check:

- `Click`: `shopperId`, `productId`, `eventTime`, `actionType`
- `product-change`: a `"type"` field, `"PRICE"` or `"STOCK"`

## Running the pipeline

1. Export the MinIO credentials into the **same terminal**:
   ```bash
   source scripts/minio-env.sh
   ```
2. Run the job:
   ```bash
   apps/gradlew -p apps :pipeline:run
   ```
3. Verify the output:
   ```bash
   kcat -C -b localhost:30016 -t recommendation -X isolation.level=read_committed
   ```
   Expect about 10s of silence, then a burst, repeating. See
   [phase 3 knowledge](docs/knowledge/phase-3-core-pipeline.md).
4. Confirm checkpoints land, in the job log:
   ```
   Completed checkpoint 5 for job 66790bc4... (49554593 bytes, checkpointDuration=383 ms)
   ```

Where settings live:

| Surface | Where | Holds |
|---|---|---|
| Flink settings | `apps/pipeline/conf/config.yaml` | state backend, checkpoint interval and mode, checkpoint dir, S3 endpoint |
| Job settings | `--key=value` flags, see `PipelineConfig.parse` | bootstrap servers, topics, consumer group, watermark bound, session gap, cooldown, transactional id prefix |

Every flag needs its value. `--bounded` alone is rejected, pass `--bounded=true`.

### Drill mode

| Flag | Effect |
|---|---|
| `--bounded=true` | Pins end offsets at job start and ends there, so two runs compare line for line. |
| `--restore-from=s3://checkpoints/phase-3/<job-id>/chk-N` | Resumes from a checkpoint. Restored offsets beat `--start-from-earliest`. |
| `--output-topic=<throwaway>` | Use when repeating a drill. Auto-creation is on. |

Do not delete and recreate `recommendation`. ArgoCD manages it.

Reference run, 2026-08-25, backlog of about 3 million Clicks:

| Metric | Value |
|---|---|
| Duration | 1m 47s |
| Browsing Sessions closed | 125,893 |
| Recommendations published | 111,988 |
| Suppressed by cooldown | 13,905 |
| Late Clicks | 0 |
| Checkpoints completed | 5 |
| Counted by a `read_committed` consumer | 111,988 |

## Inspecting the cluster with k9s

1. Install:
   ```bash
   sudo dnf install k9s
   ```
2. Confirm the context, it should print `kind-personalization-lab`:
   ```bash
   kubectl config current-context
   ```
3. Run `k9s`

| Key | Does |
|---|---|
| `:pods` then `0` | All pods in all namespaces |
| `0` | All namespaces, in any view |
| `1` | Jump to `default` |
| `:ns` | Namespace picker |
| `:<resource>` | Jump to a resource type, e.g. `:application`, `:svc` |
| `d` `e` `l` | describe, edit, logs |
| `ctrl-d` | delete |
| `ctrl-r` | force refresh |

Namespace scope is sticky across resource views. An object in another namespace
shows as `[0]` results, not an error.
