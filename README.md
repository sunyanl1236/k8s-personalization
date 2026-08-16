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

- No external NodePort configured, reachable only from inside the cluster.
  Flink's checkpoint traffic never needs host access; add a NodePort listener
  the same way Kafka's external one works (see
  [phase-1-data-platform.md](docs/knowledge/phase-1-data-platform.md)) if
  that ever changes.
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
