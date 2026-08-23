# Phase 1 knowledge: Data platform (Strimzi + Kafka)

Summarized from the Phase 1 work session, not verbatim. Companion to
[phase-0-cluster-floor.md](phase-0-cluster-floor.md); ArgoCD's own general
concepts and the `root.yaml` debugging saga live there, this file covers what
came up while actually standing up Strimzi and the `Kafka` cluster.

## Why MinIO over HDFS

- Both are technically viable. Flink's own `FileSystemCheckpointStorage`
  explicitly supports HDFS, NFS, S3, and GCS through one generic interface.
- Project-specific reason: originally scoped for real AWS S3 before pivoting
  off EKS to `kind`. MinIO is a local, API-compatible substitute for that, not
  a different technology.
- Because the pipeline only talks to Kafka and an S3-compatible endpoint, none
  of the Flink code depends on local vs. cloud. That portability breaks if
  checkpoint code used `hdfs://` instead.
- Broader reason, beyond this project: the whole ecosystem moved toward
  S3-style object storage as cloud computing decoupled storage from compute.
  It's the more transferable skill, not the cast-off option.

## Why Strimzi, and its CRDs

- Same operator pattern as cert-manager, the Flink Operator, and ArgoCD
  itself: declare a `Kafka`/`KafkaNodePool` resource, a Cluster Operator
  reconciles the real brokers.
- ZooKeeper is gone as of Strimzi 0.46+, not optional, confirmed directly:
  "Kafka 4.0 exclusively uses KRaft mode." KRaft folds cluster coordination
  into Kafka itself via Raft, the same underlying idea as etcd's own quorum
  from Phase 0.
- Strimzi's own docs recommend its Helm chart "for environments already using
  Helm," which this project already is by constraint, so that settled the
  install-method question without much debate.

Nine CRDs exist total, confirmed via `kubectl api-resources
--api-group=kafka.strimzi.io`.

Used in this project:
1. `Kafka` — cluster-wide config
2. `KafkaNodePool` — node topology
3. `KafkaTopic` — individual topics
4. `KafkaUser` — credentials and ACLs

Exist, not part of this design:
- `KafkaConnect` / `KafkaConnector` — streaming data in/out of Kafka via
  pluggable connectors; this project's generator writes directly via a
  producer client instead
- `KafkaMirrorMaker2` — cross-cluster replication; one cluster here, nothing
  to replicate to
- `KafkaBridge` — an HTTP-to-Kafka gateway; unnecessary since everything here
  uses native Kafka clients
- `KafkaRebalance` — Cruise Control-driven partition rebalancing; not part of
  the current design

## The `strimzi` Application vs. the `Kafka` resource itself

Two separate `Application` objects, deliberately:

- **`strimzi.yaml`** installs only the operator. A Helm-chart source,
  `repoURL` there is a chart repository endpoint, not a git URL, paired with
  `chart:`/`targetRevision` instead of `path:`. Creates no Kafka cluster by
  itself.
- **`strimzi-kafka-cluster.yaml`** is a second, directory-sourced Application
  pointing at `manifests/strimzi/`, where the actual `Kafka`/`KafkaNodePool`
  YAML lives.
- Split on purpose: the operator's lifecycle (chart upgrades) and the
  cluster's own config changes are genuinely independent events. One
  Application would mean every operator bump also re-evaluates cluster
  config, and vice versa.

Within any Application, two namespace-shaped fields answer different
questions:

- `metadata.namespace` (always `argocd` here) — where the *instruction*
  lives. ArgoCD's controller only watches its own namespace by default, same
  behavior Strimzi's Cluster Operator has for `Kafka` resources.
- `spec.destination.server` — *which cluster* to deploy to.
- `spec.destination.namespace` — *where the result lands* on that cluster.
- `CreateNamespace=true` — needed because a Deployment can't be created in a
  namespace that doesn't exist, and nothing else in this project provisions
  `kafka` for you.

`helm repo add` never needed persisting anywhere: it's local CLI
configuration. ArgoCD's repo-server fetches straight from the chart URL
embedded in the Application manifest, independent of anyone's local Helm
config. That URL, committed in the file, is the actual durable record.

## Two separate things the reconcile loop watches, not one

A pinned chart version (`strimzi.yaml`'s `targetRevision: 1.1.0`, never
changing) does not mean there's nothing left to watch. Two independent checks
run continuously, answering two different questions:

- **Did the instruction itself change?** Checked by polling Git, every 3
  minutes by default (from the app-of-apps section above). This is "did
  someone commit a different `targetRevision`, a new parameter, anything in
  the Application's own source block."
- **Does the live cluster match whatever the instruction currently says?**
  Checked continuously, independent of the poll. This is "even with the exact
  same pinned chart and version, does the real Deployment/RBAC/etc. still
  look like what that chart renders to."

These can diverge from each other. A chart version can stay permanently
fixed while the *live* cluster still drifts, someone runs `kubectl edit
deployment strimzi-cluster-operator` by hand, a different automation touches
the same objects, an admission webhook mutates something. None of that
touches the pinned source at all, but `SYNC STATUS` still flips to
`OutOfSync`, because the second check caught a live-state mismatch the first
check was never going to see.

This is exactly why `selfHeal: false` matters, and what the Phase 5 drift
Drill actually demonstrates: patch a live object by hand, watch it hold for a
moment, then get reverted on the next sync, entirely without the pinned
chart version ever moving. If reconciliation only fired on source changes,
that Drill would have nothing to show.

## `apiVersion` vs. Kafka version, and how to check both

Two independent axes:

- `apiVersion: kafka.strimzi.io/v1` — describes the CRD *schema*. Changes only
  when Strimzi changes its own API.
- `spec.kafka.version` (`"4.3.0"` here) — describes which real Kafka release
  actually runs. Several Kafka versions can share one unchanged schema.

Both are best answered by the live cluster, not a doc that might describe an
older release:

1. `kubectl api-resources --api-group=kafka.strimzi.io` for the schema
   version (confirmed `v1`, not the `v1beta2` several doc examples still
   show).
2. The Cluster Operator's own `STRIMZI_KAFKA_IMAGES` env var for exactly which
   Kafka versions this specific operator build supports (`4.2.0`, `4.2.1`,
   `4.3.0` for chart `1.1.0`).

`metadataVersion` was deliberately left unset. Confirmed from operator source
that it's derived automatically from `spec.kafka.version` when omitted, no
need to hardcode a metadata string.

## `KafkaTopic` and the Topic Operator

- The Topic Operator is a sidecar inside the Entity Operator pod, turned on
  by `entityOperator.topicOperator: {}` in `kafka-cluster.yaml`. It watches
  `KafkaTopic` resources and reconciles each into a real Kafka topic.
- The link to a specific `Kafka` cluster is a label, not a namespace or an
  owner reference: `strimzi.io/cluster: personalization`.
- Failure mode: a missing or wrong label produces no error on `kubectl
  apply`. The resource just never reconciles, `status.conditions` never
  populates. Looks identical to "still syncing," so check the label first
  if a `KafkaTopic` sits unready.
- `apiVersion: kafka.strimzi.io/v1` for `KafkaTopic` was inferred first,
  from `Kafka`/`KafkaNodePool` already being `v1` on this install, then
  confirmed live: `kubectl api-resources --api-group=kafka.strimzi.io`
  lists all nine CRDs, `kafkatopics` included, under `v1`, matching the
  list in "Why Strimzi, and its CRDs" above exactly.

## The four topics: `clickstream`, `product-change`, `promo-rule`, `recommendation`

- Three inputs, one output, each mapping to one thing named in
  `CONTEXT.md`. The reasoning for why each needs its own topic rather than
  a merged one is a Flink-job question, not a Kafka one, covered in
  [flink-job-walkthrough.md](flink-job-walkthrough.md).
- `partitions: 3` on all four, matching the 3-broker pool. Caps how far a
  keyed Flink operator reading a topic can parallelize later.
- `replicas: 3` on all four, matching `default.replication.factor: 3`
  already set cluster-wide in `kafka-cluster.yaml`.
- Named `product-change`, not the originally planned `price-change`. The
  original name only suggested price, not the stock-level moves it also
  needs to carry.

## Listeners, and the two-step connection

- A listener is a broker's network endpoint. Several can coexist: an
  `internal` one for pods inside the cluster, and the `nodeport` one this
  project needs for a producer on the host machine, since pod-internal
  addresses aren't reachable from outside the cluster at all.

Kafka clients never connect in one step:

1. **Bootstrap** — one well-known entry point. The client asks "what's your
   topology?"
2. **Redirect** — the broker answers with an exact host:port for whichever
   specific broker owns the data the client actually wants.
3. **Direct connection** — the client connects again, straight to that
   broker.

- `configuration.bootstrap.nodePort` is step 1's address.
- `configuration.brokers[].nodePort` are step 3's, one per broker, matching
  the four ports reserved back in Phase 0 (ADR 0002): one bootstrap plus one
  per broker, not one shared port.
- `advertisedHost`/`advertisedPort` control exactly what gets handed back in
  step 2's redirect. Unset, a broker advertises its own internal cluster
  address, correct for an internal client, useless for one outside the
  cluster. Set to `localhost`/the matching NodePort, it advertises the
  address actually reachable through the same `extraPortMappings` tunnel from
  Phase 0.

## NodePort, corrected: not one pod per node, not one Service per node

- A pod runs on exactly one node, never all six. What exists on every node is
  a **forwarding rule**, not the pod itself.

For a 3-broker cluster, concretely:

1. Strimzi creates **four Service objects total**, one bootstrap plus one per
   broker, cluster-wide records, not six, and not "one per node."
2. Each broker's Service has a selector matching only that one broker's pod.
3. Kubernetes resolves that once, centrally, into a real pod IP.
4. `kube-proxy`, running as its own copy on all six nodes, reads that same
   central record and writes the identical local rule onto every node: "port
   30017 here forwards to that same pod IP."
5. Any of the six nodes redirects you to the same place, regardless of which
   node the pod actually happens to be running on.

Why not `LoadBalancer`:
- It has the identical per-broker Service structure, confirmed from Strimzi's
  own docs: "a separate loadbalancer service is created for each broker pod."
  Just a different Service type underneath, one `kind` has no controller to
  fulfill (no MetalLB or equivalent installed). On a real cloud cluster,
  LoadBalancer would be the natural choice.

Why each broker gets its own dedicated Service, not a shared one:
- A shared Service, matching all three broker pods, would load-balance
  randomly across them, fine for a stateless UI, wrong for Kafka, where a
  client was told "broker 3 owns this partition" and must land there
  specifically. One selector matching exactly one pod removes the need for
  any randomness, there's only one destination to choose.

## The full path of an external connection, hop by hop

Two independent layers, easy to collapse into one in your head, worth
keeping separate.

- **Outer layer, plain port redirects, nothing genuinely listening at these
  numbers.** `kind`'s `extraPortMappings` (Phase 0) forwards the host's
  `localhost:30016` into one specific node container. `kube-proxy`'s
  `NodePort` rule, replicated identically on every node, then catches it
  inside that container and hands it to the right Service.
- **Inner layer, a real listening socket.** The broker process itself binds
  to `port: 9094` inside its own container, the number declared on the
  listener in `kafka-cluster.yaml`. This is the actual destination every
  redirect above is aiming at. Confirmed from Strimzi's own docs: "the port
  number specified for a listener is used within the Kafka cluster and may
  differ from the client access port... `nodeport` listeners use the port
  assigned by Kubernetes." A client never types `9094`, it only ever
  reaches it indirectly, through whichever `NodePort` got it there.

The bootstrap `NodePort` (`30016`) and the per-broker `NodePort`s
(`30017`-`30019`) behave differently at the Service layer, and this is the
missing half of the "NodePort, corrected" section above:

- The bootstrap Service's selector matches **all three broker pods**,
  since any broker can answer an initial "what's your topology" request.
  `kube-proxy` picks one arbitrarily, load-balancing across them, which is
  fine here, unlike the per-broker case above where a shared Service would
  be wrong.
- Each per-broker Service's selector matches **exactly one pod**, no
  load-balancing, since a client that was told "broker 3 owns this" has to
  land on broker 3 specifically.

How the broker that answers a bootstrap request knows another broker's
outside address: it doesn't compute or discover it. Each broker is
individually configured, in `kafka-cluster.yaml`'s `brokers[]` list, with
its own `advertisedHost`/`advertisedPort`, e.g. broker 3 is told to claim
`localhost:30017` for itself. Brokers continuously share cluster metadata
with each other as a normal part of running a cluster, including each
other's self-declared advertised address. So whichever broker answers a
bootstrap request is just relaying a value another broker announced about
itself, not generating one. Unset, a broker's default is to advertise its
own internal pod address, confirmed already in `kafka-cluster.yaml`'s own
comment, correct for an internal client, unreachable for one outside the
cluster.

## `KafkaNodePool`, splitting controller/broker, and node IDs

- Confirmed from Strimzi's schema docs: "defines a pool of Kafka nodes with
  specific roles... storage configuration cannot be updated once the pool is
  created."
- Multiple pools can exist for one `Kafka` cluster, linked via a shared
  `strimzi.io/cluster` label, not an ownerRef or namespace.
- This project moved from one combined `dual-role` pool to two separate ones,
  `controllers` and `brokers`, matching Strimzi's own documented pattern for
  this split.

The gotcha the split introduced:
- Node IDs are assigned **cluster-wide**, not restarting at 0 per pool,
  confirmed directly: "node IDs might not run sequentially within a specific
  node pool."
- Left to chance, `controllers` could claim IDs `0-2` and leave `brokers`
  with `3-5`, silently breaking a listener config written assuming brokers
  were `0/1/2`.
- Fixed with `strimzi.io/next-node-ids`, an annotation that pins a specific ID
  range to a pool at creation: `controllers` gets `[0-2]`, `brokers` gets
  `[3-5]`, deterministic rather than discovered after the fact by reading pod
  names.

## Storage: `jbod`, `persistent-claim`, `ephemeral`

Three types exist:

- **`ephemeral`** — no real persistence, backed by `emptyDir`. Docs flag it as
  "not suitable for topics with a replication factor of 1."
- **`persistent-claim`** used directly — one durable volume. Fine only if
  you're certain you'll never want a second disk.
- **`jbod`** ("Just a Bunch Of Disks") — a list wrapper around one or more
  `persistent-claim`/`ephemeral` volumes, each with its own `id`.

Deliberate choice here even for one volume: Strimzi's own docs recommend
`jbod` "for future scalability by allowing the addition of more volumes."
Adding a second disk later is appending to a list, not restructuring the
block.

## Replication factor and min-insync-replicas are different questions

- `replication.factor: 3` answers "how many physical copies of each partition
  exist?" Three, always, one leader plus two followers, spread across three
  brokers.
- `min.insync.replicas: 2` answers a different question: "how many of those
  three must confirm before a write counts as durable?"
- Three copies always get made. `2` is just how many have to agree before the
  producer stops waiting, not a statement that only two copies exist.

The five replication/ISR settings in `spec.kafka.config` all exist because
Kafka's own default replicates nothing extra, one copy per partition, which
would defeat the point of running three brokers:

1. `offsets.topic.replication.factor` and `transaction.state.log.*` protect
   Kafka's own internal bookkeeping topics.
2. `default.replication.factor` and `min.insync.replicas` protect ordinary
   topics the same way.
3. There's no separate replication mechanism for "important" data, Kafka
   only ever copies partitions, whatever they contain. Replicating
   `__consumer_offsets` and protecting offset-commit data are the same action
   described from two angles, not two different things.

## Kafka transactions

Solve a specific problem: a single logical unit of work often spans multiple
partitions or topics. Without transactions, a crash mid-write leaves consumers
seeing a partial, inconsistent result.

1. A transactional producer has a stable `transactional.id`.
2. It calls `initTransactions()` once.
3. Per unit of work: `beginTransaction()`, writes, `commitTransaction()` (or
   `abortTransaction()`).
4. A transaction coordinator on one broker tracks state
   (ongoing/committed/aborted) in `__transaction_state`, the same internal
   topic the replication settings above protect.
5. On commit, the coordinator writes a marker to every partition touched.
6. Consumers on `isolation.level=read_committed` skip anything without a
   commit marker, never seeing a partial transaction.

Directly relevant to this project: Flink's Kafka sink in `EXACTLY_ONCE` mode
uses this same mechanism, wrapping one checkpoint interval's output as one
Kafka transaction, committing only once that checkpoint fully succeeds. That's
the actual link between Flink's checkpointing (Phase 3) and Kafka's
transactions, one real atomic handoff, not two guarantees bolted together.

## MinIO Operator and the `Tenant` CRD

- Same reconciliation shape as Strimzi: the MinIO Operator watches a
  `Tenant` custom resource (`minio.min.io/v2`) and reconciles it into real
  server pods, PVCs, and Services. `Tenant` is to the MinIO Operator what
  `Kafka` is to the Strimzi Cluster Operator.
- Three CRDs exist, confirmed from the Operator's own docs, only one used
  in this project:
  1. `Tenant` — the deployment itself, used here.
  2. `PolicyBinding` (`sts.min.io/v1beta1`) — grants a Kubernetes
     `ServiceAccount` bucket access without static keys. Not needed yet,
     relevant once Flink itself authenticates to MinIO in Phase 3.
  3. `MinIOJob` (`job.min.io/v1alpha1`) — runs one-off `mc`-style commands,
     e.g. bucket creation, as a Job. Not used, since `Tenant.spec.buckets`
     already declares the one bucket needed, inline, more simply.
- `Tenant.spec.buckets` creates buckets declaratively at deploy time, the
  same GitOps-friendly shape as `KafkaTopic`, no imperative `mc mb` step
  needed for the `checkpoints` bucket.
- Same two-Application split as Strimzi: `minio-operator.yaml` (chart
  source, installs only the Operator's controller, zero MinIO pods by
  itself) and `minio-tenant.yaml` (directory source, points at
  `manifests/minio/`, where the actual `Tenant` lives). Same reasoning:
  operator lifecycle and cluster config are independent events.
- Chart version `7.1.1` confirmed live via `helm search repo
  minio-operator/operator --versions`. No local `helm repo add` needed for
  ArgoCD itself to use it, same as Strimzi's chart: ArgoCD's repo-server
  fetches directly from `repoURL`, independent of local Helm config. Local
  `helm repo add`/`search` is only useful as an optional way to browse
  available versions yourself.

## Erasure coding: `servers`, `volumesPerServer`, and `EC:N`

- `servers: 1` triggers the Operator's standalone mode, confirmed as the
  documented single-node deployment path (its own "tenant-tiny" canonical
  example), not a workaround. Distributed mode needs 2 or more.
- `volumesPerServer` is how many separate `PersistentVolumeClaims` one
  server pod gets, independent drives from MinIO's perspective. Needed
  even on one server, since a single volume gives nowhere to spread
  redundancy, one failure loses everything.
- The formula, confirmed from MinIO's docs: `N (erasure set size) = K
  (data shards) + M (parity shards)`.
- `N` is fixed by hardware, the number of drives you actually configured.
  `MINIO_STORAGE_CLASS_STANDARD=EC:N` sets `M`, the parity shard count,
  directly. `K` is never chosen on its own, it's whatever's left after
  parity's claim on a fixed drive count: `K = N - M`.
- This project: `N = 4`, from `volumesPerServer: 4`. `EC:2` sets `M = 2`.
  So `K = 4 - 2 = 2`.
- Worked example, a 4MB object: split into two 2MB data shards (`K = 2`),
  plus two 2MB parity shards computed from them (`M = 2`), one shard per
  volume. 8MB stored for a 4MB object, 2x overhead, and any 2 of the 4
  volumes can be lost without losing the object.
- Confirmed quorum thresholds, `N` = total drives: read quorum `N/2`,
  write quorum `N/2 + 1`. On this 4-drive Tenant: 2 drives healthy still
  reads everything, but falls below the write quorum of 3, so writes stop
  until a drive returns. `EC:2` on 4 drives is also the maximum parity
  MinIO allows here, capped at half the erasure set size; tolerating a
  2-drive loss while still writing through it would need more drives.

## MinIO credentials, out of band, same pattern as ArgoCD's

- `Tenant.spec.configuration.name` points at a Secret carrying
  `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`, confirmed from the Operator's
  docs to be plaintext inside `config.env`. Committing that Secret the way
  this repo commits everything else would put a plaintext root password in
  git history.
- Same reasoning `CLAUDE.md` already applies to ArgoCD's own admin
  password: created out of band, fetched with `kubectl`, never written
  into a tracked file.
- `scripts/bootstrap-minio-secret.sh` automates it: creates the
  `minio-tenant` namespace if missing, generates a random password with
  `openssl` instead of a placeholder, creates `storage-configuration`.
  Idempotent, does nothing if the Secret already exists, rather than
  silently rotating the password under a running Tenant.
- `requestAutoCert: false` on the Tenant, same reasoning as
  `kafka-cluster.yaml`'s external listener choosing `tls: false`:
  lab-scale, no extra cert management on top of everything else already
  running.
