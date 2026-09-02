# Phase 5 Operator and HA implementation plan

**Goal:** Move the frozen Phase 4 job off `MiniCluster` and onto the `kind`
cluster, managed by the Flink Kubernetes Operator, with Kubernetes High
Availability, Zone spread, and four Drills that each recover with no gap in the
`recommendation` topic.

**Architecture:** A Shadow fat jar carrying an allowlist of two dependencies is
baked into an image derived from `flink:2.2.0`, together with the S3 filesystem
plugin moved out of `opt/` into its own `plugins/` subfolder. The image is loaded
into every `kind` node. A `FlinkDeployment` in `personalization-blue` runs it in
`native` mode with two JobManagers, three TaskManagers, Kubernetes HA metadata in
MinIO, and per-role Zone spread constraints. ArgoCD syncs the operator and the
job as two Applications, with `selfHeal: false` so the drift Drill is observable.

**Tech Stack:** Flink 2.2.0, Flink Kubernetes Operator 1.15.0 (Helm),
`com.gradleup.shadow` 9.6.1, Gradle 9.7.0, Docker, `kind`, Strimzi, MinIO,
ArgoCD, cert-manager 1.21.1.

**Spec:** [Operator and HA design](../specs/2026-08-30-operator-and-ha-design.md)

## How to use this plan

This project's [CLAUDE.md](../../../CLAUDE.md) working agreement governs, and it
overrides the default shape of a plan document.

**You write every file and run every command.** Each task gives the goal, the
concept behind it, the failure mode to watch for, a skeleton or a key fragment,
and the command that proves it worked. It deliberately does not give finished
files. Work done for you is learning lost.

**A task is done when its verification command produces real output you have
read.** Not when a manifest applies. Not when ArgoCD says Synced. Synced means
the YAML matches Git. It does not mean the thing works.

**This plan prescribes no commits.** Git is yours to drive.

**Every design decision is already settled**, in the spec, with the evidence and
the command that produced it. No task here contains a decision or a verification
gate. If you find one, it is a defect in this plan, not a question for you.

Ask why something failed and you get the line and the mechanism, not a corrected
file.

## Prerequisite

Phase 4 closed on 2026-08-30, all ten tasks done. The job graph is frozen, so the
image built in Task 2 is built from final code.

Nothing in Phase 4 remains open. This plan has no cross-phase dependency.

## Progress

| # | Task | Status |
|---|---|---|
| 0 | Kafka internal listener | done 2026-08-31, listener `plain`, 3 brokers reachable on 9092 from an in-cluster pod |
| 1 | Shadow fat jar, allowlist scoped | done 2026-08-31, 24MB jar, five content checks pass, 27 tests green |
| 2 | The image, and loading it into `kind` | done 2026-08-31, tag `0.1-b606416-dirty`, both files verified in-image, present on all 3 workers |
| 3 | Namespaces and the credentials Secret | done 2026-08-31, idempotence proven by a second run, both Secrets match the source |
| 4 | The operator, as an ArgoCD Application | not started |
| 5 | The `FlinkDeployment`, the Service, the PDB | not started |
| 6 | The gap check instrument | not started |
| 7 | Drill A: kill a TaskManager | not started |
| 8 | Drill B: kill the leader JobManager | not started |
| 9 | Drill C: drain a Zone | not started |
| 10 | Drill D: ArgoCD Lua actions and drift | not started |
| 11 | Documents | not started |

Tasks 0, 1, and 3 depend on nothing and can run in any order. Task 2 needs 1.
Task 4 needs 3. Task 5 needs 0, 2, and 4. Tasks 6 to 10 are strictly sequential
after 5.

### Where to resume

**Next: Task 4.** Task 5 unblocks once it lands, since 0, 2, and 3 are closed. Task 4
follows it, and Task 5 then unblocks once 3 and 4 are done, since 0 and 2 are
already closed.

**Carry forward into Task 5.** The image tag currently on the nodes is
`lab/personalization-pipeline:0.1-b606416-dirty`. The `-dirty` suffix is real:
`HEAD` is `b606416` and seven paths are uncommitted, including
`apps/pipeline/build.gradle`, `apps/pipeline/Dockerfile`, and
`scripts/build-image.sh`. Commit those and re-run `scripts/build-image.sh`
before writing `spec.image`, or the `FlinkDeployment` will name an image that
corresponds to no commit.

Note also that rebuilding produces a **different image digest for the same tag**,
observed on 2026-08-31 as `dbc3f08` then `79c7cf38`. Docker builds are not
byte-reproducible. `kind load` overwrites by tag, so the nodes stay correct, but
a tag is a label and not an identity.

## Global constraints

Copied verbatim from the spec. Every task inherits these.

- **Flink 2.2.0** and **Operator 1.15.0**. The CRD enum value is `v2_2`.
- **No credential may enter Git.** Not in a manifest, not in a values file, not
  in a comment. The `FlinkDeployment` carries a Secret **name** only.
- **No file under `apps/` changes except `apps/pipeline/build.gradle`.** The job
  graph is frozen and the Java is correct as it stands.
- **`apps/pipeline/conf/config.yaml` is not edited.** `:pipeline:run` against
  `MiniCluster` must keep working unchanged through Phase 6 and Phase 7.
- **The `external` Kafka listener is not modified.** The generator and every
  `kcat` check on the host use it. The internal listener is added beside it.
- `automated.selfHeal` stays **`false`** on every Application. Drill D depends on
  it.
- Correct Flink 2.2 config keys, verified by `javap` on `flink-dist-2.2.0.jar`:
  `high-availability.type`, `high-availability.storageDir`,
  `jobmanager.scheduler`, `execution.checkpointing.savepoint-dir`,
  `execution.checkpointing.dir`.
- TaskManager memory is **`2gb`** with network buffers **`256mb`** min and max.
  Below that, Phase 4 Task 4's `Insufficient number of network buffers` returns.
- Ubiquitous language from [CONTEXT.md](../../../CONTEXT.md): Shopper, Click,
  Browsing Session, Product, Recommendation, Zone, Drill, Active Side, Standby
  Side, Native Variant.

## Files this phase touches

| Path | Responsibility |
|---|---|
| `apps/pipeline/build.gradle` | Shadow plugin, the `bundled` allowlist configuration, the `shadowJar` task |
| `apps/pipeline/Dockerfile` | Move the S3 plugin, place the job jar. Build context is `apps/pipeline` |
| `scripts/build-image.sh` | Build, tag with the git sha, load into `kind` |
| `scripts/bootstrap-flink-secret.sh` | Copy the MinIO credentials into both job namespaces |
| `scripts/recommendation-snapshot.sh` | The gap check instrument, used by Drills A, B, C |
| `scripts/bootstrap-phase0.sh` | Modified: ArgoCD helm install gains a values file |
| `manifests/strimzi/kafka-cluster.yaml` | Modified: a second, internal listener |
| `manifests/argocd/flink-actions-values.yaml` | The three Lua resource actions |
| `manifests/argocd-apps/flink-operator.yaml` | Application: the operator Helm chart |
| `manifests/argocd-apps/flink-job-blue.yaml` | Application: the job manifests |
| `manifests/flink/namespaces.yaml` | `personalization-blue` and `personalization-green` |
| `manifests/flink/blue/flinkdeployment.yaml` | The `FlinkDeployment` |
| `manifests/flink/blue/rest-nodeport.yaml` | NodePort Service on 30011 |
| `manifests/flink/blue/pdb.yaml` | `PodDisruptionBudget`, `minAvailable: 1` |
| `docs/runbooks/phase-5-*.md` | One runbook per Drill, four in total |
| `docs/knowledge/phase-5-operator-and-ha.md` | How this actually works |

---

## Task 0: Kafka internal listener

**Files:**
- Modify: `manifests/strimzi/kafka-cluster.yaml`, the `listeners:` block at line 25

**Interfaces produced:** the address
`personalization-kafka-bootstrap.kafka.svc.cluster.local:9092`, which Task 5
passes to the job as `--bootstrap-servers`.

**The problem, before the mechanism.** A Flink pod inside the cluster has to
reach Kafka. It cannot. Today `kubectl get svc personalization-kafka-bootstrap -n kafka`
shows exactly one port, `tcp-replication 9091`. That is Strimzi's own
broker-to-broker replication listener. It is not for clients, and no client
listener exists on the ClusterIP Service at all.

The reason is [ADR 0002](../../adr/0002-strimzi-external-listener.md), and the
ADR is not wrong. Phases 2 to 4 ran every client on the host, so a single
`nodeport` listener on 9094 was exactly right. Phase 5 is the first time a client
runs inside the cluster.

**Why not reuse the external listener from inside.** A pod can reach a node IP,
so it would half work. It would also route pod to node to NodePort to broker for
every message, and it would tie the job's correctness to
`advertisedHost` settings written for a host-side client. An internal listener is
one line of YAML and removes the whole question.

**The failure mode to watch for.** Adding a listener changes a `Kafka` resource
that Strimzi is actively reconciling. Strimzi performs a **rolling restart** of
the brokers to apply it. Watch the pods roll one at a time. If two go down at
once, stop and read the `Kafka` resource's status, because that is a
`minInSyncReplicas` problem and not a listener problem.

- [x] **Step 1: Add the listener beside the existing one.**

Inside the existing `listeners:` list, not replacing it:

```yaml
      - name: plain
        port: 9092
        type: internal
        tls: false
```

`tls: false` for the same lab-scale reason the `external` listener's own comment
already gives. Add a comment saying this listener exists for in-cluster clients
and dates from Phase 5, so the two listeners' purposes stay distinguishable.

- [x] **Step 2: Sync the Application and watch the roll.**

```bash
kubectl get pods -n kafka -w
```

Expected: the three `brokers` pods restart one at a time. The three
`controllers` pods do not restart, because a client listener does not concern
them.

- [x] **Step 3: Confirm the port reached the Service.**

```bash
kubectl get svc personalization-kafka-bootstrap -n kafka \
  -o jsonpath='{range .spec.ports[*]}{.name}{" "}{.port}{"\n"}{end}'
```

Expected: two lines now. `tcp-replication 9091` and a new `tcp-clients 9092`.

- [x] **Step 4: Prove a pod inside the cluster can actually reach it.**

This is the step that makes the task done. The Service having a port is not the
same as a broker answering on it.

```bash
kubectl run kcat-probe --rm -it --restart=Never -n kafka \
  --image=edenhill/kcat:1.7.1 -- \
  -b personalization-kafka-bootstrap.kafka.svc.cluster.local:9092 -L
```

Expected: a metadata listing naming **3** brokers with ids 3, 4, and 5, and the
topics `clickstream`, `product-change`, `promo-rule`, `recommendation`.

- [ ] **Step 5: Confirm the host path still works.**

The generator is still going to run on your host. Re-run whatever `kcat` check
you used in Phase 1 against `localhost:30016` and confirm it is unchanged.

---

## Task 1: Shadow fat jar, allowlist scoped

**Files:**
- Modify: `apps/pipeline/build.gradle`

**Interfaces produced:** `apps/pipeline/build/libs/pipeline-all.jar`, consumed by
Task 2's Dockerfile.

**The problem, before the mechanism.** The job needs to reach the container as
one file. Gradle's `jar` task produces only your own classes, so the Kafka
connector would be missing. The usual answer is a fat jar, which unpacks every
dependency into one archive.

Here that answer is dangerous. Phase 4 Task 0 recorded that `runtimeOnly` does
**not** keep a jar out of a Shadow build, because Shadow builds from the runtime
classpath. Five dependencies carry that scope today. Bundling any of them beside
a distribution that already loads them from `lib/` is a duplicate-class failure.

**Why an allowlist and not an exclusion list.** The obvious repair is
`shadowJar { dependencies { exclude(...) } }` naming the five. That repair is
wrong. Excluding `flink-streaming-java` does not exclude its transitive
`flink-core`, `flink-runtime`, and `flink-shaded-*`. Those still enter the jar.

The difference is the failure mode. A missed exclusion is a **silent** duplicate
class that wins a classpath scan, and you find it as a `NoSuchMethodError` weeks
later. A missed allowlist entry is a `NoClassDefFoundError` at job startup, in
the first thirty seconds, with the missing class named. Choose the mechanism that
fails loudly.

**What belongs in the jar, and nothing else.**

| Bundled | Why |
|---|---|
| `project(':domain')` | Your code. Nothing else supplies it |
| `flink-connector-kafka` and its transitives | Not in the image's `lib/`. Confirmed by listing `lib/` |

| Not bundled | Why |
|---|---|
| `flink-streaming-java`, `flink-clients`, `flink-connector-base` | Inside `flink-dist-2.2.0.jar` |
| `flink-cep` | `lib/flink-cep-2.2.0.jar`, from Phase 4 Task 0 |
| `flink-statebackend-rocksdb` | Provided by the distribution |
| `flink-s3-fs-hadoop` | Goes to `plugins/` in Task 2, and must not also sit on the main classpath |
| `log4j-*` | `lib/` ships 2.24.3, the build declares 2.26.1 |

**The failure mode to watch for.** Gradle configurations are not all resolvable.
A configuration you create and then only use as a parent via `extendsFrom` may
refuse to resolve when Shadow asks it for files, with
`Resolving configuration 'bundled' is not allowed`. Declare its intent
explicitly rather than relying on the legacy default, which Gradle 9 is
progressively removing.

- [x] **Step 1: Add the plugin.**

```groovy
plugins {
    id 'java'
    id 'application'
    id 'com.gradleup.shadow' version '9.6.1'
}
```

`com.github.johnrengelman.shadow` is the version most search results still show.
It is unmaintained and does not support Gradle 9. This project is on 9.7.0.

- [x] **Step 2: Declare the allowlist configuration.**

Skeleton, not the finished block. `bundled` must be resolvable, must not be
consumable, and `implementation` must extend it so the compile classpath is
unchanged:

```groovy
configurations {
    bundled {
        canBeResolved = true
        canBeConsumed = false
    }
    implementation.extendsFrom bundled
}
```

- [x] **Step 3: Move exactly two dependencies into it.**

`project(':domain')` and `flink-connector-kafka` change from `implementation` to
`bundled`. Every other line in the `dependencies` block is untouched, including
all five `runtimeOnly` declarations, which `:pipeline:run` still needs.

- [x] **Step 4: Point `shadowJar` at the allowlist and fix the name.**

```groovy
shadowJar {
    archiveFileName = 'pipeline-all.jar'
    configurations = [project.configurations.bundled]
}
```

A stable file name matters because Task 2's Dockerfile copies it by name.

- [x] **Step 5: Build it.**

```bash
apps/gradlew -p apps :pipeline:shadowJar
```

- [x] **Step 6: Verify the contents, four counts.**

This is the step that makes the task done. Do not accept `BUILD SUCCESSFUL` as
the answer, for the same reason Phase 4 Task 2 stopped trusting it.

```bash
J=apps/pipeline/build/libs/pipeline-all.jar
echo "domain        $(unzip -l $J | grep -c 'lab/personalization/domain/')"
echo "kafka conn    $(unzip -l $J | grep -c 'org/apache/flink/connector/kafka/')"
echo "flink runtime $(unzip -l $J | grep -c 'org/apache/flink/streaming/api/')"
echo "log4j         $(unzip -l $J | grep -c 'org/apache/logging/log4j/')"
```

Expected: the first two counts are greater than zero. **The last two are exactly
zero.** A non-zero third or fourth count means the allowlist is not being
honoured and the jar will collide with `lib/` at run time.

**A fifth check, because the fourth one's result looks alarming and is fine.**

```bash
unzip -l $J | grep -oE 'org/apache/flink/[a-z0-9]+/[a-z0-9]+' | sort | uniq -c
```

Expected, and observed on 2026-08-31:

```
    168 org/apache/flink/connector/kafka
     71 org/apache/flink/streaming/connectors
      3 org/apache/flink/streaming/util
```

The last two look like `flink-streaming-java` leaking in. They are not.
`flink-connector-kafka:5.0.0-2.2` ships its own classes under those legacy
package names. Confirmed with `javap` against the image that
`flink-dist-2.2.0.jar` contains neither
`org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema` nor
`org.apache.flink.streaming.util.serialization.SimpleStringSchema`. A package
name is not evidence of which artifact a class came from.

- [x] **Step 7: Confirm you broke nothing local.**

```bash
apps/gradlew -p apps :pipeline:test
```

Expected: the same 27 tests green that Phase 4 Task 7 finished on. Read the XML
report under `apps/pipeline/build/test-results/test/`, not the console summary.

---

## Task 2: The image, and loading it into `kind`

**Files:**
- Create: `apps/pipeline/Dockerfile`
- Create: `scripts/build-image.sh`

**Interfaces consumed:** `pipeline-all.jar` from Task 1.

**Interfaces produced:** the image
`lab/personalization-pipeline:0.1-<git short sha>`, present in every `kind`
node's image store. Task 5's `FlinkDeployment` names it in `spec.image`.

**The problem, before the mechanism.** Two files must be somewhere Flink looks,
and neither is.

Your job jar is not in the image at all. And `flink-s3-fs-hadoop-2.2.0.jar` **is**
in the image, at `/opt/flink/opt/`, which Flink never reads. `bin/config.sh`
lines 24 to 32 build the classpath with a `find` over `lib/` alone. Nothing
`find`s `opt/`.

`opt/` is not protected or special. It is simply on no list. Moving the jar into
`plugins/s3-fs-hadoop/` does exactly one thing: it puts the file on a list.

**Why its own subfolder, and why the name does not matter.** Flink scans each
subfolder of `plugins/` with its own isolated classloader. The image's own
`plugins/README.txt` says the folder name becomes the plugin id, so the name is
free. The dedicated subfolder is not free, because the subfolder **is** the
classloader boundary. That isolation is required here: the AWS SDK classes in
that jar are plain `com.amazonaws.*` and are not relocated, and `opt/` holds five
filesystem jars, two of which both register the scheme `s3`.

**Why `RUN cp` and not `COPY`.** `COPY` reads from the build context on your
host. The source file is already inside the base image. `COPY` cannot see it.

**Why an immutable tag.** A moving tag makes ArgoCD drift meaningless, makes a
rollback ambiguous, and produces the single most confusing failure in this phase:
new code, stale image, and a stack trace that looks like a Flink bug.

**The failure mode to watch for.** `kind` nodes do not share your host's Docker
daemon image store. If the image is not loaded, the pod sits in `ErrImagePull`
trying to reach a registry that has never heard of `lab/personalization-pipeline`.

- [x] **Step 1: Write the Dockerfile.**

Build context is `apps/pipeline`, so the jar path is relative to that directory:

```dockerfile
FROM flink:2.2.0

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.0.jar /opt/flink/plugins/s3-fs-hadoop/

COPY build/libs/pipeline-all.jar /opt/flink/usrlib/pipeline.jar
```

- [x] **Step 2: Write `scripts/build-image.sh`.**

Follow the shape the three existing scripts already use: `set -euo pipefail`, the
`info` / `ok` / `die` helpers, and a header comment saying what it does and when
to run it. It needs to:

1. Run `apps/gradlew -p apps :pipeline:shadowJar`.
2. Compute `TAG="0.1-$(git rev-parse --short HEAD)"`.
3. `docker build -t "lab/personalization-pipeline:${TAG}" apps/pipeline`.
4. `kind load docker-image "lab/personalization-pipeline:${TAG}" --name personalization-lab`.
5. Print the full tag on the last line, because you will paste it into
   `flinkdeployment.yaml`.

Refuse to build on a dirty working tree, or print a loud warning. A tag naming a
commit that does not contain the code in the image is the failure mode above,
wearing a disguise.

- [x] **Step 3: Build and load.**

```bash
./scripts/build-image.sh
```

- [x] **Step 4: Verify both files, inside the image.**

This is the step that makes the task done, and it is the whole reason the spec
chose build time over the entrypoint variable. You can check before any pod
exists:

```bash
TAG=0.1-$(git rev-parse --short HEAD)
docker run --rm --entrypoint sh lab/personalization-pipeline:$TAG -c \
  'ls -l /opt/flink/plugins/s3-fs-hadoop/ /opt/flink/usrlib/'
```

Expected: `flink-s3-fs-hadoop-2.2.0.jar` in the first directory, and
`pipeline.jar` in the second.

- [x] **Step 5: Verify the image reached the nodes.**

```bash
docker exec personalization-lab-worker crictl images | grep personalization-pipeline
```

Expected: one line with your tag. Repeat for `worker2` and `worker3`. All three
must have it, because Task 5 spreads TaskManagers across all three Zones.

**Two things this step got wrong on the first run, both worth building into the
script.**

`kind load` returns **before** containerd has finished registering the image on
every node. A check that runs immediately reports a false absence on whichever
node was slowest. Retry for up to 30 seconds per node before declaring failure.

And do not run the script as `./scripts/build-image.sh | tail -30`. A pipeline's
exit code is the **last** command's, so `tail` succeeding masks the script's
`exit 1` entirely. The script printed a red failure and the shell reported
`exit code 0`. Either drop the pipe or read `${PIPESTATUS[0]}`. This is general,
not specific to this script: any `set -euo pipefail` script piped into `tail`,
`head`, or `grep` reports the pipe's status and not its own.

---

## Task 3: Namespaces and the credentials Secret

**Files:**
- Create: `manifests/flink/namespaces.yaml`
- Create: `scripts/bootstrap-flink-secret.sh`

**Interfaces produced:** namespaces `personalization-blue` and
`personalization-green`, and a Secret named `minio-credentials` in each, with
keys `access-key` and `secret-key`. Task 4 lists both namespaces in
`watchNamespaces`. Task 5 references the Secret by name.

**Why both namespaces now, when only one runs a job.** The operator chart creates
the `flink` ServiceAccount, Role, and RoleBinding **once per watched namespace**,
which was verified in `templates/flink/service_account.yaml`. Listing green now
means Phase 7 does not have to re-sync the operator to give the Standby Side
permissions. Green stays empty in this phase.

Per [CONTEXT.md](../../../CONTEXT.md), `blue` and `green` are namespace names and
not roles. Phase 5 makes blue the Active Side, and that is a starting position.

### Why this script exists at all

Three things force it, and only the third is a choice.

**1. A pod can only mount a Secret from its own namespace.** This is a hard
Kubernetes rule, not a preference. There is no syntax for `secretKeyRef` to reach
across a namespace boundary. The credentials exist in `minio-tenant`; the Flink
pods will be in `personalization-blue`. So a second copy has to exist there. No
script can avoid that. The only question is what creates it.

**2. It cannot be a manifest in Git.** That is the project rule already applied
twice, for the ArgoCD admin password and for the MinIO root password. A Secret
manifest carries base64, which is encoding and not encryption, so committing it
puts the password in history permanently. The `FlinkDeployment` therefore carries
a Secret **name**. A name is a reference. It names the box without carrying what
is in the box.

So the Secret must be created **outside** Git. Something has to run a command.

**3. That command could just be typed.** Here is the honest version. Without the
script:

```bash
source scripts/minio-env.sh
for ns in personalization-blue personalization-green; do
  kubectl create secret generic minio-credentials -n $ns \
    --from-literal=access-key="$MINIO_ACCESS_KEY" \
    --from-literal=secret-key="$MINIO_SECRET_KEY"
done
```

Five lines. The script is thin, and it is worth saying so rather than pretending
otherwise.

### What the script actually buys

| Buys | Worth it? |
|---|---|
| **Survives a cluster rebuild.** [README.md](../../../README.md) says the bootstrap scripts must be re-run after `kind delete` plus `kind create`. Without this file you must remember the key names `access-key` and `secret-key`, and both namespaces, months later | This is the real reason |
| **Records the contract.** Task 5's `secretKeyRef` depends on those exact key names. The script is where they are written down and executable | Yes |
| **Fits the existing set.** Three bootstrap-ish scripts already exist with the same helpers and the same `README.md` table row | Yes |
| Idempotence guard | Marginal. `kubectl create` already errors on an existing Secret, which is a safe failure |

### The alternative not taken, and what it costs

In production you would not copy a Secret between namespaces by hand. You would
use the External Secrets Operator, or a reflector controller that mirrors one
Secret into many namespaces and keeps them in step.

Rejected here because it means installing another operator to manage one Secret
whose source is another pod in the same cluster. For this lab the cost is not
repaid.

**The consequence, and it is the thing that will be forgotten.** The hand-copy is
the lab-scale answer and not the general one, so a rotation in `minio-tenant`
will **not** propagate. If that password is ever rotated, `minio-credentials`
must be deleted in both namespaces and this script re-run. Say so in the script's
header.

**The failure mode to watch for.** A re-run that regenerates or overwrites a
password under a running job breaks checkpointing at the next interval, and the
error surfaces as an S3 403 that looks like a MinIO problem. The script copies,
it never generates, and a re-run against an existing Secret is a no-op.

- [x] **Step 1: Write `manifests/flink/namespaces.yaml`.**

Two `Namespace` objects. Add a comment saying green is deliberately empty until
Phase 7, so a reader does not assume it is a mistake.

- [x] **Step 2: Apply it.**

```bash
kubectl apply -f manifests/flink/namespaces.yaml
```

The namespaces must exist before Task 4, because the operator chart creates
RBAC objects **inside** them and a Role cannot be created in a namespace that
does not exist.

**Why `kubectl apply` and not an ArgoCD Application, when
[ADR 0004](../../adr/0004-gitops-from-phase-0.md) says GitOps from Phase 0.**
An ordering problem, not an exception to the ADR. The operator Application in
Task 4 creates a Role and a RoleBinding inside both namespaces during its own
sync. If the namespaces were a third Application, their creation would race the
operator's sync, and with sync waves still deferred there is nothing to order the
two. The namespaces are a precondition of the operator install, in the same
category as the `kafka` namespace that Strimzi's own Application creates through
`CreateNamespace=true`. The file stays tracked in Git either way, so the declared
state is still in the repository.

- [x] **Step 3: Write `scripts/bootstrap-flink-secret.sh`.**

It reads the same source `scripts/minio-env.sh` reads:

```bash
kubectl get secret storage-configuration -n minio-tenant \
  -o jsonpath='{.data.config\.env}' | base64 -d
```

That yields shell `export` lines. Pull `MINIO_ROOT_USER` and
`MINIO_ROOT_PASSWORD` out of it, then create `minio-credentials` in both
namespaces with keys `access-key` and `secret-key`. Loop over the two namespaces
rather than writing the block twice.

MinIO calls them root user and root password. S3 calls them access key and secret
key. Same two strings, two names. `scripts/minio-env.sh` already says this in its
header, and it is worth saying again here.

- [x] **Step 4: Run it, then run it a second time.**

```bash
./scripts/bootstrap-flink-secret.sh
./scripts/bootstrap-flink-secret.sh
```

Expected: the first run creates two Secrets. The **second run changes nothing**
and says so. That is the idempotence guard, and running it twice on purpose is
how you know it is real.

- [x] **Step 5: Verify the values match the source.**

```bash
kubectl get secret minio-credentials -n personalization-blue \
  -o jsonpath='{.data.access-key}' | base64 -d; echo
```

Expected: `minioadmin`. Compare the secret key against
`source scripts/minio-env.sh; echo "$MINIO_SECRET_KEY"`. They must be identical.
A mismatch here becomes an S3 403 in Task 5 that is very hard to read backwards.

---

## Task 4: The operator, as an ArgoCD Application

**Files:**
- Create: `manifests/argocd-apps/flink-operator.yaml`

**Interfaces consumed:** the namespaces from Task 3.

**Interfaces produced:** the `FlinkDeployment` CRD, a running operator, and the
`flink` ServiceAccount, Role, and RoleBinding in both job namespaces. Task 5's
`spec.serviceAccount: flink` refers to that ServiceAccount.

**Why cert-manager is a real dependency and not a design-spec claim.** The chart
ships `templates/cert-manager/certificate.yaml` and `issuer.yaml`, and annotates
both webhook configurations with `cert-manager.io/inject-ca-from`. This was read
from the 1.15.0 chart, not assumed. cert-manager `v1.21.1` has been installed
since Phase 0, so the dependency is already satisfied. Sync waves stay deferred
for the reason they have been deferred since Phase 0: cert-manager is Healthy
long before this Application is created, so there is nothing to order.

**What `watchNamespaces` really controls.** Two things at once, and the second is
easy to miss. It limits which namespaces the operator reconciles, and it decides
where the chart creates the job RBAC. With the list empty, RBAC is created at
cluster scope and the `flink` ServiceAccount lands only in the release namespace.
With the list set, the chart loops over it and creates a ServiceAccount, a Role,
and a RoleBinding in **each** entry.

**Why `rbac.nodesRule.create` stays `false`.** The chart's own comment says that
value is needed when `kubernetes.rest-service.exposed.type` is `NodePort`,
because Flink then lists nodes at cluster scope. Task 5 leaves that type at
`ClusterIP` and uses a hand-written Service instead, so the extra cluster-scoped
permission is not needed. Granting a permission nothing uses is how a lab drifts
away from being a useful example.

**The failure mode to watch for.** If the webhook's certificate is not ready, the
first `FlinkDeployment` you apply in Task 5 is rejected by the API server with a
webhook call failure, and the message names TLS rather than Flink. Confirm the
`Certificate` is Ready in this task, not in the next one.

- [x] **Step 1: Write the Application.**

Follow `manifests/argocd-apps/strimzi.yaml` exactly in shape, including its habit
of explaining `metadata.namespace` versus `spec.destination.namespace`. The
values that differ:

```yaml
  source:
    repoURL: https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
    chart: flink-kubernetes-operator
    targetRevision: 1.15.0
    helm:
      valuesObject:
        watchNamespaces:
          - personalization-blue
          - personalization-green
        rbac:
          nodesRule:
            create: false
  destination:
    server: https://kubernetes.default.svc
    namespace: flink-operator
```

`syncPolicy` matches Strimzi's: `selfHeal: false`, `prune: true`, and
`CreateNamespace=true` because nothing else creates `flink-operator`.

The `repoURL` is the release directory itself. It serves an `index.yaml`, which
is what makes it a valid Helm repository rather than only a download page.

- [ ] **Step 2: Commit and push. Do not `kubectl apply` this one.**

`manifests/argocd-apps/root.yaml` already watches `path: manifests/argocd-apps`,
so the root app-of-apps owns that directory. Applying the file by hand creates an
Application that ArgoCD did not create, sitting outside the app-of-apps tree
until a later push causes root to adopt it. That is self-inflicted drift, in the
phase whose point is observing drift deliberately.

Push it, and root creates the child Application itself. Two automated syncs then
chain, each on roughly a three minute poll: root syncs and creates
`flink-operator`, then `flink-operator` syncs itself and installs the chart.

To skip the wait:

```bash
kubectl -n argocd annotate app root argocd.argoproj.io/refresh=hard --overwrite
```

Watch both:

```bash
kubectl -n argocd get app root flink-operator \
  -o custom-columns='NAME:.metadata.name,SYNC:.status.sync.status,HEALTH:.status.health.status'
```

Note this differs from Task 3's namespaces, which **are** applied by hand. The
difference is ownership: nothing in ArgoCD claims `manifests/flink/`, while root
explicitly claims `manifests/argocd-apps/`.

- [ ] **Step 3: Confirm the certificate before anything else.**

```bash
kubectl get certificate -n flink-operator
```

Expected: `flink-operator-serving-cert`, `READY: True`. If it is not True,
nothing after this point will work, and the reason is in
`kubectl describe certificate`.

- [ ] **Step 4: Confirm the operator is running and the CRDs landed.**

```bash
kubectl get pods -n flink-operator
kubectl get crd | grep flink.apache.org
```

Expected: one operator pod Running with all containers ready, and four CRDs,
including `flinkdeployments.flink.apache.org`.

- [ ] **Step 5: Confirm `v2_2` is really accepted by the installed CRD.**

```bash
kubectl get crd flinkdeployments.flink.apache.org -o jsonpath=\
'{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.flinkVersion.enum}'
```

Expected: a list ending in `v2_0 v2_1 v2_2`. This is the single fact the whole
phase rests on, so read it from the installed CRD rather than from the chart
tarball.

- [ ] **Step 6: Confirm RBAC landed in both job namespaces.**

```bash
for ns in personalization-blue personalization-green; do
  echo "== $ns"
  kubectl get sa flink -n $ns
  kubectl get role,rolebinding -n $ns
done
```

Expected: in **both** namespaces, a ServiceAccount named `flink`, a Role named
`flink`, and a RoleBinding named `flink-role-binding`. Green having them while
running no job is the point of Task 3.

---

## Task 5: The `FlinkDeployment`, the Service, the PDB

**Files:**
- Create: `manifests/flink/blue/flinkdeployment.yaml`
- Create: `manifests/flink/blue/rest-nodeport.yaml`
- Create: `manifests/flink/blue/pdb.yaml`
- Create: `manifests/argocd-apps/flink-job-blue.yaml`

**Interfaces consumed:** the image tag from Task 2, the Secret from Task 3, the
`flink` ServiceAccount from Task 4, the Kafka internal listener from Task 0.

**Interfaces produced:** a RUNNING job in `personalization-blue`, checkpoints
under `s3://checkpoints/phase-5`, HA metadata under
`s3://checkpoints/phase-5-ha`, and the Flink UI on `localhost:30011`. Every Drill
depends on this task.

**The concept: one program became three.** Under `MiniCluster` there was one JVM.
`PersonalizationJob.flinkConfiguration` read the file, added the credentials from
the environment, called `FileSystem.initialize`, and everything in that one
process knew about S3.

Here there is one JobManager pod, one standby, and three TaskManager pods. Your
`main` method runs on the **leader JobManager only**. The TaskManagers hold the
RocksDB state and write the checkpoint data to MinIO, and they never run your
`main`. Nothing your code does can reach them.

That is why the configuration moves to `spec.flinkConfiguration`. The operator
renders it into `/opt/flink/conf/config.yaml` in **every** pod.

**Why no Java changes.** The image working directory is `/opt/flink`.
`PipelineConfig` defaults `flinkConfDir` to the relative path `conf`, so
`GlobalConfiguration.loadConfiguration("conf")` resolves to `/opt/flink/conf` and
reads exactly the file the operator wrote. The guard at
`PersonalizationJob.java:187` still works: with no `state.backend.type` present
the job throws instead of silently running on defaults.

**Why four environment variables from two Secret keys.** `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` are what the **TaskManagers** need, because they write
checkpoint data and never run `main`. Hadoop S3A finds them through
`EnvironmentVariableCredentialsProvider`, which is third in the default chain,
verified from `core-default.xml` inside the real jar. The chain reaches it
because `s3.access-key` is deliberately not set, so `SimpleAWSCredentialsProvider`
finds nothing and falls through. `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` exist
only because `Env.require` at `PersonalizationJob.java:191` throws on a blank
value. Two redundant variables is cheaper than editing tested Java to suit a
deployment target.

**Why the spread constraints are per role and not shared.** A single constraint
in `spec.podTemplate` would treat JobManagers and TaskManagers as one population
and spread them against each other, which is what neither needs. They go in
`spec.jobManager.podTemplate` and `spec.taskManager.podTemplate` separately, each
selecting its own `component` label.

**Why the two roles use different `whenUnsatisfiable` values.** For the
JobManager, `DoNotSchedule`: two JobManagers in one Zone is HA theatre, and when
Drill C drains a Zone, the second JobManager going Pending is the correct and
visible outcome. For the TaskManager, `ScheduleAnyway`: recovery is what every
Drill measures, and `DoNotSchedule` would leave a subtask unschedulable and block
the recovery the Drill exists to observe.

**The failure mode to watch for.** `taskmanager.memory.process.size` must be
`2gb` and `spec.taskManager.resource.memory` must be at least that. Phase 4 Task
4 hit `Insufficient number of network buffers: required 17, but only 0 available`
against the default 2048 buffers, because this graph has five `keyBy` shuffles.
The `256mb` network setting gives 8192 buffers. A container sized below the
process size hits the same wall from the other direction, as an OOMKill.

- [ ] **Step 1: Write the `FlinkDeployment`, top level.**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: personalization
  namespace: personalization-blue
spec:
  image: lab/personalization-pipeline:0.1-<the tag build-image.sh printed>
  imagePullPolicy: IfNotPresent
  flinkVersion: v2_2
  mode: native
  serviceAccount: flink
```

`imagePullPolicy: IfNotPresent` is stated explicitly and is not decoration. The
image exists only in the nodes' image stores. A pull attempt reaches no registry
and fails.

- [ ] **Step 2: Write `spec.flinkConfiguration`.**

Every value is a **string** in this map, including numbers. Carried unchanged
from `apps/pipeline/conf/config.yaml`:

```yaml
  flinkConfiguration:
    state.backend.type: rocksdb
    execution.checkpointing.interval: 10s
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.incremental: "true"
    execution.checkpointing.num-retained: "3"
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
    pipeline.generic-types: "false"
    taskmanager.memory.process.size: 2gb
    taskmanager.memory.network.min: 256mb
    taskmanager.memory.network.max: 256mb
    s3.path.style.access: "true"
```

Changed for the cluster:

```yaml
    s3.endpoint: http://minio.minio-tenant.svc.cluster.local
    execution.checkpointing.dir: s3://checkpoints/phase-5
```

New in this phase:

```yaml
    high-availability.type: kubernetes
    high-availability.storageDir: s3://checkpoints/phase-5-ha
    execution.checkpointing.savepoint-dir: s3://checkpoints/phase-5-savepoints
    jobmanager.scheduler: Adaptive
    taskmanager.numberOfTaskSlots: "2"
    kubernetes.rest-service.exposed.type: ClusterIP
```

`s3.access-key` and `s3.secret-key` are **absent on purpose**. Add a comment
saying so, because their absence looks like an oversight and is the opposite.

- [ ] **Step 3: Write the shared `podTemplate` with the four environment variables.**

The container name must be exactly `flink-main-container`. The operator matches
on that name to merge your template into the pod it generates. Any other name
adds a second container instead of configuring the one that matters.

```yaml
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: minio-credentials
                  key: access-key
```

Three more in the same shape: `AWS_SECRET_ACCESS_KEY` from `secret-key`,
`MINIO_ACCESS_KEY` from `access-key`, `MINIO_SECRET_KEY` from `secret-key`.

- [ ] **Step 4: Write the two role blocks with their spread constraints.**

```yaml
  jobManager:
    replicas: 2
    resource:
      memory: "2048m"
      cpu: 1
    podTemplate:
      spec:
        topologySpreadConstraints:
          - maxSkew: 1
            topologyKey: topology.kubernetes.io/zone
            whenUnsatisfiable: DoNotSchedule
            labelSelector:
              matchLabels:
                app: personalization
                component: jobmanager
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
    podTemplate:
      spec:
        topologySpreadConstraints:
          - maxSkew: 1
            topologyKey: topology.kubernetes.io/zone
            whenUnsatisfiable: ScheduleAnyway
            labelSelector:
              matchLabels:
                app: personalization
                component: taskmanager
```

The operator labels the pods it creates with `app: <deployment name>` and
`component: jobmanager` or `taskmanager`. Confirm those labels on a live pod in
Step 9 before trusting the selectors, because a selector that matches nothing
produces no error and no spreading.

- [ ] **Step 5: Write the `job` block.**

```yaml
  job:
    jarURI: local:///opt/flink/usrlib/pipeline.jar
    parallelism: 6
    upgradeMode: savepoint
    state: running
    args:
      - "--bootstrap-servers=personalization-kafka-bootstrap.kafka.svc.cluster.local:9092"
```

Parallelism 6 with 2 slots per TaskManager gives exactly **3** TaskManagers, one
per Zone, which is what makes Drill C legible.

Only `--bootstrap-servers` is passed. Every other `PipelineConfig` default is
already correct on the cluster, and passing a value that is not changing is a
place for the two environments to silently diverge later.

`upgradeMode: savepoint` is here for Phase 7's Promotion, which suspends with a
savepoint. It is why `execution.checkpointing.savepoint-dir` is set in Step 2.

- [ ] **Step 6: Write the NodePort Service.**

Flink cannot pin a NodePort number. `KubernetesConfigOptions` has
`kubernetes.rest-service.exposed.type` and `.exposed.node-port-address-type`, and
no port key at all, which was read from `flink-dist-2.2.0.jar`. A random port is
useless because `clusters/kind/` maps a fixed host port.

So write a plain Service, in the shape `manifests/minio/s3-nodeport.yaml` already
uses for MinIO on 30014: `type: NodePort`, `nodePort: 30011`, `port: 8081`,
`targetPort: 8081`, selecting `app: personalization` and `component: jobmanager`.

- [ ] **Step 7: Write the PodDisruptionBudget.**

`minAvailable: 1`, selecting the same two labels. The operator does not create
one.

It has no effect on `kubectl delete pod`, which is an involuntary disruption.
It has effect on `kubectl drain`, which is Drill C. Say that in a comment, so
Drill A and B do not look like the PDB failing.

- [ ] **Step 8: Write the ArgoCD Application and sync it.**

`manifests/argocd-apps/flink-job-blue.yaml`, with `metadata.name: flink-job-blue`.
Task 10 addresses it by that exact name, so a different one breaks Drill D.

A Git source pointing at `manifests/flink/blue/`, destination namespace
`personalization-blue`, `selfHeal: false`, `prune: true`. No
`CreateNamespace=true`, because Task 3 already created it and this Application
should not own its lifecycle.

- [ ] **Step 9: Watch it come up, and read the labels.**

```bash
kubectl get flinkdeployment personalization -n personalization-blue -w
kubectl get pods -n personalization-blue --show-labels
```

Expected: `JOB STATUS: RUNNING`. Five pods: two JobManagers, three TaskManagers.
Read the labels and confirm they are what Step 4 and Step 6 select on. If they
differ, fix the selectors now, before any Drill depends on them.

- [ ] **Step 10: Confirm the Zone spread actually happened.**

```bash
kubectl get pods -n personalization-blue -o custom-columns=\
'NAME:.metadata.name,NODE:.spec.nodeName,COMPONENT:.metadata.labels.component'
```

Expected: the three TaskManagers on three different workers, and the two
JobManagers on two different workers. All three Zones carry work.

- [ ] **Step 11: Confirm the plugin loaded, which is the whole of Task 2's point.**

```bash
kubectl exec -n personalization-blue deploy/personalization -- \
  ls /opt/flink/plugins/s3-fs-hadoop/
```

Then check for checkpoints in MinIO, the same way Phase 3 Task 7 did:

```bash
source scripts/minio-env.sh
# then the signed ListObjectsV2 request you used in Phase 3, against prefix phase-5
```

Expected: `chk-N` objects appearing under `phase-5`, and N increasing on a second
look about a minute later. An empty bucket with a Running job means the
filesystem never registered, and the JobManager log will say
`UnsupportedFileSystemSchemeException`.

- [ ] **Step 12: Confirm HA metadata exists, separately.**

Look under the `phase-5-ha` prefix. It is not the same thing as checkpoints, and
Drill B depends only on this one. An empty `phase-5-ha` prefix means
`high-availability.type` did not take effect, and Drill B would then quietly
prove nothing.

- [ ] **Step 13: Confirm Recommendations are being produced from the cluster.**

Start the generator on your host as usual, then:

```bash
kcat -b localhost:30016 -t recommendation -C -o -20 -e \
  -X isolation.level=read_committed
```

Expected: recent Recommendations. `read_committed` is not optional, for the
reason Phase 3 Task 8 recorded: without it you read uncommitted transactional
records and the counts will not reconcile.

- [ ] **Step 14: Open the UI.**

`http://localhost:30011`. Expected: the job graph, five operators, no failed
tasks, and a checkpoint history with successful entries.

---

## Task 6: The gap check instrument

**Files:**
- Create: `scripts/recommendation-snapshot.sh`

**Interfaces consumed:** the running job from Task 5.

**Interfaces produced:** a snapshot file of Recommendation identities, and a
comparison mode. Drills A, B, and C all use it.

**The problem, before the mechanism.** The phase's done criterion is "no gap in
the `recommendation` topic". Stated casually, that check fails for the wrong
reason.

Phase 4 Task 1 added an out-of-stock suppression rule, and the generator emits
`stock == 0` at a measured 9.6%. Phase 4 Task 4 added a second suppression:
a candidate with no trigger goes to `UNMATCHED` and is not published, removing
roughly a further fifth of the volume.

So the topic legitimately holds fewer records than there are closed Browsing
Sessions. Compare those two numbers and a correct suppression reads as a Drill
failure. The check has to compare the topic **against itself**, across the Drill.

**What identifies one Recommendation.** `Recommendation` has no session id. Its
fields are `shopperId`, `productId`, `discountPercent`, `reason`, `generatedAt`.
The identity that survives a restore is `(shopperId, generatedAt)`, because
`generatedAt` is the Browsing Session's window end, an event-time value. Phase
4's global constraints state that nothing in the output derives from wall-clock
time, which is exactly the property this check needs. Replay the same input and
the same pair is produced again.

**You do not need to parse JSON for this.**
`RecommendationSerializationSchema` sets the Kafka record **key** to `shopperId`
and the record **timestamp** to `generatedAt.toEpochMilli()`. So `kcat -f '%k %T\n'`
gives you the identity pair directly.

**The failure mode to watch for.** Omit `isolation.level=read_committed` and you
read uncommitted transactional records. Those can be aborted later, so the
"before" snapshot picks up identities that were never really published, and every
Drill then reports a false gap.

- [ ] **Step 1: Write the snapshot mode.**

```bash
kcat -b localhost:30016 -t recommendation -C -e \
  -X isolation.level=read_committed \
  -f '%k %T\n' | sort -u > "$OUT"
```

`-e` exits at end of partition, so this terminates. `-o beginning` is the
default for `-C` and is what you want: the check reads the whole topic each time,
not a tail.

- [ ] **Step 2: Write the compare mode.**

Two things must be reported separately, because they mean different things:

```bash
# 1. Gap: identities in BEFORE that are missing from AFTER. Must be empty.
comm -23 "$BEFORE" "$AFTER"

# 2. Duplicates: any identity appearing twice in AFTER. Must be empty.
cut -d' ' -f1,2 "$AFTER" | sort | uniq -d
```

A gap means recovery lost data. A duplicate means exactly-once was broken by the
restore. Print both counts and exit non-zero if either is non-empty, so a Drill
cannot be signed off by misreading the output.

Note that the snapshot is already `sort -u`, which `comm` requires.

- [ ] **Step 3: Take two snapshots with no Drill in between.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/rec-1.txt
sleep 30
./scripts/recommendation-snapshot.sh snapshot /tmp/rec-2.txt
./scripts/recommendation-snapshot.sh compare /tmp/rec-1.txt /tmp/rec-2.txt
```

Expected: **zero gaps and zero duplicates.** The second file is larger, because
30 more seconds of Recommendations were produced. Growth is not a gap.

This step is the instrument's own calibration. If it reports a gap here, with
nothing broken, then every Drill result afterwards is meaningless. Fix it now.

---

## Task 7: Drill A: kill a TaskManager

**Files:**
- Create: `docs/runbooks/phase-5-drill-a-taskmanager-kill.md`

**Interfaces consumed:** Task 5's running job, Task 6's instrument.

**The concept.** A TaskManager holds a slice of the RocksDB state in memory and
on local disk. Killing it destroys that slice. Recovery is not "restart the pod
and carry on", because the surviving TaskManagers hold state from a different
point in time than a fresh one would.

So Flink does the only consistent thing: it fails the **whole job**, then
restarts every task from the last completed checkpoint. The 10 second checkpoint
interval is what bounds how much replay that costs.

**Why the PodDisruptionBudget does nothing here.** A PDB constrains *voluntary*
disruption, which is the eviction API. `kubectl delete pod` is involuntary. The
PDB is not broken in this Drill. It is not involved. That distinction is Drill C.

**The failure mode to watch for.** If the job goes to `RECONCILING` and stays
there, read the JobManager log before touching anything. The common cause is a
replacement TaskManager that cannot be scheduled, and the reason will be in
`kubectl describe pod`, not in Flink.

- [ ] **Step 1: Snapshot before.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-a-before.txt
```

- [ ] **Step 2: Note the current checkpoint id, from the UI or the log.**

You want to be able to say afterwards which checkpoint the job restored from.
"It recovered" is weaker than "it restored from `chk-N`".

- [ ] **Step 3: Kill one TaskManager.**

```bash
kubectl delete pod -n personalization-blue \
  -l component=taskmanager --field-selector status.phase=Running \
  --wait=false | head -1
```

Delete exactly one. Deleting all three is a different Drill and tests less, since
there is no partial-failure recovery path to observe.

- [ ] **Step 4: Watch the job fail and come back.**

```bash
kubectl get flinkdeployment personalization -n personalization-blue -w
```

Expected sequence: `RUNNING`, then a restarting state, then `RUNNING` again. A
replacement TaskManager pod appears. Record the wall-clock duration.

- [ ] **Step 5: Confirm it restored from a checkpoint rather than starting empty.**

In the JobManager log, look for the restore line naming a checkpoint path under
`s3://checkpoints/phase-5`. A job that started from scratch would produce no such
line, and the topic gap check in Step 6 would then be the only thing catching it.

- [ ] **Step 6: Snapshot after, and compare.**

```bash
sleep 60
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-a-after.txt
./scripts/recommendation-snapshot.sh compare /tmp/drill-a-before.txt /tmp/drill-a-after.txt
```

Expected: **zero gaps, zero duplicates.** A pause in output during the restart is
expected and is not a gap. A lost identity is a gap.

- [ ] **Step 7: Write the runbook.**

Follow `docs/runbooks/phase-3-late-click-drill.md` in shape: the goal, the
rationale per command, and an **Observed result** section holding the real
transcript. Phase 0's runbook still carries only the predicted-behaviour version
because the output was never pasted back. Do not repeat that.

---

## Task 8: Drill B: kill the leader JobManager

**Files:**
- Create: `docs/runbooks/phase-5-drill-b-jobmanager-kill.md`

**Interfaces consumed:** Task 5's running job, Task 6's instrument.

**The concept, and why this Drill is the reason HA exists.** With one JobManager,
killing it means the job is gone: nothing knows the execution graph or which
checkpoint was last completed, until a new JobManager reads it back from
somewhere.

`high-availability.type: kubernetes` provides that somewhere. Two things are
stored, in two different places, and both matter:

- **Leader election** uses Kubernetes ConfigMaps. Exactly one JobManager holds
  the lease at a time.
- **The job's recovery pointer** lives at `high-availability.storageDir`, in
  MinIO under `phase-5-ha`.

So the standby does not start the job over. It acquires the lease, reads the
pointer, and resumes.

**Why this is not the same as Drill A.** Drill A destroyed *state*, and Flink
recovered by replaying from a checkpoint. Drill B destroys the *coordinator*.
The distinction shows up in what you look at: Drill A's evidence is the
checkpoint restore line, and Drill B's evidence is a leadership change.

**The failure mode to watch for.** If the surviving JobManager starts the job
from a fresh state instead of resuming, `phase-5-ha` was empty and Task 5 Step 12
did not really pass. The Drill would still look like a success on the pod listing
and would have proved nothing.

- [ ] **Step 1: Identify the current leader.**

```bash
kubectl get configmap -n personalization-blue | grep -i personalization
kubectl get configmap <the leader configmap> -n personalization-blue \
  -o jsonpath='{.metadata.annotations}'
```

The holder identity is in the lease annotation. Match it to a pod name and write
that name down. Without this step you cannot tell afterwards whether leadership
actually moved.

- [ ] **Step 2: Snapshot before.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-b-before.txt
```

- [ ] **Step 3: Kill the leader specifically.**

```bash
kubectl delete pod <the leader pod> -n personalization-blue
```

Killing the standby instead tests nothing, and it is an easy mistake because
both pods look identical in `kubectl get pods`.

- [ ] **Step 4: Watch leadership move.**

Re-read the lease annotation. Expected: a **different** holder identity, matching
the pod that was previously the standby.

- [ ] **Step 5: Confirm the job resumed rather than restarted from empty.**

In the new leader's log, look for the recovery line naming the HA storage
directory. Expected: it reads the pointer from `s3://checkpoints/phase-5-ha` and
restores from a checkpoint under `phase-5`.

- [ ] **Step 6: Confirm a replacement standby appeared.**

```bash
kubectl get pods -n personalization-blue -l component=jobmanager \
  -o custom-columns='NAME:.metadata.name,NODE:.spec.nodeName'
```

Expected: two JobManagers again, on two different workers. If both land on the
same worker, the `DoNotSchedule` constraint from Task 5 Step 4 is not matching,
and Drill C will then not test what it claims to.

- [ ] **Step 7: Snapshot after, compare, write the runbook.**

Same commands and same standard as Drill A. Zero gaps, zero duplicates, and a
runbook carrying the real transcript, including both lease holder identities.

---

## Task 9: Drill C: drain a Zone

**Files:**
- Create: `docs/runbooks/phase-5-drill-c-zone-drain.md`

**Interfaces consumed:** Task 5's running job and its PDB, Task 6's instrument.

**The concept, and the distinction Drills A and B could not show.**
`kubectl drain` is a **voluntary** disruption. It goes through the eviction API,
and the eviction API consults every PodDisruptionBudget that selects the pod.

That is why the PDB was inert in the last two Drills and matters here. With
`minAvailable: 1` on two JobManagers, the API server permits the first eviction
and **refuses** the second while only one remains. The drain does not fail. It
blocks and retries, which is the behaviour worth watching.

**Why a Zone here is a scheduling fact and not a failure domain.** Per
[CONTEXT.md](../../../CONTEXT.md), the Zone label drives real scheduling
decisions but is not a real failure domain, since every node is a container on
one host. This Drill therefore tests the scheduler and the disruption budget
honestly, and does not claim to test datacentre resilience.

**What each spread constraint should do here.** The drained Zone's JobManager
has nowhere to go, because `DoNotSchedule` with `maxSkew: 1` across the two
remaining Zones is already satisfied by the surviving JobManager. Expect it to
sit **Pending**, and expect that to be correct rather than a fault. The drained
Zone's TaskManager has `ScheduleAnyway`, so it lands on a remaining worker even
though that worker then carries two.

**The failure mode to watch for.** Forgetting to `uncordon` afterwards. The node
stays unschedulable, Phase 6's autoscaling then behaves strangely, and the cause
is three days behind you.

- [ ] **Step 1: Snapshot before, and record the current placement.**

```bash
./scripts/recommendation-snapshot.sh snapshot /tmp/drill-c-before.txt
kubectl get pods -n personalization-blue -o custom-columns=\
'NAME:.metadata.name,NODE:.spec.nodeName,COMPONENT:.metadata.labels.component'
```

Choose the worker that currently carries **both** a JobManager and a TaskManager.
That is the drain with something to observe.

- [ ] **Step 2: Cordon it first, as a separate step.**

```bash
kubectl cordon personalization-lab-worker2
```

Cordon marks it unschedulable but evicts nothing. Doing it separately makes the
next step's output attributable to eviction alone.

- [ ] **Step 3: Drain it, and watch the PDB refuse.**

```bash
kubectl drain personalization-lab-worker2 \
  --ignore-daemonsets --delete-emptydir-data
```

Expected: a message about `Cannot evict pod as it would violate the pod's
disruption budget`, repeated while it retries. **This message is the Drill
succeeding, not failing.** Capture it verbatim. It is the single most important
line in this runbook.

- [ ] **Step 4: Confirm the end state.**

```bash
kubectl get pods -n personalization-blue -o wide
```

Expected: the JobManager from the drained Zone is **Pending**, and
`kubectl describe pod` names the topology spread constraint as the reason. The
TaskManager has moved to a remaining worker. The job is RUNNING throughout, or
returns to RUNNING after a restart from checkpoint.

- [ ] **Step 5: Uncordon and confirm the Pending pod schedules.**

```bash
kubectl uncordon personalization-lab-worker2
kubectl get pods -n personalization-blue -o wide -w
```

Expected: the Pending JobManager schedules onto the restored worker, and the
spread returns to one JobManager per Zone.

- [ ] **Step 6: Snapshot after, compare, write the runbook.**

Zero gaps, zero duplicates. The runbook must include the eviction refusal message
from Step 3 and the scheduling reason from Step 4, both verbatim.

---

## Task 10: Drill D: ArgoCD Lua actions and drift

**Files:**
- Create: `manifests/argocd/flink-actions-values.yaml`
- Modify: `scripts/bootstrap-phase0.sh`, the `install_argocd` function
- Create: `docs/runbooks/phase-5-drill-d-argocd-drift.md`

**Interfaces consumed:** Task 5's running job.

**The concept.** GitOps means Git is the declared truth and a controller
continuously drives the cluster toward it. That is easy to say and hard to
*observe*, because a controller doing its job leaves nothing to look at.

This Drill makes it observable by breaking it on purpose. You change the live
resource so it no longer matches Git, look at the divergence while it exists, and
then let a sync erase it.

**Why `selfHeal: false` is load-bearing.** [ADR 0004](../../adr/0004-gitops-from-phase-0.md)
set it in Phase 0 specifically for this Drill, and every Application since has
carried it. With `selfHeal: true`, ArgoCD reverts the patch within seconds,
you never see the intermediate state, and the Drill silently proves nothing while
appearing to pass.

**Why a Lua action rather than `kubectl patch`.** Both would patch the field. The
Lua action is the ArgoCD-native form, it is what the design spec's coverage map
claims, and it makes the operation available from the UI where the drift is
visible in the same place. Defining it also exercises `argocd-cm` resource
customization, which is a real ArgoCD mechanism this lab otherwise never touches.

**Why a values file rather than another `--set`.** `install_argocd` currently
passes three `--set` flags. Lua is multi-line, so `--set` becomes unreadable
escaping. A `-f manifests/argocd/flink-actions-values.yaml` keeps it legible and
keeps the actions in a tracked file.

**The failure mode to watch for.** The key under `configs.cm` is
`resource.customizations.actions.<group>_<Kind>`. The separator between group and
kind is an **underscore**, and the group contains dots. Get it wrong and nothing
errors: the action simply never appears, because ArgoCD found no customization
for that resource type.

- [ ] **Step 1: Write the values file.**

```yaml
configs:
  cm:
    resource.customizations.actions.flink.apache.org_FlinkDeployment: |
      discovery.lua: |
        actions = {}
        actions["suspend"] = {}
        actions["resume"] = {}
        actions["restart"] = {}
        return actions
      definitions:
      - name: suspend
        action.lua: |
          obj.spec.job.state = "suspended"
          return obj
      - name: resume
        action.lua: |
          obj.spec.job.state = "running"
          return obj
```

Write `restart` yourself. It sets `spec.restartNonce` to a new value, which is
the field the operator watches for a forced restart. Read the CRD if you want to
confirm the field name before writing it.

- [ ] **Step 2: Add the values file to `install_argocd` and re-run it.**

```bash
./scripts/bootstrap-phase0.sh argocd
```

`helm upgrade --install` is already idempotent, which is why the function can be
re-run rather than rewritten.

- [ ] **Step 3: Confirm the actions are discoverable.**

```bash
kubectl get configmap argocd-cm -n argocd \
  -o jsonpath='{.data.resource\.customizations\.actions\.flink\.apache\.org_FlinkDeployment}'
```

Expected: your Lua, verbatim. An empty result means the key is wrong, and Step 4
would then show no actions with no error to explain it.

- [ ] **Step 4: Record the pre-Drill state.**

```bash
kubectl get flinkdeployment personalization -n personalization-blue \
  -o jsonpath='{.spec.job.state}'
argocd app get flink-job-blue
```

Expected: `running`, and the Application `Synced`.

- [ ] **Step 5: Run the `suspend` action.**

From the ArgoCD UI on `localhost:30010`, on the `FlinkDeployment` resource, or:

```bash
argocd app actions run flink-job-blue suspend \
  --kind FlinkDeployment --resource-name personalization
```

- [ ] **Step 6: Observe the drift. This is the step the phase is graded on.**

```bash
kubectl get flinkdeployment personalization -n personalization-blue \
  -o jsonpath='{.spec.job.state}'; echo
argocd app get flink-job-blue
```

Expected: the live value reads **`suspended`**, and the Application reads
**`OutOfSync`**. Git still says `running`.

Capture both outputs now. The phase's done criterion says this intermediate state
must be *observed*, not inferred from the fact that it was later reverted. If you
read this after the sync, you have no evidence and the Drill has to be re-run.

- [ ] **Step 7: Confirm the operator acted on the patch.**

```bash
kubectl get pods -n personalization-blue
```

Expected: the job is suspending or suspended, with a savepoint taken because
`upgradeMode: savepoint` is set. Look for the savepoint under
`s3://checkpoints/phase-5-savepoints`. This is also a free rehearsal of the
Phase 7 Promotion's first step.

- [ ] **Step 8: Sync, and watch it revert.**

```bash
argocd app sync flink-job-blue
```

Expected: `spec.job.state` returns to `running`, the Application returns to
`Synced`, and the job restarts from the savepoint it just took.

- [ ] **Step 9: Write the runbook.**

It must contain the Step 6 outputs verbatim. Everything else in this Drill can be
re-derived. That one cannot.

---

## Task 11: Documents

**Files:**
- Create: `docs/knowledge/phase-5-operator-and-ha.md`
- Modify: `docs/knowledge/README.md`
- Modify: `docs/superpowers/plans/status.md`
- Modify: `CONTEXT.md`
- Modify: `docs/adr/0007-s3-filesystem-plugin.md`

**The point of this task.** Every earlier phase ended here, and Phase 4's Task 9
is why Phase 5 could be designed without rediscovering the network buffer
problem. Facts that only exist in a terminal you have closed are lost.

- [ ] **Step 1: Write the knowledge doc.**

One doc per phase, matching the existing four. The things a reader cannot
reconstruct from the manifests:

- Why `opt/` does not work, stated as "it is on no list" rather than as a rule.
  `bin/config.sh` lines 24 to 32 are the evidence.
- Why the fat jar uses an allowlist, and specifically why an exclusion list fails
  silently while an allowlist fails loudly.
- The one-program-became-three explanation, and why it forces configuration out
  of `main` and into `spec.flinkConfiguration`.
- Why four environment variables carry two values.
- Why Flink cannot pin a NodePort, and why a hand-written Service is therefore
  not a workaround but the only mechanism.
- The difference between Drill A, B, and C in one table: what each destroys, what
  recovers it, and what the evidence looks like.

- [ ] **Step 2: Amend ADR 0007.**

Its "Confirmed working" section records the plugin loading from a Gradle
classpath under `MiniCluster`. Add a dated Phase 5 section recording the second
half: in a container it needs `plugins/s3-fs-hadoop/`, the folder name is
arbitrary because it becomes the plugin id, and the isolation is required because
the bundled `com.amazonaws.*` classes are not relocated.

Also record that `ENABLE_BUILT_IN_PLUGINS` was verified to work in the 2.2.0
entrypoint and was rejected anyway, with the reason: it moves one of the two
required files, so it would mean maintaining a second mechanism for the job jar.
Otherwise someone will re-evaluate this, exactly as the `flink-s3-fs-native`
detour that created the ADR.

- [ ] **Step 3: Add the missing Operations terms to `CONTEXT.md`.**

The phase introduced vocabulary the glossary does not have. At minimum: what a
Drill's "no gap" means here, given that a correct suppression removes about 10%
of Recommendations. Follow the existing entry shape, including the `_Avoid_`
line.

- [ ] **Step 4: Update `status.md`.**

Phase 5 moves to done. Follow the level of detail the Phase 3 and Phase 4
sections set: not "Task 5 done", but what was learned, what was verified rather
than assumed, and what a later phase would otherwise rediscover. Carry forward at
least these, which Phase 6 and Phase 7 both need:

- Parallelism is 6 with 2 slots, giving 3 TaskManagers. Phase 6's autoscaler
  changes parallelism, and the `256mb` network buffer setting is the headroom it
  must stay inside.
- `upgradeMode: savepoint` and both namespaces exist. Phase 7's Promotion depends
  on both.
- The chart ships a `FlinkBlueGreenDeployment` CRD. Phase 7 must evaluate it
  against [ADR 0006](../../adr/0006-blue-green-native-mode.md) rather than assume
  the hand-rolled Promotion is the only option.
- The image tag must move on every code change. A stale tag with new code is the
  failure that looks like a Flink bug.

- [ ] **Step 5: Add the four runbooks to `docs/knowledge/README.md`.**

Alongside the existing index entries, so the Drills are findable without knowing
their filenames.
