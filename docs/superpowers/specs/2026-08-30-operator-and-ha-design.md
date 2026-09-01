# Phase 5 design: Operator and HA

Date: 2026-08-30
Status: approved, ready for an implementation plan

The first real deployment. The Phase 4 job leaves `MiniCluster` and runs on the
`kind` cluster, managed by the Flink Kubernetes Operator, with High Availability,
Zone spread, and four Drills.

This document settles **every** design decision. The implementation plan that
follows it contains implementation tasks only. Nothing is left for a task to
resolve.

---

## 1. What this phase closes

[ADR 0001](../../adr/0001-minicluster-first-dev-loop.md) deferred two costs to
this phase on purpose:

1. The S3 filesystem plugin, which `MiniCluster` accepted as an ordinary
   classpath entry and a container will not.
2. HA and JobManager failover, which one embedded JobManager could never
   exercise.

Both are closed here. A third cost, unrecorded until now, is also closed: the
Kafka cluster has no listener a pod inside the cluster can use.

## 2. Facts verified before this design was written

Every value below was read from the real artifact on 2026-08-30, not recalled.
The command is given so each can be repeated.

| Fact | Evidence |
|---|---|
| `flink-s3-fs-hadoop-2.2.0.jar` is at `/opt/flink/opt/`, with 12 other optional items | `docker run --rm --entrypoint sh flink:2.2.0 -c 'ls /opt/flink/opt'` |
| `/opt/flink/lib/` holds 14 jars, including `flink-cep-2.2.0.jar` | same, on `lib` |
| `/opt/flink/plugins/` already holds 9 plugins, all metrics reporters, one jar each | same, on `plugins` |
| Flink's classpath is built by `find` over `lib/` only. Nothing reads `opt/` | `/opt/flink/bin/config.sh`, lines 24 to 32 |
| The image entrypoint honours `ENABLE_BUILT_IN_PLUGINS` and creates a **symlink** from `opt/` into a new `plugins/<name>/` folder | `/docker-entrypoint.sh`, lines 35 to 52 |
| The image working directory is `/opt/flink`, and the user is `flink` | `docker inspect flink:2.2.0` |
| `/opt/flink/conf/config.yaml` exists with stock defaults, has `taskmanager.memory.process.size: 1728m`, and has **no** `state.backend.type` | `docker run ... cat /opt/flink/conf/config.yaml` |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` is **third** in the default value of `fs.s3a.aws.credentials.provider` | `core-default.xml` inside `flink-s3-fs-hadoop-2.2.0.jar` |
| That provider reads `AWS_ACCESS_KEY_ID`, `AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`, `AWS_SECRET_KEY` | `javap -c` on the class inside the jar |
| The AWS SDK classes in that jar are **not** relocated. They are plain `com.amazonaws.*` | `unzip -l` on the jar |
| Flink Kubernetes Operator **1.15.0** is the latest release | `https://downloads.apache.org/flink/` |
| Its `FlinkDeployment` CRD `flinkVersion` enum contains **`v2_2`** | `crds/flinkdeployments.flink.apache.org-v1.yml` in the 1.15.0 Helm chart |
| Its `mode` enum is exactly `native` and `standalone` | same CRD |
| `spec.jobManager.replicas` exists and is an integer | same CRD |
| The chart **requires cert-manager**. It ships `Certificate` and `Issuer` templates, and annotates both webhook configurations with `cert-manager.io/inject-ca-from` | `templates/cert-manager/`, `templates/webhook/` |
| Setting `watchNamespaces` makes the chart create the `flink` ServiceAccount, Role, and RoleBinding **in each listed namespace** | `templates/flink/service_account.yaml`, `role.yaml`, `role_binding.yaml` |
| Flink 2.2 config keys are `high-availability.type`, `high-availability.storageDir`, `jobmanager.scheduler`, `execution.checkpointing.savepoint-dir` | `javap -c` on `flink-dist-2.2.0.jar` |
| `JobManagerOptions$SchedulerType` values are `Default`, `Adaptive`, `AdaptiveBatch` | same |
| **There is no config key that pins the REST NodePort number.** `KubernetesConfigOptions` has `kubernetes.rest-service.exposed.type` and `.exposed.node-port-address-type`, and nothing else | same |
| The Kafka bootstrap Service exposes only `tcp-replication 9091` | `kubectl get svc personalization-kafka-bootstrap -n kafka` |
| NodePort `30011` is already reserved for Flink in the cluster definition | `clusters/kind/`, Phase 0 `extraPortMappings` |
| Gradle is 9.7.0. The maintained Shadow plugin is `com.gradleup.shadow`, latest 9.6.1 | wrapper properties, Maven Central metadata |

Two of these changed the design, and both are called out where they land:
the absent NodePort key in section 6, and the absent Kafka listener in section 5.

## 3. The mechanism this phase is really about

State the failure first. The job writes a checkpoint to `s3://checkpoints/...`
every 10 seconds. On the cluster that write throws
`UnsupportedFileSystemSchemeException`, and the same code succeeded under
`MiniCluster` an hour earlier.

The reason is a list.

Flink resolves a URL scheme to an implementation with `ServiceLoader`. It asks
the classloader for every resource named
`META-INF/services/org.apache.flink.core.fs.FileSystemFactory`, reads the class
names inside, and builds a map from scheme to factory. Which entries the map
gets depends only on which jars the classloader can reach.

- Under `MiniCluster`, Gradle puts `flink-s3-fs-hadoop` on the classpath, because
  of `apps/pipeline/build.gradle` line 88. The map gets an `s3` entry.
- In the container, that jar is at `/opt/flink/opt/`. `bin/config.sh` builds the
  classpath with `find` over `lib/` alone. Nothing reads `opt/`. The map gets no
  `s3` entry.

`opt/` is not protected or special. It is simply on no list.

Flink runs the same scan a second time, once per subfolder of
`/opt/flink/plugins/`, each with its own isolated classloader. The isolation is
required here, not decorative: this jar carries `com.amazonaws.*` classes that
are **not** relocated, and `opt/` holds five filesystem jars that would otherwise
fight over the same class names. `flink-s3-fs-hadoop` and `flink-s3-fs-presto`
both register the scheme `s3`.

The plugin folder **name** is arbitrary. The image's own
`plugins/README.txt` states that the folder name becomes the plugin id. What is
required is a dedicated subfolder, because the subfolder is the classloader
boundary.

Docs:
[filesystem plugins](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/filesystems/plugins/),
[S3 filesystems](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/filesystems/s3/).

## 4. The build chain

```
apps/pipeline
    |
    |  shadowJar, bundling an allowlist only
    v
pipeline-all.jar
    |
    |  Dockerfile, FROM flink:2.2.0
    |    COPY pipeline-all.jar -> /opt/flink/usrlib/pipeline.jar
    |    RUN  mkdir -p /opt/flink/plugins/s3-fs-hadoop
    |         && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.0.jar
    |               /opt/flink/plugins/s3-fs-hadoop/
    v
lab/personalization-pipeline:0.1-<git short sha>
    |
    |  kind load docker-image
    v
every kind node's image store
    |
    |  FlinkDeployment spec.image
    v
JobManager and TaskManager containers
```

### 4.1 Decisions

| Decision | Value | Reason |
|---|---|---|
| Fat jar plugin | `com.gradleup.shadow` version `9.6.1` | `com.github.johnrengelman.shadow` is unmaintained and does not support Gradle 9. [Docs](https://gradleup.com/shadow/) |
| What is bundled | An **allowlist** configuration, holding `:domain` and `flink-connector-kafka` only | See 4.2 |
| Image base | `flink:2.2.0` | Same version the whole project has used since Phase 3 Task 0 |
| Plugin delivery | `RUN cp` from `opt/` at build time, **not** `ENABLE_BUILT_IN_PLUGINS` | See 4.3. A `RUN cp` rather than a `COPY`, because the source file is already inside the base image and a `COPY` reads from the build context |
| Job jar path | `/opt/flink/usrlib/pipeline.jar` | The user code directory in Application mode |
| `spec.job.jarURI` | `local:///opt/flink/usrlib/pipeline.jar` | The operator does not fetch or validate a `FlinkDeployment` `jarURI`. It expects the jar in the image |
| Image tag | `lab/personalization-pipeline:0.1-<git short sha>` | Immutable per build. A moving tag makes ArgoCD drift meaningless and makes a rollback ambiguous |
| `spec.imagePullPolicy` | `IfNotPresent`, stated explicitly | The image exists only in the nodes' image stores. A pull attempt reaches no registry and fails. [kind image loading](https://kind.sigs.k8s.io/docs/user/quick-start/) |

### 4.2 Why an allowlist, not an exclusion list

Phase 4 Task 0 recorded that `runtimeOnly` does not keep a jar out of a Shadow
build, because Shadow builds from the runtime classpath. Five dependencies carry
that scope today.

The obvious repair is to exclude those five in the `shadowJar` task. That repair
is wrong. Excluding `flink-streaming-java` does not exclude its transitive
`flink-core`, `flink-runtime`, and `flink-shaded-*` artifacts. Those would still
enter the jar and duplicate what `lib/` already loads.

An exclusion list fails **silently**, as a duplicate class that wins a scan
order. An allowlist cannot fail silently. Anything left out of it is a
`NoClassDefFoundError` at startup.

The allowlist holds exactly two things:

| Entry | Why it must be bundled |
|---|---|
| `project(':domain')` | Your code. Nothing else supplies it |
| `org.apache.flink:flink-connector-kafka:5.0.0-2.2` and its transitives | Not in the image's `lib/`. Confirmed by the `lib/` listing in section 2 |

Everything else is deliberately absent, including these, each for a stated
reason:

| Not bundled | Reason |
|---|---|
| `flink-streaming-java`, `flink-clients`, `flink-connector-base` | Inside `flink-dist-2.2.0.jar` in `lib/` |
| `flink-cep` | `lib/flink-cep-2.2.0.jar`, confirmed in Phase 4 Task 0 |
| `flink-statebackend-rocksdb` | Provided by the distribution |
| `flink-s3-fs-hadoop` | Goes to `plugins/`, and must not also be on the main classpath |
| `log4j-*` | `lib/` ships 2.24.3. The build declares 2.26.1. Two logging backends on one classpath is a startup warning at best and a silent binding swap at worst |

`apps/pipeline/conf/config.yaml` and the `runtimeOnly` declarations stay exactly
as they are, because `:pipeline:run` against `MiniCluster` must keep working
unchanged through Phase 6 and Phase 7.

### 4.3 Why build time, and not the entrypoint variable

`ENABLE_BUILT_IN_PLUGINS` is real and it works. It was verified, not assumed.
It is still the weaker option here, for one reason: it moves **one** of the two
required files. The job jar is not in the image at all, and the entrypoint has no
feature that puts it there. Choosing the variable means maintaining a second,
different mechanism for the second file.

The build time choice puts both files in one artifact, and the result is
checkable before any pod starts:

```bash
docker run --rm --entrypoint sh lab/personalization-pipeline:<tag> \
  -c 'ls /opt/flink/plugins/s3-fs-hadoop/ /opt/flink/usrlib/'
```

The accepted cost is a `docker build` plus a `kind load docker-image` per code
change, roughly one to two minutes. [ADR 0001](../../adr/0001-minicluster-first-dev-loop.md)
already accepted this cost for Phase 5 onward, and the job graph froze when
Phase 4 closed on 2026-08-30.

## 5. Cluster prerequisites this phase adds

### 5.1 A Kafka internal listener

This is a blocker, found by inspection rather than predicted.

`manifests/strimzi/kafka-cluster.yaml` declares one listener, `external`, type
`nodeport`, port 9094. That is [ADR 0002](../../adr/0002-strimzi-external-listener.md)
and it was correct for Phases 2 to 4, where every client ran outside the cluster.

The in-cluster Service `personalization-kafka-bootstrap` therefore exposes only
`tcp-replication 9091`, which is Strimzi's own replication listener and is not
for clients. A Flink pod has nothing to connect to.

Phase 5 adds a second listener:

```yaml
- name: plain
  port: 9092
  type: internal
  tls: false
```

`tls: false` for the same lab scale reason the `external` listener already gives
in its own comment. The `external` listener is untouched, because the generator
and every `kcat` check still use it.
[Listener reference](https://strimzi.io/docs/operators/latest/configuring).

### 5.2 Two namespaces, created now

`personalization-blue` and `personalization-green` are both created in this
phase, and both are listed in the operator's `watchNamespaces`.

Only `personalization-blue` gets a `FlinkDeployment` in Phase 5. Green stays
empty. The reason to create it now is RBAC: the chart creates the `flink`
ServiceAccount, Role, and RoleBinding once per watched namespace, so listing both
now means Phase 7 does not have to re-sync the operator to gain permissions on
the Standby Side.

Per [CONTEXT.md](../../../CONTEXT.md), `blue` and `green` are namespace names and
not roles. Phase 5 makes blue the Active Side, and that is a starting position,
not a property of the name.

### 5.3 The credentials Secret

The MinIO root credentials already exist in the cluster, in the Secret
`storage-configuration` in namespace `minio-tenant`, created out of band by
`scripts/bootstrap-minio-secret.sh`. Secrets are namespaced, so the Flink pods
cannot read that one.

A script creates `minio-credentials` in `personalization-blue` and
`personalization-green`, reading the values from the existing Secret. It follows
the two rules the existing scripts already follow: nothing is written to a
tracked file, and a re-run against an existing Secret does nothing rather than
rotating a password under a running job.

The `FlinkDeployment` in Git contains only a reference to that Secret by name.
The reference names the box. It does not carry what is in the box.

## 6. Configuration and credentials at run time

### 6.1 One program became three

Under `MiniCluster` there is one JVM. `PersonalizationJob.flinkConfiguration`
reads the file, adds the credentials from the environment, and calls
`FileSystem.initialize`. One process, one configuration, done.

On the cluster there are several JVMs:

```
JobManager  x2   <- main() runs here, on the leader only
TaskManager x3   <- main() never runs here
```

The TaskManagers hold the RocksDB state and write the checkpoint data to MinIO.
They need the endpoint and the credentials. Nothing `main()` does can reach them,
because they never run it.

So the configuration must come from outside the job, from something that
configures every pod. That is `spec.flinkConfiguration`, which the operator
renders into `/opt/flink/conf/config.yaml` in every pod.

### 6.2 No Java changes

The image working directory is `/opt/flink`.
`PipelineConfig` defaults `flinkConfDir` to the relative path `conf`. So
`GlobalConfiguration.loadConfiguration("conf")` resolves to `/opt/flink/conf`,
which is the file the operator wrote. The guard at
`PersonalizationJob.java:187` still does its job: if `state.backend.type` is
absent, the job throws instead of running silently on defaults.

No source file in `apps/` changes in this phase. Only `build.gradle` changes, and
only to add the Shadow plugin and the allowlist configuration.

### 6.3 `spec.flinkConfiguration`

Values carried across from `apps/pipeline/conf/config.yaml` unchanged:

| Key | Value | Doc |
|---|---|---|
| `state.backend.type` | `rocksdb` | [state backends](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/state_backends/) |
| `execution.checkpointing.interval` | `10s` | [config](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/) |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | same |
| `execution.checkpointing.incremental` | `true` | same |
| `execution.checkpointing.num-retained` | `3` | same |
| `execution.checkpointing.externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` | same |
| `pipeline.generic-types` | `false` | same |
| `taskmanager.memory.process.size` | `2gb` | same |
| `taskmanager.memory.network.min` | `256mb` | same |
| `taskmanager.memory.network.max` | `256mb` | same |
| `s3.path.style.access` | `true` | [S3](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/filesystems/s3/) |

The three memory values are not optional. Phase 4 Task 4 hit
`Insufficient number of network buffers: required 17, but only 0 available`
against the default 2048 buffers, because the Phase 4 graph has five `keyBy`
shuffles. A TaskManager container sized below 2gb reaches the same wall.

Values that differ from the local file:

| Key | Local | Cluster | Why |
|---|---|---|---|
| `s3.endpoint` | `http://localhost:30014` | `http://minio.minio-tenant.svc.cluster.local` | Inside the cluster, `localhost` is the pod |
| `execution.checkpointing.dir` | `s3://checkpoints/phase-3` | `s3://checkpoints/phase-5` | A separate prefix, so a Phase 5 restore cannot pick up a `MiniCluster` checkpoint |

The Kafka address also differs between local and cluster, but it is not a Flink
configuration key. It is a job argument, and 7.2 covers it.

Values new in this phase:

| Key | Value | Doc |
|---|---|---|
| `high-availability.type` | `kubernetes` | [Kubernetes HA](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/ha/kubernetes_ha/) |
| `high-availability.storageDir` | `s3://checkpoints/phase-5-ha` | same |
| `execution.checkpointing.savepoint-dir` | `s3://checkpoints/phase-5-savepoints` | required by `upgradeMode: savepoint` |
| `jobmanager.scheduler` | `Adaptive` | [config](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/) |
| `taskmanager.numberOfTaskSlots` | `2` | same |
| `kubernetes.rest-service.exposed.type` | `ClusterIP` | see 6.5 |

`s3.access-key` and `s3.secret-key` are **absent on purpose**. They are the two
values that must never enter Git.

### 6.4 Credentials, and why four environment variables

The Secret from 5.3 is projected into the `flink-main-container` of both the
JobManager and the TaskManager pods, through the shared `spec.podTemplate`.
Four variables, from two Secret keys:

| Variable | Read by | Needed on |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | `EnvironmentVariableCredentialsProvider`, third in the default S3A chain | JobManager and TaskManagers |
| `AWS_SECRET_ACCESS_KEY` | same | JobManager and TaskManagers |
| `MINIO_ACCESS_KEY` | `Env.require` at `PersonalizationJob.java:191` | JobManager, where `main()` runs |
| `MINIO_SECRET_KEY` | `Env.require` at `PersonalizationJob.java:192` | same |

The `AWS_*` pair is what makes the TaskManagers able to write checkpoint data,
since they never run `main()`. The chain reaches that provider because
`s3.access-key` is not set, so `SimpleAWSCredentialsProvider` finds nothing and
falls through.

The `MINIO_*` pair exists only because `Env.require` throws on a blank value.
Setting both pairs is redundant by two variables. That is judged cheaper than
editing working, tested Java to suit a deployment target, and it keeps the
`MiniCluster` and cluster code paths identical.
[Hadoop S3A credential providers](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html).

### 6.5 Reaching the Flink UI

Flink can expose its REST service as a NodePort, but **there is no configuration
key that pins the port number**. Only `kubernetes.rest-service.exposed.type` and
`kubernetes.rest-service.exposed.node-port-address-type` exist. A random port in
the 30000 range is useless here, because `clusters/kind/` maps a fixed host port.

So `kubernetes.rest-service.exposed.type` stays `ClusterIP`, and a hand written
Service of type `NodePort` with `nodePort: 30011` selects the JobManager pods.
`30011` was reserved for Flink in Phase 0 and has been unused since.

This is the same pattern `manifests/minio/s3-nodeport.yaml` already uses for
MinIO on 30014. A useful consequence: `rbac.nodesRule.create` in the operator
chart can stay `false`, since Flink itself never creates a NodePort Service.

## 7. The FlinkDeployment

### 7.1 Settings

| Field | Value | Reason |
|---|---|---|
| `spec.flinkVersion` | `v2_2` | Present in the 1.15.0 CRD enum |
| `spec.mode` | `native` | The Native Variant. [ADR 0005](../../adr/0005-autoscaling-two-deployment-modes.md), [ADR 0006](../../adr/0006-blue-green-native-mode.md) |
| `spec.serviceAccount` | `flink` | Created by the chart in each watched namespace |
| `spec.jobManager.replicas` | `2` | One leader, one standby |
| `spec.jobManager.resource.memory` | `2048m` | Above the stock 1600m, with room for HA metadata handling |
| `spec.taskManager.resource.memory` | `2048m` | Must match `taskmanager.memory.process.size: 2gb` |
| `spec.job.parallelism` | `6` | With 2 slots per TaskManager this gives exactly 3 TaskManagers, one per Zone |
| `spec.job.upgradeMode` | `savepoint` | Required by the Phase 7 Promotion, which suspends with a savepoint |
| `spec.job.state` | `running` | The field the drift Drill patches |
| `spec.job.args` | `["--bootstrap-servers=personalization-kafka-bootstrap.kafka.svc.cluster.local:9092"]` | See 7.2 |

### 7.2 Job arguments

`PipelineConfig.parse` defaults `bootstrapServers` to `localhost:30016`, which is
the external NodePort. Inside a pod that address reaches nothing, so this one
argument must be passed.

Every other default in `PipelineConfig` is already correct for the cluster and is
deliberately **not** passed:

| Default | Value | Correct on the cluster because |
|---|---|---|
| `consumerGroup` | `personalization-phase-3` | The same consumer group is wanted, so the job resumes where the local runs left off |
| `inputTopic`, `productChangeTopic`, `promoRuleTopic`, `outputTopic` | topic names | Topic names are cluster wide, not endpoint dependent |
| `flinkConfDir` | `conf` | Resolves to `/opt/flink/conf` from the `/opt/flink` working directory, which is the file the operator wrote |
| every `Duration` and the catalogue | Phase 3 and Phase 4 values | The job's semantics must not change when it changes host |

Passing only what must change keeps the difference between a local run and a
cluster run down to one visible line.

[CRD reference](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/custom-resource/reference/),
[job management](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/custom-resource/job-management/).

Parallelism 6 is a deliberate reduction from the 16 that `MiniCluster` defaulted
to. Three TaskManagers, one per Zone, make the Zone drain Drill legible. Six
parallel subtasks across five shuffles also sit far below the 8192 network
buffers that the 256mb setting provides, so Phase 4's failure cannot repeat at
this size.

### 7.3 Zone spread

Two constraints, and they are placed in `spec.jobManager.podTemplate` and
`spec.taskManager.podTemplate` **separately**, never in the shared
`spec.podTemplate`.

A single constraint in the shared template would treat JobManagers and
TaskManagers as one population and spread them against each other, which is not
what either needs.

| Pods | `maxSkew` | `whenUnsatisfiable` | Reason |
|---|---|---|---|
| JobManager | 1 | `DoNotSchedule` | Two JobManagers in one Zone is HA theatre. When a Zone is drained, the second JobManager going Pending is the correct and visible outcome, and the surviving one keeps leadership |
| TaskManager | 1 | `ScheduleAnyway` | Recovery is the Drill's success criterion. `DoNotSchedule` would leave a subtask unschedulable and block the recovery the Drill exists to observe |

`topologyKey` is `topology.kubernetes.io/zone` for both.
[Topology spread constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/).
Per [CONTEXT.md](../../../CONTEXT.md), a Zone here is a node label. It drives real
scheduling decisions and is not a real failure domain.

### 7.4 PodDisruptionBudget

The operator does not create one. Phase 5 adds a hand written
`PodDisruptionBudget` with `minAvailable: 1`, selecting the JobManager pods of
the blue `FlinkDeployment`.

It has no effect on a `kubectl delete pod`, which is an involuntary disruption.
It has effect on `kubectl drain`, which is Drill C.
[PodDisruptionBudget](https://kubernetes.io/docs/tasks/run-application/configure-pdb/).

## 8. GitOps wiring

Two ArgoCD Applications, following the operator plus resource pairing that
`manifests/argocd-apps/` already uses for Strimzi and MinIO, per
[ADR 0004](../../adr/0004-gitops-from-phase-0.md):

| Application | Source | Contents |
|---|---|---|
| `flink-operator.yaml` | Helm chart `flink-kubernetes-operator` 1.15.0 | `watchNamespaces: [personalization-blue, personalization-green]`, `rbac.nodesRule.create: false` |
| `flink-job-blue.yaml` | `manifests/flink/blue/` | The `FlinkDeployment`, the `PodDisruptionBudget`, the NodePort Service |

The operator depends on cert-manager, which Phase 0 installed at `v1.21.1`. The
dependency is real and was verified in the chart, not assumed from the design
spec. Sync waves stay deferred, as they have been since Phase 0, because
cert-manager is already Healthy long before this Application is created.

`automated.selfHeal` stays `false`, as set in Phase 0. Drill D depends on it. A
self-healing Application would revert the patch before it could be observed, and
the Drill would silently prove nothing.

The three ArgoCD custom Lua actions (`suspend`, `resume`, `restart`) for
`flink.apache.org/FlinkDeployment` are defined in `argocd-cm` in this phase.
`suspend` is the action Drill D uses.

## 9. Drills

Each Drill is a deliberate, repeatable act of breaking something to observe
recovery, per [CONTEXT.md](../../../CONTEXT.md). Each gets a runbook in
`docs/runbooks/` carrying the real transcript, following the pattern Phase 4
Drill C set.

| Drill | Action | What must be observed |
|---|---|---|
| A | Delete a TaskManager pod | The job restarts from the last checkpoint. A replacement TaskManager is scheduled and the job returns to RUNNING |
| B | Delete the leader JobManager pod | The standby acquires leadership. The job resumes from HA metadata in MinIO, and does **not** start from an empty state |
| C | Cordon and drain a Zone's worker node | The PodDisruptionBudget blocks the second JobManager eviction until the first is rescheduled. TaskManagers move to the remaining Zones |
| D | Run the ArgoCD `suspend` Lua action | `spec.job.state` reads `suspended` on the live resource. A manual sync reverts it to `running`. **The intermediate state must be seen**, not inferred from the end state |

### 9.1 How "no gap" is checked, and a trap in it

The phase plan's done criterion is "no gap in the recommendation topic". Stated
plainly, that check is easy to fail for the wrong reason.

Phase 4 Task 1 introduced an out of stock suppression rule. A `Recommendation`
whose Product has `stock` of zero is not published. The generator emits
`stock == 0` at a measured 9.6%. Phase 4 Task 4 added a second suppression: a
candidate with no trigger goes to `UNMATCHED` and is not published either, which
removes roughly a further fifth of the volume.

So the `recommendation` topic legitimately holds fewer records than there are
closed Browsing Sessions. Comparing the two would make correct suppression read
as a Drill failure.

The check therefore compares the topic against itself, across the Drill:

1. Before the Drill, read the `recommendation` topic and record the set of
   `sessionId` values.
2. Run the Drill.
3. After recovery, read the topic again from the beginning.
4. **No `sessionId` present in step 1 may be missing in step 3.** That is the
   gap check.
5. **No `sessionId` may appear twice.** That is the exactly once check, which the
   Phase 3 Task 8 `KafkaSink` already provides and which a restore must not
   break.

A pause in output during the restart is expected and is not a gap. A lost
`sessionId` is a gap.

## 10. Done when

- All four Drills recover, each with the section 9.1 check passing.
- Drill D's intermediate `suspended` state was actually observed and recorded in
  its runbook, not inferred.
- The image was verified to contain both files before the first deploy.
- The `recommendation` topic is being produced from the cluster, with the
  generator still running on the host against the external listener.

## 11. Out of scope, and why

| Left out | Reason |
|---|---|
| Sync wave annotations | Still nothing needs ordering. Deferred since Phase 0 for the same reason |
| The Standalone Variant | It exists only to demonstrate KEDA with reactive scaling. Phase 6, per [ADR 0005](../../adr/0005-autoscaling-two-deployment-modes.md) |
| A `FlinkDeployment` on the Standby Side | Phase 7. Green is created empty here only so its RBAC exists |
| The chart's `FlinkBlueGreenDeployment` CRD | Noted, not used. Phase 7 evaluates it against [ADR 0006](../../adr/0006-blue-green-native-mode.md) |
| Metrics, Prometheus, OTel | Phase 7 |
| Autoscaling of any kind | Phase 6 |
| Changing the external Kafka listener | The generator and every `kcat` check use it. The internal listener is added beside it |

## 12. Consequences worth recording now

- **`apps/pipeline/conf/config.yaml` and `spec.flinkConfiguration` are now two
  lists that must not drift.** They differ in exactly two values, listed in 6.3.
  A third difference appearing without a stated reason is a defect. The Kafka
  address is a third local versus cluster difference, but it is not in either
  list. It is a job argument, and 7.2 is where it lives.
- **The image tag must move on every code change.** A stale tag with new code is
  the failure mode that looks like a Flink bug and is not.
- **Phase 6 inherits parallelism 6 and 2 slots.** The Job Autoscaler changes
  parallelism deliberately, so the network buffer headroom stated in 7.1 is the
  number it has to stay inside.
- **Phase 7 inherits `upgradeMode: savepoint` and both namespaces.** The
  Promotion runbook depends on the first, and on green's RBAC existing.
