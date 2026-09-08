# ADR 0007: `flink-s3-fs-hadoop`, because `flink-s3-fs-native` is not published

Date: 2026-08-24
Status: accepted

## Context

Phase 3 Task 7 writes RocksDB checkpoints to the MinIO `Tenant` from Phase 1.
Flink does not implement an S3 client of its own. It wraps one, and Flink 2.x
ships three wrappers:

| Plugin | Underlying client | Schemes |
|---|---|---|
| `flink-s3-fs-native` | AWS SDK v2 | `s3://`, `s3a://` |
| `flink-s3-fs-presto` | Presto's S3 client | `s3://`, `s3p://` |
| `flink-s3-fs-hadoop` | Hadoop `S3AFileSystem` | `s3://`, `s3a://` |

The plan originally specified `flink-s3-fs-hadoop`, written before Task 0
confirmed Flink 2.2. On 2026-08-24 that choice was challenged as a pre-2.x
carry-over, which was a fair reading: "hadoop" in the artifact name is the
code's ancestry, not a runtime dependency on a Hadoop cluster, but the native
plugin is genuinely the newer and smaller surface, and its documentation has a
section for S3-compatible endpoints that names MinIO's exact problems.

The design was switched to `flink-s3-fs-native`, and then reverted. This ADR
records why, so nobody repeats the evaluation.

## Decision

Use **`flink-s3-fs-hadoop`**, declared `runtimeOnly` at `${flinkVersion}`.

## The evidence that settled it

`flink-s3-fs-native` **is not published to Maven Central at all.** Not at 2.2.0,
not at any version. Maven Central's coordinate search returns nothing for
`org.apache.flink:flink-s3-fs-native`, while `flink-s3-fs-hadoop` returns
`2.2.0` and `2.2.1` among others.

The failure at build time:

```
Could not resolve all files for configuration ':pipeline:runtimeClasspath'.
> Could not find org.apache.flink:flink-s3-fs-native:2.2.0.
```

The module exists in the Flink source tree at
`flink-filesystems/flink-s3-fs-native`, and its own README describes installing
it by copying a jar into `$FLINK_HOME/plugins/s3-fs-native/`. That is the whole
distribution channel. It ships in a Flink distribution's `opt/` directory and is
not a Maven artifact.

That disqualifies it for Phase 3 outright, because `MiniCluster` runs from a
Gradle classpath and there is no distribution to copy a jar out of. It would
also complicate Phase 5, where the jar would have to be baked into a custom
image rather than declared.

The documentation gave no hint of this. The gap was only visible from the
dependency resolution error, which is the general lesson worth keeping: a module
existing in a project's source tree does not mean it is consumable as a
dependency.

## What the reverted choice cost, and what it bought

Cost: one failed build and a round of doc edits in the spec and the plan.

Bought, and this is why the detour was not wasted:

- **The config key spellings are now verified rather than assumed.** `javap -c`
  on `org/apache/flink/fs/s3hadoop/S3FileSystemFactory.class`, from the real
  `flink-s3-fs-hadoop-2.2.0.jar`, shows the factory mirrors **both** spellings
  of every key: `fs.s3a.path-style-access` and `fs.s3a.path.style.access`,
  `fs.s3a.access-key` and `fs.s3a.access.key`, `fs.s3a.secret-key` and
  `fs.s3a.secret.key`. The prefixes it accepts and rewrites are `s3.`, `s3a.`
  and `fs.s3a.`. So `s3.path-style-access` and `s3.path.style.access` are
  equally correct, and the earlier worry that the spec had the wrong one was
  unfounded.
- **Both schemes are registered.** `META-INF/services/org.apache.flink.core.fs.FileSystemFactory`
  in that jar lists `S3FileSystemFactory` (scheme `s3`) and `S3AFileSystemFactory`
  (scheme `s3a`). `s3://checkpoints/phase-3` works, so the scheme change made
  during the detour did not have to be reverted.
- **`s3.region` and `s3.checksum-validation.enabled` were dropped.** Both are
  AWS SDK v2 concerns specific to the native plugin. Carrying them into a Hadoop
  configuration would have been two keys that silently do nothing.

## Rejected alternatives

**`flink-s3-fs-native` fetched by hand.** Download the jar from a Flink
distribution and commit it, or add a Gradle task to extract it. Rejected: it
puts a binary in the repo or adds build machinery, to gain a client nobody has
documented against MinIO, for a workload that only writes checkpoints.

**`flink-s3-fs-presto`.** Historically the recommended plugin for checkpointing,
faster than the Hadoop one for that access pattern. Not evaluated in depth
because the Hadoop plugin was already working-set knowledge and Phase 3's gate
is objects appearing in a bucket, not throughput. Worth revisiting only if
checkpoint duration becomes a Phase 6 problem. Note that the Hadoop plugin is
the only one documented as supporting `FileSink` through `RecoverableWriter`,
which matters if a later phase writes files rather than only checkpoints.

## Confirmed working, 2026-08-24

The plugin registers from a plain Gradle `runtimeOnly` classpath entry under
`MiniCluster`, despite the plugins page's warning about `lib/` placement in a
distribution. Checkpoint objects land in the bucket and `chk-N` increments,
verified with a signed `ListObjectsV2` request rather than from the absence of
errors.

One separate requirement, unrelated to plugin choice, is recorded in the spec:
`FileSystem.initialize(flinkConfig, null)` must be called, because the filesystem
registry is a process-wide static that the job's `Configuration` does not reach.

## Confirmed working in a container, 2026-09-07

Phase 5 recorded the second half of the story. Under `MiniCluster` the plugin
registers from a plain Gradle `runtimeOnly` classpath entry. **In the image it
does not, and cannot.**

`apps/pipeline/Dockerfile` moves the jar out of `opt/`:

```dockerfile
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.0.jar /opt/flink/plugins/s3-fs-hadoop/
```

Three facts settled by this, all verified against `flink:2.2.0`:

**`opt/` is on no list.** `bin/config.sh` builds the classpath from
`$FLINK_LIB_DIR` alone:

```bash
done < <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)
```

The plugin manager separately scans `/opt/flink/plugins`. Nothing scans
`/opt/flink/opt`. It is a shipping crate, so a jar left there produces no error
and no effect.

**The folder name is arbitrary**, because the plugin manager takes each
subdirectory name as that plugin's id. The JobManager log confirms it:
`Plugin loader with ID not found, creating it: s3-fs-hadoop`. What is not
optional is the subdirectory itself. Each one gets its own classloader, and that
isolation is required here because the shaded `flink-s3-fs-hadoop` jar carries
`com.amazonaws.*` classes that are **not** relocated.

**A `null` PluginManager throws the plugin away.** The note below about
`FileSystem.initialize(flinkConfig, null)` was correct under `MiniCluster` and
wrong in a container. With the jar reachable only through `plugins/`, passing
`null` rebuilds the filesystem registry loading no plugins, discarding the s3
filesystem the entrypoint had already registered. The JobMaster then dies with:

```
UnsupportedFileSystemSchemeException: Could not find a file system
implementation for scheme 's3'
```

Fixed on 2026-09-07 by passing a real one:

```java
FileSystem.initialize(flinkConfig, PluginUtils.createPluginManagerFromRootFolder(flinkConfig));
```

Harmless locally, where there is no `plugins/` directory and the `runtimeOnly`
classpath copy is used exactly as before.

## `ENABLE_BUILT_IN_PLUGINS` was verified to work, and rejected anyway

The 2.2.0 entrypoint supports it:

```bash
# /docker-entrypoint.sh
35:  if [ -z "$ENABLE_BUILT_IN_PLUGINS" ]; then
40:  for target_plugin in $(echo "$ENABLE_BUILT_IN_PLUGINS" | tr ';' ' '); do
```

It moves a named jar out of `opt/` into `plugins/` at container start, so it
would replace the `RUN mkdir && cp` above.

**Rejected because it handles one of the two files this image needs.** The job jar
must be `COPY`ed in regardless. Using the environment variable would mean two
different mechanisms for two files that arrive together, in exchange for removing
two lines. One `RUN` and one `COPY` side by side in one file is easier to read
than a Dockerfile plus an environment variable set somewhere else.

Recorded explicitly so this is not re-evaluated, exactly as the
`flink-s3-fs-native` detour that produced this ADR.

## Consequences

The word "hadoop" appears in the build file and means nothing operationally. No
HDFS, no YARN, no Hadoop install. It is a shaded copy of one class family.
Recorded here because it will look wrong to a reader who does not know that, and
the instinct to "modernise" it is exactly what produced this ADR.
