# Build context is apps/pipeline. Built by scripts/build-image.sh.
# java21, not the default image. The default flink:2.2.0 ships Temurin 17, and
# apps/ compiles at Java 21 (build.gradle javaVersion = '21', also in :domain,
# which is bundled into the shadow jar). Mismatch surfaces only at class load
# time, as UnsupportedClassVersionError: class file version 65.0 vs 61.0.
FROM flink:2.2.0-java21

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.0.jar /opt/flink/plugins/s3-fs-hadoop/

COPY build/libs/pipeline-all.jar /opt/flink/usrlib/pipeline.jar
