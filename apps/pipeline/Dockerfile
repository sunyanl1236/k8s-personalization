# Build context is apps/pipeline. Built by scripts/build-image.sh.
FROM flink:2.2.0

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.0.jar /opt/flink/plugins/s3-fs-hadoop/

COPY build/libs/pipeline-all.jar /opt/flink/usrlib/pipeline.jar
