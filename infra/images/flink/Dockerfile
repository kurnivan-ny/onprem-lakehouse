# Flink 1.20 LTS + Iceberg (REST catalog, S3FileIO) + Kafka SQL connector. Jars baked in at build time.
FROM flink:1.20.3-scala_2.12-java17

ARG ICEBERG_VERSION=1.10.1
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG HADOOP_VERSION=3.3.6
ARG MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl; rm -rf /var/lib/apt/lists/*; \
    cd /opt/flink/lib; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar; \
    # Prometheus reporter ships in opt/ or plugins/ depending on the build - make sure it is loadable
    if ! ls /opt/flink/plugins/metrics-prometheus/*.jar >/dev/null 2>&1; then \
      mkdir -p /opt/flink/plugins/metrics-prometheus; \
      cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/plugins/metrics-prometheus/ 2>/dev/null || true; \
    fi; \
    mkdir -p /opt/flink/checkpoints /opt/flink/jobs; \
    chown -R flink:flink /opt/flink/checkpoints /opt/flink/jobs

COPY --chown=flink:flink pipelines/event/flink-jobs/ /opt/flink/jobs/
RUN chmod +x /opt/flink/jobs/submit.sh
