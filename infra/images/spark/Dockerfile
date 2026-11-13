# Spark Connect server with Iceberg (REST catalog -> Lakekeeper) + S3FileIO (-> RustFS) + Postgres JDBC.
# Jars are baked in at build time so the server starts offline.
FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu

ARG SPARK_VERSION=3.5.8
ARG ICEBERG_VERSION=1.10.1
ARG PG_JDBC_VERSION=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl; rm -rf /var/lib/apt/lists/*; \
    cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar

COPY infra/images/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY --chmod=755 infra/images/spark/start-connect.sh /opt/spark/start-connect.sh

USER spark
EXPOSE 15002 4040
CMD ["/opt/spark/start-connect.sh"]
