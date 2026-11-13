#!/usr/bin/env bash
# Start Spark Connect in the foreground. Secrets come from env, not from the image.
# Runs Spark in local mode on this container; to scale out on-prem, point --master at a
# Spark standalone/K8s cluster - client code (sc://spark-connect:15002) does not change.
set -euo pipefail

export SPARK_NO_DAEMONIZE=true
exec /opt/spark/sbin/start-connect-server.sh \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --conf spark.sql.catalog.lakehouse.warehouse="${ICEBERG_WAREHOUSE:-lakehouse}" \
  --conf spark.sql.catalog.lakehouse.s3.access-key-id="${RUSTFS_ACCESS_KEY}" \
  --conf spark.sql.catalog.lakehouse.s3.secret-access-key="${RUSTFS_SECRET_KEY}"
