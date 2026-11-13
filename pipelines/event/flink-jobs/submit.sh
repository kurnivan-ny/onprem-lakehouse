#!/usr/bin/env bash
# Submit the Flink SQL pipeline to the session cluster - idempotent:
# skips if a job with the same name is already RUNNING.
set -euo pipefail

JM="http://flink-jobmanager:8081"
JOB_NAME="orders-pipeline"
SQL_TEMPLATE="/opt/flink/jobs/orders_pipeline.sql"

echo "[flink-sql-runner] waiting for JobManager + a free TaskManager slot..."
until curl -sf "${JM}/overview" | grep -Eq '"slots-available":[1-9]'; do sleep 3; done

if curl -sf "${JM}/jobs/overview" | grep -Eq "\"name\":\"${JOB_NAME}\"[^}]*\"state\":\"(RUNNING|CREATED|RESTARTING)\""; then
  echo "[flink-sql-runner] job '${JOB_NAME}' already running - nothing to do"
  exit 0
fi

# Client-side config: point the SQL client at the session cluster.
CONF=/tmp/flink-conf
mkdir -p "${CONF}"
cp /opt/flink/conf/log4j*.properties "${CONF}/" 2>/dev/null || true
cat > "${CONF}/config.yaml" <<EOF
rest.address: flink-jobmanager
rest.port: 8081
jobmanager.rpc.address: flink-jobmanager
execution.target: remote
EOF

# Render secrets/topics into the SQL (the SQL client does not expand env vars).
sed -e "s|__WAREHOUSE__|${ICEBERG_WAREHOUSE}|g" \
    -e "s|__S3_ACCESS_KEY__|${RUSTFS_ACCESS_KEY}|g" \
    -e "s|__S3_SECRET_KEY__|${RUSTFS_SECRET_KEY}|g" \
    -e "s|__TOPIC_EVENTS__|${TOPIC_EVENTS}|g" \
    -e "s|__TOPIC_DLQ__|${TOPIC_DLQ}|g" \
    "${SQL_TEMPLATE}" > /tmp/orders_pipeline.sql

echo "[flink-sql-runner] submitting '${JOB_NAME}'"
FLINK_CONF_DIR="${CONF}" /opt/flink/bin/sql-client.sh -f /tmp/orders_pipeline.sql
rm -f /tmp/orders_pipeline.sql
echo "[flink-sql-runner] submitted - see http://localhost:8081"
