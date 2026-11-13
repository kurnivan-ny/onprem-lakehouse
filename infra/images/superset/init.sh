#!/usr/bin/env bash
# One-shot Superset setup - idempotent.
set -euo pipefail

TRINO_URI="trino://superset@trino:8080/lakehouse"
DB_NAME="Lakehouse (Trino)"

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname Admin --lastname Lakehouse --email admin@lakehouse.local || true
superset init

# Register Trino as a database (CLI name differs across Superset versions).
superset set-database-uri -d "${DB_NAME}" -u "${TRINO_URI}" \
  || superset set_database_uri -d "${DB_NAME}" -u "${TRINO_URI}" \
  || echo "[superset-init] could not register Trino automatically - add ${TRINO_URI} in the UI"

echo "[superset-init] done"
