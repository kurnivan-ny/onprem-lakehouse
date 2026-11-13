#!/bin/bash
# Runs once on first start of platform-db: one database + user per platform service.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
  CREATE USER lakekeeper WITH PASSWORD '${LAKEKEEPER_DB_PASSWORD}';
  CREATE DATABASE lakekeeper OWNER lakekeeper;

  CREATE USER superset WITH PASSWORD '${SUPERSET_DB_PASSWORD}';
  CREATE DATABASE superset OWNER superset;

  CREATE USER openmetadata_user WITH PASSWORD '${OM_DB_PASSWORD}';
  CREATE DATABASE openmetadata_db OWNER openmetadata_user;
EOSQL
