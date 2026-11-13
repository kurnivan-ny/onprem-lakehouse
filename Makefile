# onprem-lakehouse shortcuts. `make help` lists everything.
DC := docker compose
.DEFAULT_GOAL := help

.PHONY: help init up-batch up-event up-serving up-monitoring up-catalog up-core up-all down reset ps logs \
        batch-run batch-simulate maintenance flink-submit flink-sql trino sql-samples catalog-ingest \
        test lint config

help:            ## show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

init:            ## create .env from .env.example (first time)
	@test -f .env || (cp .env.example .env && echo "created .env - review secrets")

# ---------- run ----------
up-batch: init   ## core + batch (Postgres, Spark, Dagster)
	$(DC) --profile batch up -d --build
up-event: init   ## core + event (Kafka, Flink, producer, Dagster)
	$(DC) --profile event up -d --build
up-serving: init ## core + Trino + Superset
	$(DC) --profile serving up -d --build
up-monitoring: init ## core + Prometheus + Grafana
	$(DC) --profile monitoring up -d --build
up-catalog: init ## core + OpenMetadata (heavy)
	$(DC) --profile catalog up -d --build
up-core: init    ## batch + event + serving (daily driver, ~10 GB)
	$(DC) --profile batch --profile event --profile serving up -d --build
up-all: init     ## everything incl. monitoring + catalog (~14-16 GB)
	$(DC) --profile batch --profile event --profile serving --profile monitoring --profile catalog up -d --build

down:            ## stop all containers (keeps data)
	$(DC) --profile '*' down
reset:           ## stop and DELETE all volumes (fresh start)
	$(DC) --profile '*' down -v
ps:              ## container status
	$(DC) --profile '*' ps
logs:            ## follow logs, e.g. make logs s=flink-sql-runner
	$(DC) --profile '*' logs -f $(s)

# ---------- batch ----------
batch-run:       ## run bronze -> silver -> gold now (outside the schedule)
	$(DC) exec dagster python -m jobs.run_all
batch-simulate:  ## change source data so the next run is incremental
	$(DC) exec dagster python -m jobs.simulate_changes --orders 50
maintenance:     ## compact + expire snapshots on all Iceberg tables
	$(DC) exec dagster python -c "from jobs import maintenance; print(maintenance.run())"

# ---------- event ----------
flink-submit:    ## (re)submit the Flink SQL pipeline if not running
	$(DC) --profile event run --rm flink-sql-runner
flink-sql:       ## interactive Flink SQL client
	$(DC) exec flink-jobmanager /opt/flink/bin/sql-client.sh

# ---------- serving ----------
trino:           ## Trino CLI (try: SHOW TABLES FROM lakehouse.gold;)
	$(DC) exec trino trino --catalog lakehouse
sql-samples:     ## run docs/queries.sql in Trino
	$(DC) exec -T trino trino --catalog lakehouse < docs/queries.sql

# ---------- catalog ----------
catalog-ingest:  ## push metadata (Trino tables, Kafka, Superset, source DB) into OpenMetadata
	$(DC) --profile catalog-ingest run --rm openmetadata-ingest

# ---------- dev ----------
test:            ## unit tests
	pytest -q
lint:            ## ruff
	ruff check .
config:          ## validate compose for all profiles
	@test -f .env || cp .env.example .env
	$(DC) --profile '*' config -q && echo "compose OK"
