# onprem-lakehouse

On-premise lakehouse with **batch (Spark)** and **event (Flink)** pipelines on **Apache Iceberg**.
100% open-source and free; runs on one machine with Docker. Design: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

```
Postgres ──Spark──┐                                   ┌── Trino ── Superset
                  ├─► Iceberg (RustFS + Lakekeeper) ──┤
Kafka ────Flink───┘   bronze → silver → gold          └── OpenMetadata
```

## Requirements

- Docker + Docker Compose v2.20+ (WSL2 on Windows is fine) and `make`
- RAM: core + batch ≈ 4 GB · core + event ≈ 5 GB · `up-core` ≈ 10 GB · `up-all` ≈ 14–16 GB
- First build downloads images and jars (~5 GB) — later starts are offline

## Quick start

```bash
make init            # creates .env from .env.example - change secrets if not a laptop
make up-core         # batch + event + serving   (or: up-batch / up-event / up-serving)
make ps              # wait until platform-init and flink-sql-runner show "exited (0)"
```

Then:

| Do this | Where |
|---|---|
| Run the batch pipeline | Dagster http://localhost:3000 → *Jobs* → `sales_daily` → Materialize — or `make batch-run` |
| Watch events + DLQ | Kafka UI http://localhost:8080 → `orders.events`, `orders.dlq` |
| Watch the stream job | Flink UI http://localhost:8081 → `orders-pipeline` |
| Query everything | `make trino` → `SHOW TABLES FROM lakehouse.gold;` · `make sql-samples` |
| Dashboards | Superset http://localhost:8088 (admin / admin) — Trino is pre-registered |
| Catalog of tables | Lakekeeper http://localhost:8181/ui |

Add-ons: `make up-monitoring` (Grafana http://localhost:3001) · `make up-catalog` (OpenMetadata http://localhost:8585, then set `OM_JWT_TOKEN` in `.env` and `make catalog-ingest`).

## Try the patterns

```bash
# batch: incremental + idempotent
make batch-run                 # 1st run: full load
make batch-simulate            # new orders, status changes, a customer edit in Postgres
make batch-run                 # 2nd run: only changed rows move; MERGE updates silver

# event: failure + recovery
docker compose stop flink-taskmanager      # lag builds up (Kafka UI / Grafana)
docker compose start flink-taskmanager     # job restores from checkpoint, catches up, silver stays deduped
```

## Repo layout

```
compose.yml                 entry point (includes infra/*, profiles)
Makefile                    make help
.env.example                all settings + secrets (copy to .env)
infra/
  compose.base.yml          RustFS, platform-db, Lakekeeper, platform-init        (always on)
  compose.batch.yml         source-db, spark-connect, dagster                     (batch)
  compose.event.yml         kafka, kafka-ui, flink, flink-sql-runner, producer     (event)
  compose.serving.yml       trino, superset                                       (serving)
  compose.monitoring.yml    prometheus, grafana, kafka-exporter                   (monitoring)
  compose.catalog.yml       openmetadata, opensearch                              (catalog)
  images/                   spark, flink, superset, platform-init Dockerfiles
  config/                   trino, prometheus, grafana, openmetadata, platform-db
libs/lakehouse/             shared Iceberg + data-quality helpers (Spark Connect)
pipelines/
  batch/                    Dagster definitions + Spark jobs (bronze/silver/gold, maintenance)
  event/flink-jobs/         Flink SQL pipeline + idempotent submit script
  event/producer/           sample order-event producer
contracts/orders.v1.json    event contract (enforced by Flink, tested in CI)
tests/                      pytest: Spark transforms, contract, Flink health
docs/                       ARCHITECTURE.md, queries.sql
.github/workflows/ci.yml    lint, tests, compose validation, image build
```

## Development

```bash
pip install -r requirements-dev.txt   # needs Java 17 for the Spark tests
make lint test config
```

## Notes

- `rustfs/rustfs`, `lakekeeper/catalog`, `kafka-ui` and `kafka-exporter` use `latest` — pin tags once a setup works for you.
- Secrets in `.env` are dev defaults. On a shared server, change them all and keep `.env` out of Git (already ignored).
- Back up **RustFS data and platform-db together** — Lakekeeper's metadata points at the data files.
