"""Dagster: batch medallion assets + lake maintenance + streaming health check.

  sales_bronze -> sales_silver -> sales_gold    daily 01:00 WIB   (Spark via Spark Connect)
  lake_maintenance                              daily 03:00 WIB   (all Iceberg tables, incl. Flink's)
  flink_health                                  every 5 min       (off by default; turn on with the event profile)
"""
import logging

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    job,
    op,
)

from jobs import bronze, flink_health, gold, maintenance, silver

logging.basicConfig(level=logging.INFO)


def _meta(stats: dict) -> dict:
    return {str(k): (v if isinstance(v, (int, float, str)) else str(v)) for k, v in stats.items()}


@asset(group_name="sales", description="Incremental JDBC extract of sales.* (updated_at watermark) -> Iceberg bronze")
def sales_bronze() -> MaterializeResult:
    return MaterializeResult(metadata=_meta(bronze.run()))


@asset(deps=[sales_bronze], group_name="sales", description="Latest row per key, cleaned, DQ-checked, MERGEd")
def sales_silver() -> MaterializeResult:
    return MaterializeResult(metadata=_meta(silver.run()))


@asset(deps=[sales_silver], group_name="sales", description="gold.sales_daily + gold.sales_customer_ltv")
def sales_gold() -> MaterializeResult:
    return MaterializeResult(metadata=_meta(gold.run()))


@asset(group_name="platform", description="Compaction + snapshot expiry for every Iceberg table")
def lake_maintenance() -> MaterializeResult:
    return MaterializeResult(metadata=_meta(maintenance.run()))


@op
def check_flink_job(context) -> None:
    status = flink_health.run()
    context.log.info(f"Flink job healthy: {status}")


@job(description="Fails if the Flink orders-pipeline is not RUNNING")
def flink_health_job():
    check_flink_job()


sales_daily = define_asset_job("sales_daily", selection=AssetSelection.groups("sales"))
maintenance_daily = define_asset_job("lake_maintenance_daily", selection=AssetSelection.assets(lake_maintenance))

defs = Definitions(
    assets=[sales_bronze, sales_silver, sales_gold, lake_maintenance],
    jobs=[sales_daily, maintenance_daily, flink_health_job],
    schedules=[
        ScheduleDefinition(job=sales_daily, cron_schedule="0 1 * * *", execution_timezone="Asia/Jakarta",
                           default_status=DefaultScheduleStatus.RUNNING),
        ScheduleDefinition(job=maintenance_daily, cron_schedule="0 3 * * *", execution_timezone="Asia/Jakarta",
                           default_status=DefaultScheduleStatus.RUNNING),
        ScheduleDefinition(job=flink_health_job, cron_schedule="*/5 * * * *", execution_timezone="Asia/Jakarta",
                           default_status=DefaultScheduleStatus.STOPPED),
    ],
)
