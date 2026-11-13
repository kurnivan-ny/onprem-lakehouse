"""Spark + Iceberg helpers. Tables are addressed as <catalog>.<layer>.<table>, e.g. lakehouse.silver.sales_orders."""
from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("lakehouse.iceberg")

CATALOG = os.getenv("ICEBERG_CATALOG", "lakehouse")
LAYERS = ("bronze", "silver", "gold")


def session() -> SparkSession:
    """Spark Connect session (thin client). SPARK_REMOTE=sc://host:15002"""
    return SparkSession.builder.remote(os.getenv("SPARK_REMOTE", "sc://localhost:15002")).getOrCreate()


def fq(layer: str, table: str) -> str:
    return f"{CATALOG}.{layer}.{table}"


def ensure_namespaces(spark: SparkSession) -> None:
    for layer in LAYERS:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{layer}")


def exists(spark: SparkSession, name: str) -> bool:
    catalog, namespace, table = name.split(".")
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}")
    return tables.filter(F.col("tableName") == table).limit(1).count() > 0


def max_value(spark: SparkSession, name: str, column: str):
    if not exists(spark, name):
        return None
    return spark.table(name).agg(F.max(column).alias("m")).collect()[0]["m"]


def _create(df: DataFrame, name: str, partition_by: list[str] | None) -> None:
    writer = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    if partition_by:
        writer = writer.partitionedBy(*[F.col(c) for c in partition_by])
    writer.create()


def append(spark: SparkSession, df: DataFrame, name: str, partition_by: list[str] | None = None) -> None:
    if exists(spark, name):
        df.writeTo(name).append()
    else:
        _create(df, name, partition_by)


def merge(spark: SparkSession, df: DataFrame, name: str, keys: list[str]) -> None:
    """Idempotent upsert on business keys (MERGE INTO). Safe to re-run."""
    if not exists(spark, name):
        _create(df, name, None)
        return
    view = "_src_" + name.replace(".", "_")
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {name} t USING {view} s ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def replace(df: DataFrame, name: str) -> None:
    """Atomic full rebuild (CREATE OR REPLACE) - readers never see a half-written table."""
    df.writeTo(name).using("iceberg").tableProperty("format-version", "2").createOrReplace()


def list_tables(spark: SparkSession) -> list[str]:
    out = []
    for layer in LAYERS:
        for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{layer}").collect():
            out.append(fq(layer, row["tableName"]))
    return out


def maintain(spark: SparkSession, name: str, keep_days: int = 7) -> None:
    """Compact small files (streaming + MERGE create many) and expire old snapshots."""
    _, namespace, table = name.split(".")
    ident = f"{namespace}.{table}"
    cutoff = (datetime.now(UTC) - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{ident}', "
              f"options => map('partial-progress.enabled', 'true'))")
    spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{ident}', "
              f"older_than => TIMESTAMP '{cutoff}', retain_last => 5)")
