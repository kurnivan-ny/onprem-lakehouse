"""Silver: only new bronze rows -> latest version per key -> clean -> DQ gate -> MERGE (upsert)."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from jobs.tables import SOURCE_TABLES, lake_name
from lakehouse import iceberg
from lakehouse.quality import check

log = logging.getLogger("silver")

RULES = {
    "customers": {"email has @": "email LIKE '%@%'", "country is 2 chars": "length(country) = 2"},
    "products": {"price >= 0": "price >= 0"},
    "orders": {"valid status": "status IN ('pending', 'paid', 'shipped', 'cancelled')"},
    "order_items": {"quantity > 0": "quantity > 0"},
}


def latest_per_key(df: DataFrame, pk: str) -> DataFrame:
    w = Window.partitionBy(pk).orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1")
              .drop("_rn", "_batch_id", "_source"))


def clean(table: str, df: DataFrame) -> DataFrame:
    if table == "customers":
        return (df.withColumn("email", F.lower(F.trim("email")))
                  .withColumn("full_name", F.trim("full_name"))
                  .withColumn("country", F.upper(F.trim("country"))))
    if table == "orders":
        return df.withColumn("status", F.lower(F.trim("status")))
    if table == "products":
        return df.withColumn("category", F.lower(F.trim("category")))
    return df


def run(spark: SparkSession | None = None) -> dict:
    spark = spark or iceberg.session()
    stats = {}
    for table, pk in SOURCE_TABLES.items():
        src, dst = iceberg.fq("bronze", lake_name(table)), iceberg.fq("silver", lake_name(table))
        if not iceberg.exists(spark, src):
            stats[table] = 0
            continue
        df = spark.table(src)
        last = iceberg.max_value(spark, dst, "_ingested_at")
        if last is not None:
            df = df.filter(F.col("_ingested_at") > F.lit(last))
        df = clean(table, latest_per_key(df, pk))
        n = df.count()
        if n:
            check(df, f"silver.{lake_name(table)}", pk=pk, rules=RULES.get(table))
            iceberg.merge(spark, df, dst, keys=[pk])
        stats[table] = n
        log.info("silver.%s merged %s rows", table, n)
    return stats
