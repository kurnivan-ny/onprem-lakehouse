"""Gold: business marts rebuilt from silver with an atomic CREATE OR REPLACE."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.tables import SOURCE_TABLES, lake_name
from lakehouse import iceberg

log = logging.getLogger("gold")
LOCAL_TZ = "Asia/Jakarta"


def build_marts(s: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Pure transform: silver DataFrames in -> gold DataFrames out (unit-tested)."""
    lines = (s["orders"].filter(F.col("status") != "cancelled")
             .select("order_id", "customer_id", "order_ts")
             .join(s["order_items"].select("order_id", "product_id", "quantity", "unit_price"), "order_id")
             .join(s["products"].select("product_id", "category"), "product_id")
             .join(s["customers"].select("customer_id", "full_name", "country"), "customer_id")
             .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
             .withColumn("order_date",
                         F.date_format(F.from_utc_timestamp("order_ts", LOCAL_TZ), "yyyy-MM-dd")))

    daily_sales = (lines.groupBy("order_date", "country", "category")
                   .agg(F.countDistinct("order_id").alias("orders"),
                        F.sum("quantity").alias("items"),
                        F.round(F.sum("revenue"), 2).alias("revenue")))
    customer_ltv = (lines.groupBy("customer_id", "full_name", "country")
                    .agg(F.min("order_ts").alias("first_order"), F.max("order_ts").alias("last_order"),
                         F.countDistinct("order_id").alias("orders"),
                         F.round(F.sum("revenue"), 2).alias("revenue")))
    return {"sales_daily": daily_sales, "sales_customer_ltv": customer_ltv}


def run(spark: SparkSession | None = None) -> dict:
    spark = spark or iceberg.session()
    names = {t: iceberg.fq("silver", lake_name(t)) for t in SOURCE_TABLES}
    if not all(iceberg.exists(spark, n) for n in names.values()):
        log.info("silver not complete yet - skipping gold")
        return {}
    marts = build_marts({t: spark.table(n) for t, n in names.items()})
    stats = {}
    for mart, df in marts.items():
        iceberg.replace(df, iceberg.fq("gold", mart))
        stats[mart] = spark.table(iceberg.fq("gold", mart)).count()
    log.info("gold %s", stats)
    return stats
