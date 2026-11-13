"""Bronze: incremental JDBC extract from Postgres by updated_at watermark; append raw rows + audit columns."""
from __future__ import annotations

import logging
import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.tables import SOURCE_SCHEMA, SOURCE_TABLES, lake_name
from lakehouse import iceberg

log = logging.getLogger("bronze")


def jdbc_options() -> dict:
    return {
        "url": f"jdbc:postgresql://{os.getenv('PG_HOST', 'source-db')}:{os.getenv('PG_PORT', '5432')}"
               f"/{os.environ['PG_DB']}",
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def source_query(table: str, watermark) -> str:
    """Postgres subquery for one table; watermark is a UTC datetime (session tz = UTC) or None."""
    sql = f"SELECT * FROM {SOURCE_SCHEMA}.{table}"
    if watermark is not None:
        sql += f" WHERE updated_at > TIMESTAMPTZ '{watermark:%Y-%m-%d %H:%M:%S.%f}+00'"
    return f"({sql}) AS src"


def run(spark: SparkSession | None = None) -> dict:
    spark = spark or iceberg.session()
    iceberg.ensure_namespaces(spark)
    batch_id = uuid.uuid4().hex[:12]
    stats = {}
    for table in SOURCE_TABLES:
        target = iceberg.fq("bronze", lake_name(table))
        watermark = iceberg.max_value(spark, target, "updated_at")
        df = (spark.read.format("jdbc").options(**jdbc_options())
              .option("dbtable", source_query(table, watermark)).load()
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source", F.lit(f"postgres.{SOURCE_SCHEMA}.{table}")))
        n = df.count()
        if n:
            iceberg.append(spark, df, target)
        stats[table] = n
        log.info("bronze.%s +%s rows (watermark=%s)", table, n, watermark)
    return stats
