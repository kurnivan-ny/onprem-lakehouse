"""Table maintenance for every Iceberg table (batch AND Flink-written): compaction + snapshot expiry."""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from lakehouse import iceberg

log = logging.getLogger("maintenance")


def run(spark: SparkSession | None = None) -> dict:
    spark = spark or iceberg.session()
    iceberg.ensure_namespaces(spark)
    ok, failed = [], {}
    for name in iceberg.list_tables(spark):
        try:
            iceberg.maintain(spark, name)
            ok.append(name)
        except Exception as exc:  # e.g. commit conflict with a running Flink writer - retried next run
            failed[name] = str(exc).splitlines()[0][:200]
            log.warning("maintenance failed for %s: %s", name, failed[name])
    if failed and not ok:
        raise RuntimeError(f"maintenance failed for all tables: {failed}")
    return {"maintained": len(ok), "failed": len(failed), "failed_tables": ", ".join(failed) or "-"}
