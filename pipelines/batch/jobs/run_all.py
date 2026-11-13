"""Run bronze -> silver -> gold without the orchestrator:  make batch-run"""
import logging

from jobs import bronze, gold, silver
from lakehouse import iceberg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

if __name__ == "__main__":
    spark = iceberg.session()
    print("bronze:", bronze.run(spark))
    print("silver:", silver.run(spark))
    print("gold:  ", gold.run(spark))
