"""Batch transforms on a local Spark session (needs Java; runs in CI). Same code runs on Spark Connect."""
from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from jobs.gold import build_marts  # noqa: E402
from jobs.silver import clean, latest_per_key  # noqa: E402
from lakehouse.quality import DataQualityError, check, failures  # noqa: E402

T0 = datetime(2026, 11, 1, 3, 0, 0)  # UTC -> 10:00 WIB
ING = datetime(2026, 11, 2, 0, 0, 0)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def test_latest_per_key_keeps_newest_version(spark):
    df = spark.createDataFrame(
        [(1, "pending", T0, ING, "b1", "s"), (1, "paid", T0 + timedelta(hours=1), ING, "b2", "s"),
         (2, "shipped", T0, ING, "b1", "s")],
        "order_id int, status string, updated_at timestamp, _ingested_at timestamp, _batch_id string, _source string")
    out = {r.order_id: r.status for r in latest_per_key(df, "order_id").collect()}
    assert out == {1: "paid", 2: "shipped"}
    assert "_batch_id" not in latest_per_key(df, "order_id").columns


def test_clean_customers(spark):
    df = spark.createDataFrame([(1, " Ana ", " Ana@X.COM ", "id")],
                               "customer_id int, full_name string, email string, country string")
    row = clean("customers", df).collect()[0]
    assert (row.full_name, row.email, row.country) == ("Ana", "ana@x.com", "ID")


def test_quality_gate(spark):
    df = spark.createDataFrame([(1, 5.0), (1, -1.0), (None, 2.0)], "product_id int, price double")
    f = failures(df, pk="product_id", rules={"price >= 0": "price >= 0"})
    assert f == {"price >= 0": 1, "product_id not null": 1, "product_id unique": 1}
    with pytest.raises(DataQualityError):
        check(df, "products", pk="product_id", rules={"price >= 0": "price >= 0"})


def test_gold_marts(spark):
    s = {
        "orders": spark.createDataFrame([(1, 1, T0, "paid"), (2, 1, T0, "cancelled")],
                                        "order_id int, customer_id int, order_ts timestamp, status string"),
        "order_items": spark.createDataFrame([(1, 1, 3, 10.0), (2, 1, 1, 10.0)],
                                             "order_id int, product_id int, quantity int, unit_price double"),
        "products": spark.createDataFrame([(1, "home")], "product_id int, category string"),
        "customers": spark.createDataFrame([(1, "Ana", "ID")], "customer_id int, full_name string, country string"),
    }
    marts = build_marts(s)
    daily = marts["sales_daily"].collect()
    assert len(daily) == 1
    assert (daily[0].order_date, daily[0].orders, daily[0].items, daily[0].revenue) == ("2026-11-01", 1, 3, 30.0)
    assert marts["sales_customer_ltv"].collect()[0].revenue == 30.0
