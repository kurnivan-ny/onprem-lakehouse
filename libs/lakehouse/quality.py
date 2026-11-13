"""Spark-native data-quality gate: one aggregation pass, fail before publishing bad data downstream.

Rules are SQL boolean expressions that must hold for every row, e.g. {"price >= 0": "price >= 0"}.
NULL results count as failures.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def failures(df: DataFrame, pk: str | None = None, rules: dict[str, str] | None = None) -> dict[str, int]:
    rules = dict(rules or {})
    if pk:
        rules[f"{pk} not null"] = f"{pk} IS NOT NULL"
    names = list(rules)
    aggs = [F.sum(F.when(F.coalesce(F.expr(rules[n]), F.lit(False)), 0).otherwise(1)).alias(f"r{i}")
            for i, n in enumerate(names)]
    result = {}
    if aggs:
        row = df.agg(*aggs).collect()[0]
        result = {n: int(row[f"r{i}"] or 0) for i, n in enumerate(names)}
    if pk:
        result[f"{pk} unique"] = df.groupBy(pk).count().filter("count > 1").count()
    return result


def check(df: DataFrame, name: str, pk: str | None = None, rules: dict[str, str] | None = None) -> None:
    failed = {rule: n for rule, n in failures(df, pk, rules).items() if n > 0}
    if failed:
        raise DataQualityError(f"{name}: failed checks (rule -> bad rows) {failed}")
