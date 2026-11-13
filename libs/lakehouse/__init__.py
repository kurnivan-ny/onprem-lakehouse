"""Shared lakehouse library.

  lakehouse.iceberg  - Spark session + Iceberg table helpers (append / merge / replace / maintain)
  lakehouse.quality  - Spark-native data-quality gate

Engine swap point: pipelines talk only to these helpers. Spark Connect today; Sail (Spark Connect
compatible) or a Spark cluster later by changing SPARK_REMOTE - no pipeline code changes.
"""
