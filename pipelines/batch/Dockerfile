# Dagster (UI + daemon) with the batch code. Spark runs in the spark-connect service; this image
# only carries the thin Spark Connect client. Build context = repo root.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DAGSTER_HOME=/opt/dagster/home

WORKDIR /app
COPY libs/requirements.txt /tmp/libs-requirements.txt
COPY pipelines/batch/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/libs-requirements.txt -r /tmp/requirements.txt \
    && mkdir -p $DAGSTER_HOME

COPY libs/lakehouse /app/lakehouse
COPY pipelines/batch /app

EXPOSE 3000
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "definitions"]
