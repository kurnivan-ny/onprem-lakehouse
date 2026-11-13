FROM apache/superset:5.0.0

USER root
# Superset 5 runs from a uv-managed venv at /app/.venv - drivers must be installed there.
RUN . /app/.venv/bin/activate && uv pip install --no-cache trino psycopg2-binary

COPY infra/images/superset/superset_config.py /app/superset_config.py
COPY --chmod=755 infra/images/superset/init.sh /app/init-lakehouse.sh
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset
