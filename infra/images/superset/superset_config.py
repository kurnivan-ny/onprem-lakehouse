"""Superset config: metadata in platform-db, secrets from env."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}@platform-db:5432/superset"
)
SQLLAB_TIMEOUT = 300
ROW_LIMIT = 50_000
