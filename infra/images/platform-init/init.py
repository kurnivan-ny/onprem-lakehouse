"""One-shot, idempotent platform bootstrap (safe to run on every `up`):
  1. create the lakehouse bucket in RustFS
  2. bootstrap Lakekeeper (accept terms) if not done yet
  3. register the Iceberg warehouse pointing at RustFS
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

import boto3
from botocore.client import Config

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]
BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "lakehouse")
LK = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181").rstrip("/")
DEFAULT_PROJECT = "00000000-0000-0000-0000-000000000000"


def log(msg: str) -> None:
    print(f"[platform-init] {msg}", flush=True)


def retry(fn, what: str, attempts: int = 60, delay: float = 2.0):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # service still booting
            log(f"waiting for {what} ({i}/{attempts}): {exc}")
            time.sleep(delay)
    sys.exit(f"{what} not reachable")


def http(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        LK + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "x-project-id": DEFAULT_PROJECT},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as err:
        raw = err.read()
        try:
            return err.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return err.code, {"raw": raw.decode(errors="replace")}


def ensure_bucket() -> None:
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name="us-east-1",
                      config=Config(s3={"addressing_style": "path"}))
    names = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if BUCKET in names:
        log(f"bucket '{BUCKET}' exists")
    else:
        s3.create_bucket(Bucket=BUCKET)
        log(f"bucket '{BUCKET}' created")


def lakekeeper_info() -> dict:
    status, body = http("GET", "/management/v1/info")
    if status != 200:
        raise RuntimeError(f"info returned {status}")
    return body


def ensure_bootstrap() -> None:
    info = retry(lakekeeper_info, "lakekeeper")
    if info.get("bootstrapped"):
        log("lakekeeper already bootstrapped")
        return
    status, body = http("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True})
    if status >= 300 and "bootstrap" not in json.dumps(body).lower():
        sys.exit(f"bootstrap failed: {status} {body}")
    log("lakekeeper bootstrapped")


def warehouse_exists() -> bool:
    status, body = http("GET", "/management/v1/warehouse")
    if status != 200:
        return False
    for wh in body.get("warehouses", []):
        if WAREHOUSE in (wh.get("name"), wh.get("warehouse-name")):
            return True
    return False


def ensure_warehouse() -> None:
    if warehouse_exists():
        log(f"warehouse '{WAREHOUSE}' exists")
        return
    payload = {
        "warehouse-name": WAREHOUSE,
        "project-id": DEFAULT_PROJECT,
        "storage-profile": {
            "type": "s3",
            "bucket": BUCKET,
            "key-prefix": "warehouse",
            "endpoint": S3_ENDPOINT,
            "region": "us-east-1",
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": False,
            "remote-signing-url-style": "path",
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": ACCESS_KEY,
            "aws-secret-access-key": SECRET_KEY,
        },
        "delete-profile": {"type": "hard"},
    }
    status, body = http("POST", "/management/v1/warehouse", payload)
    if status == 409 or "already exists" in json.dumps(body).lower():
        log(f"warehouse '{WAREHOUSE}' exists")
    elif status >= 300:
        sys.exit(f"warehouse creation failed: {status} {body}")
    else:
        log(f"warehouse '{WAREHOUSE}' created -> s3://{BUCKET}/warehouse")


if __name__ == "__main__":
    retry(ensure_bucket, "rustfs")
    ensure_bootstrap()
    ensure_warehouse()
    log("platform ready")
