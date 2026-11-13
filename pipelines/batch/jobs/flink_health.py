"""Check the streaming job via the Flink REST API - lets Dagster alert on the event pipeline too."""
from __future__ import annotations

import json
import os
import urllib.request

FLINK = os.getenv("FLINK_REST_URL", "http://flink-jobmanager:8081").rstrip("/")
JOB = os.getenv("FLINK_JOB_NAME", "orders-pipeline")


def _get(path: str) -> dict:
    with urllib.request.urlopen(FLINK + path, timeout=10) as resp:
        return json.loads(resp.read())


def job_status(jobs_overview: dict, name: str = JOB) -> dict:
    """Pick the most recent job with this name from /jobs/overview (pure, unit-tested)."""
    jobs = [j for j in jobs_overview.get("jobs", []) if j.get("name") == name]
    if not jobs:
        return {"state": "MISSING"}
    latest = max(jobs, key=lambda j: j.get("start-time", 0))
    return {"state": latest.get("state"), "jid": latest.get("jid"), "duration_ms": latest.get("duration")}


def run() -> dict:
    status = job_status(_get("/jobs/overview"))
    if status["state"] != "RUNNING":
        raise RuntimeError(f"Flink job '{JOB}' is {status['state']} - run `make flink-submit`")
    jid = status["jid"]
    ckpt = _get(f"/jobs/{jid}/checkpoints").get("counts", {})
    status.update(completed_checkpoints=ckpt.get("completed", 0), failed_checkpoints=ckpt.get("failed", 0))
    return status
