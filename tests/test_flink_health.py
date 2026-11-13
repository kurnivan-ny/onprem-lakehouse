from jobs.flink_health import job_status


def test_picks_latest_job_with_name():
    overview = {"jobs": [
        {"jid": "old", "name": "orders-pipeline", "state": "FAILED", "start-time": 1},
        {"jid": "new", "name": "orders-pipeline", "state": "RUNNING", "start-time": 2},
        {"jid": "x", "name": "other", "state": "RUNNING", "start-time": 3},
    ]}
    assert job_status(overview)["jid"] == "new"
    assert job_status(overview)["state"] == "RUNNING"


def test_missing_job():
    assert job_status({"jobs": []})["state"] == "MISSING"
