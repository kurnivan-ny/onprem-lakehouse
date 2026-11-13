"""Producer output must satisfy contracts/orders.v1.json; corrupted events must not."""
import json
import random
from pathlib import Path

import pytest

jsonschema = pytest.importorskip("jsonschema")
import producer  # noqa: E402  (pipelines/event/producer on pythonpath)

CONTRACT = json.loads((Path(__file__).parents[1] / "contracts" / "orders.v1.json").read_text())


def test_generated_events_match_contract():
    random.seed(7)
    for _ in range(500):
        jsonschema.validate(producer.next_event(), CONTRACT)


@pytest.mark.parametrize("kind", producer.CORRUPTIONS)
def test_corrupted_events_violate_contract(kind):
    random.seed(1)
    raw = producer.corrupt(producer.next_event(), kind)
    try:
        evt = json.loads(raw)
    except json.JSONDecodeError:
        return  # not JSON at all -> DLQ 'invalid_json'
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(evt, CONTRACT)


def test_timestamp_format_matches_flink_parser():
    ts = producer.now_iso()
    assert len(ts) == 24 and ts.endswith("Z") and ts[10] == "T"   # yyyy-MM-ddTHH:mm:ss.SSSZ
