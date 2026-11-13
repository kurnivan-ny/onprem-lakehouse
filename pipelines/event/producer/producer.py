"""Simulated order service: emits order lifecycle events (created -> paid -> shipped | cancelled) to Kafka.

Events follow contracts/orders.v1.json. A small share is deliberately broken (-> Flink routes to DLQ)
or duplicated (-> silver upsert dedups them).
"""
from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

TOPIC = os.getenv("TOPIC_EVENTS", "orders.events")
RATE = float(os.getenv("EVENTS_PER_SEC", "5"))
BAD_RATE = float(os.getenv("BAD_EVENT_RATE", "0.02"))
DUP_RATE = float(os.getenv("DUPLICATE_RATE", "0.01"))

COUNTRIES = ["ID", "SG", "MY", "TH", "VN", "PH"]
PAYMENTS = ["card", "ewallet", "bank_transfer", "cod"]
NEXT = {"order_created": ["order_paid", "order_paid", "order_paid", "order_cancelled"],
        "order_paid": ["order_shipped"]}
CORRUPTIONS = ["not_json", "missing_order_id", "negative_amount", "bad_event_type"]

open_orders: dict[str, dict] = {}  # order_id -> last event


def now_iso() -> str:
    """UTC with milliseconds and 'Z' - the exact format the Flink job parses."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def next_event() -> dict:
    if open_orders and random.random() < 0.6:  # advance an existing order
        order_id = random.choice(list(open_orders))
        etype = random.choice(NEXT[open_orders[order_id]["event_type"]])
        evt = {**open_orders[order_id], "event_id": str(uuid.uuid4()), "event_type": etype,
               "event_ts": now_iso()}
        if etype in ("order_shipped", "order_cancelled"):
            open_orders.pop(order_id)
        else:
            open_orders[order_id] = evt
        return evt
    order_id = f"ORD-{uuid.uuid4().hex[:10].upper()}"
    evt = {"event_id": str(uuid.uuid4()), "event_type": "order_created", "event_ts": now_iso(),
           "order_id": order_id, "customer_id": random.randint(1, 500),
           "country": random.choice(COUNTRIES), "payment_method": random.choice(PAYMENTS),
           "amount": round(random.uniform(5, 500), 2), "currency": "USD", "schema_version": 1}
    open_orders[order_id] = evt
    return evt


def corrupt(evt: dict, kind: str | None = None) -> bytes:
    kind = kind or random.choice(CORRUPTIONS)
    if kind == "not_json":
        return b"{this is not json"
    evt = dict(evt)
    if kind == "missing_order_id":
        evt.pop("order_id")
    elif kind == "negative_amount":
        evt["amount"] = -abs(evt["amount"]) - 1
    elif kind == "bad_event_type":
        evt["event_type"] = "order_teleported"
    return json.dumps(evt).encode()


def main() -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                         "acks": "all", "enable.idempotence": True, "linger.ms": 50})
    print(f"producing ~{RATE} events/s to {TOPIC}", flush=True)
    sent = 0
    while True:
        evt = next_event()
        payload = corrupt(evt) if random.random() < BAD_RATE else json.dumps(evt).encode()
        key = evt["order_id"].encode()             # key = order_id -> per-order ordering
        producer.produce(TOPIC, key=key, value=payload)
        if random.random() < DUP_RATE:
            producer.produce(TOPIC, key=key, value=payload)   # at-least-once duplicate
        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            print(f"sent={sent} open_orders={len(open_orders)}", flush=True)
        time.sleep(1 / RATE)


if __name__ == "__main__":
    main()
