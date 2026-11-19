"""Read a run from Kafka and compare each message with its frozen Outbox row."""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
import uuid
from pathlib import Path


def verify(db_path: Path, bootstrap: str, run_id: str, timeout: float = 15.0) -> dict:
    from confluent_kafka import Consumer

    with sqlite3.connect(str(db_path)) as db:
        rows = db.execute(
            "SELECT event_id, event_key, payload_json, status FROM kafka_outbox "
            "WHERE simulation_run_id = ?",
            (run_id,),
        ).fetchall()
    expected = {event_id: (key.encode("utf-8"), payload.encode("utf-8"), status)
                for event_id, key, payload, status in rows}
    if not expected:
        raise ValueError(f"no Outbox events for run {run_id}")
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"v1-phase3-verifier-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    found = set()
    try:
        consumer.subscribe(["traffic.observations.v1"])
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and len(found) < len(expected):
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            body = json.loads(message.value())
            if body.get("simulationRunId") != run_id:
                continue
            event_id = body["eventId"]
            if event_id not in expected:
                raise ValueError(f"unexpected event {event_id}")
            key, value, status = expected[event_id]
            if message.key() != key or message.value() != value:
                raise ValueError(f"Kafka key or bytes differ from frozen Outbox event {event_id}")
            if body.get("eventType") != "IntersectionObservation":
                raise ValueError("non-observation message in V1 topic")
            if status != "ACKED":
                raise ValueError(f"event {event_id} is not ACKED in Outbox")
            found.add(event_id)
    finally:
        consumer.close()
    if len(found) != len(expected):
        raise RuntimeError(f"only verified {len(found)}/{len(expected)} V1 events")
    return {"simulationRunId": run_id, "verifiedEvents": len(found),
            "cycles": len({json.loads(row[2])["cycleSequence"] for row in rows}),
            "outboxStatus": "ACKED", "payloadMatch": True}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", type=Path)
    parser.add_argument("run_id")
    parser.add_argument("--bootstrap", default="localhost:29092")
    args = parser.parse_args()
    print(json.dumps(verify(args.db_path, args.bootstrap, args.run_id), indent=2))


if __name__ == "__main__":
    main()
