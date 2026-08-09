"""Replay gate: Kafka window → replay tables; compare Raw∪Quarantine."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.kafka_raw.clickhouse_repository import ClickHouseRawRepository  # noqa: E402
from de.kafka_raw.config import get_settings  # noqa: E402
from de.kafka_raw.ingestion_id import raw_ingestion_id  # noqa: E402
from de.kafka_raw.validator import EventValidator  # noqa: E402

log = logging.getLogger("de.kafka_raw.replay")


def main() -> int:
    p = argparse.ArgumentParser(description="K-4 Raw replay gate")
    p.add_argument("--topic", default=None)
    p.add_argument("--partition", type=int, required=True)
    p.add_argument("--from-offset", type=int, required=True)
    p.add_argument("--to-offset", type=int, required=True)
    p.add_argument("--replay-run-id", default=None)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)

    settings = get_settings()
    topic = args.topic or settings.topic
    run_id = args.replay_run_id or f"replay-{uuid.uuid4().hex[:12]}"
    validator = EventValidator(
        Path(settings.entity_schema_path), Path(settings.run_started_schema_path)
    )
    validator.load()
    repo = ClickHouseRawRepository(settings)
    repo.connect()

    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"de-kafka-raw-replay-{run_id}",
            "client.id": f"{settings.client_id}-replay",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    tp = TopicPartition(topic, args.partition, args.from_offset)
    # check watermarks for retention
    lo, hi = c.get_watermark_offsets(tp, timeout=10.0)
    if args.from_offset < lo:
        print(
            json.dumps(
                {
                    "status": "INSUFFICIENT_RETENTION",
                    "low": lo,
                    "high": hi,
                    "from_offset": args.from_offset,
                }
            )
        )
        c.close()
        return 2

    c.assign([tp])
    seen = []
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        if msg.partition() != args.partition:
            continue
        if msg.offset() < args.from_offset:
            continue
        if msg.offset() > args.to_offset:
            break
        ts_type, ts_ms = msg.timestamp()
        ts_name = {0: "NotAvailable", 1: "CreateTime", 2: "LogAppendTime"}.get(
            ts_type, "NotAvailable"
        )
        classified = validator.classify(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            value=msg.value() or b"",
            kafka_key=msg.key(),
            headers=msg.headers(),
            broker_timestamp_ms=ts_ms if ts_ms and ts_ms > 0 else None,
            broker_timestamp_type=ts_name,
        )
        if classified.destination == "RAW":
            repo.insert_raw([classified.row], replay_run_id=run_id)
        else:
            repo.insert_quarantine([classified.row], replay_run_id=run_id)
        seen.append(
            (
                msg.topic(),
                msg.partition(),
                msg.offset(),
                classified.row["raw_ingestion_id"],
                classified.row["payload_bytes_hash"],
                classified.destination,
            )
        )
        if msg.offset() >= args.to_offset:
            break

    c.close()
    manifest = {
        "replay_run_id": run_id,
        "topic": topic,
        "partition": args.partition,
        "from_offset": args.from_offset,
        "to_offset": args.to_offset,
        "record_count": len(seen),
        "status": "OK",
        "records": [
            {
                "topic": t,
                "partition": p,
                "offset": o,
                "raw_ingestion_id": rid,
                "payload_bytes_hash": h,
                "destination": d,
            }
            for t, p, o, rid, h, d in seen
        ],
    }
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
