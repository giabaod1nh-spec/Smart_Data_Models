"""Repair missing primary Raw rows from retained Kafka offsets.

This is an operator recovery tool, not a second Raw consumer.  It reads Kafka
with manual assignment, classifies records with the canonical validator, and
fills only ``(topic, partition, offset)`` slots absent from Raw v2 or its
quarantine table.  The normal consumer group is never reset or committed by
this tool.  Run it only with the normal Raw consumer stopped.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.kafka_raw.clickhouse_repository import ClickHouseRawRepository  # noqa: E402
from de.kafka_raw.config import get_settings  # noqa: E402
from de.kafka_raw.ledger_store import LedgerStore, STATUS_QUARANTINED, STATUS_STORED  # noqa: E402
from de.kafka_raw.validator import EventValidator  # noqa: E402

log = logging.getLogger("de.tools.raw_gap_repair")
TOPIC = "traffic.entity-events.v2"


def _ranges(offsets: Iterable[int]) -> List[Tuple[int, int]]:
    """Return inclusive contiguous ranges, sorted and compacted."""
    values = sorted({int(x) for x in offsets})
    if not values:
        return []
    out: List[Tuple[int, int]] = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        out.append((start, previous))
        start = previous = value
    out.append((start, previous))
    return out


def _existing_offsets(repo: ClickHouseRawRepository, topic: str, partition: int) -> Set[int]:
    result = repo.client.query(
        f"""
        SELECT offset FROM {repo.database}.kafka_raw_events
        WHERE topic={{topic:String}} AND partition={{partition:Int32}}
        UNION DISTINCT
        SELECT offset FROM {repo.database}.kafka_quarantine_events
        WHERE topic={{topic:String}} AND partition={{partition:Int32}}
        ORDER BY offset
        """,
        parameters={"topic": topic, "partition": int(partition)},
    )
    return {int(row[0]) for row in result.result_rows}


def _classify_message(validator: EventValidator, msg: Any):
    ts_type, ts_ms = msg.timestamp()
    ts_name = {0: "NotAvailable", 1: "CreateTime", 2: "LogAppendTime"}.get(
        ts_type, "NotAvailable"
    )
    return validator.classify(
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        value=msg.value() or b"",
        kafka_key=msg.key(),
        headers=msg.headers(),
        broker_timestamp_ms=ts_ms if ts_ms and ts_ms > 0 else None,
        broker_timestamp_type=ts_name,
    )


def repair_partition(
    *,
    consumer: Any,
    repo: ClickHouseRawRepository,
    ledger: LedgerStore,
    validator: EventValidator,
    topic: str,
    partition: int,
    apply: bool,
    batch_size: int,
    from_offset: int | None = None,
    to_offset: int | None = None,
) -> Dict[str, Any]:
    from confluent_kafka import TopicPartition

    probe = TopicPartition(topic, int(partition))
    low, high = consumer.get_watermark_offsets(probe, timeout=10.0)
    start = low if from_offset is None else int(from_offset)
    end = high if to_offset is None else min(int(to_offset), high)
    if start < low or start > end:
        raise ValueError(
            f"partition {partition}: requested range [{start},{end}) outside Kafka [{low},{high})"
        )

    existing = _existing_offsets(repo, topic, partition)
    missing: Set[int] = set()
    raw_rows: List[Dict[str, Any]] = []
    quarantine_rows: List[Dict[str, Any]] = []
    repaired = 0
    scanned = 0

    def flush_pending() -> int:
        nonlocal raw_rows, quarantine_rows
        if not apply:
            raw_rows = []
            quarantine_rows = []
            return 0
        if raw_rows:
            repo.insert_raw(raw_rows)
        if quarantine_rows:
            repo.insert_quarantine(quarantine_rows)
        ledger_rows = [
            {
                **row,
                "destination": "RAW",
                "status": STATUS_STORED,
            }
            for row in raw_rows
        ] + [
            {
                **row,
                "destination": "QUARANTINE",
                "status": STATUS_QUARANTINED,
            }
            for row in quarantine_rows
        ]
        ledger.mark_complete_batch(ledger_rows)
        count = len(ledger_rows)
        raw_rows = []
        quarantine_rows = []
        return count

    consumer.assign([TopicPartition(topic, int(partition), start)])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            if scanned >= end - start:
                break
            continue
        if msg.error():
            raise RuntimeError(str(msg.error()))
        if msg.partition() != partition:
            continue
        offset = int(msg.offset())
        if offset >= end:
            break
        scanned += 1
        if offset in existing:
            continue
        classified = _classify_message(validator, msg)
        missing.add(offset)
        if classified.destination == "RAW":
            raw_rows.append(classified.row)
        else:
            quarantine_rows.append(classified.row)

        if len(raw_rows) + len(quarantine_rows) >= batch_size:
            repaired += flush_pending()

    if apply and (raw_rows or quarantine_rows):
        repaired += flush_pending()

    return {
        "partition": int(partition),
        "kafka_low_watermark": int(low),
        "kafka_high_watermark": int(high),
        "range_start": int(start),
        "range_end_exclusive": int(end),
        "records_scanned": int(scanned),
        "missing_offsets": len(missing),
        "missing_ranges": [
            {"start": int(a), "end_inclusive": int(b)} for a, b in _ranges(missing)
        ],
        "repaired_records": int(repaired),
        "apply": bool(apply),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partitions", default="0,1,2")
    parser.add_argument("--from-offset", type=int, default=None)
    parser.add_argument("--to-offset", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--apply", action="store_true", help="write missing rows; default is dry-run")
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    settings = get_settings()
    topic = settings.topic or TOPIC
    validator = EventValidator(Path(settings.entity_schema_path), Path(settings.run_started_schema_path))
    validator.load()
    repo = ClickHouseRawRepository(settings)
    ledger = LedgerStore(Path(settings.ledger_path))
    repo.connect()
    if not repo.verify_tables():
        raise SystemExit("Raw tables are not available")

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"raw-gap-repair-{int(time.time())}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        reports = [
            repair_partition(
                consumer=consumer,
                repo=repo,
                ledger=ledger,
                validator=validator,
                topic=topic,
                partition=int(partition),
                apply=args.apply,
                batch_size=args.batch_size,
                from_offset=args.from_offset,
                to_offset=args.to_offset,
            )
            for partition in args.partitions.split(",")
            if partition.strip()
        ]
        report = {"topic": topic, "apply": bool(args.apply), "partitions": reports}
        print(json.dumps(report, indent=2))
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 0
    finally:
        consumer.close()
        repo.close()
        ledger.close()


if __name__ == "__main__":
    raise SystemExit(main())
