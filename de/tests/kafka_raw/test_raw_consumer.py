"""Unit tests for K-4 Raw consumer (no live Kafka/CH required for core)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from de.kafka_raw.batch_buffer import BatchBufferManager, BufferedRecord  # noqa: E402
from de.kafka_raw.ingestion_id import payload_bytes_hash, raw_ingestion_id  # noqa: E402
from de.kafka_raw.ledger_store import STATUS_STORED, LedgerStore  # noqa: E402
from de.kafka_raw.offset_tracker import OffsetTracker  # noqa: E402
from de.kafka_raw.validator import EventValidator  # noqa: E402


@pytest.fixture
def validator():
    v = EventValidator(
        REPO / "contracts" / "events" / "traffic-entity-event-v2.schema.json",
        REPO / "contracts" / "events" / "traffic-simulation-run-started-v2.schema.json",
    )
    v.load()
    return v


def test_commit_n_plus_one():
    ot = OffsetTracker()
    for o in (10, 11, 12):
        ot.mark_completed("t", 0, o)
    n = ot.contiguous_completed_record_offset("t", 0)
    assert n == 12
    assert ot.kafka_commit_offset(n) == 13
    ot.advance_after_commit("t", 0, 12)
    assert ot.contiguous_completed_record_offset("t", 0) is None


def test_commit_gap_blocks():
    ot = OffsetTracker()
    ot.mark_completed("t", 0, 10)
    ot.mark_completed("t", 0, 12)
    n = ot.contiguous_completed_record_offset("t", 0)
    assert n == 10
    assert ot.kafka_commit_offset(n) == 11


def test_ingestion_id_stable():
    a = raw_ingestion_id("traffic.entity-events.v2", 1, 42)
    b = raw_ingestion_id("traffic.entity-events.v2", 1, 42)
    assert a == b
    assert len(a) == 64


def test_non_utf8_quarantine_base64(validator):
    raw = b"\xff\xfe not utf8"
    c = validator.classify(
        topic="t",
        partition=0,
        offset=1,
        value=raw,
        kafka_key=None,
        headers=None,
        broker_timestamp_ms=None,
        broker_timestamp_type="NotAvailable",
    )
    assert c.destination == "QUARANTINE"
    assert c.failure_stage == "DECODE"
    assert c.row["payload_encoding"] == "base64"
    assert c.row["canonical_payload_hash"] is None
    assert c.row["payload_bytes_hash"] == payload_bytes_hash(raw)


def test_run_started_raw(validator):
    example = json.loads(
        (
            REPO / "contracts" / "events" / "examples" / "run-started-event.json"
        ).read_text(encoding="utf-8")
    )
    raw = json.dumps(example).encode("utf-8")
    c = validator.classify(
        topic="t",
        partition=0,
        offset=2,
        value=raw,
        kafka_key=b"run:__run__",
        headers=None,
        broker_timestamp_ms=None,
        broker_timestamp_type="CreateTime",
    )
    assert c.destination == "RAW"
    assert c.row["entity_id"] is None
    assert c.row["canonical_payload_hash"] is not None
    assert len(c.row["canonical_payload_hash"]) == 64


def test_entity_event_example(validator):
    example = json.loads(
        (
            REPO / "contracts" / "events" / "examples" / "intersection-event.json"
        ).read_text(encoding="utf-8")
    )
    raw = json.dumps(example).encode("utf-8")
    c = validator.classify(
        topic="t",
        partition=0,
        offset=3,
        value=raw,
        kafka_key=None,
        headers=[("x", b"1")],
        broker_timestamp_ms=1_700_000_000_000,
        broker_timestamp_type="LogAppendTime",
    )
    assert c.destination == "RAW"
    assert c.row["payload_encoding"] == "utf8"
    assert c.row["payload_stored"] == raw.decode("utf-8")
    assert c.row["entity_id"]


def test_unknown_event_type_quarantine(validator):
    body = {"eventType": "Nope", "foo": 1}
    raw = json.dumps(body).encode("utf-8")
    c = validator.classify(
        topic="t",
        partition=0,
        offset=4,
        value=raw,
        kafka_key=None,
        headers=None,
        broker_timestamp_ms=None,
        broker_timestamp_type="NotAvailable",
    )
    assert c.destination == "QUARANTINE"
    assert c.failure_stage == "EVENT_TYPE"


def test_per_partition_buffers_independent():
    mgr = BatchBufferManager(batch_size=100, flush_ms=10_000)
    mgr.add(
        BufferedRecord("t", 0, 1, "a" * 64, "RAW", {"x": 1}, 10)
    )
    mgr.add(
        BufferedRecord("t", 1, 1, "b" * 64, "RAW", {"x": 2}, 10)
    )
    # force age flush artificially
    mgr.raw[( "t", 0)].first_at -= 10
    ready = mgr.ready_keys("RAW")
    assert ("t", 0) in ready
    popped = mgr.pop("RAW", ("t", 0))
    assert len(popped) == 1
    assert ("t", 1) in mgr.raw
    assert len(mgr.raw[("t", 1)].records) == 1


def test_ledger_no_pending_hot_path():
    with tempfile.TemporaryDirectory() as td:
        store = LedgerStore(Path(td) / "l.sqlite3")
        store.mark_complete(
            topic="t",
            partition=0,
            offset=5,
            raw_ingestion_id="c" * 64,
            destination="RAW",
            status=STATUS_STORED,
            payload_hash="d" * 64,
        )
        assert store.is_complete("t", 0, 5)
        assert store.max_completed_offset("t", 0) == 5
        store.close()


def test_schema_load_fail_readiness():
    v = EventValidator(Path("/no/such/entity.json"), Path("/no/such/run.json"))
    with pytest.raises(Exception):
        v.load()
    assert v.ready is False
