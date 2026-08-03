"""Physical duplicate collapse: same (topic,partition,offset)+hash, different raw ID
(Plan 3 §6.4 / §32.2 / §32.9)."""
from __future__ import annotations

from datetime import datetime, timezone

from de.silver.config import SilverSettings, SourceStream
from de.silver.readers import BronzeReader
from de.tests.silver.conftest import FakeClient, FakeQueryResult

T1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc)

RUN_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "event_type", "contract_version",
    "source", "producer_id", "producer_session_id", "simulation_run_id", "started_at",
    "scenario_id", "event_payload_json", "bronze_canonical_hash", "processed_at",
    "bronze_ingestion_id",
]


def _row(offset, raw_id, canon, processed_at, bronze_ingestion_id):
    return (
        "t1", 0, offset, raw_id, "TrafficSimulationRunStarted", "2.0.0", "sumo",
        "producer-a", "session-1", "run-1", T1, "normal", "{}", canon,
        processed_at, bronze_ingestion_id,
    )


def test_same_hash_different_raw_ingestion_id_collapses_keeping_latest_processed_at():
    rows = [
        _row(1, "raw-old", "canon-A", T1, "bing-1"),
        _row(1, "raw-new", "canon-A", T2, "bing-2"),
    ]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert len(records) == 1
    assert records[0].raw_ingestion_id == "raw-new"
    assert receipt.logical_count == 1
    assert receipt.physical_count == 2
    assert receipt.duplicate_count == 1


def test_same_hash_same_processed_at_tiebreaks_on_max_bronze_ingestion_id():
    rows = [
        _row(1, "raw-a", "canon-A", T1, "bing-1"),
        _row(1, "raw-b", "canon-A", T1, "bing-9"),
    ]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert records[0].raw_ingestion_id == "raw-b"
    assert receipt.duplicate_count == 1


def test_three_way_duplicate_collapses_to_one_logical_record():
    rows = [
        _row(1, "raw-a", "canon-A", T1, "bing-1"),
        _row(1, "raw-b", "canon-A", T2, "bing-2"),
        _row(1, "raw-c", "canon-A", T1, "bing-3"),
    ]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert len(records) == 1
    assert records[0].raw_ingestion_id == "raw-b"  # latest processed_at (T2) wins
    assert receipt.physical_count == 3
    assert receipt.logical_count == 1
    assert receipt.duplicate_count == 2


def test_no_duplicates_across_distinct_offsets():
    rows = [
        _row(1, "raw-a", "canon-A", T1, "bing-1"),
        _row(2, "raw-b", "canon-B", T1, "bing-2"),
    ]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert len(records) == 2
    assert receipt.duplicate_count == 0
    assert receipt.physical_count == 2
    assert receipt.logical_count == 2
