"""Sparse offset handling: ascending unique offsets, limit truncation, empty batch (Plan 3 §6.3/§9)."""
from __future__ import annotations

from datetime import datetime, timezone

from de.silver.config import SilverSettings, SourceStream
from de.silver.readers import BronzeReader
from de.tests.silver.conftest import FakeClient, FakeQueryResult

T1 = datetime(2026, 1, 1, tzinfo=timezone.utc)

RUN_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "event_type", "contract_version",
    "source", "producer_id", "producer_session_id", "simulation_run_id", "started_at",
    "scenario_id", "event_payload_json", "bronze_canonical_hash", "processed_at",
    "bronze_ingestion_id",
]


def _row(offset):
    return (
        "t1", 0, offset, f"raw-{offset}", "TrafficSimulationRunStarted", "2.0.0", "sumo",
        "producer-a", "session-1", "run-1", T1, "normal", "{}", f"canon-{offset}", T1,
        f"bing-{offset}",
    )


def test_sparse_offsets_preserved_in_ascending_order():
    rows = [_row(3), _row(7), _row(42)]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert [r.offset for r in records] == [3, 7, 42]
    assert receipt.first_offset == 3
    assert receipt.last_offset == 42
    assert receipt.logical_count == 3


def test_limit_truncates_to_requested_size_keeping_ascending_order():
    rows = [_row(o) for o in (1, 5, 9, 20, 33)]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=3)

    assert [r.offset for r in records] == [1, 5, 9]
    assert receipt.last_offset == 9
    assert receipt.logical_count == 3


def test_empty_result_returns_empty_receipt():
    client = FakeClient(responses=[FakeQueryResult([], RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    records, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert records == ()
    assert receipt.first_offset is None
    assert receipt.last_offset is None
    assert receipt.logical_count == 0
    assert receipt.physical_count == 0
    assert receipt.duplicate_count == 0


def test_cas_advances_to_last_returned_row_not_expected_plus_count():
    # Sparse offsets: 3 rows returned but max offset is 42, not after_offset(0) + count(3).
    rows = [_row(3), _row(7), _row(42)]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    _, receipt = reader.fetch_batch(stream, after_offset=0, limit=10)

    assert receipt.last_offset == 42
    assert receipt.last_offset != 0 + receipt.logical_count
