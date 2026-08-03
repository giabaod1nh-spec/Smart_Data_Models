"""Different canonical hash at the same source key is a permanent SOURCE_OFFSET_CONFLICT
(Plan 3 §6.4 / §32.2)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from de.silver.config import SilverSettings, SourceStream
from de.silver.readers import BronzeReader
from de.silver.repositories import SourceOffsetConflictError
from de.tests.silver.conftest import FakeClient, FakeQueryResult

T1 = datetime(2026, 1, 1, tzinfo=timezone.utc)

RUN_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "event_type", "contract_version",
    "source", "producer_id", "producer_session_id", "simulation_run_id", "started_at",
    "scenario_id", "event_payload_json", "bronze_canonical_hash", "processed_at",
    "bronze_ingestion_id",
]


def _row(offset, raw_id, canon, bronze_ingestion_id):
    return (
        "t1", 0, offset, raw_id, "TrafficSimulationRunStarted", "2.0.0", "sumo",
        "producer-a", "session-1", "run-1", T1, "normal", "{}", canon, T1, bronze_ingestion_id,
    )


def test_conflicting_canonical_hash_raises():
    rows = [
        _row(1, "raw-a", "canon-A", "bing-1"),
        _row(1, "raw-b", "canon-B", "bing-2"),
    ]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    with pytest.raises(SourceOffsetConflictError):
        reader.fetch_batch(stream, after_offset=0, limit=10)


def test_conflict_detected_even_when_other_offsets_are_clean():
    rows = [
        _row(1, "raw-a", "canon-A", "bing-1"),
        _row(2, "raw-b", "canon-B", "bing-2"),
        _row(2, "raw-c", "canon-C", "bing-3"),
    ]
    client = FakeClient(responses=[FakeQueryResult(rows, RUN_COLS)])
    reader = BronzeReader(SilverSettings(), client=client)
    stream = SourceStream("bronze_run_events", "t1", 0)

    with pytest.raises(SourceOffsetConflictError):
        reader.fetch_batch(stream, after_offset=0, limit=10)
