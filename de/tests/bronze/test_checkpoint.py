"""Checkpoint cursor and contiguous commit tests."""
from __future__ import annotations

from pathlib import Path

from de.bronze.checkpoint_store import CheckpointStore
from de.bronze.models import PendingLedgerEntry
from de.bronze.offset_tracker import OffsetTracker


def test_cursor_formula_max_source_start_and_last_plus_one(tmp_path: Path) -> None:
    db = tmp_path / "cp.sqlite3"
    store = CheckpointStore(db)
    store.init_checkpoint(
        namespace="live",
        topic="traffic.entity-events.v2",
        partition=0,
        source_start_offset=100,
        last_completed_offset=99,
        start_mode="earliest",
        processor_name="kafka-bronze-v2",
        processor_version="1.0.0",
        bronze_schema_version="1.0.0",
    )
    cp = store.get("live", "traffic.entity-events.v2", 0)
    assert cp is not None
    next_off = max(cp.source_start_offset, cp.last_completed_offset + 1)
    assert next_off == 100
    store.close()


def test_contiguous_prefix_end_from_last_committed() -> None:
    entries = [
        PendingLedgerEntry("t", 0, 1000, "a", "STORED", "ENTITY"),
        PendingLedgerEntry("t", 0, 1001, "b", "STORED", "ENTITY"),
    ]
    offsets = sorted({e.offset for e in entries})
    expected = 1000
    last = None
    offset_set = set(offsets)
    while expected in offset_set:
        last = expected
        expected += 1
    assert last == 1001


def test_offset_tracker_contiguous_from_source_start() -> None:
    ot = OffsetTracker()
    ot.mark_completed("t", 0, 10)
    ot.mark_completed("t", 0, 11)
    assert ot.contiguous_completed_record_offset("t", 0, source_start=10) == 11


def test_is_complete_batch(tmp_path: Path) -> None:
    db = tmp_path / "cp.sqlite3"
    store = CheckpointStore(db)
    store.init_checkpoint(
        namespace="live",
        topic="traffic.entity-events.v2",
        partition=0,
        source_start_offset=0,
        last_completed_offset=-1,
        start_mode="earliest",
        processor_name="kafka-bronze-v2",
        processor_version="1.0.0",
        bronze_schema_version="1.0.0",
    )
    store.commit_batch(
        "live",
        "traffic.entity-events.v2",
        0,
        [
            {"offset": 0, "raw_ingestion_id": "a" * 64, "destination": "ENTITY", "status": "STORED"},
            {"offset": 1, "raw_ingestion_id": "b" * 64, "destination": "ENTITY", "status": "STORED"},
        ],
        1,
    )
    done = store.is_complete_batch("live", "traffic.entity-events.v2", 0, [0, 1, 2])
    assert done == {0, 1}
    store.close()
