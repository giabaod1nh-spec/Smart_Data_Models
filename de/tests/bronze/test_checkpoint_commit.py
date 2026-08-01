"""Checkpoint commit_batch SQL placeholder count."""
from __future__ import annotations

from de.bronze.checkpoint_store import CheckpointStore


def test_commit_batch_inserts_ledger(tmp_path) -> None:
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
            {
                "offset": 0,
                "raw_ingestion_id": "a" * 64,
                "destination": "RAW_QUARANTINE",
                "status": "RAW_QUARANTINE_SKIPPED",
                "payload_hash": "b" * 64,
            }
        ],
        0,
    )
    assert store.is_complete("live", "traffic.entity-events.v2", 0, 0)
    cp = store.get("live", "traffic.entity-events.v2", 0)
    assert cp is not None and cp.last_completed_offset == 0
    store.close()
