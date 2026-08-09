"""Checkpoint schema / open tests."""
from __future__ import annotations

from pathlib import Path

from de.silver.checkpoint_store import SilverCheckpointStore


def test_checkpoint_schema_created(tmp_path: Path):
    db = tmp_path / "cp.sqlite3"
    store = SilverCheckpointStore(db)
    store.open()
    try:
        tables = {
            r[0]
            for r in store.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "silver_checkpoint" in tables
        assert "silver_instance_lock" in tables
        assert "silver_replay_manifest" in tables
        # no hidden event ledger
        assert "silver_processing_ledger" not in tables
        assert "silver_event_state" not in tables
        assert store.is_readable()
    finally:
        store.close()
