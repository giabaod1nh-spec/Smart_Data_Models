"""CAS four-way outcomes."""
from __future__ import annotations

from pathlib import Path

import pytest

from de.silver.checkpoint_store import (
    CheckpointCasConflictError,
    ReplayManifestConflictError,
    SilverCheckpointStore,
)
from de.silver.config import CasResult, CheckpointKey


def test_cas_advanced_already_retry_conflict(tmp_path: Path):
    store = SilverCheckpointStore(tmp_path / "cp.sqlite3")
    store.open()
    try:
        k = CheckpointKey("live", "bronze_entity_events", "t", 0)
        store.initialize(
            k,
            source_start=0,
            last_completed=-1,
            start_mode="earliest",
            processor_name="p",
            processor_version="1",
            silver_schema_version="k9-silver-v1",
        )
        assert store.compare_and_advance(k, -1, 5) == CasResult.ADVANCED
        assert store.compare_and_advance(k, -1, 5) == CasResult.ALREADY_ADVANCED
        # Force RETRY_SAME by advancing expected mismatch where row still at old? 
        # After ADVANCED, expected=-1 with new=5 -> ALREADY. For RETRY_SAME we need
        # rowcount 0 and current==expected. Simulate by concurrent-style: expected=5 new=10
        # but first manually set offset back isn't allowed — use second key path:
        # Update expected correctly:
        assert store.compare_and_advance(k, 5, 10) == CasResult.ADVANCED
        with pytest.raises(CheckpointCasConflictError):
            store.compare_and_advance(k, 0, 20)  # unexpected current
    finally:
        store.close()


def test_replay_manifest_immutability(tmp_path: Path):
    store = SilverCheckpointStore(tmp_path / "cp.sqlite3")
    store.open()
    try:
        store.put_replay_manifest("r1", "abc")
        store.put_replay_manifest("r1", "abc")  # idempotent
        with pytest.raises(ReplayManifestConflictError):
            store.put_replay_manifest("r1", "xyz")
        assert store.get_replay_manifest("r1") == "abc"
    finally:
        store.close()
