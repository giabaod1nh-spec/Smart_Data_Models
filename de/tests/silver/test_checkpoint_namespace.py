"""Checkpoint namespace file alias for suite naming."""
from __future__ import annotations

from pathlib import Path

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import CheckpointKey


def test_live_and_test_namespaces_do_not_collide(tmp_path: Path):
    store = SilverCheckpointStore(tmp_path / "cp.sqlite3")
    store.open()
    try:
        for ns, start in (("live", 0), ("test:unit1", 100)):
            k = CheckpointKey(ns, "bronze_run_events", "t", 1)
            store.initialize(
                k,
                source_start=start,
                last_completed=start - 1,
                start_mode="earliest" if ns == "live" else "explicit",
                processor_name="p",
                processor_version="1",
                silver_schema_version="k9-silver-v1",
            )
        live = store.get(CheckpointKey("live", "bronze_run_events", "t", 1))
        test = store.get(CheckpointKey("test:unit1", "bronze_run_events", "t", 1))
        assert live is not None and test is not None
        assert live.source_start_offset != test.source_start_offset
    finally:
        store.close()
