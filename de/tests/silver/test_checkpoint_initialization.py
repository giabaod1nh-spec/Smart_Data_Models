"""Checkpoint initialization and namespace isolation."""
from __future__ import annotations

from pathlib import Path

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import CheckpointKey


def _key(ns: str, table: str = "bronze_entity_events", part: int = 0) -> CheckpointKey:
    return CheckpointKey(ns, table, "traffic.entity-events.v2", part)


def test_initialize_idempotent(tmp_path: Path):
    store = SilverCheckpointStore(tmp_path / "cp.sqlite3")
    store.open()
    try:
        k = _key("live")
        r1 = store.initialize(
            k,
            source_start=10,
            last_completed=9,
            start_mode="earliest",
            processor_name="p",
            processor_version="1",
            silver_schema_version="k9-silver-v1",
        )
        r2 = store.initialize(
            k,
            source_start=10,
            last_completed=9,
            start_mode="earliest",
            processor_name="p",
            processor_version="1",
            silver_schema_version="k9-silver-v1",
        )
        assert r1.last_completed_offset == 9
        assert r2.last_completed_offset == 9
        assert store.get(k) is not None
    finally:
        store.close()


def test_namespace_isolation(tmp_path: Path):
    store = SilverCheckpointStore(tmp_path / "cp.sqlite3")
    store.open()
    try:
        live = _key("live")
        replay = _key("replay:r1")
        store.initialize(
            live,
            source_start=0,
            last_completed=-1,
            start_mode="earliest",
            processor_name="p",
            processor_version="1",
            silver_schema_version="k9-silver-v1",
        )
        store.initialize(
            replay,
            source_start=5,
            last_completed=4,
            start_mode="explicit",
            processor_name="p",
            processor_version="1",
            silver_schema_version="k9-silver-v1",
        )
        store.compare_and_advance(live, -1, 3)
        assert store.get(live).last_completed_offset == 3
        assert store.get(replay).last_completed_offset == 4
    finally:
        store.close()
