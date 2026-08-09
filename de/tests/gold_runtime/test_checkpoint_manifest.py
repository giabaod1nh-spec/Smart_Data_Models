"""Dimension lineage hash, CAS checkpoint, manifest reconciliation."""
from __future__ import annotations

from de.gold_runtime.checkpoint_store import GoldRuntimeStore
from de.gold_runtime.config import SOURCE_TABLE_DIM_RUN, SOURCE_TABLE_TRAFFIC, CasResult
from de.gold_runtime.cursor import ZERO_FACT_CURSOR, FactCursor
from de.gold_runtime.dimensions import lineage_hash
from de.gold_runtime.processing_ledger import (
    ExpectedOutputManifest,
    ManifestEntry,
    ReconcileStatus,
)
from de.gold_runtime.repositories import ExistingRow, ExistingState
from de.tests.gold_runtime.conftest import DIM_ROWS, NOW


def test_dimension_hash_is_deterministic():
    row = dict(DIM_ROWS[SOURCE_TABLE_DIM_RUN][0])
    first = lineage_hash(SOURCE_TABLE_DIM_RUN, row)
    second = lineage_hash(SOURCE_TABLE_DIM_RUN, row)
    assert first == second
    assert len(first) == 64
    row2 = dict(row)
    row2["seed"] = "99"
    assert lineage_hash(SOURCE_TABLE_DIM_RUN, row2) != first


def test_checkpoint_cas_advance_and_conflict(tmp_path):
    store = GoldRuntimeStore(tmp_path / "cp.sqlite3")
    store.open()
    try:
        store.initialize_cursor("live", SOURCE_TABLE_TRAFFIC, ZERO_FACT_CURSOR.to_json())
        row = store.get_cursor("live", SOURCE_TABLE_TRAFFIC)
        assert row is not None
        new_cursor = FactCursor(
            processed_at=NOW,
            source_topic="t",
            source_partition=0,
            source_offset=1,
            source_payload_hash="aa",
        )
        advanced = store.compare_and_advance_cursor(
            "live",
            SOURCE_TABLE_TRAFFIC,
            expected_generation=row.generation,
            cursor_json=new_cursor.to_json(),
        )
        assert advanced == CasResult.ADVANCED
        already = store.compare_and_advance_cursor(
            "live",
            SOURCE_TABLE_TRAFFIC,
            expected_generation=row.generation,
            cursor_json=new_cursor.to_json(),
        )
        assert already == CasResult.ALREADY_ADVANCED
        current = store.get_cursor("live", SOURCE_TABLE_TRAFFIC)
        assert current is not None
        assert current.generation == row.generation + 1
    finally:
        store.close()


def test_manifest_identity_set_equality():
    entry_a = ManifestEntry(
        target_table="gold_fact_traffic_window",
        logical_identity=("live", "run", "sc", "J1", "N", "wid"),
        source_set_hash="a" * 64,
        revision_seq=0,
        payload_digest="d1",
    )
    entry_b = ManifestEntry(
        target_table="gold_fact_traffic_window",
        logical_identity=("live", "run", "sc", "J1", "S", "wid"),
        source_set_hash="a" * 64,
        revision_seq=0,
        payload_digest="d2",
    )
    manifest = ExpectedOutputManifest(
        batch_id="batch-1",
        namespace="live",
        window_id="wid",
        revision_seq=0,
        entries=(entry_a, entry_b),
    )
    complete = ExistingState(
        "batch-1",
        (
            ExistingRow(entry_a.logical_identity, entry_a.source_set_hash, 0),
            ExistingRow(entry_b.logical_identity, entry_b.source_set_hash, 0),
        ),
    )
    assert manifest.reconcile(complete).status == ReconcileStatus.DURABLE
    partial = ExistingState(
        "batch-1",
        (ExistingRow(entry_a.logical_identity, entry_a.source_set_hash, 0),),
    )
    assert manifest.reconcile(partial).status == ReconcileStatus.MISSING
    conflicted = ExistingState(
        "batch-1",
        (
            ExistingRow(entry_a.logical_identity, "b" * 64, 0),
            ExistingRow(entry_b.logical_identity, entry_b.source_set_hash, 0),
        ),
    )
    assert manifest.reconcile(conflicted).status == ReconcileStatus.CONFLICTED
