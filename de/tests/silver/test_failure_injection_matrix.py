"""Failure-injection / CAS / ledger conflict tests (real SQLite + pure ledger helpers)."""
from __future__ import annotations

from pathlib import Path

import pytest

from de.silver.batch_ledger import (
    LedgerConflictError,
    LedgerEntryState,
    materialize_ledger_entry,
)
from de.silver.checkpoint_store import CheckpointCasConflictError, SilverCheckpointStore
from de.silver.config import CheckpointKey
from de.silver.contracts import DISPOSITION_PROCESSED


def test_cas_conflict_fault_path(tmp_path: Path):
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
        store.compare_and_advance(k, -1, 5)
        with pytest.raises(CheckpointCasConflictError):
            store.compare_and_advance(k, 1, 9)
    finally:
        store.close()


def test_ledger_conflict_before_checkpoint():
    existing = LedgerEntryState(
        checkpoint_namespace="live",
        source_bronze_event_id="e",
        raw_ingestion_id="r",
        payload_hash="h1",
        disposition=DISPOSITION_PROCESSED,
        target_table="silver_fact_traffic_observation",
    )
    with pytest.raises(LedgerConflictError):
        materialize_ledger_entry(
            namespace="live",
            source_bronze_event_id="e",
            raw_ingestion_id="r",
            payload_hash="h2",
            proposed_disposition=DISPOSITION_PROCESSED,
            is_replay=False,
            primary_fact_table="silver_fact_traffic_observation",
            existing_before_batch=existing,
            outputs_recovered_from_prior_attempt=False,
            processed_at=None,
        )


def test_cas_already_advanced_after_success(tmp_path: Path):
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
        from de.silver.config import CasResult

        assert store.compare_and_advance(k, -1, 5) == CasResult.ADVANCED
        assert store.compare_and_advance(k, -1, 5) == CasResult.ALREADY_ADVANCED
    finally:
        store.close()
