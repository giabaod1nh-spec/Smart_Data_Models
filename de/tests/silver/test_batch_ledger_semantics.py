"""Semantic ledger identity, target matrix, and recovery classification
(Plan 3 §11/§14/§15/§20, §32.2/§32.5/§32.6)."""
from __future__ import annotations

import pytest

from de.silver.batch_ledger import (
    LedgerClassification,
    LedgerConflictError,
    LedgerEntryState,
    classify_existing_ledger,
    is_semantically_compatible,
    materialize_ledger_entry,
    resolve_target_table,
)
from de.silver.contracts import (
    DISPOSITION_DOCUMENTED_SKIP,
    DISPOSITION_PROCESSED,
    DISPOSITION_QUARANTINED,
)


def _entry(**overrides) -> LedgerEntryState:
    base = dict(
        checkpoint_namespace="live",
        source_bronze_event_id="evt-1",
        raw_ingestion_id="raw-a",
        payload_hash="hash-1",
        disposition=DISPOSITION_PROCESSED,
        target_table="silver_fact_traffic_observation",
    )
    base.update(overrides)
    return LedgerEntryState(**base)


def test_identity_excludes_raw_ingestion_id():
    existing = _entry(raw_ingestion_id="raw-a")
    candidate = _entry(raw_ingestion_id="raw-b")
    assert is_semantically_compatible(existing, candidate)
    result = classify_existing_ledger(existing, candidate)
    assert result.classification == LedgerClassification.COMPATIBLE_TERMINAL


def test_new_when_no_existing_row():
    result = classify_existing_ledger(None, _entry())
    assert result.classification == LedgerClassification.NEW
    assert result.existing is None


def test_payload_hash_conflict():
    existing = _entry(payload_hash="hash-1")
    candidate = _entry(payload_hash="hash-2")
    result = classify_existing_ledger(existing, candidate)
    assert result.classification == LedgerClassification.CONFLICT


def test_disposition_conflict():
    existing = _entry(disposition=DISPOSITION_PROCESSED)
    candidate = _entry(disposition=DISPOSITION_QUARANTINED)
    result = classify_existing_ledger(existing, candidate)
    assert result.classification == LedgerClassification.CONFLICT


def test_target_table_conflict():
    existing = _entry(target_table="silver_fact_traffic_observation")
    candidate = _entry(target_table="silver_fact_signal_state")
    result = classify_existing_ledger(existing, candidate)
    assert result.classification == LedgerClassification.CONFLICT


def test_different_namespace_is_not_compatible():
    existing = _entry(checkpoint_namespace="live")
    candidate = _entry(checkpoint_namespace="replay:r1")
    assert not is_semantically_compatible(existing, candidate)


@pytest.mark.parametrize(
    "disposition,is_replay,primary,expected",
    [
        (DISPOSITION_PROCESSED, False, "silver_fact_run_event", "silver_fact_run_event"),
        (DISPOSITION_PROCESSED, True, "silver_fact_run_event", "silver_fact_run_event_replay"),
        (DISPOSITION_QUARANTINED, False, None, "silver_quarantine"),
        (DISPOSITION_QUARANTINED, True, None, "silver_quarantine_replay"),
        (DISPOSITION_DOCUMENTED_SKIP, False, None, "NONE"),
        (DISPOSITION_DOCUMENTED_SKIP, True, None, "NONE"),
    ],
)
def test_target_matrix(disposition, is_replay, primary, expected):
    assert (
        resolve_target_table(disposition=disposition, is_replay=is_replay, primary_fact_table=primary)
        == expected
    )


def test_target_matrix_processed_requires_primary_table():
    with pytest.raises(ValueError):
        resolve_target_table(disposition=DISPOSITION_PROCESSED, is_replay=False, primary_fact_table=None)


def test_target_matrix_no_replay_mirror_for_run_event_fails_loud():
    # All five fact tables do have replay mirrors; assert the matrix rejects unknown names instead.
    with pytest.raises(ValueError):
        resolve_target_table(
            disposition=DISPOSITION_PROCESSED, is_replay=True, primary_fact_table="not_a_table"
        )


def test_idempotent_skipped_only_for_pre_existing_terminal():
    existing = _entry()
    result = materialize_ledger_entry(
        namespace="live",
        source_bronze_event_id="evt-1",
        raw_ingestion_id="raw-new",
        payload_hash="hash-1",
        proposed_disposition=DISPOSITION_PROCESSED,
        is_replay=False,
        primary_fact_table="silver_fact_traffic_observation",
        existing_before_batch=existing,
        outputs_recovered_from_prior_attempt=False,
        processed_at=None,
    )
    assert result.disposition_reason == "IDEMPOTENT_SKIPPED"
    assert result.idempotent_observed is True
    assert result.recovered_partial is False
    assert result.entry.target_table == "silver_fact_traffic_observation"
    assert result.entry.raw_ingestion_id == "raw-a"  # existing lineage retained, not overwritten


def test_partial_recovery_is_processed_not_idempotent():
    result = materialize_ledger_entry(
        namespace="live",
        source_bronze_event_id="evt-2",
        raw_ingestion_id="raw-a",
        payload_hash="hash-2",
        proposed_disposition=DISPOSITION_PROCESSED,
        is_replay=False,
        primary_fact_table="silver_fact_signal_state",
        existing_before_batch=None,
        outputs_recovered_from_prior_attempt=True,
        processed_at="2026-01-01T00:00:00Z",
    )
    assert result.disposition_reason == "RECOVERED_PARTIAL"
    assert result.recovered_partial is True
    assert result.idempotent_observed is False
    assert result.entry.disposition == DISPOSITION_PROCESSED
    assert result.entry.target_table == "silver_fact_signal_state"


def test_partial_recovery_quarantined():
    result = materialize_ledger_entry(
        namespace="live",
        source_bronze_event_id="evt-2b",
        raw_ingestion_id="raw-a",
        payload_hash="hash-2b",
        proposed_disposition=DISPOSITION_QUARANTINED,
        is_replay=False,
        primary_fact_table=None,
        existing_before_batch=None,
        outputs_recovered_from_prior_attempt=True,
        processed_at="2026-01-01T00:00:00Z",
    )
    assert result.disposition_reason == "RECOVERED_PARTIAL"
    assert result.recovered_partial is True
    assert result.entry.target_table == "silver_quarantine"


def test_new_first_time_processing():
    result = materialize_ledger_entry(
        namespace="live",
        source_bronze_event_id="evt-3",
        raw_ingestion_id="raw-a",
        payload_hash="hash-3",
        proposed_disposition=DISPOSITION_QUARANTINED,
        is_replay=False,
        primary_fact_table=None,
        existing_before_batch=None,
        outputs_recovered_from_prior_attempt=False,
        processed_at="2026-01-01T00:00:00Z",
    )
    assert result.disposition_reason == "NEW_QUARANTINED"
    assert result.recovered_partial is False
    assert result.idempotent_observed is False
    assert result.entry.target_table == "silver_quarantine"


def test_new_first_time_processed_replay():
    result = materialize_ledger_entry(
        namespace="replay:r1",
        source_bronze_event_id="evt-4",
        raw_ingestion_id="raw-a",
        payload_hash="hash-4",
        proposed_disposition=DISPOSITION_PROCESSED,
        is_replay=True,
        primary_fact_table="silver_fact_camera_observation",
        existing_before_batch=None,
        outputs_recovered_from_prior_attempt=False,
        processed_at=None,
    )
    assert result.disposition_reason == "NEW_PROCESSED"
    assert result.entry.target_table == "silver_fact_camera_observation_replay"


def test_existing_terminal_target_retained_never_rewritten():
    existing = _entry(target_table="silver_fact_traffic_observation")
    result = materialize_ledger_entry(
        namespace="live",
        source_bronze_event_id="evt-1",
        raw_ingestion_id="raw-different",
        payload_hash="hash-1",
        proposed_disposition=DISPOSITION_PROCESSED,
        is_replay=True,  # even if runtime mode differs, the retained target wins
        primary_fact_table="silver_fact_traffic_observation",
        existing_before_batch=existing,
        outputs_recovered_from_prior_attempt=False,
        processed_at=None,
    )
    assert result.entry.target_table == "silver_fact_traffic_observation"
    assert result.disposition_reason == "IDEMPOTENT_SKIPPED"


def test_conflicting_ledger_raises():
    existing = _entry(payload_hash="hash-1")
    with pytest.raises(LedgerConflictError):
        materialize_ledger_entry(
            namespace="live",
            source_bronze_event_id="evt-1",
            raw_ingestion_id="raw-a",
            payload_hash="hash-DIFFERENT",
            proposed_disposition=DISPOSITION_PROCESSED,
            is_replay=False,
            primary_fact_table="silver_fact_traffic_observation",
            existing_before_batch=existing,
            outputs_recovered_from_prior_attempt=False,
            processed_at=None,
        )


def test_same_source_different_raw_ingestion_id_is_compatible_not_conflict():
    existing = _entry(raw_ingestion_id="raw-a")
    result = materialize_ledger_entry(
        namespace="live",
        source_bronze_event_id="evt-1",
        raw_ingestion_id="raw-different-but-same-position",
        payload_hash="hash-1",
        proposed_disposition=DISPOSITION_PROCESSED,
        is_replay=False,
        primary_fact_table="silver_fact_traffic_observation",
        existing_before_batch=existing,
        outputs_recovered_from_prior_attempt=False,
        processed_at=None,
    )
    assert result.idempotent_observed is True
    assert result.disposition_reason == "IDEMPOTENT_SKIPPED"
