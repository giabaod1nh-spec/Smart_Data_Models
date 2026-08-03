"""Silver Plan 3 — semantic ledger comparison, target matrix, recovery classification.

Implements Plan 3 §11/§14/§15/§20, clarified by §32.2, §32.5, §32.6:

- semantic ledger identity is ``(checkpoint_namespace, source_bronze_event_id)`` and
  compatibility comparison deliberately excludes ``raw_ingestion_id``;
- ``target_table`` is resolved by a fixed disposition/mode matrix, never interpolated;
- ``IDEMPOTENT_SKIPPED`` is only produced for a ledger row that already existed *before*
  the current batch was selected; a partial-recovery completion (outputs existed, ledger
  did not) is ``PROCESSED``/``QUARANTINED`` with ``recovered_partial=True`` instead.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from de.silver.contracts import (
    DISPOSITION_DOCUMENTED_SKIP,
    DISPOSITION_PROCESSED,
    DISPOSITION_QUARANTINED,
)
from de.silver.repositories import FACT_REPLAY_MAP, QUARANTINE_REPLAY_TABLE, QUARANTINE_TABLE


class LedgerConflictError(Exception):
    """Same (namespace, source_id) has incompatible payload/disposition/target; permanent."""


@dataclass(frozen=True)
class LedgerEntryState:
    """A semantic ledger row — existing or about to be materialized."""

    checkpoint_namespace: str
    source_bronze_event_id: str
    raw_ingestion_id: str
    payload_hash: str
    disposition: str
    target_table: str
    processed_at: Optional[Any] = None


class LedgerClassification(str, Enum):
    NEW = "NEW"
    COMPATIBLE_TERMINAL = "COMPATIBLE_TERMINAL"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class LedgerClassificationResult:
    classification: LedgerClassification
    existing: Optional[LedgerEntryState]


def is_semantically_compatible(existing: LedgerEntryState, candidate: LedgerEntryState) -> bool:
    """§32.2 — ``raw_ingestion_id`` is excluded from semantic identity/compatibility."""
    return (
        existing.checkpoint_namespace == candidate.checkpoint_namespace
        and existing.source_bronze_event_id == candidate.source_bronze_event_id
        and existing.payload_hash == candidate.payload_hash
        and existing.disposition == candidate.disposition
        and existing.target_table == candidate.target_table
    )


def classify_existing_ledger(
    existing: Optional[LedgerEntryState],
    candidate: LedgerEntryState,
) -> LedgerClassificationResult:
    if existing is None:
        return LedgerClassificationResult(LedgerClassification.NEW, None)
    if is_semantically_compatible(existing, candidate):
        return LedgerClassificationResult(LedgerClassification.COMPATIBLE_TERMINAL, existing)
    return LedgerClassificationResult(LedgerClassification.CONFLICT, existing)


def resolve_target_table(
    *, disposition: str, is_replay: bool, primary_fact_table: Optional[str] = None
) -> str:
    """§32.6 ledger target-table matrix. Frozen dispatch only — never interpolated."""
    if disposition == DISPOSITION_PROCESSED:
        if not primary_fact_table:
            raise ValueError("PROCESSED disposition requires primary_fact_table")
        if is_replay:
            replay = FACT_REPLAY_MAP.get(primary_fact_table)
            if not replay:
                raise ValueError(f"No replay target for {primary_fact_table!r}")
            return replay
        return primary_fact_table
    if disposition == DISPOSITION_QUARANTINED:
        return QUARANTINE_REPLAY_TABLE if is_replay else QUARANTINE_TABLE
    if disposition == DISPOSITION_DOCUMENTED_SKIP:
        return "NONE"
    raise ValueError(f"Unsupported disposition for target resolution: {disposition!r}")


@dataclass(frozen=True)
class MaterializedLedgerEntry:
    entry: LedgerEntryState
    disposition_reason: str
    recovered_partial: bool
    idempotent_observed: bool


def materialize_ledger_entry(
    *,
    namespace: str,
    source_bronze_event_id: str,
    raw_ingestion_id: str,
    payload_hash: str,
    proposed_disposition: str,
    is_replay: bool,
    primary_fact_table: Optional[str],
    existing_before_batch: Optional[LedgerEntryState],
    outputs_recovered_from_prior_attempt: bool,
    processed_at: Any,
) -> MaterializedLedgerEntry:
    """Decide the semantic ledger row to persist/observe for one source event.

    ``existing_before_batch`` must be the ledger row fetched *before* this batch began
    processing — only its presence (and compatibility) yields ``IDEMPOTENT_SKIPPED``.
    """
    if existing_before_batch is not None:
        candidate_target = existing_before_batch.target_table
    else:
        candidate_target = resolve_target_table(
            disposition=proposed_disposition,
            is_replay=is_replay,
            primary_fact_table=primary_fact_table,
        )

    candidate = LedgerEntryState(
        checkpoint_namespace=namespace,
        source_bronze_event_id=source_bronze_event_id,
        raw_ingestion_id=raw_ingestion_id,
        payload_hash=payload_hash,
        disposition=proposed_disposition,
        target_table=candidate_target,
        processed_at=processed_at,
    )

    classification = classify_existing_ledger(existing_before_batch, candidate)
    if classification.classification == LedgerClassification.CONFLICT:
        raise LedgerConflictError(
            f"Incompatible ledger row for {namespace}/{source_bronze_event_id}: "
            f"existing={existing_before_batch} candidate={candidate}"
        )
    if classification.classification == LedgerClassification.COMPATIBLE_TERMINAL:
        assert existing_before_batch is not None
        return MaterializedLedgerEntry(
            entry=existing_before_batch,
            disposition_reason="IDEMPOTENT_SKIPPED",
            recovered_partial=False,
            idempotent_observed=True,
        )

    if outputs_recovered_from_prior_attempt:
        reason = "RECOVERED_PARTIAL"
    elif proposed_disposition == DISPOSITION_PROCESSED:
        reason = "NEW_PROCESSED"
    elif proposed_disposition == DISPOSITION_QUARANTINED:
        reason = "NEW_QUARANTINED"
    else:
        reason = "NEW"

    return MaterializedLedgerEntry(
        entry=candidate,
        disposition_reason=reason,
        recovered_partial=outputs_recovered_from_prior_attempt,
        idempotent_observed=False,
    )
