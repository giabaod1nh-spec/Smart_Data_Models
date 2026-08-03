"""Silver Plan 2 — TransformationEngine (pure in-memory orchestrator)."""
from __future__ import annotations

import hashlib
from typing import Optional

from de.silver.contracts import (
    DISPOSITION_PROCESSED,
    DISPOSITION_QUARANTINED,
    EVENT_RUN_STARTED,
    VALID_ENTITY_TYPES,
)
from de.silver.dimension_builders import DimensionCandidate, build_dimensions
from de.silver.fact_builders import build_fact
from de.silver.input_models import (
    BronzeEntityInputRecord,
    BronzeInputRecord,
    BronzeRunInputRecord,
)
from de.silver.models import (
    DispositionProposal,
    SilverQuarantineEntry,
    TransformationResult,
)
from de.silver.normalizers import normalize_fields
from de.silver.routers import resolve_route
from de.silver.unwrapper import parse_entity_payload, unwrap_all_fields
from de.silver.validators import validate_fields


def _deterministic_quarantine_id(
    source_bronze_event_id: str,
    failure_stage: str,
    error_code: str,
    payload_hash: str,
) -> str:
    material = f"{source_bronze_event_id}{failure_stage}{error_code}{payload_hash}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _identity(record: BronzeInputRecord) -> tuple[str, str, str, Optional[str], Optional[str], str]:
    """Return event_id, raw_ingestion_id, payload_hash, simulation_run_id, entity_id, entity_type, source_payload."""
    if isinstance(record, BronzeEntityInputRecord):
        return (
            record.event_id,
            record.raw_ingestion_id,
            record.entity_payload_hash,
            record.simulation_run_id,
            record.entity_id,
            record.entity_type,
            record.entity_payload_json,
        )
    return (
        record.bronze_canonical_hash,
        record.raw_ingestion_id,
        record.bronze_canonical_hash,
        record.simulation_run_id,
        None,
        record.event_type,
        record.event_payload_json,
    )


class TransformationEngine:
    """10-step pure transformation: BronzeInputRecord → TransformationResult."""

    def transform(self, record: BronzeInputRecord) -> TransformationResult:
        active_stage = "CLASSIFY"
        try:
            return self._transform_inner(record)
        except Exception as exc:  # noqa: BLE001 — Plan 2 no-throw safety net
            return self._quarantine_result(
                record,
                failure_stage=active_stage,
                error_code="UNHANDLED_ENGINE_EXCEPTION",
                error_message=f"Unhandled engine exception: {exc}",
            )

    def _transform_inner(self, record: BronzeInputRecord) -> TransformationResult:
        # Step 1 — Input type inspection
        if not isinstance(record, (BronzeEntityInputRecord, BronzeRunInputRecord)):
            return self._quarantine_result(
                record,
                failure_stage="CLASSIFY",
                error_code="CLASSIFY_FAILED",
                error_message="Unsupported input record type",
            )

        # Step 2 — Single-pass JSON decode
        raw_json = (
            record.entity_payload_json
            if isinstance(record, BronzeEntityInputRecord)
            else record.event_payload_json
        )
        parse_result = parse_entity_payload(raw_json)

        # Step 3 — Classify entity/event type
        if isinstance(record, BronzeEntityInputRecord):
            classified = record.entity_type
            classify_ok = classified in VALID_ENTITY_TYPES
            classify_error = "UNKNOWN_ENTITY_TYPE"
        else:
            classified = record.event_type
            classify_ok = classified == EVENT_RUN_STARTED
            classify_error = "CLASSIFY_FAILED"

        # Step 4 — Early quarantine on parse/classify failure
        if not parse_result.is_success:
            return self._quarantine_result(
                record,
                failure_stage="PARSE",
                error_code=parse_result.error_code or "INVALID_JSON_PAYLOAD",
                error_message=parse_result.error_message or "JSON parse failed",
            )
        if not classify_ok:
            return self._quarantine_result(
                record,
                failure_stage="CLASSIFY",
                error_code=classify_error,
                error_message=f"Unrecognized type: {classified!r}",
            )

        assert parse_result.payload_dict is not None

        # Step 5 — Unwrap all fields
        unwrapped = unwrap_all_fields(classified, parse_result.payload_dict)

        # Step 6 — Normalize
        normalized = normalize_fields(classified, unwrapped)

        # Step 7 — Validate
        validation = validate_fields(classified, record, normalized)

        # Step 8 — Validation routing decision
        if not validation.is_valid:
            return self._quarantine_result(
                record,
                failure_stage=validation.failure_stage or "VALIDATE",
                error_code=validation.error_code,
                error_message=validation.error_message,
            )

        # Step 9 — Builders (only when valid)
        route = resolve_route(classified)
        if route is None:
            return self._quarantine_result(
                record,
                failure_stage="CLASSIFY",
                error_code="UNKNOWN_ENTITY_TYPE",
                error_message=f"No route for {classified!r}",
            )

        fact = build_fact(classified, record, normalized)
        dimensions = build_dimensions(classified, record, normalized)

        event_id, raw_id, payload_hash, _, _, _, _ = _identity(record)
        quality_flags = getattr(fact, "quality_flags", "") or ""
        if hasattr(fact, "quality_status"):
            quality_status = getattr(fact, "quality_status") or (
                "VALID_WITH_DEFAULT" if quality_flags else "VALID"
            )
        else:
            quality_status = "VALID_WITH_DEFAULT" if quality_flags else "VALID"

        proposal = DispositionProposal(
            source_bronze_event_id=event_id,
            raw_ingestion_id=raw_id,
            payload_hash=payload_hash,
            proposed_disposition=DISPOSITION_PROCESSED,
            primary_target_table=route.primary_target_table,
        )

        # Step 10 — Assemble result
        return TransformationResult(
            facts=(fact,),
            dimensions=dimensions,
            quarantine=None,
            proposal=proposal,
            proposed_disposition=DISPOSITION_PROCESSED,
            quality_status=quality_status,
            quality_flags=quality_flags,
        )

    def _quarantine_result(
        self,
        record: BronzeInputRecord,
        *,
        failure_stage: str,
        error_code: str,
        error_message: str,
    ) -> TransformationResult:
        event_id, raw_id, payload_hash, sim_run, entity_id, entity_type, source_payload = _identity(
            record
        )
        qid = _deterministic_quarantine_id(event_id, failure_stage, error_code, payload_hash)
        entry = SilverQuarantineEntry(
            silver_quarantine_id=qid,
            source_bronze_event_id=event_id,
            raw_ingestion_id=raw_id,
            simulation_run_id=sim_run,
            entity_id=entity_id,
            entity_type=entity_type,
            failure_stage=failure_stage,
            error_code=error_code,
            error_message=error_message,
            retryable=0,
            payload_hash=payload_hash,
            source_payload=source_payload,
            created_at=None,
        )
        proposal = DispositionProposal(
            source_bronze_event_id=event_id,
            raw_ingestion_id=raw_id,
            payload_hash=payload_hash,
            proposed_disposition=DISPOSITION_QUARANTINED,
            primary_target_table="silver_quarantine",
        )
        return TransformationResult(
            facts=(),
            dimensions=(),
            quarantine=entry,
            proposal=proposal,
            proposed_disposition=DISPOSITION_QUARANTINED,
            quality_status="QUARANTINED",
            quality_flags="",
        )


# Re-export for callers / type checkers
__all__ = ["TransformationEngine", "DimensionCandidate"]
