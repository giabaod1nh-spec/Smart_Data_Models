"""RawRow → Bronze ClickHouse rows."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from contracts.canonical_json import canonical_json

from de.bronze import (
    BRONZE_SCHEMA_VERSION,
    DEST_BRONZE_QUARANTINE,
    DEST_ENTITY,
    DEST_RUN,
    MIGRATION_VERSION,
    PROCESSOR_NAME,
    PROCESSOR_VERSION,
    SOURCE_CONTRACT_VERSION,
)
from de.bronze.canonical_hash import (
    bronze_ingestion_id,
    entity_canonical_hash,
    quarantine_canonical_hash,
    run_canonical_hash,
)
from de.bronze.models import RawRow, TransformResult
from de.bronze.validator import ValidationOutcome


class BronzeTransformer:
    def __init__(
        self,
        *,
        processor_name: str = PROCESSOR_NAME,
        processor_version: str = PROCESSOR_VERSION,
        bronze_schema_version: str = BRONZE_SCHEMA_VERSION,
        source_contract_version: str = SOURCE_CONTRACT_VERSION,
    ) -> None:
        self.processor_name = processor_name
        self.processor_version = processor_version
        self.bronze_schema_version = bronze_schema_version
        self.source_contract_version = source_contract_version

    def transform(
        self,
        raw: RawRow,
        event: Dict[str, Any],
        outcome: ValidationOutcome,
        *,
        upstream_duplicate: bool = False,
    ) -> TransformResult:
        if not outcome.ok:
            return TransformResult(
                kind="QUARANTINE",
                quarantine_row=self._quarantine_row(raw, event, outcome),
            )
        if outcome.kind == "ENTITY":
            return TransformResult(
                kind="ENTITY",
                entity_row=self._entity_row(raw, event, upstream_duplicate=upstream_duplicate),
            )
        return TransformResult(kind="RUN", run_row=self._run_row(raw, event))

    def _entity_row(
        self, raw: RawRow, event: Dict[str, Any], *, upstream_duplicate: bool
    ) -> Dict[str, Any]:
        entity = event.get("entity") or {}
        now = datetime.now(timezone.utc)
        event_json = canonical_json(event)
        row: Dict[str, Any] = {
            "topic": raw.topic,
            "partition": raw.partition,
            "offset": raw.offset,
            "raw_ingestion_id": raw.raw_ingestion_id,
            "broker_timestamp": raw.broker_timestamp,
            "raw_consumed_at": raw.consumed_at,
            "event_id": event["eventId"],
            "event_type": event.get("eventType"),
            "contract_version": event.get("contractVersion"),
            "event_version": event.get("eventVersion"),
            "source": event.get("source"),
            "producer_id": event.get("producerId"),
            "producer_session_id": event.get("producerSessionId"),
            "simulation_run_id": event["simulationRunId"],
            "simulation_time": float(event["simulationTime"]),
            "scenario_id": event.get("scenarioId"),
            "node_id": event.get("nodeId"),
            "cycle_sequence": int(event["cycleSequence"]),
            "entity_sequence": int(event["entitySequence"]),
            "cycle_entity_count": int(event["cycleEntityCount"]),
            "node_entity_count": event.get("nodeEntityCount"),
            "captured_at": _parse_ts(event.get("capturedAt")) or now,
            "entity_id": entity.get("id"),
            "entity_type": entity.get("type"),
            "entity_payload_hash": event["entityPayloadHash"],
            "entity_payload_json": canonical_json(entity),
            "upstream_duplicate_event_id": 1 if upstream_duplicate else 0,
            "event_payload_json": event_json,
            "processor_name": self.processor_name,
            "processor_version": self.processor_version,
            "bronze_schema_version": self.bronze_schema_version,
            "source_contract_version": self.source_contract_version,
            "processed_at": now,
            "validation_status": "STORED",
            "migration_version": MIGRATION_VERSION,
        }
        row["bronze_ingestion_id"] = bronze_ingestion_id(
            raw.raw_ingestion_id, self.processor_version, DEST_ENTITY
        )
        row["bronze_canonical_hash"] = entity_canonical_hash(row)
        return row

    def _run_row(self, raw: RawRow, event: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        event_json = canonical_json(event)
        row: Dict[str, Any] = {
            "topic": raw.topic,
            "partition": raw.partition,
            "offset": raw.offset,
            "raw_ingestion_id": raw.raw_ingestion_id,
            "broker_timestamp": raw.broker_timestamp,
            "raw_consumed_at": raw.consumed_at,
            "event_type": event.get("eventType"),
            "contract_version": event.get("contractVersion"),
            "event_version": event.get("eventVersion"),
            "source": event.get("source"),
            "producer_id": event["producerId"],
            "producer_session_id": event["producerSessionId"],
            "simulation_run_id": event["simulationRunId"],
            "started_at": _parse_ts(event.get("startedAt")) or now,
            "scenario_id": event.get("scenarioId"),
            "event_payload_json": event_json,
            "processor_name": self.processor_name,
            "processor_version": self.processor_version,
            "bronze_schema_version": self.bronze_schema_version,
            "source_contract_version": self.source_contract_version,
            "processed_at": now,
            "validation_status": "STORED",
            "migration_version": MIGRATION_VERSION,
        }
        row["bronze_ingestion_id"] = bronze_ingestion_id(
            raw.raw_ingestion_id, self.processor_version, DEST_RUN
        )
        row["bronze_canonical_hash"] = run_canonical_hash(row)
        return row

    def _quarantine_row(
        self, raw: RawRow, event: Dict[str, Any], outcome: ValidationOutcome
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        ref = raw.payload_stored
        row: Dict[str, Any] = {
            "topic": raw.topic,
            "partition": raw.partition,
            "offset": raw.offset,
            "raw_ingestion_id": raw.raw_ingestion_id,
            "broker_timestamp": raw.broker_timestamp,
            "raw_consumed_at": raw.consumed_at,
            "event_id": event.get("eventId"),
            "event_type": event.get("eventType"),
            "simulation_run_id": event.get("simulationRunId"),
            "failure_stage": outcome.failure_stage or "VALIDATE",
            "error_code": outcome.error_code or "UNKNOWN",
            "error_detail": outcome.error_detail or "",
            "retryable": 0,
            "payload_encoding": raw.payload_encoding,
            "payload_reference": ref,
            "payload_bytes_hash": raw.payload_bytes_hash,
            "processor_name": self.processor_name,
            "processor_version": self.processor_version,
            "bronze_schema_version": self.bronze_schema_version,
            "quarantined_at": now,
            "migration_version": MIGRATION_VERSION,
        }
        row["bronze_canonical_hash"] = quarantine_canonical_hash(row)
        return row


def _parse_ts(v: Any) -> Optional[datetime]:
    if not v or not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
