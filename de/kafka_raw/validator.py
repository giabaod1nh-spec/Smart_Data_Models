"""Load Event Delivery schemas once; classify Kafka records."""
from __future__ import annotations

import base64
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from jsonschema import Draft202012Validator

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from contracts.canonical_json import (  # noqa: E402
    canonical_hash,
    entity_payload_hash,
)
from de.kafka_raw import MIGRATION_VERSION, VALIDATOR_VERSION  # noqa: E402
from de.kafka_raw.ingestion_id import payload_bytes_hash, raw_ingestion_id  # noqa: E402

EVENT_ENTITY = "TrafficEntityObserved"
EVENT_RUN_STARTED = "TrafficSimulationRunStarted"


@dataclass
class ClassifiedRecord:
    destination: str  # RAW | QUARANTINE
    row: Dict[str, Any]
    failure_stage: Optional[str] = None
    error_code: Optional[str] = None
    error_detail: Optional[str] = None


class EventValidator:
    def __init__(self, entity_schema_path: Path, run_started_schema_path: Path) -> None:
        self.entity_schema_path = Path(entity_schema_path)
        self.run_started_schema_path = Path(run_started_schema_path)
        self._entity: Optional[Draft202012Validator] = None
        self._run: Optional[Draft202012Validator] = None
        self.ready = False
        self.load_error: Optional[str] = None

    def load(self) -> None:
        try:
            entity = json.loads(self.entity_schema_path.read_text(encoding="utf-8"))
            run = json.loads(self.run_started_schema_path.read_text(encoding="utf-8"))
            self._entity = Draft202012Validator(entity)
            self._run = Draft202012Validator(run)
            self.ready = True
            self.load_error = None
        except Exception as e:
            self.ready = False
            self.load_error = str(e)
            raise

    def classify(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        value: bytes,
        kafka_key: Optional[bytes],
        headers: Optional[list],
        broker_timestamp_ms: Optional[int],
        broker_timestamp_type: str,
    ) -> ClassifiedRecord:
        rid = raw_ingestion_id(topic, partition, offset)
        pbh = payload_bytes_hash(value)
        size = len(value)
        consumed = datetime.now(timezone.utc)
        broker_ts = (
            datetime.fromtimestamp(broker_timestamp_ms / 1000.0, tz=timezone.utc)
            if broker_timestamp_ms
            else consumed
        )
        key_s = kafka_key.decode("utf-8", errors="replace") if kafka_key else None
        headers_json = _headers_to_json(headers)

        base = {
            "topic": topic,
            "partition": int(partition),
            "offset": int(offset),
            "raw_ingestion_id": rid,
            "kafka_key": key_s,
            "kafka_headers_json": headers_json,
            "broker_timestamp": broker_ts,
            "broker_timestamp_type": broker_timestamp_type or "NotAvailable",
            "consumed_at": consumed,
            "payload_size_bytes": size,
            "payload_bytes_hash": pbh,
            "migration_version": MIGRATION_VERSION,
        }

        # DECODE
        try:
            text = value.decode("utf-8")
            encoding = "utf8"
            stored = text
        except UnicodeDecodeError:
            return ClassifiedRecord(
                destination="QUARANTINE",
                failure_stage="DECODE",
                error_code="NON_UTF8",
                error_detail="value is not utf-8",
                row={
                    **base,
                    "failed_at": consumed,
                    "error_code": "NON_UTF8",
                    "error_detail": "value is not utf-8",
                    "failure_stage": "DECODE",
                    "validator_version": VALIDATOR_VERSION,
                    "schema_version_attempted": "",
                    "payload_encoding": "base64",
                    "payload_stored": base64.b64encode(value).decode("ascii"),
                    "canonical_payload_hash": None,
                    "event_id": None,
                    "event_type": None,
                },
            )

        try:
            body = json.loads(text)
        except json.JSONDecodeError as e:
            return ClassifiedRecord(
                destination="QUARANTINE",
                failure_stage="JSON_PARSE",
                error_code="INVALID_JSON",
                error_detail=str(e),
                row={
                    **base,
                    "failed_at": consumed,
                    "error_code": "INVALID_JSON",
                    "error_detail": str(e),
                    "failure_stage": "JSON_PARSE",
                    "validator_version": VALIDATOR_VERSION,
                    "schema_version_attempted": "",
                    "payload_encoding": encoding,
                    "payload_stored": stored,
                    "canonical_payload_hash": None,
                    "event_id": None,
                    "event_type": None,
                },
            )

        if not isinstance(body, dict):
            return ClassifiedRecord(
                destination="QUARANTINE",
                failure_stage="JSON_PARSE",
                error_code="NOT_OBJECT",
                error_detail="JSON root must be object",
                row={
                    **base,
                    "failed_at": consumed,
                    "error_code": "NOT_OBJECT",
                    "error_detail": "JSON root must be object",
                    "failure_stage": "JSON_PARSE",
                    "validator_version": VALIDATOR_VERSION,
                    "schema_version_attempted": "",
                    "payload_encoding": encoding,
                    "payload_stored": stored,
                    "canonical_payload_hash": None,
                    "event_id": None,
                    "event_type": None,
                },
            )

        event_type = str(body.get("eventType") or "")
        if event_type == EVENT_ENTITY:
            validator = self._entity
            schema_name = "traffic-entity-event-v2"
        elif event_type == EVENT_RUN_STARTED:
            validator = self._run
            schema_name = "traffic-simulation-run-started-v2"
        else:
            return ClassifiedRecord(
                destination="QUARANTINE",
                failure_stage="EVENT_TYPE",
                error_code="UNKNOWN_EVENT_TYPE",
                error_detail=event_type or "<missing>",
                row={
                    **base,
                    "failed_at": consumed,
                    "error_code": "UNKNOWN_EVENT_TYPE",
                    "error_detail": event_type or "<missing>",
                    "failure_stage": "EVENT_TYPE",
                    "validator_version": VALIDATOR_VERSION,
                    "schema_version_attempted": "",
                    "payload_encoding": encoding,
                    "payload_stored": stored,
                    "canonical_payload_hash": None,
                    "event_id": body.get("eventId"),
                    "event_type": event_type or None,
                },
            )

        assert validator is not None
        errors = sorted(validator.iter_errors(body), key=lambda e: list(e.path))
        if errors:
            err = errors[0]
            return ClassifiedRecord(
                destination="QUARANTINE",
                failure_stage="SCHEMA",
                error_code="SCHEMA_INVALID",
                error_detail=err.message,
                row={
                    **base,
                    "failed_at": consumed,
                    "error_code": "SCHEMA_INVALID",
                    "error_detail": err.message,
                    "failure_stage": "SCHEMA",
                    "validator_version": VALIDATOR_VERSION,
                    "schema_version_attempted": schema_name,
                    "payload_encoding": encoding,
                    "payload_stored": stored,
                    "canonical_payload_hash": None,
                    "event_id": body.get("eventId"),
                    "event_type": event_type,
                },
            )

        # Valid → RAW
        if event_type == EVENT_ENTITY:
            entity = body.get("entity") or {}
            canon = entity_payload_hash(entity) if isinstance(entity, dict) else None
            row = {
                **base,
                "captured_at": _parse_ts(body.get("capturedAt")),
                "event_id": body.get("eventId"),
                "event_type": event_type,
                "event_version": body.get("eventVersion"),
                "contract_version": body.get("contractVersion"),
                "source": body.get("source"),
                "producer_id": body.get("producerId"),
                "producer_session_id": body.get("producerSessionId"),
                "simulation_run_id": body.get("simulationRunId"),
                "scenario_id": body.get("scenarioId"),
                "simulation_time": body.get("simulationTime"),
                "node_id": body.get("nodeId"),
                "cycle_sequence": body.get("cycleSequence"),
                "entity_sequence": body.get("entitySequence"),
                "cycle_entity_count": body.get("cycleEntityCount"),
                "node_entity_count": body.get("nodeEntityCount"),
                "entity_id": entity.get("id") if isinstance(entity, dict) else None,
                "entity_type": entity.get("type") if isinstance(entity, dict) else None,
                "payload_encoding": encoding,
                "payload_stored": stored,
                "canonical_payload_hash": canon,
            }
        else:
            # RunStarted — hash entire control payload
            canon = _sha_canonical_object(body)
            row = {
                **base,
                "captured_at": _parse_ts(body.get("startedAt")),
                "event_id": None,
                "event_type": event_type,
                "event_version": body.get("eventVersion"),
                "contract_version": body.get("contractVersion"),
                "source": body.get("source"),
                "producer_id": body.get("producerId"),
                "producer_session_id": body.get("producerSessionId"),
                "simulation_run_id": body.get("simulationRunId"),
                "scenario_id": body.get("scenarioId"),
                "simulation_time": None,
                "node_id": None,
                "cycle_sequence": None,
                "entity_sequence": None,
                "cycle_entity_count": None,
                "node_entity_count": None,
                "entity_id": None,
                "entity_type": None,
                "payload_encoding": encoding,
                "payload_stored": stored,
                "canonical_payload_hash": canon,
            }
        return ClassifiedRecord(destination="RAW", row=row)


def _sha_canonical_object(obj: dict) -> str:
    return canonical_hash(obj)

def _parse_ts(v: Any) -> Optional[datetime]:
    if not v or not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None


def _headers_to_json(headers: Optional[list]) -> str:
    if not headers:
        return "{}"
    out: Dict[str, str] = {}
    for h in headers:
        try:
            k, v = h
            out[str(k)] = (
                v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else str(v)
            )
        except Exception:
            continue
    return json.dumps(out, ensure_ascii=True, separators=(",", ":"))
