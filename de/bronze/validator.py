"""Defensive Bronze validation on already-ingested Raw rows."""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from jsonschema import Draft202012Validator

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from contracts.canonical_json import (  # noqa: E402
    compute_event_id,
    entity_payload_hash,
)

EVENT_ENTITY = "TrafficEntityObserved"
EVENT_RUN_STARTED = "TrafficSimulationRunStarted"
CONTRACT_VERSION = "2.0.0"


@dataclass
class ValidationOutcome:
    ok: bool
    kind: str  # ENTITY | RUN | QUARANTINE
    error_code: Optional[str] = None
    error_detail: Optional[str] = None
    failure_stage: Optional[str] = None


class BronzeValidator:
    def __init__(self, entity_schema_path: Path, run_started_schema_path: Path) -> None:
        self.entity_schema_path = Path(entity_schema_path)
        self.run_started_schema_path = Path(run_started_schema_path)
        self._entity: Optional[Draft202012Validator] = None
        self._run: Optional[Draft202012Validator] = None
        self.ready = False

    def load(self) -> None:
        entity = json.loads(self.entity_schema_path.read_text(encoding="utf-8"))
        run = json.loads(self.run_started_schema_path.read_text(encoding="utf-8"))
        self._entity = Draft202012Validator(entity)
        self._run = Draft202012Validator(run)
        self.ready = True

    def validate(self, event: Dict[str, Any]) -> ValidationOutcome:
        event_type = str(event.get("eventType") or "")
        if event.get("contractVersion") != CONTRACT_VERSION:
            return ValidationOutcome(
                False,
                "QUARANTINE",
                error_code="UNSUPPORTED_CONTRACT_VERSION",
                error_detail=str(event.get("contractVersion")),
                failure_stage="CLASSIFY",
            )
        if event_type == EVENT_ENTITY:
            return self._validate_entity(event)
        if event_type == EVENT_RUN_STARTED:
            return self._validate_run(event)
        return ValidationOutcome(
            False,
            "QUARANTINE",
            error_code="UNSUPPORTED_EVENT_TYPE",
            error_detail=event_type or "<missing>",
            failure_stage="CLASSIFY",
        )

    def _validate_entity(self, event: Dict[str, Any]) -> ValidationOutcome:
        assert self._entity is not None
        errors = sorted(self._entity.iter_errors(event), key=lambda e: list(e.path))
        if errors:
            return ValidationOutcome(
                False,
                "QUARANTINE",
                error_code="SCHEMA_INVALID",
                error_detail=errors[0].message,
                failure_stage="VALIDATE",
            )
        entity = event.get("entity") or {}
        if not isinstance(entity, dict):
            return ValidationOutcome(
                False,
                "QUARANTINE",
                error_code="REQUIRED_FIELD_MISSING",
                error_detail="entity",
                failure_stage="VALIDATE",
            )
        expected_hash = entity_payload_hash(entity)
        if str(event.get("entityPayloadHash") or "") != expected_hash:
            return ValidationOutcome(
                False,
                "QUARANTINE",
                error_code="ENTITY_HASH_MISMATCH",
                error_detail="entityPayloadHash mismatch",
                failure_stage="VALIDATE",
            )
        expected_event_id = compute_event_id(
            contract_version=str(event.get("contractVersion")),
            simulation_run_id=str(event.get("simulationRunId")),
            cycle_sequence=int(event.get("cycleSequence")),
            entity_id=str(entity.get("id")),
        )
        if str(event.get("eventId") or "") != expected_event_id:
            return ValidationOutcome(
                False,
                "QUARANTINE",
                error_code="EVENT_ID_MISMATCH",
                error_detail="eventId mismatch",
                failure_stage="VALIDATE",
            )
        return ValidationOutcome(True, "ENTITY")

    def _validate_run(self, event: Dict[str, Any]) -> ValidationOutcome:
        assert self._run is not None
        errors = sorted(self._run.iter_errors(event), key=lambda e: list(e.path))
        if errors:
            return ValidationOutcome(
                False,
                "QUARANTINE",
                error_code="SCHEMA_INVALID",
                error_detail=errors[0].message,
                failure_stage="VALIDATE",
            )
        return ValidationOutcome(True, "RUN")
