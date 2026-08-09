"""Self-validate Event Delivery Contract 2.0.0 before produce()."""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from contracts.canonical_json import (  # noqa: E402
    compute_event_id,
    entity_payload_hash,
    node_id_from_entity_id,
)

from integration.kafka.event_mapper import (  # noqa: E402
    CONTRACT_VERSION,
    EVENT_TYPE,
    EVENT_VERSION,
    SOURCE,
    partition_key,
)

_ALLOWED_TYPES = frozenset(
    {"Intersection", "TrafficLight", "VehicleSensor", "Camera"}
)


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: tuple[str, ...] = ()

    @property
    def reason(self) -> str:
        return "; ".join(self.errors)


def validate_entity_event(event: dict[str, Any]) -> ValidationResult:
    """Local mapper gate — fail permanent, do not send / do not quarantine locally."""
    errors: List[str] = []

    for key in (
        "eventId",
        "eventVersion",
        "contractVersion",
        "eventType",
        "source",
        "simulationRunId",
        "simulationTime",
        "cycleSequence",
        "entitySequence",
        "cycleEntityCount",
        "capturedAt",
        "entityPayloadHash",
        "entity",
        "nodeId",
    ):
        if key not in event:
            errors.append(f"missing {key}")

    if errors:
        return ValidationResult(False, tuple(errors))

    if event.get("eventVersion") != EVENT_VERSION:
        errors.append(f"eventVersion want {EVENT_VERSION}")
    if event.get("contractVersion") != CONTRACT_VERSION:
        errors.append(f"contractVersion want {CONTRACT_VERSION}")
    if event.get("eventType") != EVENT_TYPE:
        errors.append(f"eventType want {EVENT_TYPE}")
    if event.get("source") != SOURCE:
        errors.append(f"source want {SOURCE}")

    entity = event.get("entity")
    if not isinstance(entity, dict):
        errors.append("entity must be object")
        return ValidationResult(False, tuple(errors))

    entity_id = str(entity.get("id") or "")
    entity_type = str(entity.get("type") or "")
    if not entity_id:
        errors.append("entity.id empty")
    if entity_type not in _ALLOWED_TYPES:
        errors.append(f"entity.type invalid: {entity_type!r}")

    try:
        cycle_seq = int(event["cycleSequence"])
        ent_seq = int(event["entitySequence"])
        count = int(event["cycleEntityCount"])
    except (TypeError, ValueError):
        errors.append("cycleSequence/entitySequence/cycleEntityCount not int")
        return ValidationResult(False, tuple(errors))

    if count < 1:
        errors.append("cycleEntityCount < 1")
    if ent_seq < 0 or ent_seq >= count:
        errors.append(
            f"entitySequence {ent_seq} out of range for cycleEntityCount {count}"
        )

    run_id = str(event.get("simulationRunId") or "")
    if not run_id:
        errors.append("simulationRunId empty")

    node = str(event.get("nodeId") or "")
    if not node:
        errors.append("nodeId empty")
    elif entity_id:
        try:
            expected_node = node_id_from_entity_id(entity_id)
            if node != expected_node:
                errors.append(f"nodeId {node!r} != derived {expected_node!r}")
        except ValueError as e:
            errors.append(str(e))

    pk = partition_key(run_id, node) if run_id and node else ""
    if not pk or pk.startswith(":") or pk.endswith(":"):
        errors.append("partition key empty/invalid")

    if "nodeEntityCount" in event:
        try:
            nec = int(event["nodeEntityCount"])
            if nec < 1:
                errors.append("nodeEntityCount < 1")
        except (TypeError, ValueError):
            errors.append("nodeEntityCount not int")

    if entity_id and run_id:
        expected_eid = compute_event_id(
            contract_version=CONTRACT_VERSION,
            simulation_run_id=run_id,
            cycle_sequence=cycle_seq,
            entity_id=entity_id,
        )
        if event.get("eventId") != expected_eid:
            errors.append("eventId mismatch")

    expected_hash = entity_payload_hash(entity)
    if event.get("entityPayloadHash") != expected_hash:
        errors.append("entityPayloadHash mismatch")

    eid = str(event.get("eventId") or "")
    eph = str(event.get("entityPayloadHash") or "")
    if len(eid) != 64 or any(c not in "0123456789abcdef" for c in eid):
        errors.append("eventId not 64 hex")
    if len(eph) != 64 or any(c not in "0123456789abcdef" for c in eph):
        errors.append("entityPayloadHash not 64 hex")

    return ValidationResult(ok=not errors, errors=tuple(errors))


def validate_cycle_events(events: Sequence[dict[str, Any]]) -> ValidationResult:
    if not events:
        return ValidationResult(False, ("empty cycle",))
    errors: List[str] = []
    count = events[0].get("cycleEntityCount")
    seqs = []
    for i, ev in enumerate(events):
        r = validate_entity_event(ev)
        if not r.ok:
            errors.append(f"[{i}] {r.reason}")
        if ev.get("cycleEntityCount") != count:
            errors.append(f"[{i}] cycleEntityCount drift")
        if ev.get("entitySequence") != i:
            errors.append(f"[{i}] entitySequence want {i} got {ev.get('entitySequence')}")
        seqs.append(ev.get("entitySequence"))
    if count is not None and len(events) != int(count):
        errors.append(f"len(events)={len(events)} != cycleEntityCount={count}")
    expected = set(range(len(events)))
    if set(seqs) != expected:
        errors.append(f"entitySequence set incomplete: {seqs}")
    return ValidationResult(ok=not errors, errors=tuple(errors))
