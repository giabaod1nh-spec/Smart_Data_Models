"""Silver Plan 2 — single-pass JSON parse + NGSI-LD unwrap (no I/O)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from de.silver.contracts import (
    ENTITY_CAMERA,
    ENTITY_INTERSECTION,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_VEHICLE_SENSOR,
    EVENT_RUN_STARTED,
)


@dataclass(frozen=True)
class ParseResult:
    is_success: bool
    payload_dict: Optional[dict[str, Any]] = None
    error_code: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class UnwrapResult:
    value: Any
    is_success: bool = True
    error_code: str = ""
    present: bool = True


def parse_entity_payload(raw_json: str) -> ParseResult:
    if not raw_json or not str(raw_json).strip():
        return ParseResult(False, None, "INVALID_JSON_PAYLOAD", "Empty JSON string")
    try:
        data = json.loads(raw_json)
        if not isinstance(data, dict):
            return ParseResult(
                False, None, "INVALID_JSON_PAYLOAD", "JSON payload is not a dictionary object"
            )
        return ParseResult(True, data)
    except Exception as exc:  # noqa: BLE001 — no-throw policy
        return ParseResult(False, None, "INVALID_JSON_PAYLOAD", f"JSON parse error: {exc}")


def unwrap_property(node: Any) -> UnwrapResult:
    if node is None:
        return UnwrapResult(None, True, present=False)
    if isinstance(node, dict):
        if node.get("type") == "Property" or "value" in node:
            val = node.get("value")
            if isinstance(val, dict) and "@value" in val:
                return UnwrapResult(val["@value"], True)
            return UnwrapResult(val, True)
        return UnwrapResult(None, False, "MALFORMED_PROPERTY_WRAPPER")
    return UnwrapResult(node, True)


def unwrap_relationship(node: Any) -> UnwrapResult:
    if node is None:
        return UnwrapResult(None, True, present=False)
    if isinstance(node, dict):
        if node.get("type") == "Relationship" or "object" in node:
            return UnwrapResult(node.get("object"), True)
        return UnwrapResult(None, False, "MALFORMED_RELATIONSHIP_WRAPPER")
    return UnwrapResult(None, False, "INVALID_RELATIONSHIP_NODE")


def unwrap_geoproperty(node: Any) -> UnwrapResult:
    if node is None:
        return UnwrapResult(None, False, "INVALID_GEOPROPERTY_NODE", present=False)
    if not isinstance(node, dict):
        return UnwrapResult(None, False, "INVALID_GEOPROPERTY_NODE")
    val = node.get("value")
    if isinstance(val, dict):
        coords = val.get("coordinates")
        if isinstance(coords, list) and len(coords) >= 2:
            try:
                return UnwrapResult((float(coords[0]), float(coords[1])), True)
            except (ValueError, TypeError):
                return UnwrapResult(None, False, "INVALID_COORDINATE_VALUES")
    return UnwrapResult(None, False, "MALFORMED_GEOPROPERTY_WRAPPER")


def _get(payload: dict[str, Any], key: str) -> Any:
    return payload.get(key)


def unwrap_vehicle_sensor(payload: dict[str, Any]) -> dict[str, UnwrapResult]:
    return {
        "refIntersection": unwrap_relationship(_get(payload, "refIntersection")),
        "trafficDirection": unwrap_property(_get(payload, "trafficDirection")),
        "vehicleCount": unwrap_property(_get(payload, "vehicleCount")),
        "pcuEquivalent": unwrap_property(_get(payload, "pcuEquivalent")),
        "averageSpeed": unwrap_property(_get(payload, "averageSpeed")),
        "queueLength": unwrap_property(_get(payload, "queueLength")),
        "occupancyRate": unwrap_property(_get(payload, "occupancyRate")),
        "waitingVehicleCount": unwrap_property(_get(payload, "waitingVehicleCount")),
        "arrivalRatePcuPerSec": unwrap_property(_get(payload, "arrivalRatePcuPerSec")),
        "trafficStatus": unwrap_property(_get(payload, "trafficStatus")),
        "spillbackRisk": unwrap_property(_get(payload, "spillbackRisk")),
        "dominantWaitingReason": unwrap_property(_get(payload, "dominantWaitingReason")),
    }


def unwrap_traffic_light(payload: dict[str, Any]) -> dict[str, UnwrapResult]:
    return {
        "refIntersection": unwrap_relationship(_get(payload, "refIntersection")),
        "trafficDirection": unwrap_property(_get(payload, "trafficDirection")),
        "currentStatus": unwrap_property(_get(payload, "currentStatus")),
        "currentPhase": unwrap_property(_get(payload, "currentPhase")),
        "greenDurationCurrent": unwrap_property(_get(payload, "greenDurationCurrent")),
        "redDurationCurrent": unwrap_property(_get(payload, "redDurationCurrent")),
        "yellowDuration": unwrap_property(_get(payload, "yellowDuration")),
        "timingMode": unwrap_property(_get(payload, "timingMode")),
    }


def unwrap_intersection(payload: dict[str, Any]) -> dict[str, UnwrapResult]:
    entity_id = payload.get("id")
    id_ok = isinstance(entity_id, str) and bool(entity_id.strip())
    return {
        "id": UnwrapResult(entity_id if id_ok else None, id_ok, "" if id_ok else "REQUIRED_DOMAIN_FIELD_MISSING"),
        "overallTrafficStatus": unwrap_property(_get(payload, "overallTrafficStatus")),
        "derivedTrafficState": unwrap_property(_get(payload, "derivedTrafficState")),
        "currentPhase": unwrap_property(_get(payload, "currentPhase")),
        "hasActiveIncident": unwrap_property(_get(payload, "hasActiveIncident")),
        "hasSpillback": unwrap_property(_get(payload, "hasSpillback")),
        "isBoxBlocked": unwrap_property(_get(payload, "isBoxBlocked")),
        "totalVehicleCount": unwrap_property(_get(payload, "totalVehicleCount")),
        "name": unwrap_property(_get(payload, "name")),
        "location": unwrap_geoproperty(_get(payload, "location")),
    }


def unwrap_camera(payload: dict[str, Any]) -> dict[str, UnwrapResult]:
    entity_id = payload.get("id")
    id_ok = isinstance(entity_id, str) and bool(entity_id.strip())
    return {
        "refIntersection": unwrap_relationship(_get(payload, "refIntersection")),
        "id": UnwrapResult(entity_id if id_ok else None, id_ok, "" if id_ok else "REQUIRED_DOMAIN_FIELD_MISSING"),
        "vehicleCount": unwrap_property(_get(payload, "vehicleCount")),
        "averageSpeed": unwrap_property(_get(payload, "averageSpeed")),
        "occupancyRate": unwrap_property(_get(payload, "occupancyRate")),
        "trafficStatus": unwrap_property(_get(payload, "trafficStatus")),
        "incidentDetected": unwrap_property(_get(payload, "incidentDetected")),
        "confidence": unwrap_property(_get(payload, "confidence")),
        "recommendedSignalAction": unwrap_property(_get(payload, "recommendedSignalAction")),
        "incidentType": unwrap_property(_get(payload, "incidentType")),
        "incidentSeverity": unwrap_property(_get(payload, "incidentSeverity")),
    }


def unwrap_run_started(payload: dict[str, Any]) -> dict[str, UnwrapResult]:
    """RunStarted primarily uses envelope; payload unwrap is best-effort passthrough."""
    return {
        "payload": UnwrapResult(payload, True),
    }


def unwrap_all_fields(entity_or_event_type: str, payload: dict[str, Any]) -> dict[str, UnwrapResult]:
    try:
        if entity_or_event_type == ENTITY_VEHICLE_SENSOR:
            return unwrap_vehicle_sensor(payload)
        if entity_or_event_type == ENTITY_TRAFFIC_LIGHT:
            return unwrap_traffic_light(payload)
        if entity_or_event_type == ENTITY_INTERSECTION:
            return unwrap_intersection(payload)
        if entity_or_event_type == ENTITY_CAMERA:
            return unwrap_camera(payload)
        if entity_or_event_type == EVENT_RUN_STARTED:
            return unwrap_run_started(payload)
        return {}
    except Exception:  # noqa: BLE001
        return {}
