"""Silver Plan 2 — field validators with locked error precedence (pure)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from de.silver.contracts import (
    ENTITY_CAMERA,
    ENTITY_INTERSECTION,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_VEHICLE_SENSOR,
    EVENT_RUN_STARTED,
)
from de.silver.input_models import BronzeEntityInputRecord, BronzeInputRecord, BronzeRunInputRecord
from de.silver.unwrapper import UnwrapResult

# Precedence (first match wins as single quarantine error):
# CLASSIFY → PARSE → REQUIRED_ENVELOPE → REQUIRED_DOMAIN → INVALID_DIRECTION
# → INVALID_RANGE_METRIC → INVALID_RANGE_OCCUPANCY
_ERROR_RANK = {
    "CLASSIFY_FAILED": 10,
    "UNKNOWN_ENTITY_TYPE": 10,
    "INVALID_JSON_PAYLOAD": 20,
    "REQUIRED_ENVELOPE_FIELD_MISSING": 30,
    "REQUIRED_DOMAIN_FIELD_MISSING": 40,
    "MALFORMED_PROPERTY_WRAPPER": 40,
    "MALFORMED_RELATIONSHIP_WRAPPER": 40,
    "MALFORMED_GEOPROPERTY_WRAPPER": 40,
    "INVALID_RELATIONSHIP_NODE": 40,
    "INVALID_GEOPROPERTY_NODE": 40,
    "INVALID_COORDINATE_VALUES": 40,
    "INVALID_DIRECTION_ENUM": 50,
    "INVALID_RANGE_METRIC": 60,
    "INVALID_RANGE_OCCUPANCY": 70,
}


@dataclass(frozen=True)
class ValidationIssue:
    error_code: str
    error_message: str
    failure_stage: str
    field_order: int = 0


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    error_code: str = ""
    error_message: str = ""
    failure_stage: str = ""
    issues: tuple[ValidationIssue, ...] = ()


def _pick(issues: list[ValidationIssue]) -> ValidationResult:
    if not issues:
        return ValidationResult(True)
    issues_sorted = sorted(
        issues,
        key=lambda i: (_ERROR_RANK.get(i.error_code, 999), i.field_order),
    )
    top = issues_sorted[0]
    return ValidationResult(
        False,
        top.error_code,
        top.error_message,
        top.failure_stage,
        tuple(issues_sorted),
    )


def _missing(u: UnwrapResult) -> bool:
    return (not u.present) or (u.is_success and u.value is None) or (not u.is_success)


def _malformed(u: UnwrapResult) -> bool:
    return not u.is_success


def validate_entity_envelope(record: BronzeEntityInputRecord) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    order = 0

    def req(name: str, value: Any) -> None:
        nonlocal order
        order += 1
        empty = value is None or (isinstance(value, str) and not value.strip())
        if empty:
            issues.append(
                ValidationIssue(
                    "REQUIRED_ENVELOPE_FIELD_MISSING",
                    f"Missing required envelope field: {name}",
                    "VALIDATE",
                    order,
                )
            )

    req("simulation_run_id", record.simulation_run_id)
    req("scenario_id", record.scenario_id)
    req("entity_id", record.entity_id)
    req("event_id", record.event_id)
    req("raw_ingestion_id", record.raw_ingestion_id)
    req("topic", record.topic)
    req("entity_payload_hash", record.entity_payload_hash)
    if record.cycle_sequence is None:  # type: ignore[comparison-overlap]
        issues.append(
            ValidationIssue(
                "REQUIRED_ENVELOPE_FIELD_MISSING",
                "Missing required envelope field: cycle_sequence",
                "VALIDATE",
                order + 1,
            )
        )
    if record.simulation_time is None:  # type: ignore[comparison-overlap]
        issues.append(
            ValidationIssue(
                "REQUIRED_ENVELOPE_FIELD_MISSING",
                "Missing required envelope field: simulation_time",
                "VALIDATE",
                order + 2,
            )
        )
    return issues


def validate_run_envelope(record: BronzeRunInputRecord) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    order = 0

    def req(name: str, value: Any, code: str = "REQUIRED_ENVELOPE_FIELD_MISSING") -> None:
        nonlocal order
        order += 1
        empty = value is None or (isinstance(value, str) and not str(value).strip())
        if empty:
            issues.append(
                ValidationIssue(
                    code,
                    f"Missing required field: {name}",
                    "VALIDATE",
                    order,
                )
            )

    req("simulation_run_id", record.simulation_run_id)
    req("scenario_id", record.scenario_id, "REQUIRED_DOMAIN_FIELD_MISSING")
    req("producer_id", record.producer_id)
    if not isinstance(record.started_at, datetime):
        issues.append(
            ValidationIssue(
                "REQUIRED_ENVELOPE_FIELD_MISSING",
                "Missing required envelope field: started_at",
                "VALIDATE",
                order + 1,
            )
        )
    return issues


def _domain_required_unwrap(
    issues: list[ValidationIssue],
    meta: dict[str, UnwrapResult],
    key: str,
    order: int,
    label: str,
) -> None:
    u = meta.get(key, UnwrapResult(None, True, present=False))
    if _malformed(u):
        issues.append(
            ValidationIssue(
                u.error_code or "MALFORMED_PROPERTY_WRAPPER",
                f"Malformed NGSI-LD wrapper for {label}",
                "VALIDATE",
                order,
            )
        )
        return
    if _missing(u):
        issues.append(
            ValidationIssue(
                "REQUIRED_DOMAIN_FIELD_MISSING",
                f"Missing required domain field: {label}",
                "VALIDATE",
                order,
            )
        )


def validate_vehicle_sensor(normalized: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    meta: dict[str, UnwrapResult] = normalized.get("_meta", {})
    order = 100

    _domain_required_unwrap(issues, meta, "refIntersection", order, "refIntersection")
    order += 1
    if normalized.get("refIntersection") is not None:
        ref = normalized["refIntersection"]
        if not isinstance(ref, str) or not ref.strip():
            issues.append(
                ValidationIssue(
                    "REQUIRED_DOMAIN_FIELD_MISSING",
                    "Missing required domain field: refIntersection",
                    "VALIDATE",
                    order,
                )
            )
    order += 1

    u_dir = meta.get("trafficDirection", UnwrapResult(None, True, present=False))
    if _malformed(u_dir) or _missing(u_dir):
        _domain_required_unwrap(issues, meta, "trafficDirection", order, "trafficDirection")
    elif normalized.get("trafficDirection") is None:
        issues.append(
            ValidationIssue(
                "INVALID_DIRECTION_ENUM",
                f"Invalid direction enum: {normalized.get('trafficDirection_raw')!r}",
                "VALIDATE",
                order,
            )
        )
    order += 1

    for key, label in (
        ("vehicleCount", "vehicleCount"),
        ("pcuEquivalent", "pcuEquivalent"),
        ("averageSpeed", "averageSpeed"),
        ("queueLength", "queueLength"),
    ):
        u = meta.get(key, UnwrapResult(None, True, present=False))
        if _malformed(u) or _missing(u):
            _domain_required_unwrap(issues, meta, key, order, label)
        elif normalized.get(key) is None:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_METRIC",
                    f"Invalid range for metric: {label}",
                    "VALIDATE",
                    order,
                )
            )
        elif key in ("pcuEquivalent", "averageSpeed", "queueLength") and normalized[key] < 0:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_METRIC",
                    f"Invalid range for metric: {label}",
                    "VALIDATE",
                    order,
                )
            )
        order += 1

    u_occ = meta.get("occupancyRate", UnwrapResult(None, True, present=False))
    if _malformed(u_occ) or _missing(u_occ):
        _domain_required_unwrap(issues, meta, "occupancyRate", order, "occupancyRate")
    else:
        occ = normalized.get("occupancyRate")
        if occ is None:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_OCCUPANCY",
                    "Invalid occupancy range",
                    "VALIDATE",
                    order,
                )
            )
        elif occ < 0.0 or occ > 100.0:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_OCCUPANCY",
                    f"Occupancy out of range: {occ}",
                    "VALIDATE",
                    order,
                )
            )
    order += 1

    # Optional metrics: if present but coerce failed → range error
    for key, label in (
        ("waitingVehicleCount", "waitingVehicleCount"),
        ("arrivalRatePcuPerSec", "arrivalRatePcuPerSec"),
    ):
        u = meta.get(key, UnwrapResult(None, True, present=False))
        if u.present and u.is_success and u.value is not None and normalized.get(key) is None:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_METRIC",
                    f"Invalid range for metric: {label}",
                    "VALIDATE",
                    order,
                )
            )
        order += 1

    return issues


def validate_traffic_light(normalized: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    meta: dict[str, UnwrapResult] = normalized.get("_meta", {})
    order = 100

    _domain_required_unwrap(issues, meta, "refIntersection", order, "refIntersection")
    order += 1
    if normalized.get("refIntersection") is not None:
        ref = normalized["refIntersection"]
        if not isinstance(ref, str) or not ref.strip():
            issues.append(
                ValidationIssue(
                    "REQUIRED_DOMAIN_FIELD_MISSING",
                    "Missing required domain field: refIntersection",
                    "VALIDATE",
                    order,
                )
            )
    order += 1

    u_dir = meta.get("trafficDirection", UnwrapResult(None, True, present=False))
    if _malformed(u_dir) or _missing(u_dir):
        _domain_required_unwrap(issues, meta, "trafficDirection", order, "trafficDirection")
    elif normalized.get("trafficDirection") is None:
        issues.append(
            ValidationIssue(
                "INVALID_DIRECTION_ENUM",
                f"Invalid direction enum: {normalized.get('trafficDirection_raw')!r}",
                "VALIDATE",
                order,
            )
        )
    order += 1

    u_status = meta.get("currentStatus", UnwrapResult(None, True, present=False))
    if _malformed(u_status) or _missing(u_status) or normalized.get("currentStatus") is None:
        issues.append(
            ValidationIssue(
                "REQUIRED_DOMAIN_FIELD_MISSING",
                "Missing required domain field: currentStatus",
                "VALIDATE",
                order,
            )
        )
    order += 1

    u_phase = meta.get("currentPhase", UnwrapResult(None, True, present=False))
    phase = normalized.get("currentPhase")
    if _malformed(u_phase) or _missing(u_phase) or not phase:
        issues.append(
            ValidationIssue(
                "REQUIRED_DOMAIN_FIELD_MISSING",
                "Missing required domain field: currentPhase",
                "VALIDATE",
                order,
            )
        )
    order += 1

    for key, label in (
        ("greenDurationCurrent", "greenDurationCurrent"),
        ("redDurationCurrent", "redDurationCurrent"),
        ("yellowDuration", "yellowDuration"),
    ):
        u = meta.get(key, UnwrapResult(None, True, present=False))
        if u.present and u.is_success and u.value is not None:
            val = normalized.get(key)
            if val is None or val < 0:
                issues.append(
                    ValidationIssue(
                        "INVALID_RANGE_METRIC",
                        f"Invalid range for metric: {label}",
                        "VALIDATE",
                        order,
                    )
                )
        order += 1

    return issues


def validate_intersection(normalized: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    meta: dict[str, UnwrapResult] = normalized.get("_meta", {})
    order = 100

    _domain_required_unwrap(issues, meta, "id", order, "id")
    order += 1

    u_status = meta.get("overallTrafficStatus", UnwrapResult(None, True, present=False))
    if (
        _malformed(u_status)
        or _missing(u_status)
        or normalized.get("overallTrafficStatus") is None
    ):
        issues.append(
            ValidationIssue(
                "REQUIRED_DOMAIN_FIELD_MISSING",
                "Missing required domain field: overallTrafficStatus",
                "VALIDATE",
                order,
            )
        )
    order += 1

    u_phase = meta.get("currentPhase", UnwrapResult(None, True, present=False))
    if _malformed(u_phase) or _missing(u_phase) or not normalized.get("currentPhase"):
        issues.append(
            ValidationIssue(
                "REQUIRED_DOMAIN_FIELD_MISSING",
                "Missing required domain field: currentPhase",
                "VALIDATE",
                order,
            )
        )
    order += 1

    # Dim-required fields (contract §6.3)
    _domain_required_unwrap(issues, meta, "name", order, "name")
    order += 1
    u_loc = meta.get("location", UnwrapResult(None, True, present=False))
    if _malformed(u_loc) or _missing(u_loc) or normalized.get("location") is None:
        issues.append(
            ValidationIssue(
                u_loc.error_code if _malformed(u_loc) else "REQUIRED_DOMAIN_FIELD_MISSING",
                "Missing required domain field: location",
                "VALIDATE",
                order,
            )
        )
    order += 1

    u_tv = meta.get("totalVehicleCount", UnwrapResult(None, True, present=False))
    if u_tv.present and u_tv.is_success and u_tv.value is not None:
        if normalized.get("totalVehicleCount") is None:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_METRIC",
                    "Invalid range for metric: totalVehicleCount",
                    "VALIDATE",
                    order,
                )
            )
    return issues


def validate_camera(normalized: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    meta: dict[str, UnwrapResult] = normalized.get("_meta", {})
    order = 100

    _domain_required_unwrap(issues, meta, "refIntersection", order, "refIntersection")
    order += 1
    if normalized.get("refIntersection") is not None:
        ref = normalized["refIntersection"]
        if not isinstance(ref, str) or not ref.strip():
            issues.append(
                ValidationIssue(
                    "REQUIRED_DOMAIN_FIELD_MISSING",
                    "Missing required domain field: refIntersection",
                    "VALIDATE",
                    order,
                )
            )
    order += 1
    _domain_required_unwrap(issues, meta, "id", order, "id")
    order += 1

    for key, label in (
        ("vehicleCount", "vehicleCount"),
        ("averageSpeed", "averageSpeed"),
    ):
        u = meta.get(key, UnwrapResult(None, True, present=False))
        if u.present and u.is_success and u.value is not None:
            val = normalized.get(key)
            if val is None or (isinstance(val, (int, float)) and val < 0):
                issues.append(
                    ValidationIssue(
                        "INVALID_RANGE_METRIC",
                        f"Invalid range for metric: {label}",
                        "VALIDATE",
                        order,
                    )
                )
        order += 1

    u_occ = meta.get("occupancyRate", UnwrapResult(None, True, present=False))
    if u_occ.present and u_occ.is_success and u_occ.value is not None:
        occ = normalized.get("occupancyRate")
        if occ is None or occ < 0.0 or occ > 100.0:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_OCCUPANCY",
                    f"Occupancy out of range: {occ}",
                    "VALIDATE",
                    order,
                )
            )
    order += 1

    u_conf = meta.get("confidence", UnwrapResult(None, True, present=False))
    if u_conf.present and u_conf.is_success and u_conf.value is not None:
        conf = normalized.get("confidence")
        if conf is None or conf < 0.0 or conf > 1.0:
            issues.append(
                ValidationIssue(
                    "INVALID_RANGE_METRIC",
                    f"Invalid confidence range: {conf}",
                    "VALIDATE",
                    order,
                )
            )
    return issues


def validate_fields(
    entity_or_event_type: str,
    record: BronzeInputRecord,
    normalized: dict[str, Any],
) -> ValidationResult:
    try:
        issues: list[ValidationIssue] = []
        if isinstance(record, BronzeEntityInputRecord):
            issues.extend(validate_entity_envelope(record))
        elif isinstance(record, BronzeRunInputRecord):
            issues.extend(validate_run_envelope(record))

        if entity_or_event_type == ENTITY_VEHICLE_SENSOR:
            issues.extend(validate_vehicle_sensor(normalized))
        elif entity_or_event_type == ENTITY_TRAFFIC_LIGHT:
            issues.extend(validate_traffic_light(normalized))
        elif entity_or_event_type == ENTITY_INTERSECTION:
            issues.extend(validate_intersection(normalized))
        elif entity_or_event_type == ENTITY_CAMERA:
            issues.extend(validate_camera(normalized))
        elif entity_or_event_type == EVENT_RUN_STARTED:
            pass  # envelope-only

        return _pick(issues)
    except Exception as exc:  # noqa: BLE001
        return ValidationResult(
            False,
            "UNHANDLED_ENGINE_EXCEPTION",
            f"Validator exception: {exc}",
            "VALIDATE",
        )
