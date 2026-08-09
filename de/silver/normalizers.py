"""Silver Plan 2 — field normalizers (pure, deterministic)."""
from __future__ import annotations

import math
from typing import Any, Optional

from de.silver.contracts import (
    DIRECTION_MAP,
    SIGNAL_STATUS_VALUES,
    TIMING_MODE_VALUES,
    TRAFFIC_STATUS_VALUES,
)
from de.silver.unwrapper import UnwrapResult


def normalize_direction(raw: Any) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    return DIRECTION_MAP.get(raw.strip().upper())


def normalize_boolean(raw: Any) -> Optional[int]:
    if isinstance(raw, bool):
        return 1 if raw else 0
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        if raw == 1:
            return 1
        if raw == 0:
            return 0
        return None
    if isinstance(raw, str):
        s = raw.strip().lower()
        if s in ("true", "1"):
            return 1
        if s in ("false", "0"):
            return 0
    return None


def coerce_uint32(val: Any) -> Optional[int]:
    if isinstance(val, bool):
        return None
    if val is None:
        return None
    if isinstance(val, int):
        if val < 0:
            return None
        return val
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        if val < 0 or not val.is_integer():
            return None
        return int(val)
    if isinstance(val, str):
        try:
            v = float(val.strip())
            if math.isnan(v) or math.isinf(v) or v < 0 or not v.is_integer():
                return None
            return int(v)
        except ValueError:
            return None
    return None


def coerce_float32(val: Any) -> Optional[float]:
    if isinstance(val, bool):
        return None
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return float(val)
    if isinstance(val, str):
        try:
            v = float(val.strip())
            if math.isnan(v) or math.isinf(v):
                return None
            return float(v)
        except ValueError:
            return None
    return None


def normalize_upper_enum(raw: Any, allowed: Optional[set[str]] = None) -> Optional[str]:
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    s = raw.strip().upper()
    if not s:
        return None
    if allowed is not None and s not in allowed:
        return None
    return s


def _raw(result: UnwrapResult) -> Any:
    return result.value if result.is_success else None


def normalize_vehicle_sensor(unwrapped: dict[str, UnwrapResult]) -> dict[str, Any]:
    return {
        "refIntersection": _raw(unwrapped["refIntersection"]),
        "trafficDirection": normalize_direction(_raw(unwrapped["trafficDirection"])),
        "trafficDirection_raw": _raw(unwrapped["trafficDirection"]),
        "vehicleCount": coerce_uint32(_raw(unwrapped["vehicleCount"])),
        "pcuEquivalent": coerce_float32(_raw(unwrapped["pcuEquivalent"])),
        "averageSpeed": coerce_float32(_raw(unwrapped["averageSpeed"])),
        "queueLength": coerce_float32(_raw(unwrapped["queueLength"])),
        "occupancyRate": coerce_float32(_raw(unwrapped["occupancyRate"])),
        "waitingVehicleCount": coerce_uint32(_raw(unwrapped["waitingVehicleCount"])),
        "arrivalRatePcuPerSec": coerce_float32(_raw(unwrapped["arrivalRatePcuPerSec"])),
        "trafficStatus": normalize_upper_enum(
            _raw(unwrapped["trafficStatus"]), TRAFFIC_STATUS_VALUES
        ),
        "spillbackRisk": normalize_boolean(_raw(unwrapped["spillbackRisk"])),
        "dominantWaitingReason": (
            str(_raw(unwrapped["dominantWaitingReason"]))
            if _raw(unwrapped["dominantWaitingReason"]) is not None
            else None
        ),
        "_meta": {k: unwrapped[k] for k in unwrapped},
    }


def normalize_traffic_light(unwrapped: dict[str, UnwrapResult]) -> dict[str, Any]:
    return {
        "refIntersection": _raw(unwrapped["refIntersection"]),
        "trafficDirection": normalize_direction(_raw(unwrapped["trafficDirection"])),
        "trafficDirection_raw": _raw(unwrapped["trafficDirection"]),
        "currentStatus": normalize_upper_enum(
            _raw(unwrapped["currentStatus"]), SIGNAL_STATUS_VALUES
        ),
        "currentPhase": (
            str(_raw(unwrapped["currentPhase"])).strip()
            if _raw(unwrapped["currentPhase"]) is not None
            else None
        ),
        "greenDurationCurrent": coerce_float32(_raw(unwrapped["greenDurationCurrent"])),
        "redDurationCurrent": coerce_float32(_raw(unwrapped["redDurationCurrent"])),
        "yellowDuration": coerce_float32(_raw(unwrapped["yellowDuration"])),
        "timingMode": normalize_upper_enum(_raw(unwrapped["timingMode"]), TIMING_MODE_VALUES),
        "_meta": {k: unwrapped[k] for k in unwrapped},
    }


def normalize_intersection(unwrapped: dict[str, UnwrapResult]) -> dict[str, Any]:
    return {
        "id": _raw(unwrapped["id"]),
        "overallTrafficStatus": normalize_upper_enum(_raw(unwrapped["overallTrafficStatus"])),
        "derivedTrafficState": normalize_upper_enum(_raw(unwrapped["derivedTrafficState"])),
        "currentPhase": (
            str(_raw(unwrapped["currentPhase"])).strip()
            if _raw(unwrapped["currentPhase"]) is not None
            else None
        ),
        "hasActiveIncident": normalize_boolean(_raw(unwrapped["hasActiveIncident"])),
        "hasSpillback": normalize_boolean(_raw(unwrapped["hasSpillback"])),
        "isBoxBlocked": normalize_boolean(_raw(unwrapped["isBoxBlocked"])),
        "totalVehicleCount": coerce_uint32(_raw(unwrapped["totalVehicleCount"])),
        "name": (
            str(_raw(unwrapped["name"])).strip() if _raw(unwrapped["name"]) is not None else None
        ),
        "location": _raw(unwrapped["location"]),
        "_meta": {k: unwrapped[k] for k in unwrapped},
    }


def normalize_camera(unwrapped: dict[str, UnwrapResult]) -> dict[str, Any]:
    return {
        "refIntersection": _raw(unwrapped["refIntersection"]),
        "id": _raw(unwrapped["id"]),
        "vehicleCount": coerce_uint32(_raw(unwrapped["vehicleCount"])),
        "averageSpeed": coerce_float32(_raw(unwrapped["averageSpeed"])),
        "occupancyRate": coerce_float32(_raw(unwrapped["occupancyRate"])),
        "trafficStatus": normalize_upper_enum(
            _raw(unwrapped["trafficStatus"]), TRAFFIC_STATUS_VALUES
        ),
        "incidentDetected": normalize_boolean(_raw(unwrapped["incidentDetected"])),
        "confidence": coerce_float32(_raw(unwrapped["confidence"])),
        "recommendedSignalAction": normalize_upper_enum(
            _raw(unwrapped["recommendedSignalAction"])
        ),
        "incidentType": normalize_upper_enum(_raw(unwrapped["incidentType"])),
        "incidentSeverity": normalize_upper_enum(_raw(unwrapped["incidentSeverity"])),
        "_meta": {k: unwrapped[k] for k in unwrapped},
    }


def normalize_run_started(unwrapped: dict[str, UnwrapResult]) -> dict[str, Any]:
    return {"_meta": unwrapped}


def normalize_fields(entity_or_event_type: str, unwrapped: dict[str, UnwrapResult]) -> dict[str, Any]:
    from de.silver.contracts import (
        ENTITY_CAMERA,
        ENTITY_INTERSECTION,
        ENTITY_TRAFFIC_LIGHT,
        ENTITY_VEHICLE_SENSOR,
        EVENT_RUN_STARTED,
    )

    try:
        if entity_or_event_type == ENTITY_VEHICLE_SENSOR:
            return normalize_vehicle_sensor(unwrapped)
        if entity_or_event_type == ENTITY_TRAFFIC_LIGHT:
            return normalize_traffic_light(unwrapped)
        if entity_or_event_type == ENTITY_INTERSECTION:
            return normalize_intersection(unwrapped)
        if entity_or_event_type == ENTITY_CAMERA:
            return normalize_camera(unwrapped)
        if entity_or_event_type == EVENT_RUN_STARTED:
            return normalize_run_started(unwrapped)
        return {"_meta": unwrapped}
    except Exception:  # noqa: BLE001
        return {"_meta": unwrapped}
