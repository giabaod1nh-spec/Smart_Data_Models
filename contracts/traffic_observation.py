"""Technology-neutral intersection observation contract (v1).

An observation is one logical intersection at one sampling cycle. The payload
keeps the complete physical/context snapshot so downstream projections can
derive all current NGSI-LD entities without calling SUMO.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Mapping

EVENT_TYPE = "IntersectionObservation"
SCHEMA_VERSION = 1
TOPIC = "traffic.observations.v1"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def observation_event_id(run_id: str, intersection_id: str, cycle_sequence: int) -> str:
    identity = f"{SCHEMA_VERSION}|{run_id}|{intersection_id}|{cycle_sequence}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def observation_key(event: Mapping[str, Any]) -> str:
    return f"{event['simulationRunId']}:{event['intersectionId']}"


def _check_finite(value: Any, path: str = "payload") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} must be finite")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _check_finite(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check_finite(item, f"{path}[{index}]")


def validate_observation(event: Mapping[str, Any]) -> None:
    if event.get("eventType") != EVENT_TYPE or event.get("schemaVersion") != SCHEMA_VERSION:
        raise ValueError("unsupported observation event type or schema version")
    run = event.get("simulationRunId")
    node = event.get("intersectionId")
    if not isinstance(run, str) or not run or not isinstance(node, str) or not node:
        raise ValueError("simulationRunId and intersectionId are required")
    cycle = event.get("cycleSequence")
    step = event.get("simulationStep")
    if type(cycle) is not int or cycle < 0 or type(step) is not int or step < 0:
        raise ValueError("cycleSequence and simulationStep must be non-negative integers")
    sim_time = event.get("simulationTimeSeconds")
    if not isinstance(sim_time, (int, float)) or isinstance(sim_time, bool) or not math.isfinite(sim_time) or sim_time < 0:
        raise ValueError("simulationTimeSeconds must be a finite non-negative number")
    if event.get("eventId") != observation_event_id(run, node, cycle):
        raise ValueError("eventId does not match logical event identity")
    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("payload must be an object")
    if payload.get("simulation_run_id") != run or payload.get("node_id") != node:
        raise ValueError("payload identity differs from event identity")
    if not isinstance(payload.get("scenario"), str) or not payload["scenario"]:
        raise ValueError("payload scenario is required")
    if not isinstance(payload.get("control_mode"), str) or not payload["control_mode"]:
        raise ValueError("payload control_mode is required")
    if abs(float(payload.get("simulation_time_sec", -1)) - float(sim_time)) > 1e-6:
        raise ValueError("payload simulation time differs from event time")
    directions = payload.get("directions")
    colors = payload.get("colors")
    if not isinstance(directions, Mapping) or not directions or not isinstance(colors, Mapping):
        raise ValueError("directions and colors are required")
    if set(directions) != set(colors):
        raise ValueError("directions and colors must match")
    for name, direction in directions.items():
        if not isinstance(direction, Mapping):
            raise ValueError(f"direction {name} must be an object")
        for metric in ("vehicle_count", "waiting_vehicle_count", "queue_length_m", "average_speed_kmh"):
            value = direction.get(metric)
            if value is not None and (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"direction {name}.{metric} must be finite and non-negative")
    _check_finite(payload)
    if not isinstance(event.get("capturedAt"), str) or not event["capturedAt"]:
        raise ValueError("capturedAt is required")
    if event.get("payloadHash") != hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest():
        raise ValueError("payloadHash mismatch")


def build_observation(
    snapshot: Mapping[str, Any], *, cycle_sequence: int, simulation_step: int,
    captured_at: str | None = None,
) -> dict[str, Any]:
    payload = copy.deepcopy(dict(snapshot))
    run = str(payload.get("simulation_run_id") or "")
    node = str(payload.get("node_id") or "")
    event = {
        "eventId": observation_event_id(run, node, cycle_sequence),
        "eventType": EVENT_TYPE,
        "schemaVersion": SCHEMA_VERSION,
        "simulationRunId": run,
        "intersectionId": node,
        "cycleSequence": int(cycle_sequence),
        "simulationStep": int(simulation_step),
        "simulationTimeSeconds": payload.get("simulation_time_sec"),
        "capturedAt": captured_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payloadHash": hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest(),
        "payload": payload,
    }
    validate_observation(event)
    return event
