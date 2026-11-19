"""Technology-neutral, complete logical-intersection observation contract V1.

The builder selects declared domain fields. Run catalog lookup and NGSI-LD
projection are separate contextual/consumer operations.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker

from contracts.topology.v1_digest import validate_digest_syntax

EVENT_TYPE = "IntersectionObservation"
SCHEMA_VERSION = 1
TOPIC = "traffic.observations.v1"

_SCHEMA_PATH = Path(__file__).with_name("events") / "intersection-observation-v1.schema.json"
_SCHEMA = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
Draft202012Validator.check_schema(_SCHEMA)
_STRUCTURAL_VALIDATOR = Draft202012Validator(_SCHEMA, format_checker=FormatChecker())

_REQUIRED_PAYLOAD_FIELDS = (
    "node_id", "simulation_run_id", "simulation_time_sec", "scenario",
    "control_mode", "phase", "incidents",
)
_OPTIONAL_PAYLOAD_FIELDS = (
    "timing_mode", "next_phase", "phase_remaining", "phase_duration",
    "green_duration", "yellow_duration", "red_duration", "blocked_direction",
    "downstream_edge_full", "spillback_pressure", "spillback_detected",
    "intersection_box_blocked", "vehicles_blocked_at_entry",
    "yellow_commitment_count", "preemption_active", "derived_traffic_state",
    "anomaly_score", "anomaly_label",
)
_DIRECTION_FIELDS = (
    "vehicle_count", "pcu_equivalent", "left_count", "straight_count",
    "right_count", "average_speed_kmh", "waiting_vehicle_count",
    "queue_length_m", "occupancy_pct", "queue_length_vehicles", "density",
    "estimated_density_veh_per_km", "arrival_rate_pcu_per_sec",
    "dominant_waiting_reason", "theoretical_speed_kmh", "moto_front_pct",
    "approach_length_m", "full_link_halting_count", "full_link_jam_length_m",
    "storage_utilization_lane", "storage_utilization_pcu",
    "nominal_storage_pcu_geometric",
)
_CORE_METRICS = frozenset((
    "vehicle_count", "pcu_equivalent", "left_count", "straight_count",
    "right_count", "average_speed_kmh", "waiting_vehicle_count",
    "queue_length_m", "occupancy_pct",
))
_CAUSE_FIELDS = (
    "type", "source_node", "affected_node", "target_node", "link_id",
    "link", "confidence", "inference_method", "window_s", "observed_from_s",
    "observed_to_s",
)
_EVIDENCE_FIELDS = (
    "queue_slope_veh_per_s", "occupancy", "observation_window_s",
    "discharge_drop_ratio", "queue_length_m",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def observation_event_id(run_id: str, intersection_id: str, cycle_sequence: int) -> str:
    identity = f"{SCHEMA_VERSION}|{run_id}|{intersection_id}|{cycle_sequence}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def observation_key(event: Mapping[str, Any]) -> str:
    return f"{event['simulationRunId']}:{event['intersectionId']}"


def _pick(source: Mapping[str, Any], names: tuple[str, ...]) -> dict[str, Any]:
    return {name: copy.deepcopy(source[name]) for name in names if name in source}


def _pick_direction(source: Mapping[str, Any]) -> dict[str, Any]:
    result = _pick(source, _DIRECTION_FIELDS)
    for name, keys in (
        ("queue_by_movement", ("straight", "left", "right")),
        ("waiting_reason_counts", ("RED_PHASE", "CONGESTION")),
        ("metric_availability", tuple(sorted(_CORE_METRICS))),
    ):
        if name in source:
            result[name] = _pick(source[name], keys)
    if "vehicle_class_composition" in source:
        result["vehicle_class_composition"] = copy.deepcopy(source["vehicle_class_composition"])
    return result


def select_v1_payload(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze declared domain state; unknown internal snapshot fields are ignored."""
    payload = _pick(snapshot, _REQUIRED_PAYLOAD_FIELDS + _OPTIONAL_PAYLOAD_FIELDS)
    payload["colors"] = copy.deepcopy(snapshot.get("colors"))
    directions = snapshot.get("directions")
    if not isinstance(directions, Mapping):
        raise ValueError("snapshot directions must be an object")
    payload["directions"] = {
        name: _pick_direction(value) if isinstance(value, Mapping) else value
        for name, value in directions.items()
    }
    for name, keys in (
        ("derived_phenomena", ("spillback_risk", "spillback_active", "box_blocked")),
        ("operational_state", (
            "incident_active", "emergency_preemption_active",
            "downstream_restriction_active",
        )),
    ):
        if name in snapshot:
            payload[name] = _pick(snapshot[name], keys)
    if "direction_contexts" in snapshot:
        payload["direction_contexts"] = copy.deepcopy(snapshot["direction_contexts"])
    if "incidents" in payload and isinstance(payload["incidents"], list):
        payload["incidents"] = [
            _pick(item, ("type", "direction", "time")) if isinstance(item, Mapping) else item
            for item in payload["incidents"]
        ]
    if "probable_causes" in snapshot:
        causes = []
        for item in snapshot["probable_causes"]:
            if not isinstance(item, Mapping):
                causes.append(item)
                continue
            cause = _pick(item, _CAUSE_FIELDS)
            if "evidence" in item:
                cause["evidence"] = _pick(item["evidence"], _EVIDENCE_FIELDS)
            causes.append(cause)
        payload["probable_causes"] = causes
    return payload


def _check_finite(value: Any, path: str = "event") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path} must be finite")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _check_finite(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check_finite(item, f"{path}[{index}]")


def validate_observation(event: Mapping[str, Any]) -> None:
    """Validate wire shape and local semantics, without consulting any archive."""
    errors = sorted(_STRUCTURAL_VALIDATOR.iter_errors(event), key=lambda err: tuple(map(str, err.path)))
    if errors:
        error = errors[0]
        location = ".".join(map(str, error.path)) or "event"
        raise ValueError(f"{location}: {error.message}")
    _check_finite(event)
    validate_digest_syntax(event["topologyDigest"])
    run = event["simulationRunId"]
    node = event["intersectionId"]
    cycle = event["cycleSequence"]
    if event["eventId"] != observation_event_id(run, node, cycle):
        raise ValueError("eventId does not match logical observation identity")
    payload = event["payload"]
    if payload["simulation_run_id"] != run or payload["node_id"] != node:
        raise ValueError("payload identity differs from envelope identity")
    if payload["scenario"] != event["scenarioId"]:
        raise ValueError("payload scenario differs from envelope scenario")
    if payload["simulation_time_sec"] != event["simulationTimeSeconds"]:
        raise ValueError("payload simulation time differs from envelope time")
    if set(payload["directions"]) != set(payload["colors"]):
        raise ValueError("directions and colors must cover the same approaches")
    for direction_name, direction in payload["directions"].items():
        availability = direction.get("metric_availability", {})
        null_metrics = {name for name in _CORE_METRICS if direction[name] is None}
        if set(availability) != null_metrics:
            raise ValueError(
                f"direction {direction_name}: metric_availability must identify exactly the null core metrics"
            )
    actual_hash = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    if event["payloadHash"] != actual_hash:
        raise ValueError("payloadHash mismatch")


def assert_same_observation(original: Mapping[str, Any], retry: Mapping[str, Any]) -> None:
    """Contract-level retry rule; Phase 3 wires this into Outbox insertion."""
    validate_observation(original)
    validate_observation(retry)
    if original["eventId"] != retry["eventId"]:
        raise ValueError("retry eventId differs from the frozen observation")
    if original["payloadHash"] != retry["payloadHash"]:
        raise ValueError("same eventId has a conflicting payloadHash")
    if canonical_json(original) != canonical_json(retry):
        raise ValueError("same eventId must preserve the entire frozen observation")


def build_observation(
    snapshot: Mapping[str, Any], *, cycle_sequence: int, simulation_step: int,
    captured_at: str | None = None, topology_digest: str | None = None,
) -> dict[str, Any]:
    """Build from an already captured snapshot, never invent capture metadata."""
    payload = select_v1_payload(snapshot)
    run = payload.get("simulation_run_id")
    node = payload.get("node_id")
    if not isinstance(run, str) or not isinstance(node, str):
        raise ValueError("snapshot needs simulation_run_id and node_id")
    event = {
        "eventId": observation_event_id(run, node, cycle_sequence),
        "eventType": EVENT_TYPE,
        "schemaVersion": SCHEMA_VERSION,
        "simulationRunId": run,
        "intersectionId": node,
        "scenarioId": payload.get("scenario"),
        "cycleSequence": cycle_sequence,
        "simulationStep": simulation_step,
        "simulationTimeSeconds": payload.get("simulation_time_sec"),
        "capturedAt": captured_at if captured_at is not None else snapshot.get("captured_at"),
        "topologyDigest": topology_digest if topology_digest is not None else snapshot.get("topology_digest"),
        "payloadHash": hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest(),
        "payload": payload,
    }
    validate_observation(event)
    return event
