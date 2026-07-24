"""
entity_generator.py — Build NGSI-LD entity payloads from a SUMO snapshot.

Field names, enums, and required attrs follow the four entity specs:
  Camera/Camera/doc/spec.md
  Intersection/Intersection/doc/spec.md
  TrafficLight/TrafficLight/doc/spec.md
  VehicleSensor/VehicleSensor/doc/spec.md

VehicleSensor observation attrs also follow vehiclesensor_model.yaml where
spec.md is truncated (dateObserved, vehicleCount, pcuEquivalent, …).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import configuration.config as cfg

CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _prop(value: Any) -> dict:
    return {"type": "Property", "value": value}


def _datetime_prop(iso_str: str) -> dict:
    return {"type": "Property", "value": {"@type": "DateTime", "@value": iso_str}}


def _rel(object_id: str) -> dict:
    return {"type": "Relationship", "object": object_id}


def _rel_list(object_ids: List[str]) -> dict:
    """Multi-valued Relationship (array of objects)."""
    return {"type": "Relationship", "object": object_ids}


def _geoprop(lng: float, lat: float) -> dict:
    return {
        "type": "GeoProperty",
        "value": {"type": "Point", "coordinates": [lng, lat]},
    }


def _intersection_id(node_id: str) -> str:
    return f"urn:ngsi-ld:Intersection:{node_id}"


def _camera_id(node_id: str) -> str:
    return f"urn:ngsi-ld:Camera:{node_id}"


def _traffic_light_id(node_id: str, direction: str) -> str:
    return f"urn:ngsi-ld:TrafficLight:{node_id}-{direction}"


def _vehicle_sensor_id(node_id: str, traffic_dir: str) -> str:
    return f"urn:ngsi-ld:VehicleSensor:{node_id}:{traffic_dir}"


def _meta(node_id: str) -> Dict[str, Any]:
    return cfg.INTERSECTION_META.get(
        node_id, {"name": f"Intersection {node_id}", "lat": 0.0, "lng": 0.0}
    )


def build_intersection(node_id: str, snapshot: dict) -> dict:
    """Intersection — fields from Intersection/doc/spec.md + Phase 2 additive context."""
    meta = _meta(node_id)
    dirs = snapshot["directions"]
    total_vehicles = sum(d["vehicle_count"] for d in dirs.values())
    total_pcu = sum(d["pcu_equivalent"] for d in dirs.values())
    density = cfg.density_label_intersection(total_pcu)
    overall = cfg.traffic_status_from_density(density)
    incidents = snapshot.get("incidents") or []
    now = _now_iso()

    tl_ids = [_traffic_light_id(node_id, d) for d in cfg.DIRECTIONS]
    vs_ids = [
        _vehicle_sensor_id(node_id, cfg.DIRECTION_TO_TRAFFIC[d])
        for d in cfg.DIRECTIONS
    ]

    phenomena = snapshot.get("derived_phenomena") or {}
    op = snapshot.get("operational_state") or {}
    causes = snapshot.get("probable_causes") or []
    primary = causes[0] if causes else None

    entity: Dict[str, Any] = {
        "id": _intersection_id(node_id),
        "type": "Intersection",
        "name": _prop(meta["name"]),
        "location": _geoprop(float(meta["lng"]), float(meta["lat"])),
        "intersectionStatus": _prop("ACTIVE"),
        "numberOfApproaches": _prop(4),
        "frequentCongestion": _prop(overall in ("HEAVY", "CONGESTED")),
        "refTrafficLights": _rel_list(tl_ids),
        "refCameras": _rel_list([_camera_id(node_id)]),
        "refVehicleSensors": _rel_list(vs_ids),
        "dateObserved": _datetime_prop(now),
        "simulationTime": _prop(float(snapshot.get("simulation_time_sec") or 0.0)),
        "overallTrafficStatus": _prop(overall),
        "totalVehicleCount": _prop(total_vehicles),
        "hasActiveIncident": _prop(
            len(incidents) > 0 or bool(op.get("incident_active"))
        ),
        # Phase 2 additive (Strategy C) — current context only, no evidence dump
        "derivedTrafficState": _prop(
            snapshot.get("derived_traffic_state")
            or snapshot.get("aggregate_traffic_state")
            or snapshot.get("derived_aggregate_context")
            or "FREE_FLOW"
        ),
        "hasSpillback": _prop(bool(phenomena.get("spillback_active"))),
        "isBoxBlocked": _prop(bool(phenomena.get("box_blocked"))),
        "@context": CONTEXT,
    }
    if primary:
        entity["probableCauseType"] = _prop(primary.get("type"))
        src = primary.get("source_node")
        if src:
            entity["affectedBy"] = _rel(_intersection_id(str(src)))
        if primary.get("observed_to_s") is not None:
            entity["causeDetectedAt"] = _prop(float(primary["observed_to_s"]))
    return entity


def build_traffic_light(node_id: str, direction: str, snapshot: dict) -> dict:
    """TrafficLight — fields from TrafficLight/doc/spec.md. Durations from TraCI snapshot."""
    meta = _meta(node_id)
    color = snapshot["colors"][direction]
    status = cfg.COLOR_TO_STATUS.get(color.lower(), "OFF")
    traffic_dir = cfg.DIRECTION_TO_TRAFFIC[direction]
    green_dur = int(snapshot.get("green_duration") or snapshot.get("phase_duration") or 42)
    yellow_dur = int(snapshot.get("yellow_duration") or 3)
    red_dur = int(snapshot.get("red_duration") or green_dur)

    return {
        "id": _traffic_light_id(node_id, direction),
        "type": "TrafficLight",
        "name": _prop(f"{meta['name']} {traffic_dir}"),
        "location": _geoprop(float(meta["lng"]), float(meta["lat"])),
        "currentStatus": _prop(status),
        "timingMode": _prop("FIXED_TIME"),
        "workingState": _prop("OK"),
        "trafficDirection": _prop(traffic_dir),
        "greenDurationCurrent": _prop(green_dur),
        "redDurationCurrent": _prop(red_dur),
        "yellowDuration": _prop(yellow_dur),
        "refIntersection": _rel(_intersection_id(node_id)),
        "refCamera": _rel(_camera_id(node_id)),
        "@context": CONTEXT,
    }


def build_camera(node_id: str, snapshot: dict) -> dict:
    """Camera — fields from Camera/doc/spec.md."""
    meta = _meta(node_id)
    dirs = snapshot["directions"]
    total = sum(d["vehicle_count"] for d in dirs.values())
    total_pcu = sum(d["pcu_equivalent"] for d in dirs.values())
    speeds = [
        d["average_speed_kmh"] for d in dirs.values() if d["average_speed_kmh"] > 0
    ]
    avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0.0
    occs = [d["occupancy_pct"] for d in dirs.values()]
    avg_occ = round(sum(occs) / len(occs), 1) if occs else 0.0
    density = cfg.density_label_intersection(total_pcu)
    traffic_status = cfg.traffic_status_from_density(density)
    incidents = snapshot.get("incidents") or []
    has_incident = len(incidents) > 0
    now = _now_iso()

    entity: Dict[str, Any] = {
        "id": _camera_id(node_id),
        "type": "Camera",
        "name": _prop(f"{meta['name']} overview camera"),
        "location": _geoprop(float(meta["lng"]), float(meta["lat"])),
        "cameraNum": _prop(1),
        "cameraType": _prop("FIXED"),
        "cameraUsage": _prop("TRAFFIC"),
        "cameraStatus": _prop("ok"),
        "refIntersection": _rel(_intersection_id(node_id)),
        "dateObserved": _datetime_prop(now),
        "trafficDirection": _prop("ALL_DIRECTIONS"),
        "vehicleCount": _prop(total),
        "averageSpeed": _prop(avg_speed),
        "occupancyRate": _prop(avg_occ),
        "trafficStatus": _prop(traffic_status),
        "incidentDetected": _prop(has_incident),
        "confidence": _prop(1.0),  # PLACEHOLDER — no camera CV model; always 1.0
        "recommendedSignalAction": _prop("KEEP"),
        "@context": CONTEXT,
    }
    if has_incident:
        entity["incidentType"] = _prop("ACCIDENT")
        entity["incidentSeverity"] = _prop("MEDIUM")
    else:
        entity["incidentType"] = _prop("NONE")
        entity["incidentSeverity"] = _prop("NONE")
    return entity


def build_vehicle_sensor(node_id: str, direction: str, snapshot: dict) -> dict:
    """VehicleSensor — fields from VehicleSensor/doc/spec.md + model observation attrs."""
    d = snapshot["directions"][direction]
    meta = _meta(node_id)
    traffic_dir = cfg.DIRECTION_TO_TRAFFIC[direction]
    traffic_status = cfg.traffic_status_from_density(d.get("density", "LOW"))
    class_comp = d.get("vehicle_class_composition")
    now = _now_iso()

    entity: Dict[str, Any] = {
        "id": _vehicle_sensor_id(node_id, traffic_dir),
        "type": "VehicleSensor",
        "name": _prop(f"{meta['name']} {traffic_dir} approach"),
        "location": _geoprop(float(meta["lng"]), float(meta["lat"])),
        "sensorType": _prop("VIRTUAL"),
        "sensorStatus": _prop("OK"),
        "trafficDirection": _prop(traffic_dir),
        "refIntersection": _rel(_intersection_id(node_id)),
        "refCamera": _rel(_camera_id(node_id)),
        "refTrafficLight": _rel(_traffic_light_id(node_id, direction)),
        "dateObserved": _datetime_prop(now),
        "simulationTime": _prop(float(snapshot.get("simulation_time_sec") or 0.0)),
        "vehicleCount": _prop(d["vehicle_count"]),
        "pcuEquivalent": _prop(d["pcu_equivalent"]),
        "leftTurnCount": _prop(d["left_count"]),
        "straightCount": _prop(d["straight_count"]),
        "rightTurnCount": _prop(d["right_count"]),
        "averageSpeed": _prop(d["average_speed_kmh"]),
        "waitingVehicleCount": _prop(d["waiting_vehicle_count"]),
        "queueLength": _prop(d["queue_length_m"]),
        "queueStraight": _prop(d.get("queue_by_movement", {}).get("straight", 0.0)),
        "queueLeft": _prop(d.get("queue_by_movement", {}).get("left", 0.0)),
        "queueRight": _prop(d.get("queue_by_movement", {}).get("right", 0.0)),
        "occupancyRate": _prop(d["occupancy_pct"]),
        "trafficStatus": _prop(traffic_status),
        "arrivalRatePcuPerSec": _prop(d.get("arrival_rate_pcu_per_sec", 0.0)),
        "waitingReasonCounts": _prop(
            d.get("waiting_reason_counts", {"RED_PHASE": 0, "CONGESTION": 0})
        ),
        "theoreticalSpeed": _prop(d.get("theoretical_speed_kmh", 0.0)),
        "@context": CONTEXT,
    }
    dominant: Optional[str] = d.get("dominant_waiting_reason")
    if dominant is not None:
        entity["dominantWaitingReason"] = _prop(dominant)
    if class_comp is not None:
        entity["vehicleClassComposition"] = _prop(class_comp)
    # Phase 2 additive per-direction context
    dctx = (snapshot.get("direction_contexts") or {}).get(direction)
    dstates = (snapshot.get("direction_states") or {}).get(direction) or {}
    traffic_state = dctx or dstates.get("traffic_state")
    if traffic_state:
        entity["derivedTrafficState"] = _prop(traffic_state)
    phenomena = snapshot.get("derived_phenomena") or {}
    entity["spillbackRisk"] = _prop(bool(phenomena.get("spillback_risk")))
    op = snapshot.get("operational_state") or {}
    entity["operationalState"] = _prop(
        {
            "incident_active": bool(op.get("incident_active")),
            "emergency_preemption_active": bool(op.get("emergency_preemption_active")),
            "downstream_restriction_active": bool(op.get("downstream_restriction_active")),
        }
    )
    return entity


def build_all_entities(node_id: str, snapshot: dict) -> list:
    entities = [
        build_intersection(node_id, snapshot),
        build_camera(node_id, snapshot),
    ]
    for direction in cfg.DIRECTIONS:
        entities.append(build_traffic_light(node_id, direction, snapshot))
    for direction in cfg.DIRECTIONS:
        entities.append(build_vehicle_sensor(node_id, direction, snapshot))
    return entities
