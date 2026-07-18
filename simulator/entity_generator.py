"""
entity_generator.py — Tang Context Layer

Nhan snapshot (dict thuan Python) tu CityNetworkEngine.get_snapshot(node_id),
KHONG biet gi ve logic vat ly ben trong, chi doc va dinh dang lai theo NGSI-LD.
"""
from datetime import datetime, timezone
from scenarios import DIRECTIONS

CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

INTERSECTION_META = {
    "A": {"name": "Nguyen Hue - Le Loi",      "lat": 10.7769, "lng": 106.7009},
    "B": {"name": "Dien Bien Phu - Hai Ba Trung", "lat": 10.7889, "lng": 106.6917},
    "C": {"name": "Cong Hoa - Hoang Van Thu", "lat": 10.8005, "lng": 106.6520},
    "D": {"name": "Vo Van Kiet - Nguyen Tri Phuong", "lat": 10.7550, "lng": 106.6700},
}

# SDM VehicleSensor / Camera / TrafficLight shared enums
DIRECTION_TO_TRAFFIC = {
    "North": "NORTHBOUND",
    "South": "SOUTHBOUND",
    "East":  "EASTBOUND",
    "West":  "WESTBOUND",
}

# Simulator density (LOW/MEDIUM/HIGH) → SDM trafficStatus (Camera/Intersection)
DENSITY_TO_TRAFFIC_STATUS = {
    "LOW":    "LIGHT",
    "MEDIUM": "MODERATE",
    "HIGH":   "HEAVY",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _prop(value) -> dict:
    return {"type": "Property", "value": value}


def _datetime_prop(iso_str: str) -> dict:
    return {"type": "Property", "value": {"@type": "DateTime", "@value": iso_str}}


def _rel(object_id: str) -> dict:
    return {"type": "Relationship", "object": object_id}


def build_intersection(node_id: str, snapshot: dict) -> dict:
    meta = INTERSECTION_META[node_id]
    dirs = snapshot["directions"]
    total_vehicles = sum(d["vehicle_count"] for d in dirs.values())
    total_pcu = sum(d["pcu_equivalent"] for d in dirs.values())
    speeds = [d["average_speed_kmh"] for d in dirs.values() if d["average_speed_kmh"] > 0]
    avg_speed = round(sum(speeds) / len(speeds), 1) if speeds else 0.0
    total_queue = sum(d["queue_length_m"] for d in dirs.values())

    from traffic_engine import density_label_intersection
    density = density_label_intersection(total_pcu)

    incidents = snapshot["incidents"]
    incident_dir = incidents[-1]["direction"] if incidents else None

    return {
        "id":   f"urn:ngsi-ld:Intersection:{node_id}",
        "type": "Intersection",
        "name": _prop(meta["name"]),
        "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [meta["lng"], meta["lat"]]}},
        "status": _prop("ACTIVE"),
        "totalVehicleCount":     _prop(total_vehicles),
        "totalPcuEquivalent":    _prop(round(total_pcu, 2)),
        "averageSpeed":          _prop(avg_speed),
        "totalQueueLength":      _prop(round(total_queue, 1)),
        "density":               _prop(density),
        "currentPhase":          _prop(snapshot["phase"]),
        "nextPhase":             _prop(snapshot["next_phase"]),
        "phaseRemainingSeconds": _prop(snapshot["phase_remaining"]),
        "phaseDuration":         _prop(snapshot["phase_duration"]),
        "incidentDetected":      _prop(len(incidents) > 0),
        "incidentCount":         _prop(len(incidents)),
        "incidentDirection":     _prop(incident_dir),
        "scenario":              _prop(snapshot["scenario"]),
        "simulationTime":        _prop(snapshot.get("simulation_time_sec", 0.0)),
        "spillbackDetected":     _prop(snapshot.get("spillback_detected", False)),
        "spillbackPressure":     _prop(snapshot.get("spillback_pressure", 0.0)),
        "downstreamEdgeFull":    _prop(snapshot.get("downstream_edge_full", False)),
        "intersectionBoxBlocked": _prop(snapshot.get("intersection_box_blocked", False)),
        "vehiclesBlockedAtEntry": _prop(snapshot.get("vehicles_blocked_at_entry", 0)),
        "yellowCommitmentCount": _prop(snapshot.get("yellow_commitment_count", 0)),
        "preemptionActive":      _prop(snapshot.get("preemption_active", False)),
        "observedAt":            _datetime_prop(_now_iso()),
        "@context": CONTEXT,
    }


def build_traffic_light(node_id: str, direction: str, snapshot: dict) -> dict:
    color = snapshot["colors"][direction]
    return {
        "id":   f"urn:ngsi-ld:TrafficLight:{node_id}-{direction}",
        "type": "TrafficLight",
        "direction":        _prop(direction),
        "color":            _prop(color),
        "remainingSeconds": _prop(snapshot["phase_remaining"]),
        "currentPhase":     _prop(snapshot["phase"]),
        "greenDuration":    _prop(45),
        "yellowDuration":   _prop(5),
        "redDuration":      _prop(45),
        "mode":             _prop("AUTO"),
        "refIntersection":  _rel(f"urn:ngsi-ld:Intersection:{node_id}"),
        "@context": CONTEXT,
    }


def build_vehicle_sensor(node_id: str, direction: str, snapshot: dict) -> dict:
    """Build VehicleSensor entity — field names aligned with vehiclesensor_model.yaml v1.0.0."""
    d = snapshot["directions"][direction]
    meta = INTERSECTION_META[node_id]
    traffic_dir = DIRECTION_TO_TRAFFIC[direction]
    traffic_status = DENSITY_TO_TRAFFIC_STATUS.get(d["density"], "MODERATE")
    class_comp = d.get("vehicle_class_composition")

    entity = {
        "id":   f"urn:ngsi-ld:VehicleSensor:{node_id}:{traffic_dir}",
        "type": "VehicleSensor",
        "name":                 _prop(f"{meta['name']} {traffic_dir} approach"),
        "location": {
            "type": "GeoProperty",
            "value": {"type": "Point", "coordinates": [meta["lng"], meta["lat"]]},
        },
        "sensorType":           _prop("VIRTUAL"),
        "sensorStatus":         _prop("ok"),
        "trafficDirection":     _prop(traffic_dir),
        "refIntersection":      _rel(f"urn:ngsi-ld:Intersection:{node_id}"),
        "refCamera":            _rel(f"urn:ngsi-ld:Camera:{node_id}"),
        "refTrafficLight":      _rel(f"urn:ngsi-ld:TrafficLight:{node_id}-{direction}"),
        "dateObserved":         _datetime_prop(_now_iso()),
        "vehicleCount":         _prop(d["vehicle_count"]),
        "pcuEquivalent":        _prop(d["pcu_equivalent"]),
        "leftTurnCount":        _prop(d["left_count"]),
        "straightCount":        _prop(d["straight_count"]),
        "rightTurnCount":       _prop(d["right_count"]),
        "averageSpeed":         _prop(d["average_speed_kmh"]),
        "waitingVehicleCount":  _prop(d["waiting_vehicle_count"]),
        "queueLength":          _prop(d["queue_length_m"]),
        "queueStraight":        _prop(d.get("queue_by_movement", {}).get("straight", 0.0)),
        "queueLeft":            _prop(d.get("queue_by_movement", {}).get("left", 0.0)),
        "queueRight":           _prop(d.get("queue_by_movement", {}).get("right", 0.0)),
        "occupancyRate":        _prop(d["occupancy_pct"]),
        "trafficStatus":        _prop(traffic_status),
        "arrivalRatePcuPerSec": _prop(d.get("arrival_rate_pcu_per_sec", 0.0)),
        "waitingReasonCounts":  _prop(d.get("waiting_reason_counts", {"RED_PHASE": 0, "CONGESTION": 0})),
        "dominantWaitingReason": _prop(d.get("dominant_waiting_reason")),
        "theoreticalSpeed":     _prop(d.get("theoretical_speed_kmh", 0.0)),
        "motoFrontPct":         _prop(d.get("moto_front_pct", 0.0)),
        "@context": CONTEXT,
    }
    if class_comp is not None:
        entity["vehicleClassComposition"] = _prop(class_comp)
    return entity


def build_camera(node_id: str, snapshot: dict) -> dict:
    dirs = snapshot["directions"]
    total = sum(d["vehicle_count"] for d in dirs.values())
    incidents = snapshot["incidents"]

    return {
        "id":   f"urn:ngsi-ld:Camera:{node_id}",
        "type": "Camera",
        "vehicleCount":     _prop(total),
        "incidentDetected": _prop(len(incidents) > 0),
        "incidentCount":    _prop(len(incidents)),
        "cameraStatus":     _prop("ONLINE"),
        "snapshotTime":     _datetime_prop(_now_iso()),
        "refIntersection":  _rel(f"urn:ngsi-ld:Intersection:{node_id}"),
        "@context": CONTEXT,
    }


def build_all_entities(node_id: str, snapshot: dict) -> list:
    entities = [
        build_intersection(node_id, snapshot),
        build_camera(node_id, snapshot),
    ]
    for direction in DIRECTIONS:
        entities.append(build_traffic_light(node_id, direction, snapshot))
    for direction in DIRECTIONS:
        entities.append(build_vehicle_sensor(node_id, direction, snapshot))
    return entities
