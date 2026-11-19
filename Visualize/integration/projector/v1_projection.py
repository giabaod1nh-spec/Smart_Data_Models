"""Deterministic V1 observation -> Orion-LD current-state projection.

Only the archived catalog and the V1 wire event influence the mapping.  The
owned-attribute sets also define the exact cleanup boundary for replay and
run reconciliation; attributes added by another Orion writer are untouched.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from contracts.traffic_observation import validate_observation
from contracts.topology.v1_digest import topology_digest

CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
TRAFFIC_DIRECTION = {
    "North": "NORTHBOUND", "South": "SOUTHBOUND",
    "East": "EASTBOUND", "West": "WESTBOUND",
}
ENTITY_TYPES = ("Intersection", "Camera", "TrafficLight", "VehicleSensor")
STATIC = {
    "Intersection": frozenset({"name", "location", "intersectionStatus", "numberOfApproaches", "refTrafficLights", "refVehicleSensors", "refCameras"}),
    "Camera": frozenset({"name", "location", "cameraNum", "cameraType", "cameraUsage", "cameraStatus", "trafficDirection", "refIntersection"}),
    "TrafficLight": frozenset({"name", "location", "workingState", "trafficDirection", "refIntersection", "refCamera"}),
    "VehicleSensor": frozenset({"name", "location", "sensorType", "sensorStatus", "trafficDirection", "refIntersection", "refCamera", "refTrafficLight"}),
}
DYNAMIC = {
    "Intersection": frozenset({"simulationRunId", "simulationTime", "scenarioId", "dateObserved", "totalVehicleCount", "overallTrafficStatus", "frequentCongestion", "hasActiveIncident", "currentPhase", "derivedTrafficState", "hasSpillback", "isBoxBlocked", "anomalyScore", "anomalyLabel", "probableCauseType", "affectedBy", "causeDetectedAt", "metricAvailability"}),
    "Camera": frozenset({"simulationRunId", "simulationTime", "scenarioId", "dateObserved", "vehicleCount", "averageSpeed", "occupancyRate", "trafficStatus", "incidentDetected", "incidentType", "incidentSeverity", "confidence", "recommendedSignalAction", "metricAvailability"}),
    "TrafficLight": frozenset({"simulationRunId", "simulationTime", "scenarioId", "currentStatus", "currentPhase", "timingMode", "greenDurationCurrent", "redDurationCurrent", "yellowDuration", "metricAvailability"}),
    "VehicleSensor": frozenset({"simulationRunId", "simulationTime", "scenarioId", "dateObserved", "vehicleCount", "pcuEquivalent", "leftTurnCount", "straightCount", "rightTurnCount", "averageSpeed", "waitingVehicleCount", "queueLength", "occupancyRate", "queueStraight", "queueLeft", "queueRight", "trafficStatus", "arrivalRatePcuPerSec", "waitingReasonCounts", "dominantWaitingReason", "theoreticalSpeed", "vehicleClassComposition", "derivedTrafficState", "spillbackRisk", "operationalState", "metricAvailability"}),
}


def prop(value: Any) -> dict:
    return {"type": "Property", "value": value}


def rel(value: str | list[str]) -> dict:
    return {"type": "Relationship", "object": value}


def entity_id(kind: str, node: str, direction: str | None = None) -> str:
    if kind == "Intersection" or kind == "Camera":
        return f"urn:ngsi-ld:{kind}:{node}"
    if direction not in TRAFFIC_DIRECTION:
        raise ValueError(f"unsupported approach direction {direction!r}")
    if kind == "TrafficLight":
        return f"urn:ngsi-ld:TrafficLight:{node}-{direction}"
    if kind == "VehicleSensor":
        return f"urn:ngsi-ld:VehicleSensor:{node}:{TRAFFIC_DIRECTION[direction]}"
    raise ValueError(f"unsupported entity type {kind}")


def directions(catalog: Mapping[str, Any], node: str) -> tuple[str, ...]:
    tls = catalog["node_to_tls"].get(node)
    if tls is None:
        raise ValueError(f"node {node} absent from archived topology")
    group = catalog.get("node_to_tls_groups", {}).get(node, [tls])
    if not isinstance(group, list) or tls not in group:
        raise ValueError(f"node {node} primary TLS is absent from its archived group")
    result = tuple(catalog["approach_edges"].get(tls, {}))
    if not result or any(d not in TRAFFIC_DIRECTION for d in result):
        raise ValueError(f"node {node} has unsupported approaches {result}")
    return result


def expected_ids(catalog: Mapping[str, Any], node: str) -> dict[str, str]:
    result = {entity_id("Intersection", node): "Intersection", entity_id("Camera", node): "Camera"}
    for direction in directions(catalog, node):
        for kind in ("TrafficLight", "VehicleSensor"):
            result[entity_id(kind, node, direction)] = kind
    return result


def static_entities(catalog: Mapping[str, Any], node: str) -> list[dict]:
    loc = catalog["node_locations"][node]
    point = {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [float(loc["lng"]), float(loc["lat"])]}}
    name = f"OSM Intersection {node}"
    ds = directions(catalog, node)
    iid, cid = entity_id("Intersection", node), entity_id("Camera", node)
    base = {"name": prop(name), "location": point}
    entities = [
        {"id": iid, "type": "Intersection", **base, "intersectionStatus": prop("ACTIVE"), "numberOfApproaches": prop(len(ds)), "refCameras": rel([cid]), "refTrafficLights": rel([entity_id("TrafficLight", node, d) for d in ds]), "refVehicleSensors": rel([entity_id("VehicleSensor", node, d) for d in ds])},
        {"id": cid, "type": "Camera", **base, "cameraNum": prop(1), "cameraType": prop("FIXED"), "cameraUsage": prop("TRAFFIC"), "cameraStatus": prop("ok"), "trafficDirection": prop("ALL_DIRECTIONS"), "refIntersection": rel(iid)},
    ]
    for d in ds:
        tid, sid = entity_id("TrafficLight", node, d), entity_id("VehicleSensor", node, d)
        entities.append({"id": tid, "type": "TrafficLight", "name": prop(f"{name} {TRAFFIC_DIRECTION[d]}"), "location": point, "workingState": prop("OK"), "trafficDirection": prop(TRAFFIC_DIRECTION[d]), "refIntersection": rel(iid), "refCamera": rel(cid)})
        entities.append({"id": sid, "type": "VehicleSensor", "name": prop(f"{name} {TRAFFIC_DIRECTION[d]} approach"), "location": point, "sensorType": prop("VIRTUAL"), "sensorStatus": prop("OK"), "trafficDirection": prop(TRAFFIC_DIRECTION[d]), "refIntersection": rel(iid), "refCamera": rel(cid), "refTrafficLight": rel(tid)})
    return entities


@dataclass(frozen=True)
class Projection:
    entities: tuple[dict, ...]
    removals: tuple[tuple[str, str], ...]


def _set_metric(entity: dict, attr: str, value: Any, source: str, unavailable: dict) -> None:
    if value is None:
        unavailable[attr] = source
    else:
        entity[attr] = prop(value)


def _aggregate(values: list[Any]) -> Any:
    return sum(values) if all(v is not None for v in values) else None


def project(event: Mapping[str, Any], catalog: Mapping[str, Any]) -> Projection:
    validate_observation(event)
    if topology_digest(catalog) != event["topologyDigest"]:
        raise ValueError("observation topology digest differs from archive")
    node = event["intersectionId"]
    ds = directions(catalog, node)
    payload = event["payload"]
    if set(ds) != set(payload["directions"]):
        raise ValueError("observation directions differ from archived topology")
    entities = static_entities(catalog, node)
    by_id = {entity["id"]: entity for entity in entities}
    iid, cid = entity_id("Intersection", node), entity_id("Camera", node)
    intersection, camera = by_id[iid], by_id[cid]
    availability: dict[str, dict] = {eid: {} for eid in by_id}
    for entity in entities:
        entity.update({"simulationRunId": prop(event["simulationRunId"]), "simulationTime": prop(event["simulationTimeSeconds"]), "scenarioId": prop(event["scenarioId"]), "@context": CONTEXT})
        if entity["type"] in ("Intersection", "Camera", "VehicleSensor"):
            entity["dateObserved"] = prop({"@type": "DateTime", "@value": event["capturedAt"]})
    intersection["currentPhase"] = prop(payload["phase"])
    intersection["hasActiveIncident"] = prop(bool(payload["incidents"]))
    camera["incidentDetected"] = prop(bool(payload["incidents"]))
    camera["incidentType"] = prop("ACCIDENT" if payload["incidents"] else "NONE")
    camera["incidentSeverity"] = prop("MEDIUM" if payload["incidents"] else "NONE")
    if "derived_traffic_state" in payload:
        intersection["derivedTrafficState"] = prop(payload["derived_traffic_state"])
    if "spillback_detected" in payload:
        intersection["hasSpillback"] = prop(payload["spillback_detected"])
    if "intersection_box_blocked" in payload:
        intersection["isBoxBlocked"] = prop(payload["intersection_box_blocked"])
    if "anomaly_score" in payload:
        intersection["anomalyScore"] = prop(payload["anomaly_score"])
    if "anomaly_label" in payload:
        intersection["anomalyLabel"] = prop(payload["anomaly_label"])
    causes = payload.get("probable_causes") or []
    if causes:
        cause = causes[0]
        if "type" in cause:
            intersection["probableCauseType"] = prop(cause["type"])
        if cause.get("source_node") in catalog["node_to_tls"]:
            intersection["affectedBy"] = rel(entity_id("Intersection", cause["source_node"]))
        if "observed_to_s" in cause:
            intersection["causeDetectedAt"] = prop(cause["observed_to_s"])
    observations = payload["directions"]
    counts = [observations[d]["vehicle_count"] for d in ds]
    pcus = [observations[d]["pcu_equivalent"] for d in ds]
    speeds = [observations[d]["average_speed_kmh"] for d in ds]
    occupancies = [observations[d]["occupancy_pct"] for d in ds]
    for target, attr, values in ((intersection, "totalVehicleCount", counts), (camera, "vehicleCount", counts)):
        _set_metric(target, attr, _aggregate(values), "UNAVAILABLE", availability[target["id"]])
    for attr, values in (("averageSpeed", speeds), ("occupancyRate", occupancies)):
        mean = (round(sum(values) / len(values), 2) if all(v is not None for v in values) else None)
        _set_metric(camera, attr, mean, "UNAVAILABLE", availability[cid])
    # The V1 catalog does not archive the density thresholds.  Use only
    # producer-declared classifications; inventing PCU cutoffs here would make
    # replay depend on an undocumented Projector policy.
    density_values = [observations[d].get("density") for d in ds]
    if all(value in ("LOW", "MEDIUM", "HIGH") for value in density_values):
        status = {"LOW": "LIGHT", "MEDIUM": "MODERATE", "HIGH": "HEAVY"}[max(density_values, key={"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get)]
        intersection["overallTrafficStatus"] = prop(status)
        intersection["frequentCongestion"] = prop(status == "HEAVY")
        camera["trafficStatus"] = prop(status)
    sensor_fields = {"vehicle_count": "vehicleCount", "pcu_equivalent": "pcuEquivalent", "left_count": "leftTurnCount", "straight_count": "straightCount", "right_count": "rightTurnCount", "average_speed_kmh": "averageSpeed", "waiting_vehicle_count": "waitingVehicleCount", "queue_length_m": "queueLength", "occupancy_pct": "occupancyRate", "arrival_rate_pcu_per_sec": "arrivalRatePcuPerSec", "waiting_reason_counts": "waitingReasonCounts", "dominant_waiting_reason": "dominantWaitingReason", "theoretical_speed_kmh": "theoreticalSpeed", "vehicle_class_composition": "vehicleClassComposition"}
    for d in ds:
        data = observations[d]
        tid, sid = entity_id("TrafficLight", node, d), entity_id("VehicleSensor", node, d)
        light, sensor = by_id[tid], by_id[sid]
        light["currentStatus"] = prop(payload["colors"][d].upper())
        light["currentPhase"] = prop(payload["phase"])
        light["timingMode"] = prop(payload.get("timing_mode", payload["control_mode"]))
        for src, dst in (("green_duration", "greenDurationCurrent"), ("red_duration", "redDurationCurrent"), ("yellow_duration", "yellowDuration")):
            if src in payload:
                light[dst] = prop(payload[src])
        for src, dst in sensor_fields.items():
            if src not in data:
                continue
            if data[src] is None and src not in data.get("metric_availability", {}):
                continue  # optional null means no value, not a failed sensor
            _set_metric(sensor, dst, data[src], data.get("metric_availability", {}).get(src, "UNAVAILABLE"), availability[sid])
        if "queue_by_movement" in data:
            for src, dst in (("straight", "queueStraight"), ("left", "queueLeft"), ("right", "queueRight")):
                sensor[dst] = prop(data["queue_by_movement"][src])
        if "density" in data:
            sensor["trafficStatus"] = prop({"LOW": "LIGHT", "MEDIUM": "MODERATE", "HIGH": "HEAVY"}[data["density"]])
        if "direction_contexts" in payload and d in payload["direction_contexts"]:
            sensor["derivedTrafficState"] = prop(payload["direction_contexts"][d])
        if "derived_phenomena" in payload and "spillback_risk" in payload["derived_phenomena"]:
            sensor["spillbackRisk"] = prop(payload["derived_phenomena"]["spillback_risk"])
        if "operational_state" in payload:
            sensor["operationalState"] = prop(payload["operational_state"])
    removals = []
    for entity in entities:
        eid, kind = entity["id"], entity["type"]
        if availability[eid]:
            entity["metricAvailability"] = prop(availability[eid])
        removals.extend((eid, name) for name in sorted(DYNAMIC[kind] - entity.keys()))
    return Projection(tuple(entities), tuple(removals))
