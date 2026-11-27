"""Versioned isolated Analytics evidence linked to an unchanged observation V1."""

import json
from hashlib import sha256
from math import isfinite
from re import fullmatch

from contracts.traffic_observation import canonical_json, validate_observation


MAX_MEASUREMENT_BYTES = 32 * 1024
_MAX_SAMPLES = 512
_MOVEMENTS = ("left", "straight", "right")
_OCCUPANCY_METHODS = {"e2_percent_v2", "lane_percent_v2"}
_QUEUE_METHODS = {"e2_jam_movement_v1", "route_stopped_length_v1"}
_SPILLBACK_METHODS = {"e2_ratio_v2", "lane_ratio_v2"}


def digest(value):
    return isinstance(value, str) and fullmatch(r"[0-9a-f]{64}", value) is not None


def _number(value, *, through, minimum=0):
    if type(value) not in (float, int) or not minimum <= value <= through or not isfinite(value):
        raise ValueError("invalid raw measurement number")
    return value


def _samples(value, *, allow_empty=False):
    if not isinstance(value, list) or not int(not allow_empty) <= len(value) <= _MAX_SAMPLES:
        raise ValueError("invalid number of raw measurement samples")
    return value


def _keys(record, *keys):
    if not isinstance(record, dict) or set(record) != set(keys):
        raise ValueError("invalid measurement evidence fields")


def _occupancy(record):
    _keys(record, "method", "percent")
    if record["method"] not in _OCCUPANCY_METHODS:
        raise ValueError("unknown occupancy evidence method")
    values = [_number(value, through=100) for value in _samples(record["percent"])]
    return round(sum(values) / len(values), 1)


def _queue(record):
    method = record.get("method")
    if method not in _QUEUE_METHODS or not digest(record.get("scopeDigest")):
        raise ValueError("invalid queue evidence identity")
    route = method == "route_stopped_length_v1"
    _keys(record, "method", "scopeDigest", "samples", *(["haltingThreshold"] if route else []))
    totals = dict.fromkeys(_MOVEMENTS, 0.0)
    threshold = _number(record["haltingThreshold"], minimum=0.001, through=30) if route else None
    for sample in _samples(record["samples"], allow_empty=route):
        if (not isinstance(sample, list) or len(sample) != (3 if route else 2)
                or sample[0] not in _MOVEMENTS):
            raise ValueError("invalid queue component")
        length = _number(sample[1], through=100_000, minimum=0.001 if route else 0)
        movement = sample[0]
        if route:
            speed = _number(sample[2], through=30)
            if speed >= threshold:
                raise ValueError("route queue component is not a stopped vehicle")
            totals[movement] += length + 1.5
        else:
            totals[movement] = round(totals[movement] + length, 1)
    return round(max(totals.values()), 1)


def _spillback(record):
    _keys(record, "method", "percentGroups", "threshold")
    method = record["method"]
    if method not in _SPILLBACK_METHODS:
        raise ValueError("unknown spillback evidence method")
    groups = _samples(record["percentGroups"])
    if sum(len(_samples(group)) for group in groups) > _MAX_SAMPLES:
        raise ValueError("spillback evidence exceeds total sample limit")
    ratios = [[_number(value, through=100) / 100 for value in group] for group in groups]
    threshold = _number(record["threshold"], through=1)
    if method == "e2_ratio_v2":
        values = [value for group in ratios for value in group]
        average = sum(values) / len(values)
        full = any(value >= threshold for value in values)
    else:
        values = [sum(group) / len(group) for group in ratios]
        average = max(values)
        full = average >= threshold
    pressure = round(average, 3)
    return {"pressure": pressure, "downstreamFull": full, "detected": full or pressure >= threshold}


def _measurements(evidence, anchor, directions):
    _keys(evidence, "directions", "spillback")
    direction_evidence = evidence["directions"]
    if not isinstance(direction_evidence, dict) or set(direction_evidence) - set(directions):
        raise ValueError("measurement direction is outside the anchored population")
    values = {}

    def measurement(metric, direction, item, value):
        scope = item.get("scopeDigest")
        identity = ["observation_measurement_v1", anchor["runId"], anchor["generation"],
                    anchor["manifestIdentity"], anchor["topologyDigest"], anchor["eventId"],
                    anchor["payloadHash"], metric, direction, item["method"], scope]
        return {"method": item["method"], "scopeDigest": scope,
                "measurementId": sha256(canonical_json(identity).encode()).hexdigest(), "value": value}

    for direction, metrics in direction_evidence.items():
        if not isinstance(metrics, dict) or not metrics or set(metrics) - {"occupancy", "queue"}:
            raise ValueError("invalid approach evidence population")
        values[direction] = {kind: measurement(kind, direction, item,
                             (_occupancy(item) if kind == "occupancy" else _queue(item)))
                             for kind, item in metrics.items()}
    spill = evidence["spillback"]
    return {"directions": values,
            "spillback": measurement("spillback", None, spill, _spillback(spill)) if spill is not None else None}


def original_anchor(event, generation, manifest_identity):
    validate_observation(event)
    return {"runId": event["simulationRunId"], "generation": generation,
            "manifestIdentity": manifest_identity, "topologyDigest": event["topologyDigest"],
            "eventId": event["eventId"], "payloadHash": event["payloadHash"],
            "intersectionId": event["intersectionId"], "cycle": event["cycleSequence"],
            "simulationStep": event["simulationStep"], "simulationTimeSeconds": event["simulationTimeSeconds"]}


def evidence_from_isolated_capture(event, capture, *, completed_step):
    """Extract raw evidence from a separate capture at the original frozen coordinate."""
    if (capture.get("simulation_run_id") != event["simulationRunId"]
            or capture.get("node_id") != event["intersectionId"]
            or completed_step != event["simulationStep"]
            or capture.get("analytics_capture_seconds") != event["simulationTimeSeconds"]
            or set(capture["directions"]) != set(event["payload"]["directions"])):
        raise ValueError("Analytics evidence was not captured at the original event coordinate")
    return {"directions": {direction: data["analytics_measurement_evidence"]
                           for direction, data in capture["directions"].items()
                           if "analytics_measurement_evidence" in data},
            "spillback": capture.get("analytics_spillback_evidence")}


def encode_measurements(anchor, directions, evidence, *, revision=1, previous=None):
    _keys(anchor, "runId", "generation", "manifestIdentity", "topologyDigest", "eventId", "payloadHash",
          "intersectionId", "cycle", "simulationStep", "simulationTimeSeconds")
    if (type(anchor["generation"]) is not int or anchor["generation"] < 1
            or any(not digest(anchor[k]) for k in ("eventId", "payloadHash", "manifestIdentity"))):
        raise ValueError("invalid original observation anchor")
    if (type(revision) is not int or not 1 <= revision <= 2**53 - 1
            or not (previous is None if revision == 1 else digest(previous))):
        raise ValueError("invalid measurement revision lineage")
    values = _measurements(evidence, anchor, directions) if evidence is not None else None
    if values is None and revision == 1:
        raise ValueError("cannot withdraw a missing measurement")
    if values is not None and not values["directions"] and values["spillback"] is None:
        raise ValueError("no direct source measurement evidence")
    content = canonical_json({"schemaVersion": 1, "anchor": anchor, "revision": revision,
                              "previousDigest": previous, "evidence": evidence, "measurements": values}).encode()
    if len(content) > MAX_MEASUREMENT_BYTES:
        raise ValueError("measurement archive byte limit exceeded")
    return content


def verify_measurements(content, expected_digest, anchor, directions):
    if (not digest(expected_digest) or not 0 < len(content) <= MAX_MEASUREMENT_BYTES
            or sha256(content).hexdigest() != expected_digest):
        raise ValueError("measurement object digest or size mismatch")
    record = json.loads(content)
    if (record["anchor"] != anchor or content != encode_measurements(
            anchor, directions, record["evidence"], revision=record["revision"],
            previous=record["previousDigest"])):
        raise ValueError("measurement object differs from original event or raw evidence")
    return record
