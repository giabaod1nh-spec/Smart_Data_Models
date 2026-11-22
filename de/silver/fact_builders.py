"""Silver Plan 2 — fact builders with default ownership (pure)."""
from __future__ import annotations

from typing import Any, Optional, Union

from contracts.traffic_observation import canonical_json, validate_observation
from de.silver.archive_resolver import VerifiedRunContext

from de.silver.contracts import parse_urn_id
from de.silver.input_models import BronzeEntityInputRecord, BronzeRunInputRecord
from de.silver.models import (
    SilverCameraObservationFact,
    SilverIntersectionStateFact,
    SilverObservationFact,
    SilverRunEventFact,
    SilverSignalStateFact,
)
from de.silver.unwrapper import UnwrapResult

QUALITY_FLAG_PRIORITY = [
    "MISSING_WAITING_COUNT",
    "MISSING_ARRIVAL_RATE",
    "MISSING_TRAFFIC_STATUS",
    "MISSING_TIMING_MODE",
    "MISSING_TOTAL_VEHICLE_COUNT",
]

FactRecord = Union[
    SilverObservationFact,
    SilverCameraObservationFact,
    SilverSignalStateFact,
    SilverIntersectionStateFact,
    SilverRunEventFact,
]


def build_v1_fact_rows(event: dict[str, Any], context: VerifiedRunContext,
                       provenance: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Build exact V1 event bundles from the archived direction population.

    The caller supplies Bronze-verified primary delivery provenance. Processing
    timestamps are assigned by the repository when the frozen rows are written.
    """
    validate_observation(event)
    context.validate_event(event)
    payload = event["payload"]
    required_provenance = (
        "bronze_semantic_revision", "bronze_canonical_hash", "source_raw_ingestion_id",
        "source_topic", "source_partition", "source_offset",
        "source_physical_record_hash", "source_byte_hash",
    )
    missing = [key for key in required_provenance if key not in provenance]
    if missing:
        raise ValueError(f"primary provenance missing {missing}")
    common = dict(
        simulation_run_id=context.run_id, topology_digest=context.topology_digest,
        run_generation=context.generation, manifest_identity=context.manifest_identity,
        event_id=event["eventId"], cycle_sequence=event["cycleSequence"],
        simulation_time_seconds=event["simulationTimeSeconds"],
        intersection_id=event["intersectionId"], scenario_id=event["scenarioId"],
        captured_at=event["capturedAt"], source_payload_hash=event["payloadHash"],
        silver_schema_version="v1", **{key: provenance[key] for key in required_provenance},
    )
    traffic, signals = [], []
    measured_counts = []
    metrics = ("vehicle_count", "pcu_equivalent", "left_count", "straight_count",
               "right_count", "average_speed_kmh", "waiting_vehicle_count",
               "queue_length_m", "occupancy_pct")
    for direction in context.directions[event["intersectionId"]]:
        value = payload["directions"][direction]
        availability = value.get("metric_availability", {})
        flags = [f"METRIC_{status}:{name}" for name, status in sorted(availability.items())]
        traffic.append(dict(common, source_direction=direction,
            **{key: value[key] for key in metrics},
            metric_availability_json=canonical_json(availability),
            direction_payload_json=canonical_json(value),
            quality_status="VALID_WITH_UNAVAILABLE" if flags else "VALID",
            quality_flags_json=canonical_json(flags)))
        measured_counts.append(value["vehicle_count"] if "vehicle_count" not in availability else None)
        signals.append(dict(common, source_direction=direction,
            signal_color=payload["colors"][direction], phase=payload["phase"],
            next_phase=payload.get("next_phase"),
            phase_remaining_seconds=payload.get("phase_remaining"),
            phase_duration_seconds=payload.get("phase_duration"),
            green_duration_seconds=payload.get("green_duration"),
            yellow_duration_seconds=payload.get("yellow_duration"),
            red_duration_seconds=payload.get("red_duration"),
            timing_mode=payload.get("timing_mode"),
            signal_payload_json=canonical_json({key: payload[key] for key in (
                "colors", "phase", "next_phase", "phase_remaining", "phase_duration",
                "green_duration", "yellow_duration", "red_duration", "timing_mode") if key in payload}),
            quality_status="VALID", quality_flags_json="[]"))
    complete = all(count is not None for count in measured_counts)
    intersection = dict(common, phase=payload["phase"],
        control_mode=payload["control_mode"], incidents_json=canonical_json(payload["incidents"]),
        derived_traffic_state=payload.get("derived_traffic_state"),
        operational_state_json=canonical_json(payload["operational_state"])
            if "operational_state" in payload else None,
        spillback_detected=int(payload["spillback_detected"])
            if "spillback_detected" in payload else None,
        intersection_box_blocked=int(payload["intersection_box_blocked"])
            if "intersection_box_blocked" in payload else None,
        downstream_edge_full=int(payload["downstream_edge_full"])
            if "downstream_edge_full" in payload else None,
        spillback_pressure=payload.get("spillback_pressure"),
        intersection_payload_json=canonical_json(payload),
        total_vehicle_count=sum(measured_counts) if complete else None,
        total_vehicle_count_quality="COMPLETE_TOPOLOGY_SUM" if complete else "PARTIAL_OR_UNAVAILABLE")
    return dict(traffic=traffic, signal=signals, intersection=[intersection])


def format_quality_flags(flags: set[str]) -> str:
    sorted_flags = [f for f in QUALITY_FLAG_PRIORITY if f in flags]
    sorted_flags.extend(sorted(flags - set(QUALITY_FLAG_PRIORITY)))
    return "|".join(sorted_flags)


def _meta(normalized: dict[str, Any], key: str) -> UnwrapResult:
    meta = normalized.get("_meta", {})
    return meta.get(key, UnwrapResult(None, True, present=False))


def _optional_missing(normalized: dict[str, Any], key: str) -> bool:
    u = _meta(normalized, key)
    return (not u.present) or (u.is_success and u.value is None)


def _lineage_entity(record: BronzeEntityInputRecord) -> dict[str, Any]:
    return {
        "source_bronze_event_id": record.event_id,
        "source_raw_ingestion_id": record.raw_ingestion_id,
        "source_topic": record.topic,
        "source_partition": record.partition,
        "source_offset": record.offset,
        "source_payload_hash": record.entity_payload_hash,
    }


def build_traffic_observation(
    record: BronzeEntityInputRecord,
    normalized: dict[str, Any],
) -> SilverObservationFact:
    flags: set[str] = set()

    if _optional_missing(normalized, "waitingVehicleCount"):
        waiting = 0
        flags.add("MISSING_WAITING_COUNT")
    else:
        waiting = int(normalized["waitingVehicleCount"])

    if _optional_missing(normalized, "arrivalRatePcuPerSec"):
        arrival = 0.0
        flags.add("MISSING_ARRIVAL_RATE")
    else:
        arrival = float(normalized["arrivalRatePcuPerSec"])

    if _optional_missing(normalized, "trafficStatus") or normalized.get("trafficStatus") is None:
        traffic_status = "UNKNOWN"
        flags.add("MISSING_TRAFFIC_STATUS")
    else:
        traffic_status = str(normalized["trafficStatus"])

    spillback = normalized.get("spillbackRisk")
    if spillback is None:
        spillback = 0

    reason = normalized.get("dominantWaitingReason")
    if reason is None:
        reason = ""

    quality_flags = format_quality_flags(flags)
    quality_status = "VALID_WITH_DEFAULT" if flags else "VALID"

    return SilverObservationFact(
        simulation_run_id=record.simulation_run_id,
        cycle_sequence=int(record.cycle_sequence),
        simulation_time_sec=float(record.simulation_time),
        intersection_id=parse_urn_id(str(normalized["refIntersection"])),
        direction=str(normalized["trafficDirection"]),
        source_entity_id=record.entity_id,
        vehicle_count=int(normalized["vehicleCount"]),
        pcu_equivalent=float(normalized["pcuEquivalent"]),
        average_speed_kmh=float(normalized["averageSpeed"]),
        queue_length_m=float(normalized["queueLength"]),
        waiting_vehicle_count=waiting,
        occupancy_pct=float(normalized["occupancyRate"]),
        arrival_rate_pcu_per_sec=arrival,
        traffic_status=traffic_status,
        spillback_risk=int(spillback),
        dominant_waiting_reason=str(reason),
        scenario_id=record.scenario_id,
        quality_status=quality_status,
        quality_flags=quality_flags,
        processed_at=None,
        **_lineage_entity(record),
    )


def build_signal_state(
    record: BronzeEntityInputRecord,
    normalized: dict[str, Any],
) -> SilverSignalStateFact:
    flags: set[str] = set()
    if _optional_missing(normalized, "timingMode") or normalized.get("timingMode") is None:
        timing_mode = "FIXED_TIME"
        flags.add("MISSING_TIMING_MODE")
    else:
        timing_mode = str(normalized["timingMode"])

    def opt_duration(key: str) -> Optional[float]:
        if _optional_missing(normalized, key):
            return None
        return float(normalized[key])

    return SilverSignalStateFact(
        simulation_run_id=record.simulation_run_id,
        cycle_sequence=int(record.cycle_sequence),
        simulation_time_sec=float(record.simulation_time),
        intersection_id=parse_urn_id(str(normalized["refIntersection"])),
        direction=str(normalized["trafficDirection"]),
        source_entity_id=record.entity_id,
        signal_status=str(normalized["currentStatus"]),
        current_phase=str(normalized["currentPhase"]),
        green_duration_sec=opt_duration("greenDurationCurrent"),
        red_duration_sec=opt_duration("redDurationCurrent"),
        yellow_duration_sec=opt_duration("yellowDuration"),
        timing_mode=timing_mode,
        scenario_id=record.scenario_id,
        quality_flags=format_quality_flags(flags),
        processed_at=None,
        **_lineage_entity(record),
    )


def build_intersection_state(
    record: BronzeEntityInputRecord,
    normalized: dict[str, Any],
) -> SilverIntersectionStateFact:
    flags: set[str] = set()

    derived = normalized.get("derivedTrafficState")
    if derived is None:
        derived = "STABLE"

    def bool_default(key: str) -> int:
        val = normalized.get(key)
        return int(val) if val is not None else 0

    if _optional_missing(normalized, "totalVehicleCount"):
        total: Optional[int] = None
        flags.add("MISSING_TOTAL_VEHICLE_COUNT")
    else:
        total = int(normalized["totalVehicleCount"])

    return SilverIntersectionStateFact(
        simulation_run_id=record.simulation_run_id,
        cycle_sequence=int(record.cycle_sequence),
        simulation_time_sec=float(record.simulation_time),
        intersection_id=parse_urn_id(str(normalized["id"])),
        source_entity_id=record.entity_id,
        overall_traffic_status=str(normalized["overallTrafficStatus"]),
        derived_traffic_state=str(derived),
        current_phase=str(normalized["currentPhase"]),
        has_active_incident=bool_default("hasActiveIncident"),
        has_spillback=bool_default("hasSpillback"),
        is_box_blocked=bool_default("isBoxBlocked"),
        total_vehicle_count=total,
        scenario_id=record.scenario_id,
        quality_flags=format_quality_flags(flags),
        processed_at=None,
        **_lineage_entity(record),
    )


def build_camera_observation(
    record: BronzeEntityInputRecord,
    normalized: dict[str, Any],
) -> SilverCameraObservationFact:
    def opt_uint(key: str) -> Optional[int]:
        if _optional_missing(normalized, key):
            return None
        return int(normalized[key])

    def opt_float(key: str) -> Optional[float]:
        if _optional_missing(normalized, key):
            return None
        return float(normalized[key])

    traffic_status = normalized.get("trafficStatus") or "UNKNOWN"
    incident = normalized.get("incidentDetected")
    if incident is None:
        incident = 0
    confidence = normalized.get("confidence")
    if confidence is None:
        confidence = 1.0
    action = normalized.get("recommendedSignalAction") or "KEEP"
    incident_type = normalized.get("incidentType") or "NONE"
    incident_severity = normalized.get("incidentSeverity") or "NONE"

    source_id = normalized.get("id") or record.entity_id

    return SilverCameraObservationFact(
        simulation_run_id=record.simulation_run_id,
        cycle_sequence=int(record.cycle_sequence),
        simulation_time_sec=float(record.simulation_time),
        intersection_id=parse_urn_id(str(normalized["refIntersection"])),
        source_entity_id=str(source_id),
        vehicle_count=opt_uint("vehicleCount"),
        average_speed_kmh=opt_float("averageSpeed"),
        occupancy_pct=opt_float("occupancyRate"),
        traffic_status=str(traffic_status),
        incident_detected=int(incident),
        confidence=float(confidence),
        recommended_signal_action=str(action),
        incident_type=str(incident_type),
        incident_severity=str(incident_severity),
        scenario_id=record.scenario_id,
        quality_flags="",
        processed_at=None,
        **_lineage_entity(record),
    )


def build_run_event(record: BronzeRunInputRecord) -> SilverRunEventFact:
    return SilverRunEventFact(
        simulation_run_id=record.simulation_run_id,
        event_name=record.event_type,
        event_simulation_time=0.0,
        scenario_id=record.scenario_id,
        producer_id=record.producer_id,
        source_bronze_event_id=record.bronze_canonical_hash,
        source_raw_ingestion_id=record.raw_ingestion_id,
        source_topic=record.topic,
        source_partition=record.partition,
        source_offset=record.offset,
        source_payload_hash=record.bronze_canonical_hash,
        processed_at=None,
    )


def build_fact(
    entity_or_event_type: str,
    record: Any,
    normalized: dict[str, Any],
) -> FactRecord:
    from de.silver.contracts import (
        ENTITY_CAMERA,
        ENTITY_INTERSECTION,
        ENTITY_TRAFFIC_LIGHT,
        ENTITY_VEHICLE_SENSOR,
        EVENT_RUN_STARTED,
    )

    if entity_or_event_type == ENTITY_VEHICLE_SENSOR:
        return build_traffic_observation(record, normalized)
    if entity_or_event_type == ENTITY_TRAFFIC_LIGHT:
        return build_signal_state(record, normalized)
    if entity_or_event_type == ENTITY_INTERSECTION:
        return build_intersection_state(record, normalized)
    if entity_or_event_type == ENTITY_CAMERA:
        return build_camera_observation(record, normalized)
    if entity_or_event_type == EVENT_RUN_STARTED:
        return build_run_event(record)
    raise ValueError(f"No fact builder for {entity_or_event_type}")
