"""Silver contract interfaces — Plan 1 (routing, enums, NGSI unwrap helpers).

No batch processing, readers, or ClickHouse repositories.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

ENTITY_VEHICLE_SENSOR = "VehicleSensor"
ENTITY_TRAFFIC_LIGHT = "TrafficLight"
ENTITY_INTERSECTION = "Intersection"
ENTITY_CAMERA = "Camera"
EVENT_RUN_STARTED = "TrafficSimulationRunStarted"

VALID_ENTITY_TYPES = {
    ENTITY_VEHICLE_SENSOR,
    ENTITY_TRAFFIC_LIGHT,
    ENTITY_INTERSECTION,
    ENTITY_CAMERA,
}

DIRECTION_MAP = {
    "NORTHBOUND": "N",
    "NORTH": "N",
    "N": "N",
    "SOUTHBOUND": "S",
    "SOUTH": "S",
    "S": "S",
    "EASTBOUND": "E",
    "EAST": "E",
    "E": "E",
    "WESTBOUND": "W",
    "WEST": "W",
    "W": "W",
}

SIGNAL_STATUS_VALUES = {"GREEN", "RED", "YELLOW"}
TRAFFIC_STATUS_VALUES = {"LIGHT", "MODERATE", "HEAVY", "CONGESTED", "UNKNOWN"}
TIMING_MODE_VALUES = {"FIXED_TIME", "ACTUATED", "ADAPTIVE", "MANUAL"}

DISPOSITION_PROCESSED = "PROCESSED"
DISPOSITION_QUARANTINED = "QUARANTINED"
DISPOSITION_IDEMPOTENT_SKIPPED = "IDEMPOTENT_SKIPPED"
DISPOSITION_DOCUMENTED_SKIP = "DOCUMENTED_SKIP"

# entity_type / event_type → (primary_fact_table, dimension_tables, default_disposition)
ROUTING_MATRIX: Dict[str, Tuple[Optional[str], Tuple[str, ...], str]] = {
    ENTITY_VEHICLE_SENSOR: (
        "silver_fact_traffic_observation",
        ("silver_dim_approach",),
        DISPOSITION_PROCESSED,
    ),
    ENTITY_TRAFFIC_LIGHT: (
        "silver_fact_signal_state",
        (),
        DISPOSITION_PROCESSED,
    ),
    ENTITY_INTERSECTION: (
        "silver_fact_intersection_state",
        ("silver_dim_intersection",),
        DISPOSITION_PROCESSED,
    ),
    ENTITY_CAMERA: (
        "silver_fact_camera_observation",
        (),
        DISPOSITION_PROCESSED,
    ),
    EVENT_RUN_STARTED: (
        "silver_fact_run_event",
        ("silver_dim_run", "silver_dim_scenario"),
        DISPOSITION_PROCESSED,
    ),
}

MAIN_FACT_TABLES = (
    "silver_fact_traffic_observation",
    "silver_fact_signal_state",
    "silver_fact_intersection_state",
    "silver_fact_camera_observation",
    "silver_fact_run_event",
)

MAIN_DIM_TABLES = (
    "silver_dim_run",
    "silver_dim_intersection",
    "silver_dim_approach",
    "silver_dim_scenario",
)

CONTROL_TABLES = (
    "silver_quarantine",
    "silver_processing_ledger",
)

REPLAY_TABLES = (
    "silver_fact_traffic_observation_replay",
    "silver_fact_signal_state_replay",
    "silver_fact_intersection_state_replay",
    "silver_fact_camera_observation_replay",
    "silver_fact_run_event_replay",
    "silver_quarantine_replay",
    "silver_dim_run_replay",
    "silver_dim_intersection_replay",
)

ALL_SILVER_TABLES = MAIN_FACT_TABLES + MAIN_DIM_TABLES + CONTROL_TABLES + REPLAY_TABLES

LINEAGE_COLUMNS = (
    "source_bronze_event_id",
    "source_raw_ingestion_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_payload_hash",
)

# Explicit column inventories for schema parity (Plan 1 §20 / §12).
# Values are DDL column names that the contract maps into each table.
TABLE_COLUMNS: Dict[str, Set[str]] = {
    "silver_fact_traffic_observation": {
        "simulation_run_id",
        "cycle_sequence",
        "simulation_time_sec",
        "intersection_id",
        "direction",
        "source_entity_id",
        "vehicle_count",
        "pcu_equivalent",
        "average_speed_kmh",
        "queue_length_m",
        "waiting_vehicle_count",
        "occupancy_pct",
        "arrival_rate_pcu_per_sec",
        "traffic_status",
        "spillback_risk",
        "dominant_waiting_reason",
        "scenario_id",
        *LINEAGE_COLUMNS,
        "quality_status",
        "quality_flags",
        "processed_at",
        "migration_version",
    },
    "silver_fact_signal_state": {
        "simulation_run_id",
        "cycle_sequence",
        "simulation_time_sec",
        "intersection_id",
        "direction",
        "source_entity_id",
        "signal_status",
        "current_phase",
        "green_duration_sec",
        "red_duration_sec",
        "yellow_duration_sec",
        "timing_mode",
        "scenario_id",
        *LINEAGE_COLUMNS,
        "quality_flags",
        "processed_at",
        "migration_version",
    },
    "silver_fact_intersection_state": {
        "simulation_run_id",
        "cycle_sequence",
        "simulation_time_sec",
        "intersection_id",
        "source_entity_id",
        "overall_traffic_status",
        "derived_traffic_state",
        "current_phase",
        "has_active_incident",
        "has_spillback",
        "is_box_blocked",
        "total_vehicle_count",
        "scenario_id",
        *LINEAGE_COLUMNS,
        "quality_flags",
        "processed_at",
        "migration_version",
    },
    "silver_fact_camera_observation": {
        "simulation_run_id",
        "cycle_sequence",
        "simulation_time_sec",
        "intersection_id",
        "source_entity_id",
        "vehicle_count",
        "average_speed_kmh",
        "occupancy_pct",
        "traffic_status",
        "incident_detected",
        "confidence",
        "recommended_signal_action",
        "incident_type",
        "incident_severity",
        "scenario_id",
        *LINEAGE_COLUMNS,
        "quality_flags",
        "processed_at",
        "migration_version",
    },
    "silver_fact_run_event": {
        "simulation_run_id",
        "event_name",
        "event_simulation_time",
        "scenario_id",
        "producer_id",
        *LINEAGE_COLUMNS,
        "processed_at",
        "migration_version",
    },
    "silver_dim_run": {
        "simulation_run_id",
        "scenario_id",
        "seed",
        "producer_id",
        "started_at",
        "ended_at",
        "run_status",
        "contract_version",
        "node_count",
        "source_bronze_run_id",
        "created_at",
        "updated_at",
    },
    "silver_dim_intersection": {
        "intersection_id",
        "intersection_name",
        "latitude",
        "longitude",
        "network_zone",
        "connected_intersections",
        "source_hash",
        "source_bronze_event_id",
        "created_at",
        "updated_at",
    },
    "silver_dim_approach": {
        "intersection_id",
        "direction",
        "source_bronze_event_id",
        "created_at",
        "updated_at",
    },
    "silver_dim_scenario": {
        "scenario_id",
        "description",
        "created_at",
    },
    "silver_quarantine": {
        "silver_quarantine_id",
        "source_bronze_event_id",
        "raw_ingestion_id",
        "simulation_run_id",
        "entity_id",
        "entity_type",
        "failure_stage",
        "error_code",
        "error_message",
        "retryable",
        "payload_hash",
        "source_payload",
        "created_at",
        "migration_version",
    },
    "silver_processing_ledger": {
        "checkpoint_namespace",
        "source_bronze_event_id",
        "raw_ingestion_id",
        "payload_hash",
        "disposition",
        "target_table",
        "processed_at",
        "migration_version",
    },
}

# Domain property path → silver column (payload-level mappings for parity docs/tests)
ENTITY_PROPERTY_MAP: Dict[str, Dict[str, str]] = {
    ENTITY_VEHICLE_SENSOR: {
        "refIntersection.object": "intersection_id",
        "trafficDirection.value": "direction",
        "vehicleCount.value": "vehicle_count",
        "pcuEquivalent.value": "pcu_equivalent",
        "averageSpeed.value": "average_speed_kmh",
        "queueLength.value": "queue_length_m",
        "waitingVehicleCount.value": "waiting_vehicle_count",
        "occupancyRate.value": "occupancy_pct",
        "arrivalRatePcuPerSec.value": "arrival_rate_pcu_per_sec",
        "trafficStatus.value": "traffic_status",
        "spillbackRisk.value": "spillback_risk",
        "dominantWaitingReason.value": "dominant_waiting_reason",
    },
    ENTITY_TRAFFIC_LIGHT: {
        "refIntersection.object": "intersection_id",
        "trafficDirection.value": "direction",
        "currentStatus.value": "signal_status",
        "currentPhase.value": "current_phase",
        "greenDurationCurrent.value": "green_duration_sec",
        "redDurationCurrent.value": "red_duration_sec",
        "yellowDuration.value": "yellow_duration_sec",
        "timingMode.value": "timing_mode",
    },
    ENTITY_INTERSECTION: {
        "id": "intersection_id",
        "overallTrafficStatus.value": "overall_traffic_status",
        "derivedTrafficState.value": "derived_traffic_state",
        "currentPhase.value": "current_phase",
        "hasActiveIncident.value": "has_active_incident",
        "hasSpillback.value": "has_spillback",
        "isBoxBlocked.value": "is_box_blocked",
        "totalVehicleCount.value": "total_vehicle_count",
        "name.value": "intersection_name",
        "location.value.coordinates": "latitude_longitude",
    },
    ENTITY_CAMERA: {
        "refIntersection.object": "intersection_id",
        "id": "source_entity_id",
        "vehicleCount.value": "vehicle_count",
        "averageSpeed.value": "average_speed_kmh",
        "occupancyRate.value": "occupancy_pct",
        "trafficStatus.value": "traffic_status",
        "incidentDetected.value": "incident_detected",
        "confidence.value": "confidence",
        "recommendedSignalAction.value": "recommended_signal_action",
        "incidentType.value": "incident_type",
        "incidentSeverity.value": "incident_severity",
    },
}

FORBIDDEN_SILVER_DERIVATIONS = frozenset({"scenario_type", "weather"})


def parse_urn_id(urn: str) -> str:
    if not urn or not isinstance(urn, str):
        return ""
    return urn.split(":")[-1]


def normalize_direction(raw: str) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    return DIRECTION_MAP.get(raw.strip().upper())


def unwrap_property_value(node: Any) -> Any:
    """Unwrap NGSI-LD Property / plain value. Contract helper only."""
    if node is None:
        return None
    if isinstance(node, dict):
        if "value" in node:
            value = node["value"]
            if isinstance(value, dict) and "@value" in value:
                return value["@value"]
            return value
        return None
    return node


def unwrap_relationship_object(node: Any) -> Any:
    """Unwrap NGSI-LD Relationship object (string or list)."""
    if node is None:
        return None
    if isinstance(node, dict):
        return node.get("object")
    return None


def unwrap_geoproperty_coordinates(node: Any) -> Optional[List[float]]:
    """Return [lon, lat] from NGSI-LD GeoProperty, else None."""
    if not isinstance(node, dict):
        return None
    value = node.get("value")
    if not isinstance(value, dict):
        return None
    coords = value.get("coordinates")
    if isinstance(coords, list) and len(coords) >= 2:
        return [float(coords[0]), float(coords[1])]
    return None


def route_entity_type(entity_type: str) -> Tuple[Optional[str], Tuple[str, ...], str]:
    if entity_type not in ROUTING_MATRIX:
        return None, (), DISPOSITION_QUARANTINED
    return ROUTING_MATRIX[entity_type]


def assert_no_forbidden_derivations(column_names: Set[str]) -> None:
    bad = FORBIDDEN_SILVER_DERIVATIONS.intersection(column_names)
    if bad:
        raise AssertionError(f"Forbidden Silver derivation columns present: {sorted(bad)}")
