"""Silver dataclasses — Plan 1 fact/dim interfaces + Plan 2 result containers."""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional, Union


@dataclass
class SilverObservationFact:
    simulation_run_id: str
    cycle_sequence: int
    simulation_time_sec: float
    intersection_id: str
    direction: str
    source_entity_id: str
    vehicle_count: int
    pcu_equivalent: float
    average_speed_kmh: float
    queue_length_m: float
    waiting_vehicle_count: int
    occupancy_pct: float
    arrival_rate_pcu_per_sec: float
    traffic_status: str
    spillback_risk: int
    dominant_waiting_reason: str
    scenario_id: str
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    quality_status: str = "VALID"
    quality_flags: str = ""
    processed_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


@dataclass
class SilverCameraObservationFact:
    simulation_run_id: str
    cycle_sequence: int
    simulation_time_sec: float
    intersection_id: str
    source_entity_id: str
    vehicle_count: Optional[int]
    average_speed_kmh: Optional[float]
    occupancy_pct: Optional[float]
    traffic_status: str
    incident_detected: int
    confidence: float
    recommended_signal_action: str
    incident_type: str
    incident_severity: str
    scenario_id: str
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    quality_flags: str = ""
    processed_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


@dataclass
class SilverSignalStateFact:
    simulation_run_id: str
    cycle_sequence: int
    simulation_time_sec: float
    intersection_id: str
    direction: str
    source_entity_id: str
    signal_status: str
    current_phase: str
    green_duration_sec: Optional[float]
    red_duration_sec: Optional[float]
    yellow_duration_sec: Optional[float]
    timing_mode: str
    scenario_id: str
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    quality_flags: str = ""
    processed_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


@dataclass
class SilverIntersectionStateFact:
    simulation_run_id: str
    cycle_sequence: int
    simulation_time_sec: float
    intersection_id: str
    source_entity_id: str
    overall_traffic_status: str
    derived_traffic_state: str
    current_phase: str
    has_active_incident: int
    has_spillback: int
    is_box_blocked: int
    total_vehicle_count: Optional[int]
    scenario_id: str
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    quality_flags: str = ""
    processed_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


@dataclass
class SilverRunEventFact:
    simulation_run_id: str
    event_name: str
    event_simulation_time: float
    scenario_id: str
    producer_id: str
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    processed_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


@dataclass
class SilverDimRun:
    simulation_run_id: str
    scenario_id: str
    producer_id: str
    started_at: datetime
    contract_version: str
    source_bronze_run_id: str
    seed: Optional[str] = None
    ended_at: Optional[datetime] = None
    run_status: str = "RUNNING"
    node_count: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class SilverDimIntersection:
    intersection_id: str
    intersection_name: str
    latitude: float
    longitude: float
    source_hash: str
    source_bronze_event_id: str
    network_zone: str = ""
    connected_intersections: Optional[list[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class SilverDimApproach:
    intersection_id: str
    direction: str
    source_bronze_event_id: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class SilverDimScenario:
    scenario_id: str
    description: str = ""
    created_at: Optional[datetime] = None


@dataclass
class SilverQuarantineEntry:
    silver_quarantine_id: str
    source_bronze_event_id: str
    raw_ingestion_id: str
    simulation_run_id: Optional[str]
    entity_id: Optional[str]
    entity_type: Optional[str]
    failure_stage: str
    error_code: str
    error_message: str
    retryable: int = 0
    payload_hash: str = ""
    source_payload: str = ""
    created_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


@dataclass
class SilverLedgerEntry:
    checkpoint_namespace: str
    source_bronze_event_id: str
    raw_ingestion_id: str
    payload_hash: str
    disposition: str
    target_table: str
    processed_at: Optional[datetime] = None
    migration_version: str = "k9-silver-v1"


FACT_MODEL_BY_TABLE = {
    "silver_fact_traffic_observation": SilverObservationFact,
    "silver_fact_camera_observation": SilverCameraObservationFact,
    "silver_fact_signal_state": SilverSignalStateFact,
    "silver_fact_intersection_state": SilverIntersectionStateFact,
    "silver_fact_run_event": SilverRunEventFact,
}

FactRecord = Union[
    SilverObservationFact,
    SilverCameraObservationFact,
    SilverSignalStateFact,
    SilverIntersectionStateFact,
    SilverRunEventFact,
]

LINEAGE_COLUMNS = (
    "source_bronze_event_id",
    "source_raw_ingestion_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_payload_hash",
)


@dataclass(frozen=True)
class DispositionProposal:
    source_bronze_event_id: str
    raw_ingestion_id: str
    payload_hash: str
    proposed_disposition: str
    primary_target_table: str


@dataclass(frozen=True)
class TransformationResult:
    facts: tuple[FactRecord, ...]
    dimensions: tuple  # tuple[DimensionCandidate, ...] — typed in engine/dimension_builders
    quarantine: Optional[SilverQuarantineEntry]
    proposal: DispositionProposal
    proposed_disposition: str
    quality_status: str
    quality_flags: str


def model_field_names(cls: type) -> set[str]:
    return {f.name for f in fields(cls)}
