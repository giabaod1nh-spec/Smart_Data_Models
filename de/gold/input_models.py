"""Typed Silver inputs and deterministic Gold 2 transformation context."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional, Union

from de.gold.contracts import GOLD_SCHEMA_VERSION


@dataclass(frozen=True)
class SilverTrafficObservationInput:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    source_direction: str
    canonical_direction: str
    direction_mapping_version: str
    source_entity_id: str
    cycle_sequence: int
    simulation_time_sec: float
    vehicle_count: int
    pcu_equivalent: float
    average_speed_kmh: float
    queue_length_m: float
    waiting_vehicle_count: int
    occupancy_pct: float
    arrival_rate_pcu_per_sec: float
    traffic_status: str
    spillback_risk: bool
    quality_status: str
    quality_flags: tuple[str, ...]
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    processed_at: datetime
    migration_version: str = "k9-silver-v1"


@dataclass(frozen=True)
class SilverIntersectionStateInput:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    source_entity_id: str
    cycle_sequence: int
    simulation_time_sec: float
    overall_traffic_status: str
    derived_traffic_state: str
    current_phase: str
    has_active_incident: bool
    has_spillback: bool
    is_box_blocked: bool
    total_vehicle_count: Optional[int]
    quality_status: str
    quality_flags: tuple[str, ...]
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    processed_at: datetime
    migration_version: str = "k9-silver-v1"


@dataclass(frozen=True)
class SilverSignalStateInput:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    source_direction: str
    canonical_direction: str
    direction_mapping_version: str
    source_entity_id: str
    cycle_sequence: int
    simulation_time_sec: float
    signal_status: str
    current_phase: str
    green_duration_sec: Optional[float]
    red_duration_sec: Optional[float]
    yellow_duration_sec: Optional[float]
    timing_mode: str
    quality_status: str
    quality_flags: tuple[str, ...]
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    processed_at: datetime
    migration_version: str = "k9-silver-v1"


@dataclass(frozen=True)
class SilverCameraObservationInput:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    source_entity_id: str
    cycle_sequence: int
    simulation_time_sec: float
    incident_detected: bool
    confidence: float
    quality_status: str
    quality_flags: tuple[str, ...]
    source_bronze_event_id: str
    source_raw_ingestion_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    source_payload_hash: str
    processed_at: datetime
    migration_version: str = "k9-silver-v1"


SilverGoldInput = Union[
    SilverTrafficObservationInput,
    SilverIntersectionStateInput,
    SilverSignalStateInput,
    SilverCameraObservationInput,
]


@dataclass(frozen=True)
class GoldTransformationContext:
    namespace: str
    computed_at: datetime
    definition_version: str = "v1.0"
    definition_major: int = 1
    definition_minor: int = 0
    revision_seq: int = 0
    gold_schema_version: str = GOLD_SCHEMA_VERSION
    expected_rows: Mapping[str, int] | None = None
    configured_intersections: tuple[str, ...] = ()
    configured_directions: tuple[str, ...] = ("N", "S", "E", "W")
    window_closed: bool = True
    is_revision: bool = False
    analytical_age_sec: float = 0.0
    stale_after_sec: float = 600.0

    def expected(self, key: str, observed: int) -> int:
        if self.expected_rows is None:
            return observed
        return int(self.expected_rows.get(key, observed))

