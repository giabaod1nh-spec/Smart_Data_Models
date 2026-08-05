"""Frozen Gold 1 dimension, fact, and ledger records."""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class GoldDimRun:
    simulation_run_id: str
    scenario_id: str
    seed: Optional[str]
    producer_id: str
    started_at: datetime
    ended_at: Optional[datetime]
    run_status: str
    contract_version: str
    node_count: Optional[int]
    source_bronze_run_id: str
    source_hash: str
    definition_version: str
    definition_major: int
    definition_minor: int
    computed_at: datetime
    gold_schema_version: str


@dataclass(frozen=True)
class GoldDimScenario:
    scenario_id: str
    description: str
    source_hash: str
    definition_version: str
    definition_major: int
    definition_minor: int
    computed_at: datetime
    gold_schema_version: str


@dataclass(frozen=True)
class GoldDimIntersection:
    intersection_id: str
    intersection_name: str
    latitude: float
    longitude: float
    network_zone: str
    connected_intersections: list[str]
    source_hash: str
    definition_version: str
    definition_major: int
    definition_minor: int
    computed_at: datetime
    gold_schema_version: str


@dataclass(frozen=True)
class GoldDimApproach:
    intersection_id: str
    direction: str
    source_direction: str
    direction_mapping_version: str
    source_hash: str
    definition_version: str
    definition_major: int
    definition_minor: int
    computed_at: datetime
    gold_schema_version: str


@dataclass(frozen=True)
class GoldDimWindow:
    window_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    computed_at: datetime
    gold_schema_version: str


@dataclass(frozen=True)
class GoldDimMetricDefinition:
    metric_code: str
    metric_version: str
    metric_name: str
    description: str
    grain: str
    formula_identifier: str
    unit_code: str
    approval_status: str
    formula_json: str
    definition_version: str
    definition_major: int
    definition_minor: int
    computed_at: datetime
    gold_schema_version: str


@dataclass(frozen=True)
class GoldFactTrafficWindow:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    direction: str
    source_direction: str
    direction_mapping_version: str
    window_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    avg_vehicle_count: float
    max_vehicle_count: int
    latest_vehicle_count: int
    avg_pcu_equivalent: float
    max_pcu_equivalent: float
    latest_pcu_equivalent: float
    avg_speed_kmh: float
    min_speed_kmh: float
    max_speed_kmh: float
    latest_speed_kmh: float
    avg_queue_length_m: float
    max_queue_length_m: float
    latest_queue_length_m: float
    avg_waiting_vehicle_count: float
    max_waiting_vehicle_count: int
    avg_occupancy_pct: float
    max_occupancy_pct: float
    avg_arrival_rate_pcu_per_sec: float
    max_arrival_rate_pcu_per_sec: float
    spillback_observation_count: int
    spillback_ratio_pct: float
    latest_traffic_status: str
    namespace: str
    source_set_hash: str
    source_row_count: int
    source_valid_row_count: int
    source_min_simulation_time: float
    source_max_simulation_time: float
    source_min_offset: Optional[int]
    source_max_offset: Optional[int]
    source_tables: list[str]
    quality_status: str
    quality_flags: str
    analytical_freshness_status: str
    source_latest_simulation_time: float
    source_latest_processed_at: datetime
    computed_at: datetime
    gold_schema_version: str
    definition_version: str
    definition_major: int
    definition_minor: int
    revision_seq: int


@dataclass(frozen=True)
class GoldFactIntersectionWindow:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    window_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    avg_total_vehicle_count: float
    max_total_vehicle_count: Optional[int]
    latest_total_vehicle_count: Optional[int]
    latest_overall_traffic_status: str
    latest_derived_traffic_state: str
    latest_phase: str
    incident_observation_count: int
    incident_occurrence: int
    spillback_observation_count: int
    spillback_occurrence: int
    box_blocked_observation_count: int
    box_blocked_occurrence: int
    namespace: str
    source_set_hash: str
    source_row_count: int
    source_valid_row_count: int
    source_min_simulation_time: float
    source_max_simulation_time: float
    source_min_offset: Optional[int]
    source_max_offset: Optional[int]
    source_tables: list[str]
    quality_status: str
    quality_flags: str
    analytical_freshness_status: str
    source_latest_simulation_time: float
    source_latest_processed_at: datetime
    computed_at: datetime
    gold_schema_version: str
    definition_version: str
    definition_major: int
    definition_minor: int
    revision_seq: int


@dataclass(frozen=True)
class GoldFactTrafficComparison:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    direction: str
    source_direction: str
    direction_mapping_version: str
    metric_code: str
    current_window_id: str
    current_window_size_sec: int
    current_window_start_sim_sec: float
    current_window_end_sim_sec: float
    previous_window_id: str
    previous_window_start_sim_sec: float
    previous_window_end_sim_sec: float
    current_value: Optional[float]
    previous_value: Optional[float]
    absolute_change: Optional[float]
    percent_change: Optional[float]
    change_direction: str
    comparison_status: str
    namespace: str
    source_set_hash: str
    source_row_count: int
    source_valid_row_count: int
    source_min_simulation_time: float
    source_max_simulation_time: float
    source_min_offset: Optional[int]
    source_max_offset: Optional[int]
    source_tables: list[str]
    quality_status: str
    quality_flags: str
    analytical_freshness_status: str
    source_latest_simulation_time: float
    source_latest_processed_at: datetime
    computed_at: datetime
    gold_schema_version: str
    definition_version: str
    definition_major: int
    definition_minor: int
    revision_seq: int


@dataclass(frozen=True)
class GoldFactSignalOperationWindow:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    direction: str
    source_direction: str
    direction_mapping_version: str
    window_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    observation_count: int
    green_observation_count: int
    red_observation_count: int
    yellow_observation_count: int
    other_status_count: int
    green_share_pct: Optional[float]
    red_share_pct: Optional[float]
    yellow_share_pct: Optional[float]
    dominant_signal_status: str
    dominant_phase: str
    avg_configured_green_duration_sec: Optional[float]
    avg_configured_red_duration_sec: Optional[float]
    avg_configured_yellow_duration_sec: Optional[float]
    latest_timing_mode: str
    ctx_avg_queue_length_m: Optional[float]
    ctx_max_queue_length_m: Optional[float]
    namespace: str
    source_set_hash: str
    source_row_count: int
    source_valid_row_count: int
    source_min_simulation_time: float
    source_max_simulation_time: float
    source_min_offset: Optional[int]
    source_max_offset: Optional[int]
    source_tables: list[str]
    quality_status: str
    quality_flags: str
    analytical_freshness_status: str
    source_latest_simulation_time: float
    source_latest_processed_at: datetime
    computed_at: datetime
    gold_schema_version: str
    definition_version: str
    definition_major: int
    definition_minor: int
    revision_seq: int


@dataclass(frozen=True)
class GoldFactKpiResult:
    simulation_run_id: str
    scenario_id: str
    intersection_id: str
    direction: str
    source_direction: str
    direction_mapping_version: str
    window_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    metric_code: str
    metric_version: str
    numeric_value: Optional[float]
    unit_code: str
    status: str
    explanation_json: str
    namespace: str
    source_set_hash: str
    source_row_count: int
    source_valid_row_count: int
    source_min_simulation_time: float
    source_max_simulation_time: float
    source_min_offset: Optional[int]
    source_max_offset: Optional[int]
    source_tables: list[str]
    quality_status: str
    quality_flags: str
    analytical_freshness_status: str
    source_latest_simulation_time: float
    source_latest_processed_at: datetime
    computed_at: datetime
    gold_schema_version: str
    definition_version: str
    definition_major: int
    definition_minor: int
    revision_seq: int


@dataclass(frozen=True)
class GoldProcessingLedger:
    namespace: str
    source_set_hash: str
    definition_version: str
    definition_major: int
    definition_minor: int
    revision_seq: int
    disposition: str
    computed_at: datetime
    error_message: str
    gold_schema_version: str


FACT_MODEL_BY_TABLE = {
    "gold_fact_traffic_window": GoldFactTrafficWindow,
    "gold_fact_intersection_window": GoldFactIntersectionWindow,
    "gold_fact_traffic_comparison": GoldFactTrafficComparison,
    "gold_fact_signal_operation_window": GoldFactSignalOperationWindow,
    "gold_fact_kpi_result": GoldFactKpiResult,
}


def model_field_names(cls: type) -> set[str]:
    """Return a dataclass model's schema field names."""
    return {field.name for field in fields(cls)}
