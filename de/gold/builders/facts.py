"""Map Gold 2 aggregate/calculator results to frozen Gold 1 models."""
from __future__ import annotations

import json

from de.gold.aggregators.intersection_window import IntersectionAggregate, IntersectionTrafficRollup
from de.gold.aggregators.signal_operation_window import SignalAggregate
from de.gold.aggregators.traffic_window import TrafficAggregate
from de.gold.builders.network import NetworkOverview
from de.gold.calculators.comparison import ComparisonResult
from de.gold.calculators.explanation import KpiCalculation
from de.gold.calculators.priority import PriorityResult
from de.gold.contracts import DIRECTION_MAPPING_VERSION
from de.gold.input_models import GoldTransformationContext, SilverGoldInput
from de.gold.lineage import LineageMetadata, build_lineage
from de.gold.models import (
    GoldDimMetricDefinition,
    GoldFactIntersectionWindow,
    GoldFactKpiResult,
    GoldFactSignalOperationWindow,
    GoldFactTrafficComparison,
    GoldFactTrafficWindow,
)
from de.gold.quality import analytical_freshness, combine_quality
from de.gold.windowing import WindowKey


def _common(context: GoldTransformationContext, lineage: LineageMetadata, quality: str, flags: tuple[str, ...], freshness: str) -> dict:
    return {
        "namespace": context.namespace,
        "source_set_hash": lineage.source_set_hash,
        "source_row_count": lineage.source_row_count,
        "source_valid_row_count": lineage.source_valid_row_count,
        "source_min_simulation_time": lineage.source_min_simulation_time,
        "source_max_simulation_time": lineage.source_max_simulation_time,
        "source_min_offset": lineage.source_min_offset,
        "source_max_offset": lineage.source_max_offset,
        "source_tables": list(lineage.source_tables),
        "quality_status": quality,
        "quality_flags": "|".join(flags),
        "analytical_freshness_status": freshness,
        "source_latest_simulation_time": lineage.source_latest_simulation_time,
        "source_latest_processed_at": lineage.source_latest_processed_at,
        "computed_at": context.computed_at,
        "gold_schema_version": context.gold_schema_version,
        "definition_version": context.definition_version,
        "definition_major": context.definition_major,
        "definition_minor": context.definition_minor,
        "revision_seq": context.revision_seq,
    }


def build_traffic_fact(item: TrafficAggregate, context: GoldTransformationContext) -> GoldFactTrafficWindow:
    return GoldFactTrafficWindow(
        simulation_run_id=item.window.simulation_run_id, scenario_id=item.window.scenario_id,
        intersection_id=item.intersection_id, direction=item.direction,
        source_direction=item.source_direction, direction_mapping_version=DIRECTION_MAPPING_VERSION,
        window_id=item.window.window_id, window_size_sec=item.window.window_size_sec,
        window_start_sim_sec=item.window.window_start_sim_sec, window_end_sim_sec=item.window.window_end_sim_sec,
        avg_vehicle_count=item.avg_vehicle_count, max_vehicle_count=item.max_vehicle_count, latest_vehicle_count=item.latest_vehicle_count,
        avg_pcu_equivalent=item.avg_pcu_equivalent, max_pcu_equivalent=item.max_pcu_equivalent, latest_pcu_equivalent=item.latest_pcu_equivalent,
        avg_speed_kmh=item.avg_speed_kmh, min_speed_kmh=item.min_speed_kmh, max_speed_kmh=item.max_speed_kmh, latest_speed_kmh=item.latest_speed_kmh,
        avg_queue_length_m=item.avg_queue_length_m, max_queue_length_m=item.max_queue_length_m, latest_queue_length_m=item.latest_queue_length_m,
        avg_waiting_vehicle_count=item.avg_waiting_vehicle_count, max_waiting_vehicle_count=item.max_waiting_vehicle_count,
        avg_occupancy_pct=item.avg_occupancy_pct, max_occupancy_pct=item.max_occupancy_pct,
        avg_arrival_rate_pcu_per_sec=item.avg_arrival_rate_pcu_per_sec, max_arrival_rate_pcu_per_sec=item.max_arrival_rate_pcu_per_sec,
        spillback_observation_count=item.spillback_observation_count, spillback_ratio_pct=item.spillback_ratio_pct,
        latest_traffic_status=item.latest_traffic_status,
        **_common(context, item.lineage, item.quality_status, item.quality_flags, item.analytical_freshness_status),
    )


def build_intersection_fact(item: IntersectionAggregate, context: GoldTransformationContext) -> GoldFactIntersectionWindow:
    return GoldFactIntersectionWindow(
        simulation_run_id=item.window.simulation_run_id, scenario_id=item.window.scenario_id,
        intersection_id=item.intersection_id, window_id=item.window.window_id,
        window_size_sec=item.window.window_size_sec, window_start_sim_sec=item.window.window_start_sim_sec,
        window_end_sim_sec=item.window.window_end_sim_sec,
        avg_total_vehicle_count=item.avg_total_vehicle_count, max_total_vehicle_count=item.max_total_vehicle_count,
        latest_total_vehicle_count=item.latest_total_vehicle_count, latest_overall_traffic_status=item.latest_overall_traffic_status,
        latest_derived_traffic_state=item.latest_derived_traffic_state, latest_phase=item.latest_phase,
        incident_observation_count=item.incident_observation_count, incident_occurrence=item.incident_occurrence,
        spillback_observation_count=item.spillback_observation_count, spillback_occurrence=item.spillback_occurrence,
        box_blocked_observation_count=item.box_blocked_observation_count, box_blocked_occurrence=item.box_blocked_occurrence,
        **_common(context, item.lineage, item.quality_status, item.quality_flags, item.analytical_freshness_status),
    )


def build_signal_fact(item: SignalAggregate, context: GoldTransformationContext) -> GoldFactSignalOperationWindow:
    return GoldFactSignalOperationWindow(
        simulation_run_id=item.window.simulation_run_id, scenario_id=item.window.scenario_id,
        intersection_id=item.intersection_id, direction=item.direction, source_direction=item.source_direction,
        direction_mapping_version=DIRECTION_MAPPING_VERSION, window_id=item.window.window_id,
        window_size_sec=item.window.window_size_sec, window_start_sim_sec=item.window.window_start_sim_sec,
        window_end_sim_sec=item.window.window_end_sim_sec, observation_count=item.observation_count,
        green_observation_count=item.green_observation_count, red_observation_count=item.red_observation_count,
        yellow_observation_count=item.yellow_observation_count, other_status_count=item.other_status_count,
        green_share_pct=item.green_share_pct, red_share_pct=item.red_share_pct, yellow_share_pct=item.yellow_share_pct,
        dominant_signal_status=item.dominant_signal_status, dominant_phase=item.dominant_phase,
        avg_configured_green_duration_sec=item.avg_configured_green_duration_sec,
        avg_configured_red_duration_sec=item.avg_configured_red_duration_sec,
        avg_configured_yellow_duration_sec=item.avg_configured_yellow_duration_sec,
        latest_timing_mode=item.latest_timing_mode, ctx_avg_queue_length_m=item.ctx_avg_queue_length_m,
        ctx_max_queue_length_m=item.ctx_max_queue_length_m,
        **_common(context, item.lineage, item.quality_status, item.quality_flags, item.analytical_freshness_status),
    )


def build_kpi_fact(rollup: IntersectionTrafficRollup, calculation: KpiCalculation, context: GoldTransformationContext) -> GoldFactKpiResult:
    lineage = build_lineage(rollup.source_rows)
    freshness = analytical_freshness(closed=context.window_closed, revision=context.is_revision, age_sec=context.analytical_age_sec, stale_after_sec=context.stale_after_sec, quality_status=calculation.quality_status)
    return GoldFactKpiResult(
        simulation_run_id=rollup.window.simulation_run_id, scenario_id=rollup.window.scenario_id,
        intersection_id=rollup.intersection_id, direction="", source_direction="",
        direction_mapping_version=DIRECTION_MAPPING_VERSION, window_id=rollup.window.window_id,
        window_size_sec=rollup.window.window_size_sec, window_start_sim_sec=rollup.window.window_start_sim_sec,
        window_end_sim_sec=rollup.window.window_end_sim_sec, metric_code=calculation.metric_code,
        metric_version=calculation.metric_version, numeric_value=calculation.numeric_value,
        unit_code=calculation.unit_code, status=calculation.status,
        explanation_json=calculation.explanation_json,
        **_common(context, lineage, calculation.quality_status, calculation.quality_flags, freshness),
    )


def build_rank_fact(rollup: IntersectionTrafficRollup, priority: PriorityResult, context: GoldTransformationContext) -> GoldFactKpiResult:
    calculation = KpiCalculation(
        "PRIORITY_RANK", "v1.0", None if priority.rank is None else float(priority.rank),
        "ORDINAL", "UNRANKED" if priority.rank is None else "RANKED",
        priority.calculation.quality_status, priority.calculation.quality_flags,
        json.dumps({"rule_version":"bd3_priority_rank_v1","rank":priority.rank}, sort_keys=True, separators=(",", ":")),
    )
    return build_kpi_fact(rollup, calculation, context)


def build_comparison_fact(
    *, current_window: WindowKey, previous_window: WindowKey, intersection_id: str,
    direction: str, source_direction: str, metric_code: str, comparison: ComparisonResult,
    source_rows: tuple[SilverGoldInput, ...], quality_status: str,
    quality_flags: tuple[str, ...], context: GoldTransformationContext,
) -> GoldFactTrafficComparison:
    lineage = build_lineage(source_rows)
    freshness = analytical_freshness(closed=context.window_closed, revision=context.is_revision, age_sec=context.analytical_age_sec, stale_after_sec=context.stale_after_sec, quality_status=quality_status)
    return GoldFactTrafficComparison(
        simulation_run_id=current_window.simulation_run_id, scenario_id=current_window.scenario_id,
        intersection_id=intersection_id, direction=direction, source_direction=source_direction,
        direction_mapping_version=DIRECTION_MAPPING_VERSION, metric_code=metric_code,
        current_window_id=current_window.window_id, current_window_size_sec=current_window.window_size_sec,
        current_window_start_sim_sec=current_window.window_start_sim_sec, current_window_end_sim_sec=current_window.window_end_sim_sec,
        previous_window_id=previous_window.window_id, previous_window_start_sim_sec=previous_window.window_start_sim_sec,
        previous_window_end_sim_sec=previous_window.window_end_sim_sec, current_value=comparison.current_value,
        previous_value=comparison.previous_value, absolute_change=comparison.absolute_change,
        percent_change=comparison.percent_change, change_direction=comparison.change_direction,
        comparison_status=comparison.comparison_status,
        **_common(context, lineage, quality_status, quality_flags, freshness),
    )


def build_network_fact(item: NetworkOverview, context: GoldTransformationContext) -> GoldFactKpiResult:
    lineage = build_lineage(item.source_rows)
    payload = {
        "avg_occupancy_pct": item.avg_occupancy_pct,
        "avg_queue_length_m": item.avg_queue_length_m,
        "avg_speed_kmh": item.avg_speed_kmh,
        "congested_intersection_count": item.congested_intersection_count,
        "coverage_ratio": item.coverage_ratio,
        "incident_intersection_count": item.incident_intersection_count,
        "latest_total_vehicle_count": item.latest_total_vehicle_count,
        "max_queue_length_m": item.max_queue_length_m,
        "observed_intersection_count": item.observed_intersection_count,
        "quality_status": item.quality_status,
        "rule_version": "network-overview-v1",
        "spillback_intersection_count": item.spillback_intersection_count,
        "top_priority_intersection_id": item.top_priority_intersection_id,
        "traffic_state_distribution": dict(item.traffic_state_distribution),
    }
    freshness = analytical_freshness(closed=context.window_closed, revision=context.is_revision, age_sec=context.analytical_age_sec, stale_after_sec=context.stale_after_sec, quality_status=item.quality_status)
    return GoldFactKpiResult(
        simulation_run_id=item.window.simulation_run_id, scenario_id=item.window.scenario_id,
        intersection_id="", direction="", source_direction="", direction_mapping_version=DIRECTION_MAPPING_VERSION,
        window_id=item.window.window_id, window_size_sec=item.window.window_size_sec,
        window_start_sim_sec=item.window.window_start_sim_sec, window_end_sim_sec=item.window.window_end_sim_sec,
        metric_code="NETWORK_OVERVIEW_WINDOW", metric_version="v1.0", numeric_value=None,
        unit_code="COMPOSITE_SUMMARY", status=item.quality_status,
        explanation_json=json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        **_common(context, lineage, item.quality_status, item.quality_flags, freshness),
    )


def build_network_metric_definition(context: GoldTransformationContext) -> GoldDimMetricDefinition:
    return GoldDimMetricDefinition(
        metric_code="NETWORK_OVERVIEW_WINDOW", metric_version="v1.0",
        metric_name="Network Overview Window", description="Composite analytical network summary for one closed window.",
        grain="namespace,run,scenario,window", formula_identifier="network-overview-v1",
        unit_code="COMPOSITE_SUMMARY", approval_status="APPROVED",
        formula_json=json.dumps({"decision":"G1-CC-BD-B","persistence":"G1-CC-BD-F"}, sort_keys=True, separators=(",", ":")),
        definition_version=context.definition_version, definition_major=context.definition_major,
        definition_minor=context.definition_minor, computed_at=context.computed_at,
        gold_schema_version=context.gold_schema_version,
    )
