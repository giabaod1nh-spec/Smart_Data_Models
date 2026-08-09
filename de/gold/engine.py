"""Deterministic, pure in-memory Gold 2 orchestration."""
from __future__ import annotations

from dataclasses import dataclass

from de.gold.aggregators.intersection_window import (
    IntersectionTrafficRollup,
    aggregate_intersection_windows,
    rollup_directions_to_intersections,
)
from de.gold.aggregators.signal_operation_window import aggregate_signal_windows
from de.gold.aggregators.traffic_window import TrafficAggregate, aggregate_traffic_windows
from de.gold.builders.facts import (
    build_comparison_fact,
    build_intersection_fact,
    build_kpi_fact,
    build_network_fact,
    build_network_metric_definition,
    build_rank_fact,
    build_signal_fact,
    build_traffic_fact,
)
from de.gold.builders.network import build_network_overviews
from de.gold.calculators.comparison import compare_values
from de.gold.calculators.congestion import calculate_congestion
from de.gold.calculators.priority import PriorityResult, calculate_priority, rank_priorities
from de.gold.canonicalization import canonicalize_record
from de.gold.contracts import DIRECTION_MAPPING_VERSION, canonical_window_id
from de.gold.deduplication import deduplicate
from de.gold.input_models import (
    GoldTransformationContext,
    SilverCameraObservationInput,
    SilverGoldInput,
    SilverIntersectionStateInput,
    SilverSignalStateInput,
    SilverTrafficObservationInput,
)
from de.gold.models import (
    GoldDimMetricDefinition,
    GoldFactIntersectionWindow,
    GoldFactKpiResult,
    GoldFactSignalOperationWindow,
    GoldFactTrafficComparison,
    GoldFactTrafficWindow,
)
from de.gold.quality import combine_quality, sorted_flags
from de.gold.validation import ValidationResult, validate_context, validate_input
from de.gold.windowing import WindowKey, expand_windows


TRAFFIC_COMPARISON_METRICS = (
    ("AVG_SPEED_KMH", "avg_speed_kmh"),
    ("AVG_QUEUE_LENGTH_M", "avg_queue_length_m"),
    ("MAX_QUEUE_LENGTH_M", "max_queue_length_m"),
    ("AVG_OCCUPANCY_PCT", "avg_occupancy_pct"),
    ("AVG_VEHICLE_COUNT", "avg_vehicle_count"),
    ("AVG_ARRIVAL_RATE_PCU_PER_SEC", "avg_arrival_rate_pcu_per_sec"),
)


@dataclass(frozen=True)
class RejectedRecord:
    record: SilverGoldInput
    validation: ValidationResult


@dataclass(frozen=True)
class OutputLineageEvidence:
    output_type: str
    business_key: tuple
    source_set_hash: str
    source_row_count: int


@dataclass(frozen=True)
class GoldTransformationResult:
    traffic_windows: tuple[GoldFactTrafficWindow, ...]
    intersection_windows: tuple[GoldFactIntersectionWindow, ...]
    comparisons: tuple[GoldFactTrafficComparison, ...]
    signal_operation_windows: tuple[GoldFactSignalOperationWindow, ...]
    kpi_results: tuple[GoldFactKpiResult, ...]
    metric_definitions: tuple[GoldDimMetricDefinition, ...]
    warnings: tuple[str, ...]
    unsupported_records: tuple[RejectedRecord, ...]
    conflict_evidence: tuple[tuple, ...]
    lineage_evidence: tuple[OutputLineageEvidence, ...]


def _traffic_identity(item: TrafficAggregate) -> tuple:
    return (
        item.window.simulation_run_id, item.window.scenario_id, item.intersection_id,
        item.direction, item.window.window_size_sec, item.window.window_start_sim_sec,
    )


def _rollup_identity(item: IntersectionTrafficRollup) -> tuple:
    return (
        item.window.simulation_run_id, item.window.scenario_id, item.intersection_id,
        item.window.window_size_sec, item.window.window_start_sim_sec,
    )


def _previous_window(window: WindowKey) -> WindowKey:
    start = window.window_start_sim_sec - window.window_size_sec
    end = window.window_start_sim_sec
    return WindowKey(
        window.simulation_run_id, window.scenario_id, window.window_size_sec,
        start, end,
        canonical_window_id(
            window.simulation_run_id, window.scenario_id,
            window.window_size_sec, start, end,
        ),
    )


def _output_sort_key(item: object) -> tuple:
    return tuple(
        getattr(item, name, "")
        for name in (
            "simulation_run_id", "scenario_id", "window_size_sec",
            "window_start_sim_sec", "intersection_id", "direction", "metric_code",
        )
    )


class GoldTransformationEngine:
    """Execute only the frozen Gold 2 algorithm; never performs I/O."""

    def transform(
        self,
        records: tuple[SilverGoldInput, ...],
        context: GoldTransformationContext,
    ) -> GoldTransformationResult:
        validate_context(context)
        accepted: list[SilverGoldInput] = []
        rejected: list[RejectedRecord] = []
        warnings: list[str] = []
        for record in records:
            validation = validate_input(record)
            if not validation.valid:
                rejected.append(RejectedRecord(record, validation))
                warnings.extend(
                    f"{record.source_bronze_event_id}:{error}"
                    for error in validation.errors
                )
                continue
            canonical = canonicalize_record(record)
            if isinstance(canonical, SilverCameraObservationInput):
                warnings.append(f"{canonical.source_bronze_event_id}:CAMERA_EVIDENCE_ONLY")
                continue
            accepted.append(canonical)

        deduplicated = deduplicate(tuple(accepted))
        windowed = expand_windows(deduplicated.records)
        traffic = aggregate_traffic_windows(windowed, context)
        intersections = aggregate_intersection_windows(windowed, context)
        conflict_windows = expand_windows(deduplicated.conflicted_records)
        traffic_conflicts = {
            (
                item.window.simulation_run_id, item.window.scenario_id,
                item.record.intersection_id, item.record.canonical_direction,
                item.window.window_size_sec, item.window.window_start_sim_sec,
            )
            for item in conflict_windows
            if isinstance(item.record, SilverTrafficObservationInput)
        }
        intersection_conflicts = {
            (
                item.window.simulation_run_id, item.window.scenario_id,
                item.record.intersection_id, item.window.window_size_sec,
                item.window.window_start_sim_sec,
            )
            for item in conflict_windows
            if isinstance(item.record, SilverIntersectionStateInput)
        }
        signal_conflicts = {
            (
                item.window.simulation_run_id, item.window.scenario_id,
                item.record.intersection_id, item.record.canonical_direction,
                item.window.window_size_sec, item.window.window_start_sim_sec,
            )
            for item in conflict_windows
            if isinstance(item.record, SilverSignalStateInput)
        }
        conflicted_rollup_windows = {
            (run, scenario, intersection_id, size, start)
            for run, scenario, intersection_id, _direction, size, start in traffic_conflicts
        }
        traffic = tuple(item for item in traffic if _traffic_identity(item) not in traffic_conflicts)
        intersections = tuple(
            item for item in intersections
            if (
                item.window.simulation_run_id, item.window.scenario_id,
                item.intersection_id, item.window.window_size_sec,
                item.window.window_start_sim_sec,
            ) not in intersection_conflicts
        )
        rollups = tuple(
            item for item in rollup_directions_to_intersections(traffic, context)
            if _rollup_identity(item) not in conflicted_rollup_windows
        )
        signals = aggregate_signal_windows(windowed, traffic, context)
        signals = tuple(
            item for item in signals
            if (
                item.window.simulation_run_id, item.window.scenario_id,
                item.intersection_id, item.direction, item.window.window_size_sec,
                item.window.window_start_sim_sec,
            ) not in signal_conflicts
        )

        traffic_map = {_traffic_identity(item): item for item in traffic}
        rollup_map = {_rollup_identity(item): item for item in rollups}
        intersection_map = {
            (item.window.window_id, item.intersection_id): item for item in intersections
        }

        congestion = {
            (item.window.window_id, item.intersection_id): calculate_congestion(item)
            for item in rollups
        }
        unranked: list[PriorityResult] = []
        rollup_by_window: dict[str, list[IntersectionTrafficRollup]] = {}
        for item in rollups:
            rollup_by_window.setdefault(item.window.window_id, []).append(item)
            previous = rollup_map.get((
                item.window.simulation_run_id, item.window.scenario_id,
                item.intersection_id, item.window.window_size_sec,
                item.window.window_start_sim_sec - item.window.window_size_sec,
            ))
            unranked.append(calculate_priority(
                item,
                congestion[(item.window.window_id, item.intersection_id)],
                intersection_map.get((item.window.window_id, item.intersection_id)),
                previous,
            ))

        ranked: list[PriorityResult] = []
        for window_id in sorted(rollup_by_window):
            candidates = tuple(item for item in unranked if item.window_id == window_id)
            ranked.extend(rank_priorities(candidates))
        priority_map = {(item.window_id, item.intersection_id): item for item in ranked}

        comparisons: list[GoldFactTrafficComparison] = []
        for current in traffic:
            previous_key = (
                current.window.simulation_run_id, current.window.scenario_id,
                current.intersection_id, current.direction,
                current.window.window_size_sec,
                current.window.window_start_sim_sec - current.window.window_size_sec,
            )
            previous = traffic_map.get(previous_key)
            previous_window = previous.window if previous else _previous_window(current.window)
            source_rows = current.source_rows + (() if previous is None else previous.source_rows)
            statuses = (current.quality_status,) + (() if previous is None else (previous.quality_status,))
            quality = combine_quality(statuses)
            flags = sorted_flags(
                current.quality_flags,
                (() if previous is not None else ("NO_PREVIOUS_WINDOW",)),
                (() if previous is None else previous.quality_flags),
            )
            for metric_code, field_name in TRAFFIC_COMPARISON_METRICS:
                result = compare_values(
                    float(getattr(current, field_name)),
                    None if previous is None else float(getattr(previous, field_name)),
                )
                comparisons.append(build_comparison_fact(
                    current_window=current.window,
                    previous_window=previous_window,
                    intersection_id=current.intersection_id,
                    direction=current.direction,
                    source_direction=current.source_direction,
                    metric_code=metric_code,
                    comparison=result,
                    source_rows=source_rows,
                    quality_status=quality,
                    quality_flags=flags,
                    context=context,
                ))

        for current in rollups:
            previous = rollup_map.get((
                current.window.simulation_run_id, current.window.scenario_id,
                current.intersection_id, current.window.window_size_sec,
                current.window.window_start_sim_sec - current.window.window_size_sec,
            ))
            previous_window = previous.window if previous else _previous_window(current.window)
            current_congestion = congestion[(current.window.window_id, current.intersection_id)]
            previous_congestion = None if previous is None else congestion.get((previous.window.window_id, previous.intersection_id))
            current_priority = priority_map[(current.window.window_id, current.intersection_id)]
            previous_priority = None if previous is None else priority_map.get((previous.window.window_id, previous.intersection_id))
            source_rows = current.source_rows + (() if previous is None else previous.source_rows)
            quality = combine_quality((current.quality_status,) + (() if previous is None else (previous.quality_status,)))
            flags = sorted_flags(current.quality_flags, (() if previous is not None else ("NO_PREVIOUS_WINDOW",)), (() if previous is None else previous.quality_flags))
            for metric_code, current_value, previous_value in (
                ("CONGESTION_SCORE_WINDOW", current_congestion.numeric_value, None if previous_congestion is None else previous_congestion.numeric_value),
                ("INTERSECTION_PRIORITY_WINDOW", current_priority.score, None if previous_priority is None else previous_priority.score),
            ):
                comparisons.append(build_comparison_fact(
                    current_window=current.window, previous_window=previous_window,
                    intersection_id=current.intersection_id, direction="", source_direction="",
                    metric_code=metric_code,
                    comparison=compare_values(current_value, previous_value),
                    source_rows=source_rows, quality_status=quality,
                    quality_flags=flags, context=context,
                ))

        kpis: list[GoldFactKpiResult] = []
        for rollup in rollups:
            key = (rollup.window.window_id, rollup.intersection_id)
            kpis.append(build_kpi_fact(rollup, congestion[key], context))
            kpis.append(build_kpi_fact(rollup, priority_map[key].calculation, context))
            kpis.append(build_rank_fact(rollup, priority_map[key], context))

        overviews = build_network_overviews(
            rollups, intersections, congestion, tuple(ranked), context,
        )
        kpis.extend(build_network_fact(item, context) for item in overviews)

        traffic_facts = tuple(sorted((build_traffic_fact(item, context) for item in traffic), key=_output_sort_key))
        intersection_facts = tuple(sorted((build_intersection_fact(item, context) for item in intersections), key=_output_sort_key))
        comparison_facts = tuple(sorted(comparisons, key=_output_sort_key))
        signal_facts = tuple(sorted((build_signal_fact(item, context) for item in signals), key=_output_sort_key))
        kpi_facts = tuple(sorted(kpis, key=_output_sort_key))
        lineage_evidence = tuple(
            OutputLineageEvidence(
                type(item).__name__, _output_sort_key(item),
                item.source_set_hash, item.source_row_count,
            )
            for item in (*traffic_facts, *intersection_facts, *comparison_facts, *signal_facts, *kpi_facts)
        )
        return GoldTransformationResult(
            traffic_windows=traffic_facts,
            intersection_windows=intersection_facts,
            comparisons=comparison_facts,
            signal_operation_windows=signal_facts,
            kpi_results=kpi_facts,
            metric_definitions=(build_network_metric_definition(context),) if overviews else (),
            warnings=tuple(sorted(set(warnings))),
            unsupported_records=tuple(sorted(rejected, key=lambda item: item.record.source_bronze_event_id)),
            conflict_evidence=deduplicated.conflicts,
            lineage_evidence=lineage_evidence,
        )
