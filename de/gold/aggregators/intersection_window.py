"""Intersection-state windows and approved direction-to-intersection roll-up."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean

from de.gold.aggregators.traffic_window import TrafficAggregate
from de.gold.input_models import GoldTransformationContext, SilverIntersectionStateInput, SilverTrafficObservationInput
from de.gold.latest_selector import select_latest
from de.gold.lineage import LineageMetadata, build_lineage, canonical_source_set_hash
from de.gold.quality import analytical_freshness, combine_quality, numeric_allowed, pooled_coverage, sorted_flags
from de.gold.windowing import WindowKey, WindowedRecord


@dataclass(frozen=True)
class IntersectionAggregate:
    window: WindowKey
    intersection_id: str
    avg_total_vehicle_count: float
    max_total_vehicle_count: int | None
    latest_total_vehicle_count: int | None
    latest_overall_traffic_status: str
    latest_derived_traffic_state: str
    latest_phase: str
    incident_observation_count: int
    incident_occurrence: int
    spillback_observation_count: int
    spillback_occurrence: int
    box_blocked_observation_count: int
    box_blocked_occurrence: int
    quality_status: str
    quality_flags: tuple[str, ...]
    analytical_freshness_status: str
    coverage_ratio: float | None
    lineage: LineageMetadata
    source_rows: tuple[SilverIntersectionStateInput, ...]


@dataclass(frozen=True)
class IntersectionTrafficRollup:
    window: WindowKey
    intersection_id: str
    avg_queue_length_m: float
    max_queue_length_m: float
    avg_speed_kmh: float
    avg_occupancy_pct: float
    spillback_ratio_pct: float
    vehicle_weight: float
    quality_status: str
    quality_flags: tuple[str, ...]
    coverage_ratio: float | None
    source_rows: tuple[SilverTrafficObservationInput, ...]
    source_set_hash: str


def aggregate_intersection_windows(items: tuple[WindowedRecord, ...], context: GoldTransformationContext) -> tuple[IntersectionAggregate, ...]:
    groups: dict[tuple, list[WindowedRecord]] = {}
    for item in items:
        row = item.record
        if isinstance(row, SilverIntersectionStateInput):
            key = (row.simulation_run_id, row.scenario_id, row.intersection_id, item.window.window_size_sec, item.window.window_start_sim_sec)
            groups.setdefault(key, []).append(item)
    results: list[IntersectionAggregate] = []
    for key in sorted(groups):
        grouped = groups[key]
        rows = tuple(item.record for item in grouped)
        valid = tuple(row for row in rows if numeric_allowed(row.quality_status))
        if not valid:
            continue
        latest = select_latest(valid)
        if latest.record is None:
            continue
        winner = latest.record
        assert isinstance(winner, SilverIntersectionStateInput)
        totals = [row.total_vehicle_count for row in valid if row.total_vehicle_count is not None]
        coverage = pooled_coverage(len(valid), context.expected(f"intersection|{key}", len(rows)))
        quality = combine_quality(tuple(row.quality_status for row in rows) + (coverage.status,))
        lineage = build_lineage(rows, valid_records=valid)
        results.append(IntersectionAggregate(
            window=grouped[0].window, intersection_id=key[2],
            avg_total_vehicle_count=fmean(totals) if totals else 0.0,
            max_total_vehicle_count=max(totals) if totals else None,
            latest_total_vehicle_count=winner.total_vehicle_count,
            latest_overall_traffic_status=winner.overall_traffic_status,
            latest_derived_traffic_state=winner.derived_traffic_state,
            latest_phase=winner.current_phase,
            incident_observation_count=sum(1 for row in valid if row.has_active_incident),
            incident_occurrence=int(any(row.has_active_incident for row in valid)),
            spillback_observation_count=sum(1 for row in valid if row.has_spillback),
            spillback_occurrence=int(any(row.has_spillback for row in valid)),
            box_blocked_observation_count=sum(1 for row in valid if row.is_box_blocked),
            box_blocked_occurrence=int(any(row.is_box_blocked for row in valid)),
            quality_status=quality,
            quality_flags=sorted_flags(*(row.quality_flags for row in rows)),
            analytical_freshness_status=analytical_freshness(closed=context.window_closed, revision=context.is_revision, age_sec=context.analytical_age_sec, stale_after_sec=context.stale_after_sec, quality_status=quality),
            coverage_ratio=coverage.ratio, lineage=lineage, source_rows=valid,
        ))
    return tuple(results)


def rollup_directions_to_intersections(traffic: tuple[TrafficAggregate, ...], context: GoldTransformationContext) -> tuple[IntersectionTrafficRollup, ...]:
    groups: dict[tuple, list[TrafficAggregate]] = {}
    for item in traffic:
        key = (item.window.simulation_run_id, item.window.scenario_id, item.intersection_id, item.window.window_size_sec, item.window.window_start_sim_sec)
        groups.setdefault(key, []).append(item)
    results: list[IntersectionTrafficRollup] = []
    cardinal = set(context.configured_directions)
    for key in sorted(groups):
        all_items = groups[key]
        eligible = [item for item in all_items if item.direction in cardinal and numeric_allowed(item.quality_status)]
        if not eligible:
            continue
        total_weight = sum(item.avg_vehicle_count for item in eligible if item.avg_vehicle_count > 0)
        speed = (
            sum(item.avg_speed_kmh * item.avg_vehicle_count for item in eligible if item.avg_vehicle_count > 0) / total_weight
            if total_weight > 0 else fmean(item.avg_speed_kmh for item in eligible)
        )
        valid_rows = sum(item.lineage.source_valid_row_count for item in eligible)
        expected = sum(context.expected(f"direction|{key}|{direction}", 0) for direction in context.configured_directions)
        if expected == 0:
            expected = sum(item.lineage.source_row_count for item in eligible)
        coverage = pooled_coverage(valid_rows, expected)
        unknown_rows = tuple(row for item in all_items if item.direction == "UNKNOWN" for row in item.source_rows)
        statuses = tuple(item.quality_status for item in eligible) + (coverage.status,)
        quality = combine_quality(statuses)
        flags = sorted_flags(*(item.quality_flags for item in all_items))
        if unknown_rows:
            flags = sorted_flags(flags, ("NON_CANONICAL_DIRECTION",))
            if quality == "VALID":
                quality = "VALID_WITH_GAPS"
        evidence_rows = tuple(row for item in all_items for row in item.source_rows)
        results.append(IntersectionTrafficRollup(
            window=eligible[0].window, intersection_id=key[2],
            avg_queue_length_m=fmean(item.avg_queue_length_m for item in eligible),
            max_queue_length_m=max(item.max_queue_length_m for item in eligible),
            avg_speed_kmh=speed,
            avg_occupancy_pct=fmean(item.avg_occupancy_pct for item in eligible),
            spillback_ratio_pct=(100.0 * sum(item.spillback_observation_count for item in eligible) / valid_rows) if valid_rows else 0.0,
            vehicle_weight=sum(item.avg_vehicle_count for item in eligible),
            quality_status=quality, quality_flags=flags, coverage_ratio=coverage.ratio,
            source_rows=evidence_rows, source_set_hash=canonical_source_set_hash(evidence_rows),
        ))
    return tuple(results)

