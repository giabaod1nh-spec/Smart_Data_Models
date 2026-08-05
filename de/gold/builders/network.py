"""Approved in-memory Network Overview builder."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import fmean

from de.gold.aggregators.intersection_window import IntersectionAggregate, IntersectionTrafficRollup
from de.gold.calculators.explanation import KpiCalculation, round_half_up
from de.gold.calculators.priority import PriorityResult
from de.gold.input_models import GoldTransformationContext, SilverGoldInput
from de.gold.quality import combine_quality, numeric_allowed, pooled_coverage, sorted_flags
from de.gold.windowing import WindowKey


@dataclass(frozen=True)
class NetworkOverview:
    window: WindowKey
    observed_intersection_count: int
    latest_total_vehicle_count: int
    avg_speed_kmh: float
    avg_queue_length_m: float
    max_queue_length_m: float
    avg_occupancy_pct: float
    traffic_state_distribution: tuple[tuple[str, int], ...]
    congested_intersection_count: int
    incident_intersection_count: int
    spillback_intersection_count: int
    top_priority_intersection_id: str
    coverage_ratio: float | None
    quality_status: str
    quality_flags: tuple[str, ...]
    source_rows: tuple[SilverGoldInput, ...]


def _unique_rows(rows: list[SilverGoldInput]) -> tuple[SilverGoldInput, ...]:
    selected: dict[tuple[str, str], SilverGoldInput] = {}
    for row in rows:
        selected[(row.source_bronze_event_id, row.source_payload_hash)] = row
    return tuple(sorted(selected.values(), key=lambda row: (row.source_topic, row.source_partition, row.source_offset, row.source_bronze_event_id)))


def build_network_overviews(
    rollups: tuple[IntersectionTrafficRollup, ...],
    intersections: tuple[IntersectionAggregate, ...],
    congestion: dict[tuple[str, str], KpiCalculation],
    priorities: tuple[PriorityResult, ...],
    context: GoldTransformationContext,
) -> tuple[NetworkOverview, ...]:
    rollup_groups: dict[str, list[IntersectionTrafficRollup]] = {}
    for item in rollups:
        rollup_groups.setdefault(item.window.window_id, []).append(item)
    intersection_map = {(item.window.window_id, item.intersection_id): item for item in intersections}
    priority_map = {(item.window_id, item.intersection_id): item for item in priorities}
    results: list[NetworkOverview] = []
    for window_id in sorted(rollup_groups):
        items = sorted(rollup_groups[window_id], key=lambda item: item.intersection_id)
        eligible = [item for item in items if numeric_allowed(item.quality_status)]
        if not eligible:
            continue
        total_weight = sum(item.vehicle_weight for item in eligible if item.vehicle_weight > 0)
        speed = (
            sum(item.avg_speed_kmh * item.vehicle_weight for item in eligible if item.vehicle_weight > 0) / total_weight
            if total_weight > 0 else fmean(item.avg_speed_kmh for item in eligible)
        )
        linked_intersections = [intersection_map[(window_id, item.intersection_id)] for item in eligible if (window_id, item.intersection_id) in intersection_map]
        state_counts = Counter(item.latest_derived_traffic_state for item in linked_intersections)
        priority_items = [priority_map[(window_id, item.intersection_id)] for item in eligible if (window_id, item.intersection_id) in priority_map]
        ranked = [item for item in priority_items if item.rank == 1]
        valid_rows = sum(sum(1 for row in item.source_rows if numeric_allowed(row.quality_status)) for item in eligible)
        expected = context.expected(f"network|{window_id}", valid_rows)
        coverage = pooled_coverage(valid_rows, expected)
        quality = combine_quality(tuple(item.quality_status for item in eligible) + tuple(item.quality_status for item in linked_intersections) + (coverage.status,))
        source_rows = _unique_rows([row for item in eligible for row in item.source_rows] + [row for item in linked_intersections for row in item.source_rows])
        results.append(NetworkOverview(
            window=eligible[0].window,
            observed_intersection_count=len({item.intersection_id for item in eligible}),
            latest_total_vehicle_count=sum(int(item.latest_total_vehicle_count or 0) for item in linked_intersections),
            avg_speed_kmh=round_half_up(speed),
            avg_queue_length_m=round_half_up(fmean(item.avg_queue_length_m for item in eligible)),
            max_queue_length_m=round_half_up(max(item.max_queue_length_m for item in eligible)),
            avg_occupancy_pct=round_half_up(fmean(item.avg_occupancy_pct for item in eligible)),
            traffic_state_distribution=tuple(sorted(state_counts.items())),
            congested_intersection_count=sum(1 for item in eligible if (congestion.get((window_id, item.intersection_id)) and (congestion[(window_id, item.intersection_id)].numeric_value or 0) >= 70)),
            incident_intersection_count=sum(item.incident_occurrence for item in linked_intersections),
            spillback_intersection_count=sum(item.spillback_occurrence for item in linked_intersections),
            top_priority_intersection_id=ranked[0].intersection_id if ranked else "",
            coverage_ratio=coverage.ratio,
            quality_status=quality,
            quality_flags=sorted_flags(*(item.quality_flags for item in eligible), *(item.quality_flags for item in linked_intersections)),
            source_rows=source_rows,
        ))
    return tuple(results)

