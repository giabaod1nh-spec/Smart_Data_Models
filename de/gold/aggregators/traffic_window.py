"""Traffic direction-window aggregation over Silver snapshot/gauge rows."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean

from de.gold.input_models import GoldTransformationContext, SilverTrafficObservationInput
from de.gold.latest_selector import select_latest
from de.gold.lineage import LineageMetadata, build_lineage
from de.gold.quality import analytical_freshness, combine_quality, numeric_allowed, pooled_coverage, sorted_flags
from de.gold.windowing import WindowKey, WindowedRecord


@dataclass(frozen=True)
class TrafficAggregate:
    window: WindowKey
    intersection_id: str
    direction: str
    source_direction: str
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
    quality_status: str
    quality_flags: tuple[str, ...]
    analytical_freshness_status: str
    coverage_ratio: float | None
    lineage: LineageMetadata
    source_rows: tuple[SilverTrafficObservationInput, ...]


def _key(item: WindowedRecord) -> tuple:
    row = item.record
    assert isinstance(row, SilverTrafficObservationInput)
    return (row.simulation_run_id, row.scenario_id, row.intersection_id, row.canonical_direction, item.window.window_size_sec, item.window.window_start_sim_sec)


def aggregate_traffic_windows(items: tuple[WindowedRecord, ...], context: GoldTransformationContext) -> tuple[TrafficAggregate, ...]:
    groups: dict[tuple, list[WindowedRecord]] = {}
    for item in items:
        if isinstance(item.record, SilverTrafficObservationInput):
            groups.setdefault(_key(item), []).append(item)
    results: list[TrafficAggregate] = []
    for key in sorted(groups):
        grouped = groups[key]
        rows = tuple(item.record for item in grouped)
        valid = tuple(row for row in rows if numeric_allowed(row.quality_status))
        if not valid:
            continue
        latest = select_latest(valid)
        if latest.conflicted or latest.record is None:
            continue
        winner = latest.record
        assert isinstance(winner, SilverTrafficObservationInput)
        expected_key = f"traffic|{key}"
        coverage = pooled_coverage(len(valid), context.expected(expected_key, len(rows)))
        quality = combine_quality(tuple(row.quality_status for row in rows) + (coverage.status,))
        if key[3] == "UNKNOWN" and quality == "VALID":
            quality = "VALID_WITH_GAPS"
        flags = sorted_flags(*(row.quality_flags for row in rows), (("NON_CANONICAL_DIRECTION",) if key[3] == "UNKNOWN" else ()))
        lineage = build_lineage(rows, valid_records=valid)
        freshness = analytical_freshness(closed=context.window_closed, revision=context.is_revision, age_sec=context.analytical_age_sec, stale_after_sec=context.stale_after_sec, quality_status=quality)
        results.append(TrafficAggregate(
            window=grouped[0].window, intersection_id=key[2], direction=key[3],
            source_direction=winner.source_direction,
            avg_vehicle_count=fmean(row.vehicle_count for row in valid),
            max_vehicle_count=max(row.vehicle_count for row in valid),
            latest_vehicle_count=winner.vehicle_count,
            avg_pcu_equivalent=fmean(row.pcu_equivalent for row in valid),
            max_pcu_equivalent=max(row.pcu_equivalent for row in valid),
            latest_pcu_equivalent=winner.pcu_equivalent,
            avg_speed_kmh=fmean(row.average_speed_kmh for row in valid),
            min_speed_kmh=min(row.average_speed_kmh for row in valid),
            max_speed_kmh=max(row.average_speed_kmh for row in valid),
            latest_speed_kmh=winner.average_speed_kmh,
            avg_queue_length_m=fmean(row.queue_length_m for row in valid),
            max_queue_length_m=max(row.queue_length_m for row in valid),
            latest_queue_length_m=winner.queue_length_m,
            avg_waiting_vehicle_count=fmean(row.waiting_vehicle_count for row in valid),
            max_waiting_vehicle_count=max(row.waiting_vehicle_count for row in valid),
            avg_occupancy_pct=fmean(row.occupancy_pct for row in valid),
            max_occupancy_pct=max(row.occupancy_pct for row in valid),
            avg_arrival_rate_pcu_per_sec=fmean(row.arrival_rate_pcu_per_sec for row in valid),
            max_arrival_rate_pcu_per_sec=max(row.arrival_rate_pcu_per_sec for row in valid),
            spillback_observation_count=sum(1 for row in valid if row.spillback_risk),
            spillback_ratio_pct=100.0 * sum(1 for row in valid if row.spillback_risk) / len(valid),
            latest_traffic_status=winner.traffic_status,
            quality_status=quality, quality_flags=flags,
            analytical_freshness_status=freshness, coverage_ratio=coverage.ratio,
            lineage=lineage, source_rows=valid,
        ))
    return tuple(results)

