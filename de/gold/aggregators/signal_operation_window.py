"""Signal operation window aggregation (not performance scoring)."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import fmean

from de.gold.aggregators.traffic_window import TrafficAggregate
from de.gold.input_models import GoldTransformationContext, SilverSignalStateInput
from de.gold.latest_selector import select_latest
from de.gold.lineage import LineageMetadata, build_lineage
from de.gold.quality import analytical_freshness, combine_quality, numeric_allowed, pooled_coverage, sorted_flags
from de.gold.windowing import WindowKey, WindowedRecord


@dataclass(frozen=True)
class SignalAggregate:
    window: WindowKey
    intersection_id: str
    direction: str
    source_direction: str
    observation_count: int
    green_observation_count: int
    red_observation_count: int
    yellow_observation_count: int
    other_status_count: int
    green_share_pct: float | None
    red_share_pct: float | None
    yellow_share_pct: float | None
    dominant_signal_status: str
    dominant_phase: str
    avg_configured_green_duration_sec: float | None
    avg_configured_red_duration_sec: float | None
    avg_configured_yellow_duration_sec: float | None
    latest_timing_mode: str
    ctx_avg_queue_length_m: float | None
    ctx_max_queue_length_m: float | None
    quality_status: str
    quality_flags: tuple[str, ...]
    analytical_freshness_status: str
    lineage: LineageMetadata
    source_rows: tuple[SilverSignalStateInput, ...]


def _mode(values: list[str]) -> str:
    counts = Counter(values)
    highest = max(counts.values())
    return min(value for value, count in counts.items() if count == highest)


def _avg(values: list[float | None]) -> float | None:
    present = [float(value) for value in values if value is not None]
    return fmean(present) if present else None


def aggregate_signal_windows(items: tuple[WindowedRecord, ...], traffic: tuple[TrafficAggregate, ...], context: GoldTransformationContext) -> tuple[SignalAggregate, ...]:
    traffic_map = {(item.window.window_id, item.intersection_id, item.direction): item for item in traffic}
    groups: dict[tuple, list[WindowedRecord]] = {}
    for item in items:
        row = item.record
        if isinstance(row, SilverSignalStateInput):
            key = (row.simulation_run_id, row.scenario_id, row.intersection_id, row.canonical_direction, item.window.window_size_sec, item.window.window_start_sim_sec)
            groups.setdefault(key, []).append(item)
    results: list[SignalAggregate] = []
    for key in sorted(groups):
        grouped = groups[key]
        rows = tuple(item.record for item in grouped)
        valid = tuple(row for row in rows if numeric_allowed(row.quality_status))
        if not valid:
            continue
        latest = select_latest(valid).record
        if latest is None:
            continue
        assert isinstance(latest, SilverSignalStateInput)
        counts = Counter(row.signal_status for row in valid)
        total = len(valid)
        coverage = pooled_coverage(total, context.expected(f"signal|{key}", len(rows)))
        quality = combine_quality(tuple(row.quality_status for row in rows) + (coverage.status,))
        traffic_context = traffic_map.get((grouped[0].window.window_id, key[2], key[3]))
        results.append(SignalAggregate(
            window=grouped[0].window, intersection_id=key[2], direction=key[3], source_direction=latest.source_direction,
            observation_count=total, green_observation_count=counts.get("GREEN", 0), red_observation_count=counts.get("RED", 0), yellow_observation_count=counts.get("YELLOW", 0), other_status_count=total-counts.get("GREEN", 0)-counts.get("RED", 0)-counts.get("YELLOW", 0),
            green_share_pct=100.0*counts.get("GREEN", 0)/total, red_share_pct=100.0*counts.get("RED", 0)/total, yellow_share_pct=100.0*counts.get("YELLOW", 0)/total,
            dominant_signal_status=_mode([row.signal_status for row in valid]), dominant_phase=_mode([row.current_phase for row in valid]),
            avg_configured_green_duration_sec=_avg([row.green_duration_sec for row in valid]), avg_configured_red_duration_sec=_avg([row.red_duration_sec for row in valid]), avg_configured_yellow_duration_sec=_avg([row.yellow_duration_sec for row in valid]), latest_timing_mode=latest.timing_mode,
            ctx_avg_queue_length_m=traffic_context.avg_queue_length_m if traffic_context else None, ctx_max_queue_length_m=traffic_context.max_queue_length_m if traffic_context else None,
            quality_status=quality, quality_flags=sorted_flags(*(row.quality_flags for row in rows)), analytical_freshness_status=analytical_freshness(closed=context.window_closed, revision=context.is_revision, age_sec=context.analytical_age_sec, stale_after_sec=context.stale_after_sec, quality_status=quality),
            lineage=build_lineage(rows, valid_records=valid), source_rows=valid,
        ))
    return tuple(results)
