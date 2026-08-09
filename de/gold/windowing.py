"""Gold 60/300 second window assignment."""
from __future__ import annotations

import math
from dataclasses import dataclass

from de.gold.contracts import WINDOW_SIZES_SEC, canonical_window_id
from de.gold.input_models import SilverGoldInput


@dataclass(frozen=True)
class WindowKey:
    simulation_run_id: str
    scenario_id: str
    window_size_sec: int
    window_start_sim_sec: float
    window_end_sim_sec: float
    window_id: str


@dataclass(frozen=True)
class WindowedRecord:
    window: WindowKey
    record: SilverGoldInput


def assign_window(record: SilverGoldInput, size: int) -> WindowKey:
    if size not in WINDOW_SIZES_SEC:
        raise ValueError(f"unsupported window size: {size}")
    value = float(record.simulation_time_sec)
    if not math.isfinite(value) or value < 0:
        raise ValueError("simulation_time_sec must be finite and non-negative")
    start = float(math.floor(value / size) * size)
    end = start + float(size)
    return WindowKey(
        record.simulation_run_id, record.scenario_id, size, start, end,
        canonical_window_id(record.simulation_run_id, record.scenario_id, size, start, end),
    )


def expand_windows(records: tuple[SilverGoldInput, ...]) -> tuple[WindowedRecord, ...]:
    expanded = [WindowedRecord(assign_window(record, size), record) for record in records for size in WINDOW_SIZES_SEC]
    return tuple(sorted(expanded, key=lambda item: (item.window.window_start_sim_sec, item.window.window_size_sec, item.record.intersection_id, item.record.source_partition, item.record.source_offset)))

