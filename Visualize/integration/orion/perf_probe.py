"""Temporary performance probe for Orion publish audit (ORION_PERF_AUDIT=1)."""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator

log = logging.getLogger("orion_perf")


def enabled() -> bool:
    return os.getenv("ORION_PERF_AUDIT", "").lower() in ("1", "true", "yes")


@dataclass
class CycleStats:
    sim_time: float = 0.0
    wall_start: float = 0.0
    node_count: int = 0
    entity_count: int = 0
    request_count: int = 0
    patch_count: int = 0
    post_count: int = 0
    post_append_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    node_durations_ms: dict[str, float] = field(default_factory=dict)


_current_cycle: CycleStats | None = None


def start_cycle(sim_time: float, node_count: int) -> CycleStats:
    global _current_cycle
    _current_cycle = CycleStats(
        sim_time=sim_time,
        wall_start=time.perf_counter(),
        node_count=node_count,
    )
    log.info(
        "publish_cycle_start sim_time=%.3f node_count=%d entity_count=0",
        sim_time,
        node_count,
    )
    return _current_cycle


def end_cycle() -> None:
    global _current_cycle
    if _current_cycle is None:
        return
    total_ms = (time.perf_counter() - _current_cycle.wall_start) * 1000.0
    log.info(
        "publish_cycle_end sim_time=%.3f total_duration_ms=%.2f request_count=%d "
        "patch_count=%d post_count=%d post_append_count=%d success_count=%d failure_count=%d entity_count=%d",
        _current_cycle.sim_time,
        total_ms,
        _current_cycle.request_count,
        _current_cycle.patch_count,
        _current_cycle.post_count,
        _current_cycle.post_append_count,
        _current_cycle.success_count,
        _current_cycle.failure_count,
        _current_cycle.entity_count,
    )
    _current_cycle = None


def record_entity(entity_id: str, method: str, status: int, duration_ms: float, ok: bool) -> None:
    if _current_cycle is None:
        return
    _current_cycle.entity_count += 1
    _current_cycle.request_count += 1
    m = method.upper()
    if m == "PATCH":
        _current_cycle.patch_count += 1
    elif m == "POST":
        _current_cycle.post_count += 1
    elif m == "POST_APPEND":
        _current_cycle.post_append_count += 1
    if ok:
        _current_cycle.success_count += 1
    else:
        _current_cycle.failure_count += 1
    log.info(
        "entity_publish entity_id=%s method=%s status=%s duration_ms=%.2f",
        entity_id,
        method,
        status,
        duration_ms,
    )


def record_node_duration(node: str, duration_ms: float) -> None:
    if _current_cycle is not None:
        _current_cycle.node_durations_ms[node] = duration_ms


@contextmanager
def timed(label: str) -> Iterator[None]:
    t0 = time.perf_counter()
    yield
    if enabled():
        log.info("timing label=%s duration_ms=%.2f", label, (time.perf_counter() - t0) * 1000.0)
