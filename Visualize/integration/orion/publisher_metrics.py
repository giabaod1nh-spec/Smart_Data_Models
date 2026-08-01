"""Thread-safe metrics snapshot for the async Orion publisher."""
from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


@dataclass
class MetricsSnapshot:
    cycle_sequence: int = 0
    simulation_time: float = 0.0
    queue_depth: int = 0
    effective_depth: int = 0
    queue_capacity: int = 0
    oldest_pending_age_ms: float = 0.0
    realtime_lag_sim_sec: float = 0.0
    enqueue_duration_ms: float = 0.0
    publish_cycle_duration_ms: float = 0.0
    entity_success_count: int = 0
    entity_failure_count: int = 0
    retry_count: int = 0
    backpressure_count: int = 0
    simulation_pause_duration_ms: float = 0.0
    last_enqueued_simulation_time: Optional[float] = None
    last_fully_published_simulation_time: Optional[float] = None
    lag_anchor_simulation_time: Optional[float] = None
    oldest_pending_simulation_time: Optional[float] = None
    publisher_state: str = "READY"
    worker_alive: bool = False
    in_flight_cycle_id: Optional[int] = None
    # sequential: >= 0 linear cursor; batch: -1 (no linear cursor — see publish_mode)
    in_flight_entity_index: int = 0
    entity_retry_count: int = 0
    shutdown_pending_count: int = 0
    flushed_cycles: int = 0
    remaining_cycles: int = 0
    publish_mode: str = "sequential"
    # Batch-mode fields (meaningful when publish_mode == "batch")
    batch_request_count: int = 0
    batch_request_full_success_count: int = 0
    batch_request_partial_count: int = 0
    batch_cycle_success_count: int = 0
    batch_entity_submitted_count: int = 0
    batch_entity_confirmed_success_count: int = 0
    batch_retry_count_total: int = 0
    batch_ambiguous_request_count: int = 0
    batch_remaining_entity_count: int = 0
    batch_attempt_number: int = 0
    batch_last_request_duration_ms: float = 0.0
    # K-4.5 cutover rehearsal — exact direct-Orion counters (not Orion inventory)
    legacy_orion_cycles_enqueued_total: int = 0
    legacy_orion_cycles_published_total: int = 0
    legacy_orion_entity_success_total: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PublisherMetrics:
    """Cross-thread metrics store; always expose via snapshot()."""

    def __init__(
        self, queue_capacity: int = 5, publish_mode: str = "sequential"
    ) -> None:
        self._lock = threading.Lock()
        self._data = MetricsSnapshot(
            queue_capacity=queue_capacity, publish_mode=publish_mode
        )

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(**asdict(self._data))

    def update(self, **kwargs: Any) -> None:
        with self._lock:
            for k, v in kwargs.items():
                if hasattr(self._data, k):
                    setattr(self._data, k, v)

    def incr(self, field_name: str, amount: int = 1) -> None:
        with self._lock:
            cur = getattr(self._data, field_name, 0)
            setattr(self._data, field_name, cur + amount)

    def add_pause_ms(self, ms: float) -> None:
        with self._lock:
            self._data.simulation_pause_duration_ms += ms

    def set_lag(
        self,
        *,
        current_simulation_time: float,
        last_fully_published: Optional[float],
        oldest_pending_sim_t: Optional[float],
        oldest_captured_at: Optional[float],
    ) -> float:
        """Compute and store realtime_lag; return lag seconds."""
        with self._lock:
            self._data.simulation_time = current_simulation_time
            self._data.last_fully_published_simulation_time = last_fully_published
            self._data.oldest_pending_simulation_time = oldest_pending_sim_t
            if last_fully_published is not None:
                anchor = last_fully_published
            else:
                anchor = oldest_pending_sim_t
            self._data.lag_anchor_simulation_time = anchor
            if anchor is None:
                lag = 0.0
            else:
                lag = max(0.0, current_simulation_time - anchor)
            self._data.realtime_lag_sim_sec = lag
            if oldest_captured_at is not None:
                self._data.oldest_pending_age_ms = max(
                    0.0, (time.monotonic() - oldest_captured_at) * 1000.0
                )
            else:
                self._data.oldest_pending_age_ms = 0.0
            return lag
