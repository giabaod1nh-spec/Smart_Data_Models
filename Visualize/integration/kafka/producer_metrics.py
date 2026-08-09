"""Kafka producer metrics — invariant: created = acked + failed + pending."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ProducerMetricsSnapshot:
    events_created_total: int
    events_enqueued_total: int
    events_acked_total: int
    events_failed_total: int
    events_rejected_total: int
    events_pending: int
    produce_enqueue_duration_ms: float
    ack_latency_ms: float
    last_acked_cycle_sequence: Optional[int]
    producer_state: str
    fanout_total_duration_ms: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "events_created_total": self.events_created_total,
            "events_enqueued_total": self.events_enqueued_total,
            "events_acked_total": self.events_acked_total,
            "events_failed_total": self.events_failed_total,
            "events_rejected_total": self.events_rejected_total,
            "events_pending": self.events_pending,
            "produce_enqueue_duration_ms": self.produce_enqueue_duration_ms,
            "ack_latency_ms": self.ack_latency_ms,
            "last_acked_cycle_sequence": self.last_acked_cycle_sequence,
            "producer_state": self.producer_state,
            "fanout_total_duration_ms": self.fanout_total_duration_ms,
        }


class ProducerMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events_created_total = 0
        self.events_enqueued_total = 0
        self.events_acked_total = 0
        self.events_failed_total = 0
        self.events_rejected_total = 0
        self.produce_enqueue_duration_ms = 0.0
        self.ack_latency_ms = 0.0
        self.last_acked_cycle_sequence: Optional[int] = None
        self.fanout_total_duration_ms = 0.0
        self._pending_event_ids: set[str] = set()

    def mark_created(self, event_id: str) -> None:
        with self._lock:
            self.events_created_total += 1
            self._pending_event_ids.add(event_id)

    def mark_enqueued(self, duration_ms: float = 0.0) -> None:
        with self._lock:
            self.events_enqueued_total += 1
            if duration_ms:
                self.produce_enqueue_duration_ms = duration_ms

    def mark_acked(self, event_id: str, *, cycle_sequence: int, latency_ms: float) -> None:
        with self._lock:
            self._pending_event_ids.discard(event_id)
            self.events_acked_total += 1
            self.ack_latency_ms = latency_ms
            self.last_acked_cycle_sequence = cycle_sequence

    def mark_failed(self, event_id: str) -> None:
        with self._lock:
            self._pending_event_ids.discard(event_id)
            self.events_failed_total += 1

    def mark_rejected(self, event_id: Optional[str] = None) -> None:
        with self._lock:
            self.events_rejected_total += 1
            if event_id:
                self._pending_event_ids.discard(event_id)
                self.events_failed_total += 1

    def set_fanout_ms(self, ms: float) -> None:
        with self._lock:
            self.fanout_total_duration_ms = ms

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_event_ids)

    def pending_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._pending_event_ids)

    def snapshot(self, *, producer_state: str) -> ProducerMetricsSnapshot:
        with self._lock:
            pending = len(self._pending_event_ids)
            return ProducerMetricsSnapshot(
                events_created_total=self.events_created_total,
                events_enqueued_total=self.events_enqueued_total,
                events_acked_total=self.events_acked_total,
                events_failed_total=self.events_failed_total,
                events_rejected_total=self.events_rejected_total,
                events_pending=pending,
                produce_enqueue_duration_ms=self.produce_enqueue_duration_ms,
                ack_latency_ms=self.ack_latency_ms,
                last_acked_cycle_sequence=self.last_acked_cycle_sequence,
                producer_state=producer_state,
                fanout_total_duration_ms=self.fanout_total_duration_ms,
            )

    def invariant_ok(self) -> bool:
        snap = self.snapshot(producer_state="")
        return snap.events_created_total == (
            snap.events_acked_total + snap.events_failed_total + snap.events_pending
        )
