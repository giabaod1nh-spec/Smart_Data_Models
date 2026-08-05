"""In-process counters, latency percentiles and the immutable health snapshot.

Label sets are bounded and enumerated here: no run id, window id or source payload
is ever used as a metric label.
"""
from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from de.gold_runtime.config import LateClass

LATE_CLASS_LABELS: tuple[str, ...] = tuple(item.value for item in LateClass)
STREAM_LABELS: tuple[str, ...] = ("traffic", "intersection", "signal", "camera")
MAX_LATENCY_SAMPLES = 512


def utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
    return float(ordered[index])


@dataclass
class Metrics:
    batches_total: int = 0
    rows_read_total: int = 0
    windows_processed_total: int = 0
    facts_written_total: int = 0
    dimensions_written_total: int = 0
    quarantines_total: int = 0
    duplicates_total: int = 0
    retries_total: int = 0
    cas_conflicts_total: int = 0
    recovered_work_units_total: int = 0
    replay_batches_total: int = 0
    revisions_total: int = 0
    uncertain_write_reconciliations: int = 0
    readiness_transitions: int = 0
    idempotent_windows_total: int = 0
    fault_code: str = ""
    fault_message: str = ""
    last_batch_at: str = ""
    last_checkpoint_at: str = ""
    last_progress_at: str = ""
    last_batch_id: str = ""
    last_window_id: str = ""
    watermark: Optional[float] = None
    watermark_age_sec: float = 0.0
    late_rows: dict[str, int] = field(
        default_factory=lambda: {label: 0 for label in LATE_CLASS_LABELS}
    )
    source_lag: dict[str, float] = field(
        default_factory=lambda: {label: 0.0 for label in STREAM_LABELS}
    )
    ledger_recovery_counts: dict[str, int] = field(default_factory=dict)
    _latency_samples: list[float] = field(default_factory=list, repr=False)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def mark_batch(self, rows: int, *, batch_id: str = "", window_id: str = "") -> None:
        with self.lock:
            self.batches_total += 1
            self.rows_read_total += int(rows)
            self.last_batch_at = utc_str()
            self.last_progress_at = self.last_batch_at
            if batch_id:
                self.last_batch_id = batch_id
            if window_id:
                self.last_window_id = window_id

    def mark_checkpoint(self) -> None:
        with self.lock:
            self.last_checkpoint_at = utc_str()
            self.last_progress_at = self.last_checkpoint_at

    def mark_window(self, latency_sec: float) -> None:
        with self.lock:
            self.windows_processed_total += 1
            self._latency_samples.append(max(0.0, float(latency_sec)))
            if len(self._latency_samples) > MAX_LATENCY_SAMPLES:
                del self._latency_samples[: len(self._latency_samples) - MAX_LATENCY_SAMPLES]

    def mark_late(self, late_class: LateClass, count: int = 1) -> None:
        with self.lock:
            self.late_rows[late_class.value] = self.late_rows.get(late_class.value, 0) + count

    def set_lag(self, lag: dict[str, float]) -> None:
        with self.lock:
            for label in STREAM_LABELS:
                self.source_lag[label] = float(lag.get(label, 0.0))

    def set_fault(self, code: str, message: str) -> None:
        with self.lock:
            self.fault_code = code
            self.fault_message = message

    def clear_fault(self) -> None:
        with self.lock:
            self.fault_code = ""
            self.fault_message = ""

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self._latency_samples)
            data = {
                "batches_total": self.batches_total,
                "rows_read_total": self.rows_read_total,
                "windows_processed_total": self.windows_processed_total,
                "facts_written_total": self.facts_written_total,
                "dimensions_written_total": self.dimensions_written_total,
                "quarantines_total": self.quarantines_total,
                "duplicates_total": self.duplicates_total,
                "retries_total": self.retries_total,
                "cas_conflicts_total": self.cas_conflicts_total,
                "recovered_work_units_total": self.recovered_work_units_total,
                "replay_batches_total": self.replay_batches_total,
                "revisions_total": self.revisions_total,
                "uncertain_write_reconciliations": self.uncertain_write_reconciliations,
                "readiness_transitions": self.readiness_transitions,
                "idempotent_windows_total": self.idempotent_windows_total,
                "late_rows": dict(self.late_rows),
                "source_lag": dict(self.source_lag),
                "ledger_recovery_counts": dict(self.ledger_recovery_counts),
                "watermark": self.watermark,
                "watermark_age_sec": self.watermark_age_sec,
                "processing_latency_p50_sec": _percentile(samples, 0.50),
                "processing_latency_p95_sec": _percentile(samples, 0.95),
                "processing_latency_max_sec": max(samples) if samples else 0.0,
                "last_batch_at": self.last_batch_at,
                "last_checkpoint_at": self.last_checkpoint_at,
                "last_progress_at": self.last_progress_at,
                "last_batch_id": self.last_batch_id,
                "last_window_id": self.last_window_id,
                "fault_code": self.fault_code,
                "fault_message": self.fault_message,
            }
            return deepcopy(data)


@dataclass(frozen=True)
class HealthSnapshot:
    state: str
    ready: bool
    worker_alive: bool
    reader_initialized: bool
    clickhouse_ok: bool
    sqlite_ok: bool
    schema_ok: bool
    lock_held: bool
    namespace: str
    mode: str
    shutdown_requested: bool
    snapshot_at: str
    metrics: dict[str, Any]
    fault_code: str = ""
    fault_message: str = ""
    last_batch_id: str = ""
    last_window_id: str = ""
    last_checkpoint_at: str = ""
    watermark: Optional[float] = None
    non_terminal_work_units: int = 0
    reason: str = ""
