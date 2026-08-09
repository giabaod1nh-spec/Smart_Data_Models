"""Bronze processor metrics."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class Metrics:
    state: str = "STARTING"
    raw_rows_read_total: int = 0
    bronze_rows_stored_total: int = 0
    bronze_quarantined_total: int = 0
    bronze_idempotent_skip_total: int = 0
    bronze_conflict_total: int = 0
    bronze_retry_total: int = 0
    bronze_end_of_data_total: int = 0
    bronze_gap_wait_total: int = 0
    bronze_physical_duplicate_total: int = 0
    batch_duration_ms: float = 0.0
    insert_latency_ms: float = 0.0
    rows_per_sec: float = 0.0
    checkpoint_offset: Dict[str, int] = field(default_factory=dict)
    source_lag_offsets: Dict[str, int] = field(default_factory=dict)
    last_successful_process_time: Optional[float] = None
    last_successful_checkpoint_time: Optional[float] = None
    fault_message: Optional[str] = None
    _batch_start: Optional[float] = None
    _rows_window: int = 0
    _window_start: float = field(default_factory=time.time)

    def begin_batch(self) -> None:
        self._batch_start = time.time()

    def end_batch(self, rows: int) -> None:
        if self._batch_start is not None:
            self.batch_duration_ms = (time.time() - self._batch_start) * 1000.0
        self._rows_window += rows
        elapsed = time.time() - self._window_start
        if elapsed >= 1.0:
            self.rows_per_sec = self._rows_window / elapsed
            self._rows_window = 0
            self._window_start = time.time()
        self.last_successful_process_time = time.time()

    def mark_checkpoint(self, topic: str, partition: int, offset: int) -> None:
        self.checkpoint_offset[f"{topic}:{partition}"] = offset
        self.last_successful_checkpoint_time = time.time()

    def snapshot(self) -> dict:
        return {
            "state": self.state,
            "raw_rows_read_total": self.raw_rows_read_total,
            "bronze_rows_stored_total": self.bronze_rows_stored_total,
            "bronze_quarantined_total": self.bronze_quarantined_total,
            "bronze_idempotent_skip_total": self.bronze_idempotent_skip_total,
            "bronze_end_of_data_total": self.bronze_end_of_data_total,
            "bronze_gap_wait_total": self.bronze_gap_wait_total,
            "checkpoint_offset": dict(self.checkpoint_offset),
            "source_lag_offsets": dict(self.source_lag_offsets),
            "last_successful_process_time": self.last_successful_process_time,
            "last_successful_checkpoint_time": self.last_successful_checkpoint_time,
            "batch_duration_ms": self.batch_duration_ms,
            "rows_per_sec": self.rows_per_sec,
            "fault_message": self.fault_message,
        }
