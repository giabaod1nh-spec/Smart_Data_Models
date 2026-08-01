"""K-4 Raw consumer metrics."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Metrics:
    records_stored: int = 0
    records_quarantined: int = 0
    batch_insert_latency_ms: float = 0.0
    commit_latency_ms: float = 0.0
    records_seen: int = 0
    _window_start: float = field(default_factory=time.monotonic)
    _window_records: int = 0
    records_per_sec: float = 0.0
    last_successful_commit_time: Optional[float] = None  # epoch seconds
    state: str = "STARTING"
    fault_message: Optional[str] = None
    partition_offsets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def note_record(self, n: int = 1) -> None:
        with self._lock:
            self.records_seen += n
            self._window_records += n
            elapsed = time.monotonic() - self._window_start
            if elapsed >= 1.0:
                self.records_per_sec = self._window_records / elapsed
                self._window_start = time.monotonic()
                self._window_records = 0

    def note_commit(self) -> None:
        with self._lock:
            self.last_successful_commit_time = time.time()

    def note_partition_offsets(self, rows: list[Dict[str, Any]]) -> None:
        with self._lock:
            self.partition_offsets = {
                f"{row['topic']}:{row['partition']}": dict(row) for row in rows
            }

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "records_stored": self.records_stored,
                "records_quarantined": self.records_quarantined,
                "batch_insert_latency_ms": self.batch_insert_latency_ms,
                "commit_latency_ms": self.commit_latency_ms,
                "records_seen": self.records_seen,
                "records_per_sec": self.records_per_sec,
                "last_successful_commit_time": self.last_successful_commit_time,
                "state": self.state,
                "fault_message": self.fault_message,
                "partition_offsets": [
                    dict(self.partition_offsets[key])
                    for key in sorted(self.partition_offsets)
                ],
            }
