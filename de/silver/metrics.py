"""Silver Plan 3 — in-process metrics + immutable health snapshot fields."""
from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class Metrics:
    batches_total: int = 0
    records_processed_total: int = 0
    quarantined_total: int = 0
    idempotent_observed_count: int = 0
    recovered_partial_count: int = 0
    uncertain_write_reconciliations: int = 0
    retries_total: int = 0
    fault_code: str = ""
    fault_message: str = ""
    last_checkpoint_at: str = ""
    last_batch_at: str = ""
    last_progress_at: str = ""
    source_lag: dict[str, int] = field(default_factory=dict)
    suppressed_dimension_candidates: int = 0
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def mark_batch(self, n: int) -> None:
        with self.lock:
            self.batches_total += 1
            self.records_processed_total += n
            self.last_batch_at = _utc()
            self.last_progress_at = self.last_batch_at

    def mark_checkpoint(self) -> None:
        with self.lock:
            self.last_checkpoint_at = _utc()
            self.last_progress_at = self.last_checkpoint_at

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            data = {
                "batches_total": self.batches_total,
                "records_processed_total": self.records_processed_total,
                "quarantined_total": self.quarantined_total,
                "idempotent_observed_count": self.idempotent_observed_count,
                "recovered_partial_count": self.recovered_partial_count,
                "uncertain_write_reconciliations": self.uncertain_write_reconciliations,
                "retries_total": self.retries_total,
                "fault_code": self.fault_code,
                "fault_message": self.fault_message,
                "last_checkpoint_at": self.last_checkpoint_at,
                "last_batch_at": self.last_batch_at,
                "last_progress_at": self.last_progress_at,
                "source_lag": dict(self.source_lag),
                "suppressed_dimension_candidates": self.suppressed_dimension_candidates,
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
    streams: tuple[dict[str, Any], ...] = ()
