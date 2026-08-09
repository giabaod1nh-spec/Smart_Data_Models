"""Process-local command registry (RC-2)."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID

from control.models import (
    ControlCommandStatus,
    DispatchStatus,
    ExecutionStatus,
    LifecycleStatus,
    ObservationStatus,
)

MAX_RECORDS = 10_000
TERMINAL_RETENTION_SEC = 900.0

TERMINAL_LIFECYCLE = {
    LifecycleStatus.COMPLETED,
    LifecycleStatus.FAILED,
    LifecycleStatus.EXPIRED,
    LifecycleStatus.UNKNOWN_OUTCOME,
}


@dataclass
class RegistryRecord:
    status: ControlCommandStatus
    terminal_at: Optional[float] = None


class CommandRegistry:
    """Thread-safe process-local command status store."""

    def __init__(
        self,
        *,
        max_records: int = MAX_RECORDS,
        terminal_retention_sec: float = TERMINAL_RETENTION_SEC,
    ):
        self._lock = threading.RLock()
        self._records: Dict[str, RegistryRecord] = {}
        self._max_records = max_records
        self._terminal_retention_sec = terminal_retention_sec

    def put(self, status: ControlCommandStatus) -> None:
        key = str(status.commandId)
        with self._lock:
            if key in self._records:
                return
            terminal_at = (
                time.monotonic()
                if status.lifecycleStatus in TERMINAL_LIFECYCLE
                else None
            )
            self._records[key] = RegistryRecord(status=status, terminal_at=terminal_at)
            self._evict_if_needed()

    def get(self, command_id: UUID) -> Optional[ControlCommandStatus]:
        with self._lock:
            rec = self._records.get(str(command_id))
            return rec.status if rec else None

    def update(self, status: ControlCommandStatus) -> None:
        key = str(status.commandId)
        with self._lock:
            rec = self._records.get(key)
            if rec is None:
                self.put(status)
                return
            rec.status = status
            if status.lifecycleStatus in TERMINAL_LIFECYCLE and rec.terminal_at is None:
                rec.terminal_at = time.monotonic()

    def contains(self, command_id: UUID) -> bool:
        with self._lock:
            return str(command_id) in self._records

    def _evict_if_needed(self) -> None:
        if len(self._records) <= self._max_records:
            return
        now = time.monotonic()
        terminals: List[tuple[float, str]] = []
        for cid, rec in self._records.items():
            if rec.status.lifecycleStatus not in TERMINAL_LIFECYCLE:
                continue
            ts = rec.terminal_at if rec.terminal_at is not None else now
            if now - ts >= self._terminal_retention_sec:
                terminals.append((ts, cid))
        terminals.sort(key=lambda x: x[0])
        for _, cid in terminals:
            if len(self._records) <= self._max_records:
                break
            self._records.pop(cid, None)

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(timezone.utc)
