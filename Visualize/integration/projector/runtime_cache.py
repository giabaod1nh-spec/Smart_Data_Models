"""Read-only runtime cache for GET /current-run (RT-D)."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class RuntimeStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CATCH_UP = "CATCH_UP"
    DEGRADED = "DEGRADED"
    RECOVERING = "RECOVERING"
    FAULTED = "FAULTED"


@dataclass
class RuntimeSnapshot:
    simulation_run_id: Optional[str]
    scenario_id: Optional[str]
    simulation_time: float
    status: str
    last_applied_cycle: int
    freshness_seconds: Optional[float]
    updated_at: str

    def to_response(self) -> Dict[str, Any]:
        return {
            "simulationRunId": self.simulation_run_id,
            "scenarioId": self.scenario_id,
            "simulationTime": self.simulation_time,
            "status": self.status,
            "lastAppliedCycle": self.last_applied_cycle,
            "freshnessSeconds": self.freshness_seconds,
            "updatedAt": self.updated_at,
        }


class RuntimeCache:
    """Updated only after Orion success → SQLite commit → offset completion."""

    def __init__(self) -> None:
        self._snapshot: Optional[RuntimeSnapshot] = None
        self._faulted = False
        self._recovering = False

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def mark_recovering(self) -> None:
        self._recovering = True

    def mark_faulted(self) -> None:
        self._faulted = True

    def clear_fault(self) -> None:
        self._faulted = False
        self._recovering = False

    def update_after_apply(
        self,
        *,
        simulation_run_id: str,
        scenario_id: Optional[str],
        simulation_time: float,
        last_applied_cycle: int,
        freshness_seconds: Optional[float],
        status: RuntimeStatus,
    ) -> None:
        self._snapshot = RuntimeSnapshot(
            simulation_run_id=simulation_run_id,
            scenario_id=scenario_id,
            simulation_time=simulation_time,
            status=status.value,
            last_applied_cycle=last_applied_cycle,
            freshness_seconds=freshness_seconds,
            updated_at=self._utc_now(),
        )
        self._recovering = False
        self._faulted = False

    def rebuild_from_store(self, store, *, source: str, producer_id: str) -> None:
        active = store.get_active_run(source=source, producer_id=producer_id)
        runtime = store.get_runtime_state()
        if active is None:
            self._snapshot = None
            self._recovering = False
            return
        self._snapshot = RuntimeSnapshot(
            simulation_run_id=str(active["simulation_run_id"]),
            scenario_id=runtime.get("scenario_id") if runtime else None,
            simulation_time=float(runtime.get("simulation_time") or 0) if runtime else 0.0,
            status=str(runtime.get("status") or RuntimeStatus.ACTIVE.value) if runtime else RuntimeStatus.ACTIVE.value,
            last_applied_cycle=int(runtime.get("last_applied_cycle") or 0) if runtime else 0,
            freshness_seconds=runtime.get("freshness_seconds") if runtime else None,
            updated_at=str(runtime.get("updated_at") or self._utc_now()) if runtime else self._utc_now(),
        )
        self._recovering = False

    def http_status(self) -> tuple[int, Optional[Dict[str, Any]]]:
        if self._faulted:
            return 503, None
        if self._recovering:
            return 503, None
        if self._snapshot is None or not self._snapshot.simulation_run_id:
            return 204, None
        return 200, self._snapshot.to_response()
