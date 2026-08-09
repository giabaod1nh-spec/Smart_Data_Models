"""Authoritative simulation runtime state (RC3-T1)."""
from __future__ import annotations

from enum import Enum


class SimulationRuntimeState(str, Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


def runtime_state_of(engine) -> SimulationRuntimeState:
    if engine is None:
        return SimulationRuntimeState.STOPPED
    state = getattr(engine, "_runtime_state", None)
    if isinstance(state, SimulationRuntimeState):
        return state
    if getattr(engine, "_started", False) and getattr(engine, "_traci", None) is not None:
        return SimulationRuntimeState.RUNNING
    return SimulationRuntimeState.STOPPED


def mutation_allowed(engine) -> tuple[bool, str | None]:
    """Return (allowed, active_run_id)."""
    if engine is None:
        return False, None
    state = runtime_state_of(engine)
    if state != SimulationRuntimeState.RUNNING:
        return False, None
    if not getattr(engine, "_started", False) or getattr(engine, "_traci", None) is None:
        return False, None
    run_id = getattr(engine, "simulation_run_id", None)
    if not run_id:
        return False, None
    return True, str(run_id)
