"""Command execution lifecycle tracking on simulation thread (RC-3)."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from control.command_registry import CommandRegistry
from control.models import (
    CommandType,
    ControlCommandStatus,
    ControlErrorBody,
    ControlStatusLinks,
    DispatchStatus,
    ExecutionStatus,
    LifecycleStatus,
    ObservationStatus,
)
from runtime.command_queue import Command

log = logging.getLogger(__name__)

TRANSITION_TIMEOUT_SEC = 30.0


@dataclass
class ActiveExecution:
    command_id: UUID
    command_type: str
    node_id: Optional[str] = None
    target_phase: Optional[str] = None
    target_seconds: Optional[int] = None
    scenario_id: Optional[str] = None
    started_sim_t: float = 0.0
    transition_deadline_sim_t: Optional[float] = None
    result_payload: Dict[str, Any] = field(default_factory=dict)


class CommandExecutionTracker:
    """Tracks in-flight commands until APPLIED_AT_SUMO or FAILED_AT_RUNTIME."""

    def __init__(self, registry: CommandRegistry):
        self.registry = registry
        self._active: Dict[str, ActiveExecution] = {}

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _update_status(self, command_id: UUID, **fields) -> None:
        current = self.registry.get(command_id)
        if current is None:
            return
        self.registry.update(current.model_copy(update={**fields, "updatedAt": self._utc_now()}))

    def on_command_start(self, cmd: Command, engine) -> None:
        if not cmd.command_id:
            return
        try:
            cid = UUID(cmd.command_id)
        except ValueError:
            return
        if self.registry.get(cid) is None:
            now = self._utc_now()
            self.registry.put(
                ControlCommandStatus(
                    commandId=cid,
                    lifecycleStatus=LifecycleStatus.QUEUED,
                    dispatchStatus=DispatchStatus.ACCEPTED,
                    executionStatus=ExecutionStatus.QUEUED,
                    observationStatus=ObservationStatus.NOT_REQUESTED,
                    expectedRunId=str(getattr(engine, "simulation_run_id", "")),
                    acceptedRunId=str(getattr(engine, "simulation_run_id", "")),
                    createdAt=now,
                    updatedAt=now,
                    error=None,
                    result=None,
                    links=ControlStatusLinks(self=f"/commands/{cid}"),
                )
            )
        sim_t = float(getattr(engine, "simulation_time_sec", 0.0) or 0.0)
        self._update_status(
            cid,
            lifecycleStatus=LifecycleStatus.APPLYING,
            executionStatus=ExecutionStatus.EXECUTING,
        )
        active = ActiveExecution(
            command_id=cid,
            command_type=cmd.name,
            started_sim_t=sim_t,
        )
        if cmd.name == "force_phase":
            active.node_id = cmd.kwargs.get("node_id")
            active.target_phase = cmd.kwargs.get("phase")
        elif cmd.name == "set_green_duration":
            active.node_id = cmd.kwargs.get("node_id")
            active.target_seconds = int(cmd.kwargs.get("seconds", 0))
        elif cmd.name == "set_scenario":
            active.scenario_id = cmd.kwargs.get("scenario")
            active.node_id = cmd.kwargs.get("target_intersection")
        elif cmd.name == "set_demand_profile":
            active.scenario_id = cmd.kwargs.get("profile")
        elif cmd.name == "add_overlay":
            active.node_id = cmd.kwargs.get("intersection_id")
        elif cmd.name == "set_control_mode":
            active.scenario_id = cmd.kwargs.get("mode")
        self._active[str(cid)] = active

    def on_command_success(self, cmd: Command, engine, result: Any) -> None:
        if not cmd.command_id:
            return
        try:
            cid = UUID(cmd.command_id)
        except ValueError:
            return
        key = str(cid)
        active = self._active.get(key)
        sim_t = float(getattr(engine, "simulation_time_sec", 0.0) or 0.0)

        if cmd.name == "force_phase" and active:
            sig = engine.signals.get(active.node_id or "")
            pending = getattr(sig, "_pending_target", None) if sig else None
            if pending:
                active.transition_deadline_sim_t = sim_t + TRANSITION_TIMEOUT_SEC
                self._update_status(
                    cid,
                    executionStatus=ExecutionStatus.TRANSITIONING,
                    result={"targetPhase": active.target_phase, "pending": True},
                )
                return
            self._complete_applied(cid, active, engine, result)
            return

        if cmd.name == "set_scenario" and isinstance(result, dict):
            if active:
                active.result_payload = result
            self._complete_applied(cid, active, engine, result)
            return

        if cmd.name == "set_green_duration" and active:
            sig = engine.signals.get(active.node_id or "")
            current = sig.current_phase_name(engine._traci) if sig and engine._traci else ""
            payload = {
                "seconds": active.target_seconds,
                "currentPhase": current,
                "programWideGreen": True,
            }
            self._complete_applied(cid, active, engine, payload)
            return

        if cmd.name == "set_demand_profile" and isinstance(result, dict):
            self._complete_applied(cid, active, engine, {"profile": result.get("profile_id") or active.scenario_id, **result})
            return

        if cmd.name == "add_overlay" and isinstance(result, dict):
            self._complete_applied(cid, active, engine, result)
            return

        if cmd.name == "remove_overlay":
            overlay_id = cmd.kwargs.get("overlay_id")
            self._complete_applied(cid, active, engine, {"overlayId": overlay_id, "removed": bool(result)})
            return

        if cmd.name == "set_control_mode":
            mode = cmd.kwargs.get("mode")
            self._complete_applied(cid, active, engine, {"mode": mode})
            return

        self._complete_applied(cid, active, engine, result if isinstance(result, dict) else {})

    def on_command_error(self, cmd: Command, error: BaseException) -> None:
        if not cmd.command_id:
            return
        try:
            cid = UUID(cmd.command_id)
        except ValueError:
            return
        code = "TRACI_OPERATION_FAILED"
        if isinstance(error, ValueError):
            code = "INVALID_COMMAND"
        err = ControlErrorBody(code=code, message=str(error)[:512])
        self._update_status(
            cid,
            lifecycleStatus=LifecycleStatus.FAILED,
            dispatchStatus=DispatchStatus.ACCEPTED,
            executionStatus=ExecutionStatus.FAILED_AT_RUNTIME,
            error=err,
        )
        self._active.pop(str(cid), None)

    def tick(self, engine) -> None:
        if not self._active:
            return
        sim_t = float(getattr(engine, "simulation_time_sec", 0.0) or 0.0)
        traci = getattr(engine, "_traci", None)
        done_keys = []
        for key, active in list(self._active.items()):
            if active.command_type != "force_phase" or not active.target_phase:
                continue
            sig = engine.signals.get(active.node_id or "")
            if sig is None or traci is None:
                continue
            current = sig.current_phase_name(traci)
            pending = getattr(sig, "_pending_target", None)
            if active.transition_deadline_sim_t is not None and sim_t > active.transition_deadline_sim_t:
                self._fail_transition(active.command_id, "TRANSITION_TIMEOUT")
                done_keys.append(key)
                continue
            if pending:
                continue
            if current == active.target_phase:
                self._complete_applied(
                    active.command_id,
                    active,
                    engine,
                    {"targetPhase": active.target_phase, "appliedSimulationTime": sim_t},
                )
                done_keys.append(key)
        for k in done_keys:
            self._active.pop(k, None)

    def _complete_applied(
        self,
        command_id: UUID,
        active: Optional[ActiveExecution],
        engine,
        result: Any,
    ) -> None:
        sim_t = float(getattr(engine, "simulation_time_sec", 0.0) or 0.0)
        run_id = getattr(engine, "simulation_run_id", None)
        payload = dict(result) if isinstance(result, dict) else {}
        payload.setdefault("appliedSimulationTime", sim_t)
        if run_id:
            payload.setdefault("acceptedRunId", str(run_id))
        payload.setdefault("cycleSequence", getattr(engine, "_observation_seq", 0))
        self._update_status(
            command_id,
            lifecycleStatus=LifecycleStatus.COMPLETED,
            dispatchStatus=DispatchStatus.ACCEPTED,
            executionStatus=ExecutionStatus.APPLIED_AT_SUMO,
            acceptedRunId=str(run_id) if run_id else None,
            result=payload,
            error=None,
        )
        self._active.pop(str(command_id), None)

    def _fail_transition(self, command_id: UUID, code: str) -> None:
        err = ControlErrorBody(code=code, message="phase transition timed out")
        self._update_status(
            command_id,
            lifecycleStatus=LifecycleStatus.FAILED,
            executionStatus=ExecutionStatus.FAILED_AT_RUNTIME,
            error=err,
        )


def drain_with_tracking(
    queue,
    handlers: Dict[str, Any],
    registry: CommandRegistry,
    engine,
    max_n: int = 50,
) -> int:
    tracker = getattr(engine, "_command_tracker", None)
    if tracker is None:
        tracker = CommandExecutionTracker(registry)
        engine._command_tracker = tracker

    return queue.drain(
        handlers,
        max_n=max_n,
        on_start=lambda cmd: tracker.on_command_start(cmd, engine),
        on_success=lambda cmd, result: tracker.on_command_success(cmd, engine, result),
        on_error=lambda cmd, err: tracker.on_command_error(cmd, err),
    )
