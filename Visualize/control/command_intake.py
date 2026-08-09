"""Canonical command intake — envelope validation and legacy handler mapping."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple
from uuid import UUID

import configuration.config as cfg
from control.command_registry import CommandRegistry
from control.models import (
    CommandType,
    ControlCommandEnvelope,
    ControlCommandStatus,
    ControlErrorBody,
    ControlStatusLinks,
    DispatchStatus,
    ExecutionStatus,
    LifecycleStatus,
    ObservationStatus,
)
from control.runtime_state import mutation_allowed
from runtime.command_queue import CommandQueue, QueueFullError


class CommandIntakeError(Exception):
    def __init__(self, *, http_status: int, code: str, message: str):
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _initial_status(env: ControlCommandEnvelope) -> ControlCommandStatus:
    now = _utc_now()
    return ControlCommandStatus(
        commandId=env.commandId,
        lifecycleStatus=LifecycleStatus.RECEIVED,
        dispatchStatus=DispatchStatus.PENDING,
        executionStatus=ExecutionStatus.NOT_STARTED,
        observationStatus=ObservationStatus.NOT_REQUESTED,
        expectedRunId=env.expectedRunId,
        acceptedRunId=None,
        createdAt=now,
        updatedAt=now,
        error=None,
        result=None,
        links=ControlStatusLinks(self=f"/commands/{env.commandId}"),
    )


def _runtime_running(engine) -> Tuple[bool, Optional[str]]:
    if engine is None or not getattr(engine, "_started", False):
        return False, None
    run_id = getattr(engine, "simulation_run_id", None)
    if not run_id:
        return False, None
    return True, str(run_id)


def _validate_runtime(engine, env: ControlCommandEnvelope) -> None:
    allowed, active_run = mutation_allowed(engine)
    if not allowed:
        raise CommandIntakeError(
            http_status=503,
            code="SIMULATION_NOT_RUNNING",
            message="simulation not running",
        )
    now = _utc_now()
    expires = env.expiresAt.replace(tzinfo=timezone.utc) if env.expiresAt.tzinfo is None else env.expiresAt
    if expires < now:
        raise CommandIntakeError(http_status=409, code="COMMAND_EXPIRED", message="command expired")
    if active_run != env.expectedRunId:
        raise CommandIntakeError(http_status=409, code="STALE_RUN", message="expectedRunId mismatch")
    if env.commandType in (CommandType.FORCE_PHASE, CommandType.SET_GREEN_DURATION):
        iid = env.target.intersectionId
        if iid and hasattr(engine, "signals"):
            sig = engine.signals.get(iid)
            if sig and getattr(sig, "preemption_active", False):
                raise CommandIntakeError(
                    http_status=409,
                    code="PREEMPTION_ACTIVE",
                    message="preemption active",
                )


def _legacy_enqueue_name(env: ControlCommandEnvelope) -> Tuple[str, dict]:
    t = env.commandType
    target = env.target
    payload = env.payload
    if t == CommandType.FORCE_PHASE:
        phase = payload.get("phase")
        if phase not in cfg.PHASE_SEQUENCE:
            raise CommandIntakeError(http_status=400, code="INVALID_PHASE", message="invalid phase")
        iid = target.intersectionId
        if not iid or iid not in cfg.NODE_TO_TLS:
            raise CommandIntakeError(http_status=400, code="INVALID_TARGET", message="invalid intersection")
        return "force_phase", {"node_id": iid, "phase": phase}
    if t == CommandType.SET_GREEN_DURATION:
        seconds = payload.get("seconds")
        iid = target.intersectionId
        if not iid or iid not in cfg.NODE_TO_TLS:
            raise CommandIntakeError(http_status=400, code="INVALID_TARGET", message="invalid intersection")
        if not isinstance(seconds, int) or seconds < 10 or seconds > 120:
            raise CommandIntakeError(http_status=400, code="INVALID_DURATION", message="invalid duration")
        return "set_green_duration", {"node_id": iid, "seconds": seconds}
    if t == CommandType.SET_SCENARIO:
        scenario = payload.get("scenario")
        if scenario not in cfg.SCENARIO_IDS:
            raise CommandIntakeError(http_status=400, code="INVALID_SCENARIO", message="invalid scenario")
        return "set_scenario", {
            "scenario": scenario,
            "target_intersection": target.intersectionId,
            "target_direction": target.direction,
        }
    if t == CommandType.SET_DEMAND_PROFILE:
        return "set_demand_profile", {"profile": payload.get("profile")}
    if t == CommandType.ADD_OVERLAY:
        return "add_overlay", {
            "overlay_type": payload.get("overlayType"),
            "intersection_id": target.intersectionId,
            "direction": payload.get("direction") or target.direction,
        }
    if t == CommandType.REMOVE_OVERLAY:
        return "remove_overlay", {"overlay_id": payload.get("overlayId")}
    if t == CommandType.SET_CONTROL_MODE:
        return "set_control_mode", {"mode": payload.get("mode")}
    if t == CommandType.EMERGENCY_PREEMPTION:
        return "set_control_mode", {"mode": "PREEMPTION_ENABLED"}
    raise CommandIntakeError(http_status=400, code="INVALID_COMMAND", message="unsupported command")


def accept_command(
    *,
    engine,
    registry: CommandRegistry,
    command_queue: CommandQueue,
    envelope: ControlCommandEnvelope,
) -> ControlCommandStatus:
    if registry.contains(envelope.commandId):
        existing = registry.get(envelope.commandId)
        if existing is not None:
            return existing

    status = _initial_status(envelope)
    registry.put(status)

    try:
        _validate_runtime(engine, envelope)
        name, kwargs = _legacy_enqueue_name(envelope)
        running, active_run = _runtime_running(engine)
        status = status.model_copy(
            update={
                "lifecycleStatus": LifecycleStatus.VALIDATED,
                "acceptedRunId": active_run,
                "updatedAt": _utc_now(),
            }
        )
        registry.update(status)
        command_queue.enqueue(name, command_id=str(envelope.commandId), **kwargs)
        status = status.model_copy(
            update={
                "lifecycleStatus": LifecycleStatus.QUEUED,
                "dispatchStatus": DispatchStatus.ACCEPTED,
                "executionStatus": ExecutionStatus.QUEUED,
                "updatedAt": _utc_now(),
            }
        )
        registry.update(status)
        return status
    except QueueFullError as e:
        err = ControlErrorBody(code="QUEUE_FULL", message=str(e))
        status = status.model_copy(
            update={
                "lifecycleStatus": LifecycleStatus.FAILED,
                "dispatchStatus": DispatchStatus.FAILED,
                "executionStatus": ExecutionStatus.NOT_STARTED,
                "error": err,
                "updatedAt": _utc_now(),
            }
        )
        registry.update(status)
        raise CommandIntakeError(http_status=503, code="QUEUE_FULL", message=str(e)) from e
    except CommandIntakeError:
        raise
    except Exception as e:
        err = ControlErrorBody(code="INVALID_COMMAND", message=str(e)[:512])
        status = status.model_copy(
            update={
                "lifecycleStatus": LifecycleStatus.FAILED,
                "dispatchStatus": DispatchStatus.FAILED,
                "error": err,
                "updatedAt": _utc_now(),
            }
        )
        registry.update(status)
        raise CommandIntakeError(http_status=400, code="INVALID_COMMAND", message=str(e)) from e
