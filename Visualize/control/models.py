"""Control command v1 Pydantic models — contract parity."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CommandType(str, Enum):
    FORCE_PHASE = "FORCE_PHASE"
    SET_GREEN_DURATION = "SET_GREEN_DURATION"
    SET_SCENARIO = "SET_SCENARIO"
    SET_DEMAND_PROFILE = "SET_DEMAND_PROFILE"
    ADD_OVERLAY = "ADD_OVERLAY"
    REMOVE_OVERLAY = "REMOVE_OVERLAY"
    SET_CONTROL_MODE = "SET_CONTROL_MODE"
    EMERGENCY_PREEMPTION = "EMERGENCY_PREEMPTION"


class LifecycleStatus(str, Enum):
    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    QUEUED = "QUEUED"
    APPLYING = "APPLYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


class DispatchStatus(str, Enum):
    PENDING = "PENDING"
    DISPATCHING = "DISPATCHING"
    ACCEPTED = "ACCEPTED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class ExecutionStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    QUEUED = "QUEUED"
    EXECUTING = "EXECUTING"
    TRANSITIONING = "TRANSITIONING"
    APPLIED_AT_SUMO = "APPLIED_AT_SUMO"
    FAILED_AT_RUNTIME = "FAILED_AT_RUNTIME"


class ObservationStatus(str, Enum):
    NOT_REQUESTED = "NOT_REQUESTED"
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    MISMATCH = "MISMATCH"
    TIMED_OUT = "TIMED_OUT"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_OBSERVABLE = "NOT_OBSERVABLE"


class ControlTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intersectionId: Optional[str] = Field(default=None, max_length=64)
    direction: Optional[str] = Field(default=None, max_length=32)
    overlayId: Optional[str] = Field(default=None, max_length=128)


class ControlCommandEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contractVersion: Literal["1.0"] = "1.0"
    commandId: UUID
    commandType: CommandType
    target: ControlTarget
    payload: Dict[str, Any]
    expectedRunId: str = Field(min_length=1, max_length=128)
    idempotencyKey: str = Field(pattern=r"^[A-Za-z0-9._:-]+$", min_length=1, max_length=128)
    requestedAt: datetime
    expiresAt: datetime
    source: Literal["DASHBOARD"] = "DASHBOARD"


class ControlErrorBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str = Field(max_length=512)


class ControlStatusLinks(BaseModel):
    model_config = ConfigDict(extra="forbid")

    self: str


class ControlCommandStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    commandId: UUID
    lifecycleStatus: LifecycleStatus
    dispatchStatus: DispatchStatus
    executionStatus: ExecutionStatus
    observationStatus: ObservationStatus
    expectedRunId: str = Field(min_length=1)
    acceptedRunId: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    error: Optional[ControlErrorBody] = None
    result: Optional[Dict[str, Any]] = None
    links: ControlStatusLinks
