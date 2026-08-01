"""Bronze domain models."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ResolveKind(str, Enum):
    RAW_VALID = "RAW_VALID"
    RAW_QUARANTINE_SKIPPED = "RAW_QUARANTINE_SKIPPED"
    END_OF_AVAILABLE_DATA = "END_OF_AVAILABLE_DATA"
    OFFSET_GAP_WAIT = "OFFSET_GAP_WAIT"
    OFFSET_GAP_FAULT = "OFFSET_GAP_FAULT"


@dataclass
class RawRow:
    topic: str
    partition: int
    offset: int
    raw_ingestion_id: str
    broker_timestamp: Any
    consumed_at: Any
    payload_encoding: str
    payload_stored: str
    payload_bytes_hash: str
    event_id: Optional[str] = None
    event_type: Optional[str] = None
    simulation_run_id: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolvedRecord:
    kind: ResolveKind
    topic: str
    partition: int
    offset: int
    raw_row: Optional[RawRow] = None
    quarantine_row: Optional[Dict[str, Any]] = None


@dataclass
class PendingLedgerEntry:
    topic: str
    partition: int
    offset: int
    raw_ingestion_id: str
    status: str
    destination: str
    payload_hash: Optional[str] = None
    event_id: Optional[str] = None


@dataclass
class TransformResult:
    kind: str  # ENTITY | RUN | QUARANTINE
    entity_row: Optional[Dict[str, Any]] = None
    run_row: Optional[Dict[str, Any]] = None
    quarantine_row: Optional[Dict[str, Any]] = None


@dataclass
class WindowManifest:
    topic: str
    partitions: List[Dict[str, Any]]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WindowManifest":
        return cls(topic=str(data["topic"]), partitions=list(data["partitions"]))
