"""Silver Plan 3 — configuration, shared runtime enums, and namespace guards."""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from de.silver import MIGRATION_VERSION, PROCESSOR_NAME, PROCESSOR_VERSION

_REPO = Path(__file__).resolve().parents[2]

NAMESPACE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

SOURCE_TABLE_ENTITY = "bronze_entity_events"
SOURCE_TABLE_RUN = "bronze_run_events"
SOURCE_TABLES = frozenset({SOURCE_TABLE_ENTITY, SOURCE_TABLE_RUN})

BACKOFF_SCHEDULE_SEC = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
READ_RETRY_SCHEDULE_SEC = (0.5, 1.0, 2.0, 4.0, 8.0)


class ProcessorState(str, Enum):
    STARTING = "STARTING"
    RECOVERING = "RECOVERING"
    READY = "READY"
    RETRYING = "RETRYING"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class DestinationMode(str, Enum):
    MAIN = "main"
    REPLAY = "replay"


class CasResult(str, Enum):
    ADVANCED = "ADVANCED"
    ALREADY_ADVANCED = "ALREADY_ADVANCED"
    RETRY_SAME = "RETRY_SAME"
    CAS_CONFLICT = "CAS_CONFLICT"


class FactReconcileResult(str, Enum):
    MISSING = "MISSING"
    EXACT_MATCH = "EXACT_MATCH"
    SOURCE_MATCH_PAYLOAD_CONFLICT = "SOURCE_MATCH_PAYLOAD_CONFLICT"
    BUSINESS_KEY_OWNED_BY_OTHER_SOURCE = "BUSINESS_KEY_OWNED_BY_OTHER_SOURCE"
    PHYSICAL_DUPLICATE_EXACT = "PHYSICAL_DUPLICATE_EXACT"


@dataclass(frozen=True)
class SourceStream:
    source_table: str
    topic: str
    partition: int

    def __post_init__(self) -> None:
        if self.source_table not in SOURCE_TABLES:
            raise ValueError(f"Invalid source_table: {self.source_table}")
        if self.partition < 0:
            raise ValueError("partition must be non-negative")


@dataclass(frozen=True)
class CheckpointKey:
    checkpoint_namespace: str
    source_table: str
    topic: str
    partition_id: int


@dataclass(frozen=True)
class ReadReceipt:
    first_offset: Optional[int]
    last_offset: Optional[int]
    logical_count: int
    physical_count: int
    duplicate_count: int


class SilverConfigError(ValueError):
    """Permanent configuration / namespace guard failure."""


def validate_namespace_id(run_id: str) -> str:
    if not NAMESPACE_ID_RE.fullmatch(run_id or ""):
        raise SilverConfigError(f"Invalid namespace id: {run_id!r}")
    return run_id


def live_namespace() -> str:
    return "live"


def replay_namespace(run_id: str) -> str:
    return f"replay:{validate_namespace_id(run_id)}"


def make_test_namespace(run_id: str) -> str:
    return f"test:{validate_namespace_id(run_id)}"


def assert_live_namespace(namespace: str) -> None:
    if namespace != "live":
        raise SilverConfigError(f"Live mode requires namespace 'live', got {namespace!r}")


def assert_replay_namespace(namespace: str, run_id: str) -> None:
    expected = replay_namespace(run_id)
    if namespace != expected:
        raise SilverConfigError(
            f"Replay namespace mismatch: expected {expected!r}, got {namespace!r}"
        )


class SilverSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SILVER_", extra="ignore")

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "smart_traffic"
    clickhouse_secure: bool = False
    clickhouse_connect_timeout: float = 5.0
    clickhouse_query_timeout: float = 30.0

    checkpoint_path: str = str(_REPO / "de" / "artifacts" / "silver" / "checkpoint.sqlite3")
    namespace: str = "live"
    destination_mode: str = DestinationMode.MAIN.value
    replay_run_id: str = ""

    topic_allowlist: str = "traffic.entity-events.v2,traffic.simulation-events.v2"
    batch_size: int = 500
    poll_interval_sec: float = 2.0
    discovery_interval_sec: float = 30.0
    readiness_stale_sec: float = 120.0
    health_snapshot_max_age_sec: float = 5.0

    health_host: str = "0.0.0.0"
    health_port: int = 8095

    single_instance: bool = True
    worker_count: int = 1

    processor_name: str = PROCESSOR_NAME
    processor_version: str = PROCESSOR_VERSION
    silver_schema_version: str = MIGRATION_VERSION

    @field_validator("batch_size")
    @classmethod
    def _batch_size(cls, v: int) -> int:
        if not 1 <= int(v) <= 500:
            raise ValueError("batch_size must be in 1..500")
        return int(v)

    @field_validator("worker_count")
    @classmethod
    def _worker_count(cls, v: int) -> int:
        if int(v) != 1:
            raise ValueError("worker_count must be 1")
        return 1

    @field_validator("destination_mode")
    @classmethod
    def _dest_mode(cls, v: str) -> str:
        if v not in {DestinationMode.MAIN.value, DestinationMode.REPLAY.value}:
            raise ValueError("destination_mode must be main|replay")
        return v

    def topic_list(self) -> tuple[str, ...]:
        return tuple(t.strip() for t in self.topic_allowlist.split(",") if t.strip())

    def redacted_dict(self) -> dict:
        data = self.model_dump()
        if data.get("clickhouse_password"):
            data["clickhouse_password"] = "***"
        return data

    def validate_mode_guards(self) -> None:
        if not self.single_instance:
            raise SilverConfigError("single_instance must be true")
        if self.destination_mode == DestinationMode.MAIN.value:
            assert_live_namespace(self.namespace)
            if self.replay_run_id:
                raise SilverConfigError("live mode forbids replay_run_id")
        elif self.destination_mode == DestinationMode.REPLAY.value:
            if not self.replay_run_id:
                raise SilverConfigError("replay mode requires replay_run_id")
            assert_replay_namespace(self.namespace, self.replay_run_id)
        else:
            raise SilverConfigError(f"Unknown destination_mode: {self.destination_mode}")


@lru_cache
def get_settings() -> SilverSettings:
    return SilverSettings()
