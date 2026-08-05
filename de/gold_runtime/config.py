"""Gold 3 runtime configuration, runtime enums and namespace guards.

Every value fixed by Gold Runtime Contract v1 is validated here, before any
ClickHouse or SQLite connection is opened. No P0-locked value has an implicit
default: the three source cadences must be supplied by the environment or by an
explicit constructor argument.
"""
from __future__ import annotations

import re
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Final, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from de.gold.contracts import GOLD_SCHEMA_VERSION, WINDOW_SIZES_SEC
from de.gold_runtime import PROCESSOR_NAME, PROCESSOR_VERSION

_REPO = Path(__file__).resolve().parents[2]

# ── Values fixed by Gold 1/Gold 2/Gold Runtime Contract v1 ──────────────────

GOLD_DATABASE: Final = "smart_traffic"
DEFINITION_VERSION: Final = "v1.0"
DEFINITION_MAJOR: Final = 1
DEFINITION_MINOR: Final = 0
BACKOFF_SCHEDULE_SEC: Final = (0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
RETRY_MAX_ATTEMPTS: Final = len(BACKOFF_SCHEDULE_SEC)
ALLOWED_LATENESS_SEC: Final = 0.0
WATERMARK_DELAY_SEC: Final = 0.0
MAX_REVISION_SEQ: Final = 1
COVERAGE_THRESHOLD: Final = 0.80
SILVER_FETCH_CEILING: Final = 500
DEFAULT_HEALTH_PORT: Final = 8096

NAMESPACE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
LIVE_NAMESPACE: Final = "live"

SOURCE_TABLE_TRAFFIC: Final = "silver_fact_traffic_observation"
SOURCE_TABLE_INTERSECTION: Final = "silver_fact_intersection_state"
SOURCE_TABLE_SIGNAL: Final = "silver_fact_signal_state"
SOURCE_TABLE_CAMERA: Final = "silver_fact_camera_observation"
SOURCE_TABLE_RUN_EVENT: Final = "silver_fact_run_event"
SOURCE_TABLE_DIM_RUN: Final = "silver_dim_run"
SOURCE_TABLE_DIM_SCENARIO: Final = "silver_dim_scenario"
SOURCE_TABLE_DIM_INTERSECTION: Final = "silver_dim_intersection"
SOURCE_TABLE_DIM_APPROACH: Final = "silver_dim_approach"

FACT_SOURCE_TABLES: Final = (
    SOURCE_TABLE_TRAFFIC,
    SOURCE_TABLE_INTERSECTION,
    SOURCE_TABLE_SIGNAL,
    SOURCE_TABLE_CAMERA,
    SOURCE_TABLE_RUN_EVENT,
)
DIM_SOURCE_TABLES: Final = (
    SOURCE_TABLE_DIM_RUN,
    SOURCE_TABLE_DIM_SCENARIO,
    SOURCE_TABLE_DIM_INTERSECTION,
    SOURCE_TABLE_DIM_APPROACH,
)
ALLOWED_SOURCE_TABLES: Final = FACT_SOURCE_TABLES + DIM_SOURCE_TABLES
# Bronze/Raw/quarantine/ledger/replay-mirror reads are forbidden for the live reader.
FORBIDDEN_SOURCE_FRAGMENTS: Final = ("bronze", "raw", "quarantine", "ledger", "_replay")

RUN_SCOPE_ALL: Final = "all"


class GoldConfigError(ValueError):
    """Permanent configuration or namespace-guard failure."""


class ProcessorState(str, Enum):
    STARTING = "STARTING"
    RECOVERING = "RECOVERING"
    READY = "READY"
    PROCESSING = "PROCESSING"
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


class WindowState(str, Enum):
    OPEN = "OPEN"
    ELIGIBLE = "ELIGIBLE"
    PROCESSING = "PROCESSING"
    CLOSED = "CLOSED"
    REVISED = "REVISED"


class WorkUnitState(str, Enum):
    RECEIVED = "RECEIVED"
    TRANSFORMED = "TRANSFORMED"
    PERSISTED = "PERSISTED"
    CHECKPOINTED = "CHECKPOINTED"
    REPLAYED = "REPLAYED"
    QUARANTINED = "QUARANTINED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    PERSISTENCE_UNKNOWN = "PERSISTENCE_UNKNOWN"
    CONFLICTED = "CONFLICTED"


TERMINAL_WORK_UNIT_STATES: Final = frozenset({
    WorkUnitState.CHECKPOINTED,
    WorkUnitState.REPLAYED,
    WorkUnitState.QUARANTINED,
    WorkUnitState.CONFLICTED,
})


class LateClass(str, Enum):
    ON_TIME = "ON_TIME"
    LATE_BEFORE_CLOSE = "LATE_BEFORE_CLOSE"
    LATE_AFTER_CLOSE = "LATE_AFTER_CLOSE"
    CONFLICT = "CONFLICT"


# ── Namespace helpers ───────────────────────────────────────────────────────


def validate_namespace_id(replay_id: str) -> str:
    if not NAMESPACE_ID_RE.fullmatch(replay_id or ""):
        raise GoldConfigError(f"Invalid replay id: {replay_id!r}")
    return replay_id


def live_namespace() -> str:
    return LIVE_NAMESPACE


def replay_namespace(replay_id: str) -> str:
    return f"replay:{validate_namespace_id(replay_id)}"


def is_replay_namespace(namespace: str) -> bool:
    return namespace.startswith("replay:") and NAMESPACE_ID_RE.fullmatch(namespace[7:]) is not None


def validate_namespace(namespace: str) -> str:
    if namespace == LIVE_NAMESPACE or is_replay_namespace(namespace):
        return namespace
    raise GoldConfigError(f"Invalid Gold namespace: {namespace!r}")


def _parse_csv(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(raw).split(",") if item.strip())


class GoldSettings(BaseSettings):
    """`GOLD_`-prefixed runtime settings; validated before any connection."""

    model_config = SettingsConfigDict(env_prefix="GOLD_", extra="ignore")

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = GOLD_DATABASE
    clickhouse_secure: bool = False
    clickhouse_connect_timeout: float = 5.0
    clickhouse_query_timeout: float = 30.0

    namespace: str = LIVE_NAMESPACE
    destination_mode: str = DestinationMode.MAIN.value
    replay_id: str = ""
    replay_namespace: str = ""

    checkpoint_path: str = str(_REPO / "de" / "artifacts" / "gold" / "checkpoint.sqlite3")
    instance_lock_path: str = str(_REPO / "de" / "artifacts" / "gold" / "instance.lock")

    source_tables: str = ",".join(ALLOWED_SOURCE_TABLES)
    window_sizes_sec: str = ",".join(str(size) for size in WINDOW_SIZES_SEC)
    run_scope: str = RUN_SCOPE_ALL

    poll_interval_sec: float = 2.0
    silver_fetch_batch_size: int = SILVER_FETCH_CEILING
    max_windows_per_cycle: int = 1

    # Gold Runtime Contract v1 / Appendix P: three independent required cadences.
    traffic_expected_cadence_sec: float
    intersection_expected_cadence_sec: float
    signal_expected_cadence_sec: float
    camera_expected_cadence_sec: Optional[float] = None
    expected_observation_cadence_sec: Optional[float] = None

    allowed_lateness_sec: float = ALLOWED_LATENESS_SEC
    watermark_delay_sec: float = WATERMARK_DELAY_SEC
    analytical_stale_threshold_sec: float = 600.0

    retry_max_attempts: int = RETRY_MAX_ATTEMPTS
    retry_initial_delay_sec: float = BACKOFF_SCHEDULE_SEC[0]
    retry_max_delay_sec: float = BACKOFF_SCHEDULE_SEC[-1]

    health_host: str = "0.0.0.0"
    health_port: int = DEFAULT_HEALTH_PORT
    health_snapshot_max_age_sec: float = 5.0
    readiness_lag_threshold_sec: float = 120.0
    shutdown_timeout_sec: float = 30.0

    backfill_start: str = ""
    backfill_end: str = ""

    definition_version: str = DEFINITION_VERSION
    gold_schema_version: str = GOLD_SCHEMA_VERSION
    processor_name: str = PROCESSOR_NAME
    processor_version: str = PROCESSOR_VERSION
    dry_run: bool = False

    # ── field validators ────────────────────────────────────────────────────

    @field_validator("clickhouse_database")
    @classmethod
    def _database(cls, value: str) -> str:
        if value != GOLD_DATABASE:
            raise ValueError(f"clickhouse_database must be {GOLD_DATABASE!r}")
        return value

    @field_validator("clickhouse_host", "clickhouse_user")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("value must be non-empty")
        return value

    @field_validator("clickhouse_port", "health_port")
    @classmethod
    def _port(cls, value: int) -> int:
        if not 1 <= int(value) <= 65535:
            raise ValueError("port must be in 1..65535")
        return int(value)

    @field_validator("definition_version")
    @classmethod
    def _definition_version(cls, value: str) -> str:
        if value != DEFINITION_VERSION:
            raise ValueError(f"definition_version must be {DEFINITION_VERSION!r}")
        return value

    @field_validator("gold_schema_version")
    @classmethod
    def _schema_version(cls, value: str) -> str:
        if value != GOLD_SCHEMA_VERSION:
            raise ValueError(f"gold_schema_version must be {GOLD_SCHEMA_VERSION!r}")
        return value

    @field_validator("allowed_lateness_sec")
    @classmethod
    def _lateness(cls, value: float) -> float:
        if float(value) != ALLOWED_LATENESS_SEC:
            raise ValueError("allowed_lateness_sec is fixed at 0 by Gold Runtime Contract v1")
        return float(value)

    @field_validator("watermark_delay_sec")
    @classmethod
    def _delay(cls, value: float) -> float:
        if float(value) != WATERMARK_DELAY_SEC:
            raise ValueError("watermark_delay_sec is fixed at 0 by Gold Runtime Contract v1")
        return float(value)

    @field_validator("silver_fetch_batch_size")
    @classmethod
    def _batch_size(cls, value: int) -> int:
        if not 1 <= int(value) <= SILVER_FETCH_CEILING:
            raise ValueError(f"silver_fetch_batch_size must be in 1..{SILVER_FETCH_CEILING}")
        return int(value)

    @field_validator("max_windows_per_cycle")
    @classmethod
    def _max_windows(cls, value: int) -> int:
        if int(value) < 1:
            raise ValueError("max_windows_per_cycle must be >= 1")
        return int(value)

    @field_validator("poll_interval_sec", "analytical_stale_threshold_sec", "shutdown_timeout_sec")
    @classmethod
    def _positive(cls, value: float) -> float:
        if float(value) <= 0:
            raise ValueError("value must be > 0")
        return float(value)

    @field_validator(
        "traffic_expected_cadence_sec",
        "intersection_expected_cadence_sec",
        "signal_expected_cadence_sec",
    )
    @classmethod
    def _cadence(cls, value: float) -> float:
        if float(value) <= 0:
            raise ValueError("expected cadence must be > 0 (producer-evidenced)")
        return float(value)

    @field_validator("camera_expected_cadence_sec", "expected_observation_cadence_sec")
    @classmethod
    def _optional_cadence(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and float(value) <= 0:
            raise ValueError("cadence must be > 0 when supplied")
        return None if value is None else float(value)

    @field_validator("retry_max_attempts")
    @classmethod
    def _attempts(cls, value: int) -> int:
        if not 1 <= int(value) <= RETRY_MAX_ATTEMPTS:
            raise ValueError(f"retry_max_attempts must be in 1..{RETRY_MAX_ATTEMPTS}")
        return int(value)

    @field_validator("readiness_lag_threshold_sec")
    @classmethod
    def _non_negative(cls, value: float) -> float:
        if float(value) < 0:
            raise ValueError("readiness_lag_threshold_sec must be non-negative")
        return float(value)

    @field_validator("destination_mode")
    @classmethod
    def _mode(cls, value: str) -> str:
        if value not in {DestinationMode.MAIN.value, DestinationMode.REPLAY.value}:
            raise ValueError("destination_mode must be main|replay")
        return value

    @field_validator("run_scope")
    @classmethod
    def _run_scope(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("run_scope must be 'all' or a non-empty run id list")
        return value

    # ── derived accessors ───────────────────────────────────────────────────

    def source_table_list(self) -> tuple[str, ...]:
        return _parse_csv(self.source_tables)

    def window_size_list(self) -> tuple[int, ...]:
        return tuple(int(item) for item in _parse_csv(self.window_sizes_sec))

    def run_scope_list(self) -> tuple[str, ...]:
        """Empty tuple means 'all runs'."""
        if self.run_scope.strip() == RUN_SCOPE_ALL:
            return ()
        return _parse_csv(self.run_scope)

    def processes_all_runs(self) -> bool:
        return not self.run_scope_list()

    def is_replay(self) -> bool:
        return self.destination_mode == DestinationMode.REPLAY.value

    def cadence_for(self, stream: str) -> float:
        return {
            "traffic": self.traffic_expected_cadence_sec,
            "intersection": self.intersection_expected_cadence_sec,
            "signal": self.signal_expected_cadence_sec,
        }[stream]

    def retry_delay(self, attempt_index: int) -> float:
        slot = BACKOFF_SCHEDULE_SEC[min(max(attempt_index, 0), len(BACKOFF_SCHEDULE_SEC) - 1)]
        return min(max(slot, self.retry_initial_delay_sec), self.retry_max_delay_sec)

    def redacted_dict(self) -> dict:
        data = self.model_dump()
        if data.get("clickhouse_password"):
            data["clickhouse_password"] = "***"
        return data

    # ── cross-field validation (must run before connecting) ─────────────────

    def validate_all(self) -> "GoldSettings":
        self._validate_sources()
        self._validate_windows()
        self._validate_cadences()
        self._validate_namespace_and_paths()
        self._validate_retry()
        return self

    def _validate_sources(self) -> None:
        tables = self.source_table_list()
        if not tables:
            raise GoldConfigError("source_tables allowlist must not be empty")
        if len(set(tables)) != len(tables):
            raise GoldConfigError("source_tables contains duplicates")
        unknown = [table for table in tables if table not in ALLOWED_SOURCE_TABLES]
        if unknown:
            raise GoldConfigError(f"source_tables outside the Silver allowlist: {unknown}")
        forbidden = [
            table for table in tables
            if any(fragment in table for fragment in FORBIDDEN_SOURCE_FRAGMENTS)
        ]
        if forbidden:
            raise GoldConfigError(f"forbidden Gold3 sources: {forbidden}")

    def _validate_windows(self) -> None:
        sizes = self.window_size_list()
        if len(set(sizes)) != len(sizes):
            raise GoldConfigError("window_sizes_sec contains duplicates")
        if sizes != tuple(WINDOW_SIZES_SEC):
            raise GoldConfigError(f"window_sizes_sec must equal {tuple(WINDOW_SIZES_SEC)}")

    def _validate_cadences(self) -> None:
        alias = self.expected_observation_cadence_sec
        if alias is None:
            return
        required = (
            self.traffic_expected_cadence_sec,
            self.intersection_expected_cadence_sec,
            self.signal_expected_cadence_sec,
        )
        if any(float(alias) != float(value) for value in required):
            raise GoldConfigError(
                "expected_observation_cadence_sec is a compatibility alias and may be set "
                "only when it equals all three required source cadences"
            )

    def _validate_namespace_and_paths(self) -> None:
        validate_namespace(self.namespace)
        checkpoint = Path(self.checkpoint_path)
        lock = Path(self.instance_lock_path)
        if "silver" in checkpoint.as_posix().lower():
            raise GoldConfigError("checkpoint_path must not be shared with Silver")
        if checkpoint == lock:
            raise GoldConfigError("checkpoint_path and instance_lock_path must differ")
        if self.destination_mode == DestinationMode.MAIN.value:
            if self.namespace != LIVE_NAMESPACE:
                raise GoldConfigError("live mode requires namespace 'live'")
            if self.replay_id or self.replay_namespace:
                raise GoldConfigError("live mode forbids replay settings")
            if self.backfill_start or self.backfill_end:
                raise GoldConfigError("live mode forbids backfill settings")
            if self.dry_run:
                raise GoldConfigError("dry_run cannot be true in live mode")
        else:
            if not self.replay_id:
                raise GoldConfigError("replay mode requires replay_id")
            expected = replay_namespace(self.replay_id)
            if self.namespace != expected:
                raise GoldConfigError(
                    f"replay namespace mismatch: expected {expected!r}, got {self.namespace!r}"
                )
            if self.replay_namespace and self.replay_namespace != expected:
                raise GoldConfigError("replay_namespace must equal the resolved replay namespace")
            if bool(self.backfill_start) != bool(self.backfill_end):
                raise GoldConfigError("backfill requires both start and end")
            if self.backfill_start and self.backfill_start >= self.backfill_end:
                raise GoldConfigError("backfill_start must be < backfill_end")

    def _validate_retry(self) -> None:
        if self.retry_max_delay_sec < self.retry_initial_delay_sec:
            raise GoldConfigError("retry_max_delay_sec must be >= retry_initial_delay_sec")
        if self.retry_initial_delay_sec <= 0:
            raise GoldConfigError("retry_initial_delay_sec must be > 0")


@lru_cache
def get_settings() -> GoldSettings:
    return GoldSettings().validate_all()
