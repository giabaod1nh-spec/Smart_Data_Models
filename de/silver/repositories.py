"""Silver Plan 3 — SilverClickHouseRepository: static dispatch, reads, inserts, reconciliation.

Implements Plan 3 §12 (Repository API Contract). Table names are never interpolated from
caller-supplied strings; every target is dispatched through a frozen allowlist/mapping and
``InvalidTargetTableError`` is raised before any SQL is built for an unknown target.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import clickhouse_connect

from de.silver.config import DestinationMode, FactReconcileResult, SilverSettings, SourceStream
from de.silver.contracts import MAIN_DIM_TABLES, MAIN_FACT_TABLES, TABLE_COLUMNS
from de.silver.dimension_builders import DimensionCandidate
from de.silver.models import (
    FACT_MODEL_BY_TABLE,
    SilverDimApproach,
    SilverDimIntersection,
    SilverDimRun,
    SilverDimScenario,
    SilverLedgerEntry,
    SilverQuarantineEntry,
)

# ── Exception taxonomy (Plan 3 §12.3) ───────────────────────────────────────


class SilverRepositoryError(Exception):
    """Base class for all Silver repository failures."""


class RetryableRepositoryError(SilverRepositoryError):
    """Connection reset, timeout, temporary server/network unavailability."""


class UncertainWriteError(SilverRepositoryError):
    """Client cannot determine whether a synchronous insert committed."""


class SchemaMismatchError(SilverRepositoryError):
    """Missing table/column/type; permanent."""


class SourceOffsetConflictError(SilverRepositoryError):
    """Conflicting Bronze rows for one source key (different canonical hash); permanent."""


class FactBusinessKeyConflictError(SilverRepositoryError):
    """One Plan 1 fact business key is owned by a different source ID/hash; permanent."""


class LedgerConflictError(SilverRepositoryError):
    """Same (namespace, source_id) has incompatible payload/disposition/target; permanent."""


class InvalidTargetTableError(SilverRepositoryError):
    """Target outside the static allowlist; permanent. Raised before SQL execution."""


class ReplayModeGuardError(SilverRepositoryError):
    """A main-table write was attempted while destination_mode='replay', or vice versa."""


# ── Static allowlists / replay mappings (Plan 3 §12.2) ──────────────────────

QUARANTINE_TABLE = "silver_quarantine"
QUARANTINE_REPLAY_TABLE = "silver_quarantine_replay"
LEDGER_TABLE = "silver_processing_ledger"

FACT_REPLAY_MAP: Dict[str, str] = {
    "silver_fact_traffic_observation": "silver_fact_traffic_observation_replay",
    "silver_fact_signal_state": "silver_fact_signal_state_replay",
    "silver_fact_intersection_state": "silver_fact_intersection_state_replay",
    "silver_fact_camera_observation": "silver_fact_camera_observation_replay",
    "silver_fact_run_event": "silver_fact_run_event_replay",
}

# Approach/Scenario have no replay mirror in DDL 004 (Plan 3 §17.3 / §32.10).
DIM_REPLAY_MAP: Dict[str, Optional[str]] = {
    "silver_dim_run": "silver_dim_run_replay",
    "silver_dim_intersection": "silver_dim_intersection_replay",
    "silver_dim_approach": None,
    "silver_dim_scenario": None,
}

DIM_MODEL_BY_TABLE: Dict[str, type] = {
    "silver_dim_run": SilverDimRun,
    "silver_dim_intersection": SilverDimIntersection,
    "silver_dim_approach": SilverDimApproach,
    "silver_dim_scenario": SilverDimScenario,
}

# Plan 1 business key per fact table (ORDER BY columns double as the business key).
FACT_BUSINESS_KEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "silver_fact_traffic_observation": (
        "simulation_run_id", "intersection_id", "direction", "source_entity_id",
        "simulation_time_sec",
    ),
    "silver_fact_signal_state": (
        "simulation_run_id", "intersection_id", "direction", "source_entity_id",
        "simulation_time_sec",
    ),
    "silver_fact_intersection_state": (
        "simulation_run_id", "intersection_id", "source_entity_id", "simulation_time_sec",
    ),
    "silver_fact_camera_observation": (
        "simulation_run_id", "intersection_id", "source_entity_id", "simulation_time_sec",
    ),
    "silver_fact_run_event": ("simulation_run_id", "event_simulation_time"),
}

DIM_BUSINESS_KEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "silver_dim_run": ("simulation_run_id",),
    "silver_dim_intersection": ("intersection_id",),
    "silver_dim_approach": ("intersection_id", "direction"),
    "silver_dim_scenario": ("scenario_id",),
}


# ── Result / receipt dataclasses ────────────────────────────────────────────


@dataclass(frozen=True)
class FactIdentity:
    source_bronze_event_id: str
    source_payload_hash: str
    business_key: Tuple[Any, ...]
    source_topic: str
    source_partition: int
    source_offset: int


@dataclass(frozen=True)
class FactReconciliation:
    result: FactReconcileResult
    source_bronze_event_id: str


@dataclass(frozen=True)
class LedgerEntryState:
    checkpoint_namespace: str
    source_bronze_event_id: str
    raw_ingestion_id: str
    payload_hash: str
    disposition: str
    target_table: str


@dataclass(frozen=True)
class WriteReceipt:
    attempted: Tuple[str, ...]
    confirmed: Tuple[str, ...]
    uncertain: Tuple[str, ...]


# ── Small pure helpers ───────────────────────────────────────────────────────


def _cell(value: Any) -> Any:
    """Normalize ClickHouse cell values (FixedString often arrives as bytes)."""
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace").rstrip("\x00")
    return value


def _text(value: Any) -> str:
    value = _cell(value)
    if value is None:
        return ""
    return str(value)


def _rows_to_dicts(result: Any, columns: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    cols = list(columns) if columns is not None else list(result.column_names)
    return [{c: _cell(v) for c, v in zip(cols, row)} for row in result.result_rows]


def _scalar_str(value: Any) -> str:
    value = _cell(value)
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _key_str(key: Sequence[Any]) -> str:
    return "\x1f".join(_scalar_str(v) for v in key)


def _business_key_expr(columns: Sequence[str]) -> str:
    if len(columns) == 1:
        return f"toString({columns[0]})"
    pieces: List[str] = []
    for i, col in enumerate(columns):
        if i > 0:
            pieces.append("'\\x1f'")
        pieces.append(f"toString({col})")
    return "concat(" + ", ".join(pieces) + ")"


def _row_key(row: Dict[str, Any], key_cols: Sequence[str]) -> Tuple[Any, ...]:
    return tuple(row[c] for c in key_cols)


def classify_fact_identity(
    identity: FactIdentity,
    existing_rows: Sequence[Dict[str, Any]],
    key_cols: Sequence[str],
) -> FactReconciliation:
    """Pure classification (Plan 3 §12.1/§32.4) — testable without a live client."""
    id_matches = [
        r for r in existing_rows
        if _text(r.get("source_bronze_event_id")) == identity.source_bronze_event_id
    ]
    key_matches = [
        r for r in existing_rows
        if tuple(_cell(v) for v in _row_key(r, key_cols)) == tuple(identity.business_key)
    ]

    def _is_exact(row: Dict[str, Any]) -> bool:
        return (
            _text(row.get("source_payload_hash")) == identity.source_payload_hash
            and tuple(_cell(v) for v in _row_key(row, key_cols)) == tuple(identity.business_key)
        )

    if len(id_matches) > 1:
        if all(_is_exact(r) for r in id_matches):
            return FactReconciliation(
                FactReconcileResult.PHYSICAL_DUPLICATE_EXACT, identity.source_bronze_event_id
            )
        return FactReconciliation(
            FactReconcileResult.SOURCE_MATCH_PAYLOAD_CONFLICT, identity.source_bronze_event_id
        )
    if len(id_matches) == 1:
        if _is_exact(id_matches[0]):
            return FactReconciliation(FactReconcileResult.EXACT_MATCH, identity.source_bronze_event_id)
        return FactReconciliation(
            FactReconcileResult.SOURCE_MATCH_PAYLOAD_CONFLICT, identity.source_bronze_event_id
        )
    if key_matches:
        return FactReconciliation(
            FactReconcileResult.BUSINESS_KEY_OWNED_BY_OTHER_SOURCE, identity.source_bronze_event_id
        )
    return FactReconciliation(FactReconcileResult.MISSING, identity.source_bronze_event_id)


def _row_values(row: Any, cols: Sequence[str]) -> List[Any]:
    return [getattr(row, c) for c in cols]


def _looks_uncertain(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(token in text for token in ("timeout", "reset", "broken pipe", "connection aborted"))


# ── Repository ───────────────────────────────────────────────────────────────


class SilverClickHouseRepository:
    """Static target dispatch, reads, inserts, and reconciliation (Plan 3 §12)."""

    def __init__(self, settings: SilverSettings, client: Any = None) -> None:
        self.settings = settings
        self.database = settings.clickhouse_database
        self._client = client

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        if self._client is not None:
            return
        self._client = clickhouse_connect.get_client(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_user,
            password=self.settings.clickhouse_password,
            database=self.database,
            secure=self.settings.clickhouse_secure,
            connect_timeout=self.settings.clickhouse_connect_timeout,
            send_receive_timeout=self.settings.clickhouse_query_timeout,
            settings={"async_insert": 0, "wait_end_of_query": 1},
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Silver repository not connected")
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    # -- guards ---------------------------------------------------------------

    def _guard_mode(self, *, replay: bool) -> None:
        mode = self.settings.destination_mode
        if replay and mode != DestinationMode.REPLAY.value:
            raise ReplayModeGuardError(
                "Cannot write a replay target while destination_mode='main'"
            )
        if not replay and mode != DestinationMode.MAIN.value:
            raise ReplayModeGuardError(
                "Cannot write a main target while destination_mode='replay'"
            )

    def _resolve_fact_table(self, target: str, replay_run_id: Optional[str]) -> str:
        if replay_run_id:
            replay = FACT_REPLAY_MAP.get(target)
            if not replay:
                raise InvalidTargetTableError(f"No replay target for {target!r}")
            return replay
        return target

    def _resolve_dim_table(self, target: str, replay_run_id: Optional[str]) -> Optional[str]:
        if replay_run_id:
            return DIM_REPLAY_MAP.get(target)
        return target

    def _write(self, physical_table: str, data: List[List[Any]], cols: List[str]) -> None:
        try:
            self.client.insert(f"{self.database}.{physical_table}", data, column_names=cols)
        except Exception as exc:  # noqa: BLE001 — reclassified into the exception taxonomy
            if _looks_uncertain(exc):
                raise UncertainWriteError(str(exc)) from exc
            raise RetryableRepositoryError(str(exc)) from exc

    # -- schema verification ---------------------------------------------------

    def verify_schema(self, mode: str) -> Dict[str, bool]:
        if mode == DestinationMode.MAIN.value:
            tables: Tuple[str, ...] = (
                MAIN_FACT_TABLES + MAIN_DIM_TABLES + (QUARANTINE_TABLE, LEDGER_TABLE)
            )
        elif mode == DestinationMode.REPLAY.value:
            tables = (
                tuple(FACT_REPLAY_MAP.values())
                + tuple(v for v in DIM_REPLAY_MAP.values() if v)
                + (QUARANTINE_REPLAY_TABLE, LEDGER_TABLE)
            )
        else:
            raise ValueError(f"Unknown schema verification mode: {mode!r}")
        report: Dict[str, bool] = {}
        for table in tables:
            r = self.client.query(
                "SELECT count() FROM system.tables WHERE database={db:String} AND name={n:String}",
                parameters={"db": self.database, "n": table},
            )
            ok = bool(r.result_rows) and int(r.result_rows[0][0]) >= 1
            report[table] = ok
            if not ok:
                raise SchemaMismatchError(f"Missing Silver table: {table}")
        return report

    # -- ledger -----------------------------------------------------------------

    def find_ledger_entries(
        self, namespace: str, source_ids: Sequence[str]
    ) -> Dict[str, LedgerEntryState]:
        if len(source_ids) > 500:
            raise ValueError("find_ledger_entries accepts at most 500 IDs")
        if not source_ids:
            return {}
        sql = f"""
            SELECT source_bronze_event_id, raw_ingestion_id, payload_hash, disposition, target_table
            FROM {self.database}.{LEDGER_TABLE}
            WHERE checkpoint_namespace = {{ns:String}}
              AND source_bronze_event_id IN {{ids:Array(String)}}
        """
        r = self.client.query(sql, parameters={"ns": namespace, "ids": list(source_ids)})
        out: Dict[str, LedgerEntryState] = {}
        for row in _rows_to_dicts(r):
            out[str(row["source_bronze_event_id"])] = LedgerEntryState(
                checkpoint_namespace=namespace,
                source_bronze_event_id=str(row["source_bronze_event_id"]),
                raw_ingestion_id=str(row["raw_ingestion_id"]),
                payload_hash=str(row["payload_hash"]),
                disposition=str(row["disposition"]),
                target_table=str(row["target_table"]),
            )
        return out

    def insert_ledger_batch(
        self, namespace: str, rows: Sequence[SilverLedgerEntry]
    ) -> WriteReceipt:
        if not rows:
            return WriteReceipt((), (), ())
        for row in rows:
            if row.checkpoint_namespace != namespace:
                raise ValueError(
                    f"Ledger row namespace {row.checkpoint_namespace!r} != {namespace!r}"
                )
        cols = [f.name for f in fields(SilverLedgerEntry)]
        data = [_row_values(r, cols) for r in rows]
        attempted = tuple(r.source_bronze_event_id for r in rows)
        self._write(LEDGER_TABLE, data, cols)
        return WriteReceipt(attempted, attempted, ())

    # -- facts --------------------------------------------------------------

    def find_fact_states(
        self,
        target: str,
        identities: Sequence[FactIdentity],
        *,
        replay_run_id: Optional[str] = None,
    ) -> Dict[str, FactReconciliation]:
        if target not in MAIN_FACT_TABLES:
            raise InvalidTargetTableError(f"Invalid fact target: {target!r}")
        if not identities:
            return {}
        physical = self._resolve_fact_table(target, replay_run_id)
        key_cols = FACT_BUSINESS_KEY_COLUMNS[target]
        ids = [i.source_bronze_event_id for i in identities]
        keys = [_key_str(i.business_key) for i in identities]
        sql = f"""
            SELECT source_bronze_event_id, source_payload_hash, {", ".join(key_cols)}
            FROM {self.database}.{physical}
            WHERE source_bronze_event_id IN {{ids:Array(String)}}
               OR {_business_key_expr(key_cols)} IN {{keys:Array(String)}}
        """
        r = self.client.query(sql, parameters={"ids": ids, "keys": keys})
        rows = _rows_to_dicts(r)
        return {
            identity.source_bronze_event_id: classify_fact_identity(identity, rows, key_cols)
            for identity in identities
        }

    def insert_fact_batch(
        self, target: str, rows: Sequence[Any], *, replay_run_id: Optional[str] = None
    ) -> WriteReceipt:
        if target not in MAIN_FACT_TABLES:
            raise InvalidTargetTableError(f"Invalid fact target: {target!r}")
        if not rows:
            return WriteReceipt((), (), ())
        self._guard_mode(replay=bool(replay_run_id))
        physical = self._resolve_fact_table(target, replay_run_id)
        model_cls = FACT_MODEL_BY_TABLE[target]
        cols = [f.name for f in fields(model_cls)]
        data = [_row_values(r, cols) for r in rows]
        if replay_run_id:
            cols = cols + ["replay_run_id"]
            data = [row + [replay_run_id] for row in data]
        attempted = tuple(getattr(r, "source_bronze_event_id") for r in rows)
        self._write(physical, data, cols)
        return WriteReceipt(attempted, attempted, ())

    # -- dimensions -----------------------------------------------------------

    def fetch_current_dimension_states(
        self, candidates: Sequence[DimensionCandidate], *, replay_run_id: Optional[str] = None
    ) -> Dict[Tuple[str, Tuple[str, ...]], Optional[Dict[str, Any]]]:
        out: Dict[Tuple[str, Tuple[str, ...]], Optional[Dict[str, Any]]] = {}
        by_target: Dict[str, List[DimensionCandidate]] = {}
        for c in candidates:
            by_target.setdefault(c.target_table, []).append(c)
        for target, group in by_target.items():
            if target not in MAIN_DIM_TABLES:
                raise InvalidTargetTableError(f"Invalid dimension target: {target!r}")
            physical = self._resolve_dim_table(target, replay_run_id)
            if physical is None:
                for c in group:
                    out[(target, c.business_key)] = None
                continue
            key_cols = DIM_BUSINESS_KEY_COLUMNS[target]
            cols = sorted(TABLE_COLUMNS[target])
            keys = [_key_str(c.business_key) for c in group]
            sql = f"""
                SELECT {", ".join(cols)}
                FROM {self.database}.{physical} FINAL
                WHERE {_business_key_expr(key_cols)} IN {{keys:Array(String)}}
            """
            r = self.client.query(sql, parameters={"keys": keys})
            rows = _rows_to_dicts(r, cols)
            by_key = {tuple(str(row[c]) for c in key_cols): row for row in rows}
            for c in group:
                key_norm = tuple(str(v) for v in c.business_key)
                out[(target, c.business_key)] = by_key.get(key_norm)
        return out

    def find_exact_dimension_versions(
        self, candidates: Sequence[DimensionCandidate], *, replay_run_id: Optional[str] = None
    ) -> Dict[Tuple[str, Tuple[str, ...], str], bool]:
        out: Dict[Tuple[str, Tuple[str, ...], str], bool] = {}
        by_target: Dict[str, List[DimensionCandidate]] = {}
        for c in candidates:
            by_target.setdefault(c.target_table, []).append(c)
        for target, group in by_target.items():
            if target not in MAIN_DIM_TABLES:
                raise InvalidTargetTableError(f"Invalid dimension target: {target!r}")
            physical = self._resolve_dim_table(target, replay_run_id)
            if physical is None:
                for c in group:
                    out[(target, c.business_key, c.source_hash)] = False
                continue
            key_cols = DIM_BUSINESS_KEY_COLUMNS[target]
            keys = [_key_str(c.business_key) for c in group]
            sql = f"""
                SELECT {", ".join(key_cols)}
                FROM {self.database}.{physical}
                WHERE {_business_key_expr(key_cols)} IN {{keys:Array(String)}}
            """
            r = self.client.query(sql, parameters={"keys": keys})
            rows = _rows_to_dicts(r, key_cols)
            existing_keys = {tuple(str(row[c]) for c in key_cols) for row in rows}
            for c in group:
                key_norm = tuple(str(v) for v in c.business_key)
                out[(target, c.business_key, c.source_hash)] = key_norm in existing_keys
        return out

    def insert_dimension_batch(
        self, target: str, rows: Sequence[Any], *, replay_run_id: Optional[str] = None
    ) -> WriteReceipt:
        if target not in MAIN_DIM_TABLES:
            raise InvalidTargetTableError(f"Invalid dimension target: {target!r}")
        if not rows:
            return WriteReceipt((), (), ())
        self._guard_mode(replay=bool(replay_run_id))
        physical = self._resolve_dim_table(target, replay_run_id)
        if physical is None:
            raise InvalidTargetTableError(
                f"No replay table for {target!r}; candidate must be suppressed, not inserted"
            )
        model_cls = DIM_MODEL_BY_TABLE[target]
        cols = [f.name for f in fields(model_cls)]
        data = [_row_values(r, cols) for r in rows]
        if replay_run_id:
            cols = cols + ["replay_run_id"]
            data = [row + [replay_run_id] for row in data]
        key_cols = DIM_BUSINESS_KEY_COLUMNS[target]
        attempted = tuple(_key_str(tuple(getattr(r, c) for c in key_cols)) for r in rows)
        self._write(physical, data, cols)
        return WriteReceipt(attempted, attempted, ())

    # -- quarantine -------------------------------------------------------------

    def find_quarantine_ids(
        self, source_ids: Sequence[str], *, replay_run_id: Optional[str] = None
    ) -> Set[str]:
        if not source_ids:
            return set()
        physical = QUARANTINE_REPLAY_TABLE if replay_run_id else QUARANTINE_TABLE
        sql = f"""
            SELECT silver_quarantine_id FROM {self.database}.{physical}
            WHERE source_bronze_event_id IN {{ids:Array(String)}}
        """
        r = self.client.query(sql, parameters={"ids": list(source_ids)})
        return {str(row[0]) for row in r.result_rows}

    def insert_quarantine_batch(
        self, rows: Sequence[SilverQuarantineEntry], *, replay_run_id: Optional[str] = None
    ) -> WriteReceipt:
        if not rows:
            return WriteReceipt((), (), ())
        self._guard_mode(replay=bool(replay_run_id))
        physical = QUARANTINE_REPLAY_TABLE if replay_run_id else QUARANTINE_TABLE
        cols = [f.name for f in fields(SilverQuarantineEntry)]
        data = [_row_values(r, cols) for r in rows]
        if replay_run_id:
            cols = cols + ["replay_run_id"]
            data = [row + [replay_run_id] for row in data]
        attempted = tuple(r.silver_quarantine_id for r in rows)
        self._write(physical, data, cols)
        return WriteReceipt(attempted, attempted, ())

    # -- offsets ------------------------------------------------------------

    def source_max_offset(self, stream: SourceStream) -> Optional[int]:
        sql = f"""
            SELECT max(offset) FROM {self.database}.{stream.source_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
        """
        r = self.client.query(
            sql, parameters={"topic": stream.topic, "part": int(stream.partition)}
        )
        if not r.result_rows or r.result_rows[0][0] is None:
            return None
        return int(r.result_rows[0][0])
