"""Gold 3 runtime SQLite store — Appendix Q schema with generation CAS.

Tables are exactly ``gold_runtime_checkpoint``, ``gold_runtime_window_state``,
``gold_runtime_work_unit`` and ``gold_runtime_lease``. WAL, ``synchronous=FULL``,
foreign keys, busy timeout and ``BEGIN IMMEDIATE`` are mandatory; terminal states
are immutable and no value is ever blind-overwritten.
"""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from de.gold_runtime import RUNTIME_MIGRATION_VERSION
from de.gold_runtime.config import (
    TERMINAL_WORK_UNIT_STATES,
    CasResult,
    WindowState,
    WorkUnitState,
)
from de.gold_runtime.window_scheduler import WindowStateError, assert_transition

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS gold_runtime_checkpoint (
    namespace TEXT NOT NULL,
    source_name TEXT NOT NULL,
    cursor_json TEXT NOT NULL,
    generation INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (namespace, source_name)
);

CREATE TABLE IF NOT EXISTS gold_runtime_window_state (
    namespace TEXT NOT NULL,
    window_id TEXT NOT NULL,
    revision_seq INTEGER NOT NULL,
    state TEXT NOT NULL,
    watermark REAL,
    batch_id TEXT,
    source_set_hash TEXT,
    output_digest TEXT,
    attempt_count INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (namespace, window_id, revision_seq)
);

CREATE TABLE IF NOT EXISTS gold_runtime_work_unit (
    batch_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    window_id TEXT NOT NULL,
    revision_seq INTEGER NOT NULL,
    state TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    expected_manifest_json TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_runtime_lease (
    namespace TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    generation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_runtime_migration (
    migration_version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);
"""

RUNTIME_TABLES: tuple[str, ...] = (
    "gold_runtime_checkpoint",
    "gold_runtime_window_state",
    "gold_runtime_work_unit",
    "gold_runtime_lease",
)


class CheckpointError(Exception):
    """Base runtime-store failure."""


class CheckpointBusyError(CheckpointError):
    """Transient SQLite busy/locked."""


class CheckpointCasConflictError(CheckpointError):
    """Permanent CAS conflict; never resolved by overwriting."""


class TerminalStateImmutableError(CheckpointError):
    """A terminal work-unit or window state cannot be mutated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class CheckpointRow:
    namespace: str
    source_name: str
    cursor_json: str
    generation: int
    updated_at: str


@dataclass(frozen=True)
class WindowStateRow:
    namespace: str
    window_id: str
    revision_seq: int
    state: str
    watermark: Optional[float]
    batch_id: Optional[str]
    source_set_hash: Optional[str]
    output_digest: Optional[str]
    attempt_count: int
    updated_at: str


@dataclass(frozen=True)
class WorkUnitRow:
    batch_id: str
    namespace: str
    window_id: str
    revision_seq: int
    state: str
    input_digest: str
    expected_manifest_json: str
    attempt_count: int
    last_error: Optional[str]
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class LeaseRow:
    namespace: str
    owner_id: str
    lease_token: str
    acquired_at: str
    expires_at: str
    generation: int


class GoldRuntimeStore:
    """SQLite authority for non-terminal runtime state, cursors and leases."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        if str(self.db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    # -- lifecycle ------------------------------------------------------------

    def open(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            self._conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, isolation_level=None
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.executescript(SCHEMA)
            self._conn.execute(
                "INSERT INTO gold_runtime_migration (migration_version, applied_at) "
                "VALUES (?,?) ON CONFLICT(migration_version) DO NOTHING",
                (RUNTIME_MIGRATION_VERSION, _utc_now()),
            )

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise CheckpointError("runtime store not open")
        return self._conn

    def is_readable(self) -> bool:
        try:
            with self._lock:
                self.conn.execute("SELECT 1 FROM gold_runtime_checkpoint LIMIT 1")
            return True
        except Exception:
            return False

    def pragma(self, name: str) -> Any:
        with self._lock:
            row = self.conn.execute(f"PRAGMA {name}").fetchone()
            return None if row is None else row[0]

    def migration_version(self) -> str:
        with self._lock:
            row = self.conn.execute(
                "SELECT migration_version FROM gold_runtime_migration LIMIT 1"
            ).fetchone()
            return "" if row is None else str(row["migration_version"])

    # -- cursors ---------------------------------------------------------------

    def get_cursor(self, namespace: str, source_name: str) -> Optional[CheckpointRow]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM gold_runtime_checkpoint WHERE namespace=? AND source_name=?",
                (namespace, source_name),
            ).fetchone()
            return None if row is None else CheckpointRow(**dict(row))

    def initialize_cursor(
        self, namespace: str, source_name: str, cursor_json: str
    ) -> CheckpointRow:
        with self._lock:
            self._execute(
                "INSERT INTO gold_runtime_checkpoint "
                "(namespace, source_name, cursor_json, generation, updated_at) "
                "VALUES (?,?,?,0,?) "
                "ON CONFLICT(namespace, source_name) DO NOTHING",
                (namespace, source_name, cursor_json, _utc_now()),
            )
            row = self.get_cursor(namespace, source_name)
            if row is None:
                raise CheckpointError("initialize_cursor produced no row")
            return row

    def compare_and_advance_cursor(
        self,
        namespace: str,
        source_name: str,
        *,
        expected_generation: int,
        cursor_json: str,
    ) -> CasResult:
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cursor = self.conn.execute(
                    "UPDATE gold_runtime_checkpoint "
                    "SET cursor_json=?, generation=generation+1, updated_at=? "
                    "WHERE namespace=? AND source_name=? AND generation=?",
                    (cursor_json, _utc_now(), namespace, source_name, int(expected_generation)),
                )
                if cursor.rowcount == 1:
                    self.conn.execute("COMMIT")
                    return CasResult.ADVANCED
                current = self.conn.execute(
                    "SELECT cursor_json, generation FROM gold_runtime_checkpoint "
                    "WHERE namespace=? AND source_name=?",
                    (namespace, source_name),
                ).fetchone()
                self.conn.execute("COMMIT")
                if current is None:
                    raise CheckpointCasConflictError("checkpoint row missing during CAS")
                if (
                    int(current["generation"]) == int(expected_generation) + 1
                    and str(current["cursor_json"]) == cursor_json
                ):
                    return CasResult.ALREADY_ADVANCED
                if int(current["generation"]) == int(expected_generation):
                    return CasResult.RETRY_SAME
                raise CheckpointCasConflictError(
                    f"CAS conflict on {source_name}: expected generation "
                    f"{expected_generation}, found {int(current['generation'])}"
                )
            except CheckpointCasConflictError:
                self._rollback()
                raise
            except sqlite3.OperationalError as exc:
                self._rollback()
                raise self._classify(exc) from exc

    # -- window state ------------------------------------------------------------

    def get_window_state(
        self, namespace: str, window_id: str, revision_seq: int
    ) -> Optional[WindowStateRow]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM gold_runtime_window_state "
                "WHERE namespace=? AND window_id=? AND revision_seq=?",
                (namespace, window_id, int(revision_seq)),
            ).fetchone()
            return None if row is None else WindowStateRow(**dict(row))

    def latest_window_state(self, namespace: str, window_id: str) -> Optional[WindowStateRow]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM gold_runtime_window_state "
                "WHERE namespace=? AND window_id=? ORDER BY revision_seq DESC LIMIT 1",
                (namespace, window_id),
            ).fetchone()
            return None if row is None else WindowStateRow(**dict(row))

    def upsert_window_state(
        self,
        namespace: str,
        window_id: str,
        revision_seq: int,
        *,
        state: WindowState,
        watermark: Optional[float] = None,
        batch_id: Optional[str] = None,
        source_set_hash: Optional[str] = None,
        output_digest: Optional[str] = None,
    ) -> WindowStateRow:
        with self._lock:
            self._execute(
                "INSERT INTO gold_runtime_window_state "
                "(namespace, window_id, revision_seq, state, watermark, batch_id, "
                " source_set_hash, output_digest, attempt_count, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,0,?) "
                "ON CONFLICT(namespace, window_id, revision_seq) DO NOTHING",
                (
                    namespace, window_id, int(revision_seq), state.value, watermark,
                    batch_id, source_set_hash, output_digest, _utc_now(),
                ),
            )
            row = self.get_window_state(namespace, window_id, revision_seq)
            if row is None:
                raise CheckpointError("upsert_window_state produced no row")
            return row

    def cas_window_state(
        self,
        namespace: str,
        window_id: str,
        revision_seq: int,
        *,
        expected_state: WindowState,
        new_state: WindowState,
        watermark: Optional[float] = None,
        batch_id: Optional[str] = None,
        source_set_hash: Optional[str] = None,
        output_digest: Optional[str] = None,
        increment_attempt: bool = False,
    ) -> CasResult:
        assert_transition(expected_state, new_state)
        with self._lock:
            current = self.get_window_state(namespace, window_id, revision_seq)
            if current is None:
                raise CheckpointCasConflictError("window state row missing during CAS")
            if current.state in {WindowState.CLOSED.value, WindowState.REVISED.value} and (
                new_state is not WindowState.REVISED
            ):
                raise TerminalStateImmutableError(
                    f"window {window_id} is {current.state}; a closed window never reopens"
                )
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cursor = self.conn.execute(
                    "UPDATE gold_runtime_window_state "
                    "SET state=?, watermark=COALESCE(?, watermark), "
                    "    batch_id=COALESCE(?, batch_id), "
                    "    source_set_hash=COALESCE(?, source_set_hash), "
                    "    output_digest=COALESCE(?, output_digest), "
                    "    attempt_count=attempt_count+?, updated_at=? "
                    "WHERE namespace=? AND window_id=? AND revision_seq=? AND state=?",
                    (
                        new_state.value, watermark, batch_id, source_set_hash, output_digest,
                        1 if increment_attempt else 0, _utc_now(),
                        namespace, window_id, int(revision_seq), expected_state.value,
                    ),
                )
                if cursor.rowcount == 1:
                    self.conn.execute("COMMIT")
                    return CasResult.ADVANCED
                latest = self.conn.execute(
                    "SELECT state FROM gold_runtime_window_state "
                    "WHERE namespace=? AND window_id=? AND revision_seq=?",
                    (namespace, window_id, int(revision_seq)),
                ).fetchone()
                self.conn.execute("COMMIT")
                if latest is None:
                    raise CheckpointCasConflictError("window state row missing during CAS")
                if str(latest["state"]) == new_state.value:
                    return CasResult.ALREADY_ADVANCED
                if str(latest["state"]) == expected_state.value:
                    return CasResult.RETRY_SAME
                raise CheckpointCasConflictError(
                    f"window CAS conflict: expected {expected_state.value}, "
                    f"found {latest['state']}"
                )
            except (CheckpointCasConflictError, WindowStateError):
                self._rollback()
                raise
            except sqlite3.OperationalError as exc:
                self._rollback()
                raise self._classify(exc) from exc

    def max_revision_seq(self, namespace: str, window_id: str) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT max(revision_seq) AS mx FROM gold_runtime_window_state "
                "WHERE namespace=? AND window_id=?",
                (namespace, window_id),
            ).fetchone()
            return -1 if row is None or row["mx"] is None else int(row["mx"])

    def closed_window_ends(self, namespace: str) -> tuple[str, ...]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT DISTINCT window_id FROM gold_runtime_window_state "
                "WHERE namespace=? AND state IN (?,?)",
                (namespace, WindowState.CLOSED.value, WindowState.REVISED.value),
            ).fetchall()
            return tuple(str(row["window_id"]) for row in rows)

    # -- work units ---------------------------------------------------------------

    def get_work_unit(self, batch_id: str) -> Optional[WorkUnitRow]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM gold_runtime_work_unit WHERE batch_id=?", (batch_id,)
            ).fetchone()
            return None if row is None else WorkUnitRow(**dict(row))

    def upsert_work_unit(
        self,
        *,
        batch_id: str,
        namespace: str,
        window_id: str,
        revision_seq: int,
        state: WorkUnitState,
        input_digest: str,
        expected_manifest_json: str,
    ) -> WorkUnitRow:
        now = _utc_now()
        with self._lock:
            self._execute(
                "INSERT INTO gold_runtime_work_unit "
                "(batch_id, namespace, window_id, revision_seq, state, input_digest, "
                " expected_manifest_json, attempt_count, last_error, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,0,'',?,?) "
                "ON CONFLICT(batch_id) DO NOTHING",
                (
                    batch_id, namespace, window_id, int(revision_seq), state.value,
                    input_digest, expected_manifest_json, now, now,
                ),
            )
            row = self.get_work_unit(batch_id)
            if row is None:
                raise CheckpointError("upsert_work_unit produced no row")
            return row

    def set_work_unit_state(
        self,
        batch_id: str,
        state: WorkUnitState,
        *,
        last_error: str = "",
        expected_manifest_json: Optional[str] = None,
        increment_attempt: bool = False,
    ) -> WorkUnitRow:
        with self._lock:
            current = self.get_work_unit(batch_id)
            if current is None:
                raise CheckpointError(f"unknown work unit {batch_id}")
            if WorkUnitState(current.state) in TERMINAL_WORK_UNIT_STATES:
                if WorkUnitState(current.state) is state:
                    return current
                raise TerminalStateImmutableError(
                    f"work unit {batch_id} is terminal in {current.state}"
                )
            self._execute(
                "UPDATE gold_runtime_work_unit "
                "SET state=?, last_error=?, "
                "    expected_manifest_json=COALESCE(?, expected_manifest_json), "
                "    attempt_count=attempt_count+?, updated_at=? "
                "WHERE batch_id=?",
                (
                    state.value, last_error, expected_manifest_json,
                    1 if increment_attempt else 0, _utc_now(), batch_id,
                ),
            )
            row = self.get_work_unit(batch_id)
            if row is None:
                raise CheckpointError("set_work_unit_state produced no row")
            return row

    def non_terminal_work_units(self, namespace: str) -> tuple[WorkUnitRow, ...]:
        terminal = [state.value for state in TERMINAL_WORK_UNIT_STATES]
        placeholders = ",".join("?" for _ in terminal)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM gold_runtime_work_unit "
                f"WHERE namespace=? AND state NOT IN ({placeholders}) "
                "ORDER BY created_at, batch_id",
                (namespace, *terminal),
            ).fetchall()
            return tuple(WorkUnitRow(**dict(row)) for row in rows)

    # -- lease -----------------------------------------------------------------

    def get_lease(self, namespace: str) -> Optional[LeaseRow]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM gold_runtime_lease WHERE namespace=?", (namespace,)
            ).fetchone()
            return None if row is None else LeaseRow(**dict(row))

    def write_lease(
        self,
        namespace: str,
        *,
        owner_id: str,
        lease_token: str,
        acquired_at: str,
        expires_at: str,
    ) -> LeaseRow:
        with self._lock:
            self._execute(
                "INSERT INTO gold_runtime_lease "
                "(namespace, owner_id, lease_token, acquired_at, expires_at, generation) "
                "VALUES (?,?,?,?,?,0) "
                "ON CONFLICT(namespace) DO UPDATE SET "
                "  owner_id=excluded.owner_id, lease_token=excluded.lease_token, "
                "  acquired_at=excluded.acquired_at, expires_at=excluded.expires_at, "
                "  generation=gold_runtime_lease.generation+1",
                (namespace, owner_id, lease_token, acquired_at, expires_at),
            )
            row = self.get_lease(namespace)
            if row is None:
                raise CheckpointError("write_lease produced no row")
            return row

    def release_lease(self, namespace: str, lease_token: str) -> bool:
        with self._lock:
            cursor = self._execute(
                "DELETE FROM gold_runtime_lease WHERE namespace=? AND lease_token=?",
                (namespace, lease_token),
            )
            return cursor.rowcount == 1

    # -- helpers -----------------------------------------------------------------

    def _execute(self, sql: str, parameters: Sequence[Any]) -> sqlite3.Cursor:
        try:
            return self.conn.execute(sql, tuple(parameters))
        except sqlite3.OperationalError as exc:
            raise self._classify(exc) from exc

    def _rollback(self) -> None:
        try:
            self.conn.execute("ROLLBACK")
        except Exception:
            pass

    @staticmethod
    def _classify(exc: sqlite3.OperationalError) -> CheckpointError:
        text = str(exc).lower()
        if "locked" in text or "busy" in text:
            return CheckpointBusyError(str(exc))
        return CheckpointError(str(exc))
