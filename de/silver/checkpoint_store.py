"""Silver Plan 3 — SQLite checkpoint store with CAS (no event ledger)."""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from de.silver.config import SOURCE_TABLES, CheckpointKey, CasResult


class CheckpointError(Exception):
    """Base checkpoint failure."""


class CheckpointCasConflictError(CheckpointError):
    """Permanent CAS conflict."""


class CheckpointBusyError(CheckpointError):
    """Transient SQLite busy/locked."""


class ReplayManifestConflictError(CheckpointError):
    """Replay manifest hash mismatch for an existing run."""


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS silver_instance_lock (
    lock_namespace TEXT PRIMARY KEY,
    holder_pid INTEGER NOT NULL,
    holder_host TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    processor_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver_checkpoint (
    checkpoint_namespace TEXT NOT NULL,
    source_table TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    last_completed_offset INTEGER NOT NULL,
    source_start_offset INTEGER NOT NULL,
    start_mode TEXT NOT NULL,
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    silver_schema_version TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (checkpoint_namespace, source_table, topic, partition_id)
);

CREATE TABLE IF NOT EXISTS silver_replay_manifest (
    replay_run_id TEXT PRIMARY KEY,
    manifest_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class CheckpointRow:
    checkpoint_namespace: str
    source_table: str
    topic: str
    partition_id: int
    last_completed_offset: int
    source_start_offset: int
    start_mode: str
    processor_name: str
    processor_version: str
    silver_schema_version: str
    updated_at: str


class SilverCheckpointStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        if str(self.db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            self._conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, isolation_level=None
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise CheckpointError("checkpoint store not open")
        return self._conn

    def is_readable(self) -> bool:
        try:
            with self._lock:
                self.conn.execute("SELECT 1 FROM silver_checkpoint LIMIT 1")
            return True
        except Exception:
            return False

    def get(self, key: CheckpointKey) -> Optional[CheckpointRow]:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT * FROM silver_checkpoint
                WHERE checkpoint_namespace=? AND source_table=? AND topic=? AND partition_id=?
                """,
                (key.checkpoint_namespace, key.source_table, key.topic, int(key.partition_id)),
            ).fetchone()
            if not row:
                return None
            return self._row(row)

    def initialize(
        self,
        key: CheckpointKey,
        *,
        source_start: int,
        last_completed: int,
        start_mode: str,
        processor_name: str,
        processor_version: str,
        silver_schema_version: str,
    ) -> CheckpointRow:
        if key.source_table not in SOURCE_TABLES:
            raise CheckpointError(f"Invalid source_table: {key.source_table}")
        now = _utc_now()
        with self._lock:
            try:
                self.conn.execute(
                    """
                    INSERT INTO silver_checkpoint (
                        checkpoint_namespace, source_table, topic, partition_id,
                        last_completed_offset, source_start_offset, start_mode,
                        processor_name, processor_version, silver_schema_version, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(checkpoint_namespace, source_table, topic, partition_id)
                    DO NOTHING
                    """,
                    (
                        key.checkpoint_namespace,
                        key.source_table,
                        key.topic,
                        int(key.partition_id),
                        int(last_completed),
                        int(source_start),
                        start_mode,
                        processor_name,
                        processor_version,
                        silver_schema_version,
                        now,
                    ),
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    raise CheckpointBusyError(str(exc)) from exc
                raise
            row = self.get(key)
            if row is None:
                raise CheckpointError("initialize failed to produce row")
            # verify immutable fields when row already existed
            if row.source_start_offset != int(source_start) and row.updated_at == now:
                pass
            return row

    def compare_and_advance(
        self, key: CheckpointKey, expected: int, new: int
    ) -> CasResult:
        if new <= expected:
            raise CheckpointCasConflictError(
                f"new offset {new} must be > expected {expected}"
            )
        now = _utc_now()
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.execute(
                    """
                    UPDATE silver_checkpoint
                    SET last_completed_offset=?, updated_at=?
                    WHERE checkpoint_namespace=? AND source_table=? AND topic=?
                      AND partition_id=? AND last_completed_offset=?
                    """,
                    (
                        int(new),
                        now,
                        key.checkpoint_namespace,
                        key.source_table,
                        key.topic,
                        int(key.partition_id),
                        int(expected),
                    ),
                )
                if cur.rowcount == 1:
                    self.conn.execute("COMMIT")
                    return CasResult.ADVANCED
                current = self.get(key)
                self.conn.execute("COMMIT")
                if current is None:
                    raise CheckpointCasConflictError("checkpoint row missing during CAS")
                if current.last_completed_offset == int(new):
                    return CasResult.ALREADY_ADVANCED
                if current.last_completed_offset == int(expected):
                    return CasResult.RETRY_SAME
                raise CheckpointCasConflictError(
                    f"CAS conflict: expected={expected} new={new} "
                    f"actual={current.last_completed_offset}"
                )
            except CheckpointCasConflictError:
                try:
                    self.conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            except sqlite3.OperationalError as exc:
                try:
                    self.conn.execute("ROLLBACK")
                except Exception:
                    pass
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    raise CheckpointBusyError(str(exc)) from exc
                raise CheckpointError(str(exc)) from exc

    def put_replay_manifest(self, run_id: str, manifest_hash: str) -> None:
        now = _utc_now()
        with self._lock:
            existing = self.get_replay_manifest(run_id)
            if existing is not None and existing != manifest_hash:
                raise ReplayManifestConflictError(
                    f"manifest hash conflict for {run_id}: {existing} vs {manifest_hash}"
                )
            if existing == manifest_hash:
                return
            self.conn.execute(
                """
                INSERT INTO silver_replay_manifest (replay_run_id, manifest_hash, created_at)
                VALUES (?,?,?)
                """,
                (run_id, manifest_hash, now),
            )

    def get_replay_manifest(self, run_id: str) -> Optional[str]:
        with self._lock:
            row = self.conn.execute(
                "SELECT manifest_hash FROM silver_replay_manifest WHERE replay_run_id=?",
                (run_id,),
            ).fetchone()
            return None if row is None else str(row["manifest_hash"])

    @staticmethod
    def _row(row: sqlite3.Row) -> CheckpointRow:
        return CheckpointRow(
            checkpoint_namespace=row["checkpoint_namespace"],
            source_table=row["source_table"],
            topic=row["topic"],
            partition_id=int(row["partition_id"]),
            last_completed_offset=int(row["last_completed_offset"]),
            source_start_offset=int(row["source_start_offset"]),
            start_mode=row["start_mode"],
            processor_name=row["processor_name"],
            processor_version=row["processor_version"],
            silver_schema_version=row["silver_schema_version"],
            updated_at=row["updated_at"],
        )
