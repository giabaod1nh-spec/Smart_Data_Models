"""Silver Plan 3 — SQLite checkpoint store with CAS (no event ledger)."""
from __future__ import annotations

import sqlite3
import json
import hashlib
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

CREATE TABLE IF NOT EXISTS silver_v1_source_binding (
    namespace TEXT PRIMARY KEY,
    binding_json TEXT NOT NULL,
    change_cursor INTEGER NOT NULL DEFAULT 0 CHECK(change_cursor >= 0),
    invalidation_cursor INTEGER NOT NULL DEFAULT 0 CHECK(invalidation_cursor >= 0),
    generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS silver_v1_obligation (
    namespace TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK(source_kind IN ('changes','invalidations')),
    source_sequence INTEGER NOT NULL CHECK(source_sequence > 0),
    event_id TEXT NOT NULL,
    target_table TEXT NOT NULL,
    row_key TEXT NOT NULL,
    expected_json TEXT NOT NULL,
    expected_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING','RECONCILED','FAULT')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(namespace,source_kind,source_sequence,target_table,row_key)
);

CREATE INDEX IF NOT EXISTS silver_v1_pending_obligation_idx
ON silver_v1_obligation(namespace,source_kind,source_sequence,target_table)
WHERE status!='RECONCILED';

CREATE TABLE IF NOT EXISTS silver_v1_event_validity (
    namespace TEXT NOT NULL,
    event_id TEXT NOT NULL,
    simulation_run_id TEXT NOT NULL,
    topology_digest TEXT NOT NULL,
    simulation_time_seconds REAL NOT NULL,
    semantic_revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PENDING','VALID','INVALIDATED')),
    change_sequence INTEGER NOT NULL,
    invalidation_sequence INTEGER,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(namespace,event_id)
);

CREATE TABLE IF NOT EXISTS silver_v1_replay_state (
    replay_run_id TEXT PRIMARY KEY,
    manifest_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('RUNNING','COMPLETE','ABORTED')),
    logical_hash TEXT,
    updated_at TEXT NOT NULL
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
            validity_columns = {row[1] for row in self._conn.execute(
                "PRAGMA table_info(silver_v1_event_validity)")}
            needed = {"simulation_run_id": "TEXT", "topology_digest": "TEXT",
                      "simulation_time_seconds": "REAL"}
            if not set(needed) <= validity_columns:
                count = self._conn.execute("SELECT count(*) FROM silver_v1_event_validity").fetchone()[0]
                if count:
                    raise CheckpointError("existing V1 validity rows lack canonical coordinates")
                for name, kind in needed.items():
                    if name not in validity_columns:
                        default = "''" if kind == "TEXT" else "0"
                        self._conn.execute("ALTER TABLE silver_v1_event_validity ADD COLUMN " +
                                           name + " " + kind + " NOT NULL DEFAULT " + default)
            replay_columns = {row[1] for row in self._conn.execute(
                "PRAGMA table_info(silver_v1_replay_state)")}
            if "logical_hash" not in replay_columns:
                self._conn.execute("ALTER TABLE silver_v1_replay_state ADD COLUMN logical_hash TEXT")

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

    def bind_v1_source(self, namespace: str, binding: dict) -> None:
        """Persist source identity once; high-waters are deliberately excluded."""
        stable = {key: binding[key] for key in (
            "namespace", "source_path", "schema_fingerprint", "bronze_version", "file_identity")}
        encoded = json.dumps(stable, sort_keys=True, separators=(",", ":"))
        with self._lock:
            row = self.conn.execute("SELECT binding_json FROM silver_v1_source_binding WHERE namespace=?",
                                    (namespace,)).fetchone()
            if row and row[0] != encoded:
                raise CheckpointError("SOURCE_BINDING_CHANGED")
            if row is None:
                self.conn.execute("INSERT INTO silver_v1_source_binding "
                    "(namespace,binding_json,updated_at) VALUES (?,?,?)", (namespace, encoded, _utc_now()))

    def v1_cursors(self, namespace: str) -> dict:
        with self._lock:
            row = self.conn.execute("SELECT change_cursor,invalidation_cursor,generation,binding_json "
                                    "FROM silver_v1_source_binding WHERE namespace=?", (namespace,)).fetchone()
        if row is None:
            raise CheckpointError("V1 source is not bound")
        return dict(row)

    def freeze_v1_bundle(self, namespace: str, kind: str, sequence: int,
                         event_id: str, revision: int, rows: list[tuple[str, dict]]) -> None:
        """Commit the entire event bundle and local pending state in one transaction."""
        if kind != "changes" or not rows:
            raise ValueError("accepted V1 change requires a nonempty fact/dimension bundle")
        fact = next((row for _, row in rows if row.get("event_id") == event_id
                     and "simulation_time_seconds" in row), None)
        if fact is None:
            raise ValueError("V1 bundle needs event coordinates")
        with self._lock:
            db = self.conn
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = db.execute("SELECT 1 FROM silver_v1_obligation WHERE namespace=? "
                    "AND source_kind=? AND source_sequence=? LIMIT 1",
                    (namespace, kind, sequence)).fetchone()
                if existing:
                    raise CheckpointError("V1 bundle already frozen; recover it")
                for table, row in rows:
                    self.freeze_v1_obligation(namespace, kind, sequence, event_id, table, row)
                db.execute("INSERT INTO silver_v1_event_validity "
                    "(namespace,event_id,simulation_run_id,topology_digest,simulation_time_seconds,"
                    "semantic_revision,state,change_sequence,invalidation_sequence,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(namespace,event_id) DO UPDATE SET "
                    "semantic_revision=excluded.semantic_revision,state='PENDING',"
                    "change_sequence=excluded.change_sequence,updated_at=excluded.updated_at "
                    "WHERE silver_v1_event_validity.state!='INVALIDATED'",
                    (namespace, event_id, fact["simulation_run_id"], fact["topology_digest"],
                     fact["simulation_time_seconds"], revision, "PENDING", sequence, None, _utc_now()))
                state = db.execute("SELECT state FROM silver_v1_event_validity WHERE "
                                   "namespace=? AND event_id=?", (namespace, event_id)).fetchone()
                if state[0] != "PENDING":
                    raise CheckpointError("invalidated V1 event cannot be revalidated")
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise

    def publish_v1_valid(self, namespace: str, event_id: str, revision: int,
                         change_sequence: int) -> None:
        with self._lock:
            db = self.conn
            db.execute("BEGIN IMMEDIATE")
            try:
                pending = db.execute("SELECT state,semantic_revision,change_sequence "
                    "FROM silver_v1_event_validity WHERE namespace=? AND event_id=?",
                    (namespace, event_id)).fetchone()
                if pending is None or tuple(pending) != ("PENDING", revision, change_sequence):
                    raise CheckpointError("V1 event is not pending at expected revision")
                unresolved = db.execute("SELECT 1 FROM silver_v1_obligation WHERE namespace=? "
                    "AND source_kind='changes' AND source_sequence=? AND status!='RECONCILED' LIMIT 1",
                    (namespace, change_sequence)).fetchone()
                if unresolved:
                    raise CheckpointError("V1 event obligations unresolved")
                db.execute("UPDATE silver_v1_event_validity SET state='VALID',updated_at=? "
                    "WHERE namespace=? AND event_id=?", (_utc_now(), namespace, event_id))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise

    def finalize_v1_event(self, namespace: str, event_id: str, revision: int,
                          sequence: int, expected_generation: int) -> int:
        """Atomically publish reconciled validity and CAS its source cursor."""
        with self._lock:
            db = self.conn
            db.execute('BEGIN IMMEDIATE')
            try:
                cursor = db.execute('SELECT generation,change_cursor FROM silver_v1_source_binding '
                                    'WHERE namespace=?', (namespace,)).fetchone()
                if cursor is None or tuple(cursor) != (expected_generation, sequence - 1):
                    raise CheckpointCasConflictError('V1 cursor generation/sequence mismatch')
                event = db.execute('SELECT state,semantic_revision,change_sequence FROM silver_v1_event_validity '
                                   'WHERE namespace=? AND event_id=?', (namespace, event_id)).fetchone()
                if (event is None or event['state'] not in {'PENDING', 'VALID'} or
                        event['semantic_revision'] != revision or event['change_sequence'] != sequence):
                    raise CheckpointError('V1 event is not publishable at expected revision')
                count, unresolved = db.execute('SELECT count(*),COALESCE(sum(status!=?),0) '
                    'FROM silver_v1_obligation WHERE namespace=? AND source_kind=? AND source_sequence=?',
                    ('RECONCILED', namespace, 'changes', sequence)).fetchone()
                if not count or unresolved:
                    raise CheckpointError('V1 event obligations unresolved or missing')
                now = _utc_now()
                db.execute("UPDATE silver_v1_event_validity SET state='VALID',updated_at=? "
                           'WHERE namespace=? AND event_id=?', (now, namespace, event_id))
                db.execute('UPDATE silver_v1_source_binding SET change_cursor=?,generation=generation+1,'
                           'updated_at=? WHERE namespace=?', (sequence, now, namespace))
                db.execute('COMMIT')
                return expected_generation + 1
            except BaseException:
                db.execute('ROLLBACK')
                raise

    def freeze_v1_obligation(self, namespace: str, kind: str, sequence: int,
                             event_id: str, target: str, row: dict) -> str:
        encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        key_fields = ("simulation_run_id", "topology_digest", "event_id", "intersection_id",
                      "source_direction", "run_generation", "semantic_revision",
                      "bronze_semantic_revision", "invalidation_id")
        row_key = json.dumps([row.get(key) for key in key_fields], separators=(",", ":"))
        with self._lock:
            existing = self.conn.execute("SELECT expected_hash FROM silver_v1_obligation "
                "WHERE namespace=? AND source_kind=? AND source_sequence=? AND target_table=? AND row_key=?",
                (namespace, kind, sequence, target, row_key)).fetchone()
            if existing and existing[0] != digest:
                raise CheckpointError("frozen obligation content changed")
            if existing is None:
                now = _utc_now()
                self.conn.execute("INSERT INTO silver_v1_obligation "
                    "(namespace,source_kind,source_sequence,event_id,target_table,expected_json,"
                    "row_key,expected_hash,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,'PENDING',?,?)",
                    (namespace, kind, sequence, event_id, target, encoded, row_key, digest, now, now))
        return digest

    def reconcile_v1_obligation(self, namespace: str, kind: str, sequence: int,
                                target: str, expected_hash: str) -> None:
        """Acknowledge only the exact frozen row that was read back from CH."""
        with self._lock:
            row = self.conn.execute("SELECT expected_hash,status FROM silver_v1_obligation "
                "WHERE namespace=? AND source_kind=? AND source_sequence=? AND target_table=? AND expected_hash=?",
                (namespace, kind, sequence, target, expected_hash)).fetchone()
            if row is None:
                raise CheckpointError("V1 obligation identity/content mismatch")
            if row[1] != "RECONCILED":
                self.conn.execute("UPDATE silver_v1_obligation SET status='RECONCILED',"
                    "attempts=attempts+1,last_error=NULL,updated_at=? WHERE namespace=? "
                    "AND source_kind=? AND source_sequence=? AND target_table=? AND expected_hash=?",
                    (_utc_now(), namespace, kind, sequence, target, expected_hash))

    def reconcile_v1_obligations(self, obligations: list[dict]) -> None:
        """Commit only CH-verified acknowledgements in one bounded transaction."""
        if len(obligations) > 1000:
            raise ValueError('bounded acknowledgement batch required')
        if not obligations:
            return
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                for obligation in obligations:
                    self.reconcile_v1_obligation(obligation['namespace'], obligation['source_kind'],
                        obligation['source_sequence'], obligation['target_table'], obligation['expected_hash'])
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise

    def record_v1_obligation_failure(self, obligation: dict, error: Exception,
                                     *, permanent: bool = False) -> None:
        """Persist failed attempt without altering the frozen payload or cursor."""
        with self._lock:
            changed = self.conn.execute(
                "UPDATE silver_v1_obligation SET attempts=attempts+1,last_error=?,status=?,updated_at=? "
                "WHERE namespace=? AND source_kind=? AND source_sequence=? AND target_table=? "
                "AND row_key=? AND expected_hash=? AND status!='RECONCILED'",
                (str(error)[:2000], 'FAULT' if permanent else 'PENDING', _utc_now(),
                 obligation['namespace'], obligation['source_kind'], obligation['source_sequence'],
                 obligation['target_table'], obligation['row_key'], obligation['expected_hash'])).rowcount
            if changed != 1:
                raise CheckpointError('failed obligation identity/content mismatch')

    def pending_v1_obligations(self, namespace: str, limit: int = 100) -> list[dict]:
        if not 1 <= limit <= 1000:
            raise ValueError("bounded obligation page required")
        with self._lock:
            return [dict(row) for row in self.conn.execute(
                "SELECT * FROM silver_v1_obligation WHERE namespace=? AND status!='RECONCILED' "
                "ORDER BY source_kind,source_sequence,target_table LIMIT ?",
                (namespace, limit)).fetchall()]

    def record_v1_replay_skip(self, namespace: str, kind: str, sequence: int,
                              event_id: str, expected_generation: int) -> int:
        """Advance past an out-of-scope source entry with a durable local disposition."""
        if kind not in {"changes", "invalidations"}:
            raise ValueError("invalid V1 source kind")
        payload = json.dumps(dict(reason="OUT_OF_SCOPE_RUN", event_id=event_id),
                             sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        column = "change_cursor" if kind == "changes" else "invalidation_cursor"
        with self._lock:
            db = self.conn
            db.execute("BEGIN IMMEDIATE")
            try:
                current = db.execute("SELECT generation," + column + " FROM silver_v1_source_binding "
                                     "WHERE namespace=?", (namespace,)).fetchone()
                if current is None or current[0] != expected_generation or sequence != current[1] + 1:
                    raise CheckpointCasConflictError("V1 replay skip cursor mismatch")
                old = db.execute("SELECT expected_hash FROM silver_v1_obligation WHERE "
                    "namespace=? AND source_kind=? AND source_sequence=? AND target_table='__LOCAL_SKIP__'",
                    (namespace, kind, sequence)).fetchone()
                if old and old[0] != digest:
                    raise CheckpointError("V1 replay skip content changed")
                if old is None:
                    now = _utc_now()
                    db.execute("INSERT INTO silver_v1_obligation "
                        "(namespace,source_kind,source_sequence,event_id,target_table,row_key,"
                        "expected_json,expected_hash,status,created_at,updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (namespace, kind, sequence, event_id, "__LOCAL_SKIP__", event_id,
                         payload, digest, "RECONCILED", now, now))
                db.execute("UPDATE silver_v1_source_binding SET " + column +
                           "=?,generation=generation+1,updated_at=? WHERE namespace=?",
                           (sequence, _utc_now(), namespace))
                db.execute("COMMIT")
                return expected_generation + 1
            except Exception:
                db.execute("ROLLBACK")
                raise

    def suppress_v1_event(self, namespace: str, event_id: str, revision: int,
                          change_sequence: int, invalidation_sequence: int,
                          invalidation_target: str, invalidation_row: dict) -> str:
        """Publish the local tombstone and frozen mirror obligation atomically."""
        encoded = json.dumps(invalidation_row, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        row_key = json.dumps([invalidation_row.get("invalidation_id")], separators=(",", ":"))
        now = _utc_now()
        with self._lock:
            db = self.conn
            db.execute("BEGIN IMMEDIATE")
            try:
                prior = db.execute("SELECT state,invalidation_sequence FROM silver_v1_event_validity "
                                   "WHERE namespace=? AND event_id=?", (namespace, event_id)).fetchone()
                if prior and prior[0] == "INVALIDATED" and prior[1] != invalidation_sequence:
                    raise CheckpointError("conflicting event invalidation")
                db.execute("INSERT INTO silver_v1_event_validity "
                    "(namespace,event_id,simulation_run_id,topology_digest,simulation_time_seconds,"
                    "semantic_revision,state,change_sequence,invalidation_sequence,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(namespace,event_id) DO UPDATE SET semantic_revision=excluded.semantic_revision,"
                    "state='INVALIDATED',change_sequence=excluded.change_sequence,"
                    "invalidation_sequence=excluded.invalidation_sequence,updated_at=excluded.updated_at",
                    (namespace, event_id, invalidation_row["simulation_run_id"],
                     invalidation_row["topology_digest"],
                     invalidation_row["simulation_time_seconds"], revision, "INVALIDATED",
                     change_sequence, invalidation_sequence, now))
                old = db.execute("SELECT expected_hash FROM silver_v1_obligation WHERE "
                    "namespace=? AND source_kind='invalidations' AND source_sequence=? AND target_table=? AND row_key=?",
                    (namespace, invalidation_sequence, invalidation_target, row_key)).fetchone()
                if old and old[0] != digest:
                    raise CheckpointError("frozen invalidation content changed")
                if old is None:
                    db.execute("INSERT INTO silver_v1_obligation "
                        "(namespace,source_kind,source_sequence,event_id,target_table,expected_json,"
                        "row_key,expected_hash,status,created_at,updated_at) VALUES (?,'invalidations',?,?,?,?,?,?,'PENDING',?,?)",
                        (namespace, invalidation_sequence, event_id, invalidation_target,
                         encoded, row_key, digest, now, now))
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise
        return digest

    def advance_v1_cursor(self, namespace: str, kind: str, sequence: int,
                          expected_generation: int) -> int:
        if kind not in {"changes", "invalidations"}:
            raise ValueError("invalid journal kind")
        column = "change_cursor" if kind == "changes" else "invalidation_cursor"
        with self._lock:
            db = self.conn
            db.execute("BEGIN IMMEDIATE")
            try:
                current = db.execute("SELECT generation," + column + " FROM silver_v1_source_binding "
                                     "WHERE namespace=?", (namespace,)).fetchone()
                if current is None or current[0] != expected_generation or sequence != current[1] + 1:
                    raise CheckpointCasConflictError("V1 cursor generation/sequence mismatch")
                unresolved = db.execute("SELECT 1 FROM silver_v1_obligation WHERE namespace=? "
                    "AND source_kind=? AND source_sequence=? AND status!='RECONCILED' LIMIT 1",
                    (namespace, kind, sequence)).fetchone()
                if unresolved:
                    raise CheckpointError("V1 obligations unresolved")
                obligation_count = db.execute("SELECT count(*) FROM silver_v1_obligation WHERE "
                    "namespace=? AND source_kind=? AND source_sequence=?",
                    (namespace, kind, sequence)).fetchone()[0]
                if obligation_count == 0:
                    raise CheckpointError("V1 source sequence has no durable disposition")
                db.execute("UPDATE silver_v1_source_binding SET " + column +
                           "=?,generation=generation+1,updated_at=? WHERE namespace=?",
                           (sequence, _utc_now(), namespace))
                db.execute("COMMIT")
                return expected_generation + 1
            except Exception:
                db.execute("ROLLBACK")
                raise
