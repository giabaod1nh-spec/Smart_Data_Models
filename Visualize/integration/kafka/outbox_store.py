"""SQLite durable outbox store (K-2b) — FULL sync, cycle-atomic append."""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from integration.kafka.outbox_schema import (
    EVENT_KIND_ENTITY,
    EVENT_KIND_RUN_STARTED,
    PENDING_STATUSES,
    REDRIVE_STATUSES,
    SCHEMA_SQL,
    STATUS_ACKED,
    STATUS_FAILED_PERMANENT,
    STATUS_FAILED_RETRYABLE,
    STATUS_OUTBOXED,
    STATUS_QUEUED,
)

log = logging.getLogger(__name__)


class _WriteGate:
    """Write mutex that lets the TraCI cycle append cut ahead of worker writes.

    SQLite serialises writers anyway; without an explicit fairness policy a hot
    delivery loop starves the append thread until `busy_timeout` expires. Worker
    transactions are batched and short, so an append waits for at most one of
    them.
    """

    def __init__(self) -> None:
        self._writer = threading.Lock()
        self._cv = threading.Condition()
        self._priority_waiting = 0

    @contextmanager
    def priority(self):
        with self._cv:
            self._priority_waiting += 1
        try:
            with self._writer:
                yield
        finally:
            with self._cv:
                self._priority_waiting -= 1
                self._cv.notify_all()

    @contextmanager
    def background(self):
        with self._cv:
            while self._priority_waiting:
                self._cv.wait(0.1)
        with self._writer:
            yield


class OutboxAppendError(Exception):
    """Cycle was not durable — TraCI must FAULT/pause."""


class OutboxDuplicateError(OutboxAppendError):
    """Duplicate event_id — cycle rolled back."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(frozen=True)
class OutboxRow:
    event_id: str
    simulation_run_id: str
    cycle_sequence: int
    entity_sequence: int
    event_key: str
    topic: str
    payload_json: str
    payload_hash: str
    event_kind: str = EVENT_KIND_ENTITY
    outbox_sequence: int = 0


def run_started_event_id(*, producer_session_id: str, simulation_run_id: str) -> str:
    raw = f"TrafficSimulationRunStarted|{producer_session_id}|{simulation_run_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def payload_hash_bytes(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class OutboxRecord:
    event_id: str
    simulation_run_id: str
    cycle_sequence: int
    entity_sequence: int
    event_key: str
    topic: str
    payload_json: str
    payload_hash: str
    event_kind: str
    outbox_sequence: int
    status: str
    attempt_count: int
    next_retry_at: Optional[str]
    last_error: Optional[str]
    kafka_partition: Optional[int]
    kafka_offset: Optional[int]
    created_at: str
    queued_at: Optional[str]
    acked_at: Optional[str]
    updated_at: str


class KafkaOutboxStore:
    """Thread-safe SQLite outbox: one connection per thread, WAL + synchronous=FULL.

    The TraCI thread and the delivery worker never share a connection or a
    Python-level lock, so a worker status write cannot stall a cycle append.
    Concurrency is left to SQLite's own write lock (short transactions only).
    """

    def __init__(
        self,
        db_path: Path,
        *,
        disk_warn_free_bytes: int = 512 * 1024 * 1024,
        disk_fault_free_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.disk_warn_free_bytes = int(disk_warn_free_bytes)
        self.disk_fault_free_bytes = int(disk_fault_free_bytes)
        self._state_lock = threading.RLock()
        self._write_gate = _WriteGate()
        self._sequence_lock = threading.Lock()
        self._conns_lock = threading.Lock()
        self._all_conns: List[sqlite3.Connection] = []
        self._closed = False
        self._local = threading.local()
        self._append_durations_ms: List[float] = []
        self._append_gate_wait_ms: List[float] = []
        self._append_insert_ms: List[float] = []
        self._append_commit_ms: List[float] = []
        self._last_append_ms: float = 0.0
        self._faulted = False
        self._fault_message: Optional[str] = None
        self._degraded = False
        self._migrate(self._conn)
        row = self._conn.execute(
            "SELECT COALESCE(MAX(outbox_sequence), 0) + 1 AS n FROM kafka_outbox"
        ).fetchone()
        self._next_sequence = int(row["n"] if row else 1)

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,  # manual transactions
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        with self._conns_lock:
            if self._closed:
                conn.close()
                raise OutboxAppendError("outbox store is closed")
            self._all_conns.append(conn)
        return conn

    @property
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
        return conn

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=FULL")
        # Verify — refuse NORMAL for no-loss gate
        row = cur.execute("PRAGMA synchronous").fetchone()
        sync_val = int(row[0]) if row else -1
        # FULL == 2 on SQLite
        if sync_val != 2:
            raise RuntimeError(
                f"PRAGMA synchronous must be FULL (2), got {sync_val} — no-loss gate"
            )
        cur.execute("PRAGMA busy_timeout=5000")
        cur.execute("PRAGMA foreign_keys=ON")
        # Checkpoints are driven explicitly (worker thread / close) so a cycle
        # COMMIT on the TraCI thread never inherits checkpoint fsync latency.
        cur.execute("PRAGMA wal_autocheckpoint=0")

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        # SCHEMA_SQL may run against a pre-RunStarted database. It must not
        # reference additive columns in an index until those columns have been
        # added below; otherwise SQLite aborts before the migration can run.
        conn.executescript(SCHEMA_SQL)
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(kafka_outbox)")}
        if "event_kind" not in cols:
            conn.execute(
                "ALTER TABLE kafka_outbox "
                "ADD COLUMN event_kind TEXT NOT NULL DEFAULT 'entity'"
            )
        if "outbox_sequence" not in cols:
            conn.execute(
                "ALTER TABLE kafka_outbox "
                "ADD COLUMN outbox_sequence INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            """
            UPDATE kafka_outbox
            SET outbox_sequence = rowid
            WHERE outbox_sequence IS NULL OR outbox_sequence = 0
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outbox_sequence ON kafka_outbox(outbox_sequence)"
        )

    def _reserve_outbox_sequences(self, count: int) -> int:
        """Reserve a contiguous process-local sequence range without a hot-path MAX query."""
        if count <= 0:
            raise ValueError("sequence reservation count must be positive")
        with self._sequence_lock:
            start = self._next_sequence
            self._next_sequence += int(count)
            return start

    @staticmethod
    def _record_bounded(samples: List[float], value: float, limit: int = 5000) -> None:
        samples.append(float(value))
        if len(samples) > limit:
            del samples[: len(samples) - limit]

    def checkpoint_wal(self, mode: str = "PASSIVE") -> None:
        """Bound WAL growth off the TraCI thread. PASSIVE never blocks writers."""
        if mode not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
            raise ValueError(f"invalid checkpoint mode: {mode}")
        try:
            with self._write_gate.background():
                self._conn.execute(f"PRAGMA wal_checkpoint({mode})")
        except sqlite3.Error as e:
            log.warning("wal checkpoint failed: %s", e)

    def close(self) -> None:
        with self._conns_lock:
            if self._closed:
                return
            self._closed = True
            conns = list(self._all_conns)
            self._all_conns.clear()
        for i, c in enumerate(conns):
            try:
                if i == 0:
                    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error:
                pass
            try:
                c.close()
            except sqlite3.Error:
                pass
        self._local = threading.local()

    @property
    def is_faulted(self) -> bool:
        return self._faulted

    @property
    def is_degraded(self) -> bool:
        return self._degraded

    @property
    def fault_message(self) -> Optional[str]:
        return self._fault_message

    def check_disk(self) -> Dict[str, Any]:
        usage = shutil.disk_usage(str(self.db_path.parent))
        free = int(usage.free)
        with self._state_lock:
            if free < self.disk_fault_free_bytes:
                self._faulted = True
                self._fault_message = f"disk_free_bytes={free} below fault threshold"
                self._degraded = True
            elif free < self.disk_warn_free_bytes:
                self._degraded = True
            return {
                "disk_free_bytes": free,
                "disk_total_bytes": int(usage.total),
                "degraded": self._degraded,
                "faulted": self._faulted,
            }

    def append_cycle(self, rows: Sequence[OutboxRow]) -> float:
        """Insert entire cycle atomically. Returns duration_ms. Raises OutboxAppendError."""
        if not rows:
            raise OutboxAppendError("empty cycle")
        if self._faulted:
            raise OutboxAppendError(self._fault_message or "outbox faulted")

        disk = self.check_disk()
        if disk["faulted"]:
            raise OutboxAppendError(self._fault_message or "disk full")

        now = _utc_now_iso()
        seq_start = self._reserve_outbox_sequences(len(rows))
        params = [
            (
                r.event_id,
                r.simulation_run_id,
                int(r.cycle_sequence),
                int(r.entity_sequence),
                r.event_key,
                r.topic,
                r.payload_json,
                r.payload_hash,
                STATUS_OUTBOXED,
                now,
                now,
                getattr(r, "event_kind", EVENT_KIND_ENTITY),
                seq_start + i,
            )
            for i, r in enumerate(rows)
        ]
        conn = self._conn
        t0 = time.perf_counter()
        gate_wait_ms = 0.0
        insert_ms = 0.0
        commit_ms = 0.0
        try:
            gate_t0 = time.perf_counter()
            with self._write_gate.priority():
                gate_wait_ms = (time.perf_counter() - gate_t0) * 1000.0
                conn.execute("BEGIN IMMEDIATE")
                insert_t0 = time.perf_counter()
                conn.executemany(
                    """
                    INSERT INTO kafka_outbox (
                        event_id, simulation_run_id, cycle_sequence, entity_sequence,
                        event_key, topic, payload_json, payload_hash,
                        status, attempt_count, next_retry_at, last_error,
                        kafka_partition, kafka_offset,
                        created_at, queued_at, acked_at, updated_at,
                        event_kind, outbox_sequence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, NULL, NULL, ?, ?, ?)
                    """,
                    params,
                )
                insert_ms = (time.perf_counter() - insert_t0) * 1000.0
                commit_t0 = time.perf_counter()
                conn.execute("COMMIT")
                commit_ms = (time.perf_counter() - commit_t0) * 1000.0
        except sqlite3.IntegrityError as e:
            self._rollback_quiet(conn)
            raise OutboxDuplicateError(str(e)) from e
        except Exception as e:
            self._rollback_quiet(conn)
            with self._state_lock:
                self._faulted = True
                self._fault_message = f"append/COMMIT fail: {e}"
                msg = self._fault_message
            raise OutboxAppendError(msg) from e

        ms = (time.perf_counter() - t0) * 1000.0
        with self._state_lock:
            self._last_append_ms = ms
            self._record_bounded(self._append_durations_ms, ms)
            self._record_bounded(self._append_gate_wait_ms, gate_wait_ms)
            self._record_bounded(self._append_insert_ms, insert_ms)
            self._record_bounded(self._append_commit_ms, commit_ms)
        return ms

    def append_run_started(
        self,
        *,
        simulation_run_id: str,
        producer_session_id: str,
        topic: str,
        payload_json: str,
        event_key: str,
        event_id: str,
        payload_hash: str,
    ) -> float:
        """Insert RunStarted control row (outbox-internal sentinels)."""
        if self._faulted:
            raise OutboxAppendError(self._fault_message or "outbox faulted")
        disk = self.check_disk()
        if disk["faulted"]:
            raise OutboxAppendError(self._fault_message or "disk full")
        now = _utc_now_iso()
        seq = self._reserve_outbox_sequences(1)
        conn = self._conn
        t0 = time.perf_counter()
        try:
            with self._write_gate.priority():
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    INSERT INTO kafka_outbox (
                        event_id, simulation_run_id, cycle_sequence, entity_sequence,
                        event_key, topic, payload_json, payload_hash,
                        status, attempt_count, next_retry_at, last_error,
                        kafka_partition, kafka_offset,
                        created_at, queued_at, acked_at, updated_at,
                        event_kind, outbox_sequence
                    ) VALUES (?, ?, -1, -1, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, NULL, NULL, ?, ?, ?)
                    """,
                    (
                        event_id,
                        simulation_run_id,
                        event_key,
                        topic,
                        payload_json,
                        payload_hash,
                        STATUS_OUTBOXED,
                        now,
                        now,
                        EVENT_KIND_RUN_STARTED,
                        seq,
                    ),
                )
                conn.execute("COMMIT")
        except sqlite3.IntegrityError as e:
            self._rollback_quiet(conn)
            raise OutboxDuplicateError(str(e)) from e
        except Exception as e:
            self._rollback_quiet(conn)
            with self._state_lock:
                self._faulted = True
                self._fault_message = f"append_run_started fail: {e}"
            raise OutboxAppendError(self._fault_message) from e
        return (time.perf_counter() - t0) * 1000.0

    def is_run_started_acked(self, simulation_run_id: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM kafka_outbox
            WHERE simulation_run_id = ? AND event_kind = ? AND status = ?
            LIMIT 1
            """,
            (simulation_run_id, EVENT_KIND_RUN_STARTED, STATUS_ACKED),
        ).fetchone()
        return row is not None

    @staticmethod
    def _rollback_quiet(conn: sqlite3.Connection) -> None:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def _write_batch(self, sql: str, params: Sequence[Sequence[Any]]) -> int:
        """Apply many status updates in a single short transaction (one fsync)."""
        if not params:
            return 0
        conn = self._conn
        try:
            with self._write_gate.background():
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.executemany(sql, params)
                n = int(cur.rowcount or 0)
                conn.execute("COMMIT")
                return n
        except Exception:
            self._rollback_quiet(conn)
            raise

    def recover_orphaned_queued(self) -> int:
        """QUEUED from dead process → FAILED_RETRYABLE for redrive."""
        now = _utc_now_iso()
        return self._write_batch(
            """
            UPDATE kafka_outbox
            SET status = ?, next_retry_at = ?, updated_at = ?,
                last_error = COALESCE(last_error, 'orphaned QUEUED on restart')
            WHERE status = ?
            """,
            [(STATUS_FAILED_RETRYABLE, now, now, STATUS_QUEUED)],
        )

    def fetch_eligible(self, *, limit: int, now_iso: Optional[str] = None) -> List[OutboxRecord]:
        now = now_iso or _utc_now_iso()
        cur = self._conn.execute(
            f"""
            SELECT * FROM kafka_outbox
            WHERE status IN ({",".join("?" for _ in REDRIVE_STATUSES)})
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY outbox_sequence ASC
            LIMIT ?
            """,
            (*REDRIVE_STATUSES, now, int(limit)),
        )
        return [self._row_to_record(r) for r in cur.fetchall()]

    def mark_queued_batch(self, event_ids: Sequence[str]) -> int:
        now = _utc_now_iso()
        return self._write_batch(
            """
            UPDATE kafka_outbox
            SET status = ?, queued_at = ?, updated_at = ?,
                attempt_count = attempt_count + 1
            WHERE event_id = ? AND status IN (?, ?)
            """,
            [
                (
                    STATUS_QUEUED,
                    now,
                    now,
                    eid,
                    STATUS_OUTBOXED,
                    STATUS_FAILED_RETRYABLE,
                )
                for eid in event_ids
            ],
        )

    def mark_queued(self, event_id: str) -> None:
        self.mark_queued_batch([event_id])

    def mark_acked_batch(self, acks: Sequence[tuple[str, int, int]]) -> int:
        """acks = [(event_id, partition, offset)]."""
        now = _utc_now_iso()
        return self._write_batch(
            """
            UPDATE kafka_outbox
            SET status = ?, kafka_partition = ?, kafka_offset = ?,
                acked_at = ?, updated_at = ?, last_error = NULL
            WHERE event_id = ?
            """,
            [
                (STATUS_ACKED, int(part), int(off), now, now, eid)
                for eid, part, off in acks
            ],
        )

    def mark_acked(
        self,
        event_id: str,
        *,
        partition: int,
        offset: int,
    ) -> None:
        self.mark_acked_batch([(event_id, partition, offset)])

    def mark_failed_retryable_batch(
        self, failures: Sequence[tuple[str, str, str]]
    ) -> int:
        """failures = [(event_id, error, next_retry_at)]."""
        now = _utc_now_iso()
        return self._write_batch(
            """
            UPDATE kafka_outbox
            SET status = ?, last_error = ?, next_retry_at = ?, updated_at = ?
            WHERE event_id = ?
            """,
            [
                (STATUS_FAILED_RETRYABLE, str(error)[:2000], nxt, now, eid)
                for eid, error, nxt in failures
            ],
        )

    def mark_failed_retryable(
        self,
        event_id: str,
        *,
        error: str,
        next_retry_at: str,
    ) -> None:
        self.mark_failed_retryable_batch([(event_id, error, next_retry_at)])

    def mark_failed_permanent_batch(
        self, failures: Sequence[tuple[str, str]]
    ) -> int:
        """failures = [(event_id, error)]."""
        now = _utc_now_iso()
        return self._write_batch(
            """
            UPDATE kafka_outbox
            SET status = ?, last_error = ?, updated_at = ?, next_retry_at = NULL
            WHERE event_id = ?
            """,
            [
                (STATUS_FAILED_PERMANENT, str(error)[:2000], now, eid)
                for eid, error in failures
            ],
        )

    def mark_failed_permanent(self, event_id: str, *, error: str) -> None:
        self.mark_failed_permanent_batch([(event_id, error)])

    def cleanup_acked(self, *, older_than_days: int = 7, batch_size: int = 500) -> int:
        """Delete ACKED older than retention. Never touches pending/FAILED."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=int(older_than_days))
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return self._write_batch(
            """
            DELETE FROM kafka_outbox
            WHERE event_id IN (
                SELECT event_id FROM kafka_outbox
                WHERE status = ? AND acked_at IS NOT NULL AND acked_at < ?
                LIMIT ?
            )
            """,
            [(STATUS_ACKED, cutoff, int(batch_size))],
        )

    def get(self, event_id: str) -> Optional[OutboxRecord]:
        cur = self._conn.execute(
            "SELECT * FROM kafka_outbox WHERE event_id = ?", (event_id,)
        )
        row = cur.fetchone()
        return self._row_to_record(row) if row else None

    def count_by_status(self) -> Dict[str, int]:
        cur = self._conn.execute(
            "SELECT status, COUNT(*) AS c FROM kafka_outbox GROUP BY status"
        )
        return {str(r["status"]): int(r["c"]) for r in cur.fetchall()}

    def capacity_metrics(self) -> Dict[str, Any]:
        disk = self.check_disk()
        conn = self._conn
        cur = conn.execute(
            f"""
            SELECT COUNT(*) AS n,
                   COALESCE(SUM(LENGTH(payload_json)), 0) AS bytes,
                   MIN(created_at) AS oldest
            FROM kafka_outbox
            WHERE status IN ({",".join("?" for _ in PENDING_STATUSES)})
            """,
            PENDING_STATUSES,
        )
        row = cur.fetchone()
        permanent = conn.execute(
            "SELECT COUNT(*) AS c FROM kafka_outbox WHERE status = ?",
            (STATUS_FAILED_PERMANENT,),
        ).fetchone()
        oldest = row["oldest"] if row else None
        oldest_age = None
        if oldest:
            dt = _parse_iso(str(oldest))
            if dt:
                oldest_age = max(
                    0.0, (datetime.now(timezone.utc) - dt).total_seconds()
                )
        with self._state_lock:
            durations = list(self._append_durations_ms)
            gate_wait = list(self._append_gate_wait_ms)
            insert = list(self._append_insert_ms)
            commit = list(self._append_commit_ms)

        def percentile(values: Sequence[float], q: float) -> Optional[float]:
            if not values:
                return None
            ordered = sorted(float(v) for v in values)
            idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
            return ordered[idx]

        p95 = percentile(durations, 0.95)
        return {
            "outbox_pending_rows": int(row["n"] or 0) if row else 0,
            "outbox_pending_bytes": int(row["bytes"] or 0) if row else 0,
            "oldest_pending_age_sec": oldest_age,
            "disk_free_bytes": disk["disk_free_bytes"],
            "outbox_failed_permanent_count": int(permanent["c"] or 0) if permanent else 0,
            "outbox_append_last_ms": self._last_append_ms,
            "outbox_append_p95_ms": p95,
            "outbox_append_p99_ms": percentile(durations, 0.99),
            "outbox_append_max_ms": max(durations) if durations else None,
            "outbox_append_sample_count": len(durations),
            "outbox_append_spikes_gt_500ms": sum(1 for value in durations if value > 500.0),
            "outbox_gate_wait_p95_ms": percentile(gate_wait, 0.95),
            "outbox_insert_p95_ms": percentile(insert, 0.95),
            "outbox_commit_p95_ms": percentile(commit, 0.95),
            "counts": self.count_by_status(),
            "faulted": self._faulted,
            "degraded": self._degraded,
            "fault_message": self._fault_message,
        }

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> OutboxRecord:
        keys = row.keys()
        return OutboxRecord(
            event_id=str(row["event_id"]),
            simulation_run_id=str(row["simulation_run_id"]),
            cycle_sequence=int(row["cycle_sequence"]),
            entity_sequence=int(row["entity_sequence"]),
            event_key=str(row["event_key"]),
            topic=str(row["topic"]),
            payload_json=str(row["payload_json"]),
            payload_hash=str(row["payload_hash"]),
            event_kind=str(row["event_kind"]) if "event_kind" in keys else EVENT_KIND_ENTITY,
            outbox_sequence=int(row["outbox_sequence"]) if "outbox_sequence" in keys else 0,
            status=str(row["status"]),
            attempt_count=int(row["attempt_count"] or 0),
            next_retry_at=row["next_retry_at"],
            last_error=row["last_error"],
            kafka_partition=row["kafka_partition"],
            kafka_offset=row["kafka_offset"],
            created_at=str(row["created_at"]),
            queued_at=row["queued_at"],
            acked_at=row["acked_at"],
            updated_at=str(row["updated_at"]),
        )


def compute_next_retry_at(attempt_count: int, *, base_sec: float = 0.5, cap_sec: float = 60.0) -> str:
    import random

    exp = min(cap_sec, base_sec * (2 ** max(0, attempt_count)))
    jitter = 0.5 + random.random()  # 0.5x–1.5x
    delay = exp * jitter
    return (
        datetime.now(timezone.utc) + timedelta(seconds=delay)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def events_to_outbox_rows(
    events: Sequence[dict[str, Any]],
    *,
    topic: str,
) -> List[OutboxRow]:
    from integration.kafka.event_mapper import partition_key

    rows: List[OutboxRow] = []
    for ev in events:
        entity = ev.get("entity") if isinstance(ev.get("entity"), dict) else {}
        run_id = str(ev["simulationRunId"])
        node_id = str(ev["nodeId"])
        payload = json.dumps(ev, ensure_ascii=True, separators=(",", ":"))
        rows.append(
            OutboxRow(
                event_id=str(ev["eventId"]),
                simulation_run_id=run_id,
                cycle_sequence=int(ev["cycleSequence"]),
                entity_sequence=int(ev["entitySequence"]),
                event_key=partition_key(run_id, node_id),
                topic=topic,
                payload_json=payload,
                payload_hash=str(ev["entityPayloadHash"]),
            )
        )
    return rows
