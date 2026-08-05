"""Projector SQLite store — FULL sync, single-TX apply, retention."""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from integration.projector.schema import (
    ACTIVE,
    COMPLETED_STATUSES,
    INACTIVE,
    RUNTIME_STATE_KEY,
    SCHEMA_SQL,
    STATUS_APPLIED,
    STATUS_PENDING,
)

log = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _safe_rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass


class ProjectorStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)

    def _apply_pragmas(self) -> None:
        cur = self._conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=FULL")
        sync = int(cur.execute("PRAGMA synchronous").fetchone()[0])
        if sync != 2:
            raise RuntimeError(f"projector requires synchronous=FULL, got {sync}")
        # Do not let SQLite's default 1000-page auto-checkpoint run inside an
        # apply transaction.  FULL durability is unchanged: each COMMIT still
        # fsyncs the WAL.  Checkpointing happens on orderly close/startup rather
        # than injecting multi-hundred-ms spikes into the realtime path.
        cur.execute("PRAGMA wal_autocheckpoint=0")
        cur.execute("PRAGMA busy_timeout=5000")
        cur.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ── active run ──────────────────────────────────────────────────

    def activate_run(
        self,
        *,
        source: str,
        producer_id: str,
        producer_session_id: str,
        simulation_run_id: str,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    UPDATE projector_active_runs SET status = ?, activated_at = activated_at
                    WHERE source = ? AND producer_id = ? AND status = ?
                    """,
                    (INACTIVE, source, producer_id, ACTIVE),
                )
                self._conn.execute(
                    """
                    INSERT INTO projector_active_runs
                    (source, producer_id, producer_session_id, simulation_run_id, activated_at, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, producer_id) DO UPDATE SET
                        producer_session_id=excluded.producer_session_id,
                        simulation_run_id=excluded.simulation_run_id,
                        activated_at=excluded.activated_at,
                        status=excluded.status
                    """,
                    (
                        source,
                        producer_id,
                        producer_session_id,
                        simulation_run_id,
                        now,
                        ACTIVE,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                _safe_rollback(self._conn)
                raise

    def get_active_run(self, *, source: str, producer_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM projector_active_runs
                WHERE source = ? AND producer_id = ? AND status = ?
                """,
                (source, producer_id, ACTIVE),
            ).fetchone()
            return dict(row) if row else None

    def is_active_simulation_run(
        self, *, source: str, producer_id: str, simulation_run_id: str
    ) -> bool:
        active = self.get_active_run(source=source, producer_id=producer_id)
        return bool(active and active["simulation_run_id"] == simulation_run_id)

    # ── ledger / state TX ───────────────────────────────────────────

    def apply_batch_tx(
        self,
        *,
        ledger_rows: Sequence[dict],
        entity_updates: Sequence[dict],
        superseded_event_ids: Sequence[str] = (),
    ) -> None:
        """Single TX: ledger upserts + entity state + mark superseded completed."""
        now = _utc_now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for r in ledger_rows:
                    self._conn.execute(
                        """
                        INSERT INTO projector_event_ledger (
                            event_id, topic, partition, offset, simulation_run_id,
                            cycle_sequence, entity_id, replacement_event_id, status,
                            payload_hash, created_at, completed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(event_id) DO UPDATE SET
                            status=excluded.status,
                            replacement_event_id=COALESCE(excluded.replacement_event_id, projector_event_ledger.replacement_event_id),
                            completed_at=excluded.completed_at
                        """,
                        (
                            r["event_id"],
                            r["topic"],
                            int(r["partition"]),
                            int(r["offset"]),
                            r["simulation_run_id"],
                            int(r["cycle_sequence"]),
                            r["entity_id"],
                            r.get("replacement_event_id"),
                            r["status"],
                            r["payload_hash"],
                            r.get("created_at") or now,
                            r.get("completed_at") or now,
                        ),
                    )
                for e in entity_updates:
                    self._conn.execute(
                        """
                        INSERT INTO projector_entity_state (
                            simulation_run_id, entity_id, last_cycle_sequence,
                            last_event_id, last_payload_hash, last_applied_at,
                            last_simulation_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(simulation_run_id, entity_id) DO UPDATE SET
                            last_cycle_sequence=excluded.last_cycle_sequence,
                            last_event_id=excluded.last_event_id,
                            last_payload_hash=excluded.last_payload_hash,
                            last_applied_at=excluded.last_applied_at,
                            last_simulation_time=excluded.last_simulation_time
                        """,
                        (
                            e["simulation_run_id"],
                            e["entity_id"],
                            int(e["last_cycle_sequence"]),
                            e["last_event_id"],
                            e["last_payload_hash"],
                            now,
                            e.get("last_simulation_time"),
                        ),
                    )
                for eid in superseded_event_ids:
                    self._conn.execute(
                        """
                        UPDATE projector_event_ledger
                        SET status = ?, completed_at = ?
                        WHERE event_id = ? AND status = ?
                        """,
                        (
                            "COALESCED_SUPERSEDED",
                            now,
                            eid,
                            STATUS_PENDING,
                        ),
                    )
                self._conn.execute("COMMIT")
            except Exception:
                _safe_rollback(self._conn)
                raise

    def get_ledger(self, event_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM projector_event_ledger WHERE event_id = ?", (event_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_ledgers(self, event_ids: Sequence[str]) -> Dict[str, dict]:
        """Batch ledger lookup — one parameterized IN query for a cycle."""
        ids = [str(e) for e in event_ids if e]
        if not ids:
            return {}
        # Chunk to stay under SQLite variable limits on very large cycles.
        out: Dict[str, dict] = {}
        chunk_size = 400
        with self._lock:
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i : i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                rows = self._conn.execute(
                    f"SELECT * FROM projector_event_ledger WHERE event_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    out[str(row["event_id"])] = dict(row)
        return out

    def get_entity_state(self, simulation_run_id: str, entity_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM projector_entity_state
                WHERE simulation_run_id = ? AND entity_id = ?
                """,
                (simulation_run_id, entity_id),
            ).fetchone()
            return dict(row) if row else None

    def set_committed_offset(self, topic: str, partition: int, offset: int) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO projector_partition_commits (topic, partition, committed_offset, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(topic, partition) DO UPDATE SET
                    committed_offset=excluded.committed_offset,
                    updated_at=excluded.updated_at
                """,
                (topic, int(partition), int(offset), now),
            )

    def get_committed_offset(self, topic: str, partition: int) -> Optional[int]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT committed_offset FROM projector_partition_commits
                WHERE topic = ? AND partition = ?
                """,
                (topic, int(partition)),
            ).fetchone()
            return int(row["committed_offset"]) if row else None

    def has_any_commits(self, topic: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM projector_partition_commits WHERE topic = ? LIMIT 1",
                (topic,),
            ).fetchone()
            return row is not None

    def set_runtime_state(
        self,
        *,
        simulation_run_id: Optional[str],
        scenario_id: Optional[str],
        simulation_time: float,
        status: str,
        last_applied_cycle: int,
        freshness_seconds: Optional[float],
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO projector_runtime_state (
                    state_key, simulation_run_id, scenario_id, simulation_time,
                    status, last_applied_cycle, freshness_seconds, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    simulation_run_id=excluded.simulation_run_id,
                    scenario_id=excluded.scenario_id,
                    simulation_time=excluded.simulation_time,
                    status=excluded.status,
                    last_applied_cycle=excluded.last_applied_cycle,
                    freshness_seconds=excluded.freshness_seconds,
                    updated_at=excluded.updated_at
                """,
                (
                    RUNTIME_STATE_KEY,
                    simulation_run_id,
                    scenario_id,
                    float(simulation_time),
                    status,
                    int(last_applied_cycle),
                    freshness_seconds,
                    now,
                ),
            )

    def get_runtime_state(self) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM projector_runtime_state WHERE state_key = ?",
                (RUNTIME_STATE_KEY,),
            ).fetchone()
            return dict(row) if row else None

    def rebuild_completed_offsets(
        self, topic: str, partition: int
    ) -> Optional[int]:
        """Highest contiguous completed offset from ledger (or None)."""
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT offset, status FROM projector_event_ledger
                WHERE topic = ? AND partition = ?
                ORDER BY offset ASC
                """,
                (topic, int(partition)),
            ).fetchall()
        if not rows:
            return None
        expected = int(rows[0]["offset"])
        last = None
        for r in rows:
            off = int(r["offset"])
            if off != expected:
                break
            if r["status"] not in COMPLETED_STATUSES:
                break
            last = off
            expected = off + 1
        return last

    def cleanup(
        self,
        *,
        state_retention_hours: float = 24.0,
        ledger_retention_hours: float = 24.0,
    ) -> Dict[str, int]:
        now = datetime.now(timezone.utc)
        state_cut = (now - timedelta(hours=state_retention_hours)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        ledger_cut = (now - timedelta(hours=ledger_retention_hours)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        with self._lock:
            # delete entity state for INACTIVE runs older than cutoff
            inactive_runs = [
                r["simulation_run_id"]
                for r in self._conn.execute(
                    "SELECT DISTINCT simulation_run_id FROM projector_active_runs WHERE status = ?",
                    (INACTIVE,),
                ).fetchall()
            ]
            state_deleted = 0
            for run_id in inactive_runs:
                cur = self._conn.execute(
                    """
                    DELETE FROM projector_entity_state
                    WHERE simulation_run_id = ? AND last_applied_at < ?
                    """,
                    (run_id, state_cut),
                )
                state_deleted += int(cur.rowcount or 0)

            # ledger: completed + older than cutoff + offset <= committed
            commits = {
                (r["topic"], int(r["partition"])): int(r["committed_offset"])
                for r in self._conn.execute(
                    "SELECT topic, partition, committed_offset FROM projector_partition_commits"
                ).fetchall()
            }
            ledger_deleted = 0
            placeholders = ",".join("?" for _ in COMPLETED_STATUSES)
            rows = self._conn.execute(
                f"""
                SELECT event_id, topic, partition, offset FROM projector_event_ledger
                WHERE status IN ({placeholders})
                  AND completed_at IS NOT NULL AND completed_at < ?
                  AND event_id NOT IN (
                    SELECT replacement_event_id FROM projector_event_ledger
                    WHERE replacement_event_id IS NOT NULL
                      AND status NOT IN ({placeholders})
                  )
                """,
                (*COMPLETED_STATUSES, ledger_cut, *COMPLETED_STATUSES),
            ).fetchall()
            for r in rows:
                key = (r["topic"], int(r["partition"]))
                committed = commits.get(key)
                if committed is None or int(r["offset"]) > committed:
                    continue
                cur = self._conn.execute(
                    "DELETE FROM projector_event_ledger WHERE event_id = ?",
                    (r["event_id"],),
                )
                ledger_deleted += int(cur.rowcount or 0)
        return {"state_deleted": state_deleted, "ledger_deleted": ledger_deleted}

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            ledger_n = self._conn.execute(
                "SELECT COUNT(*) AS c FROM projector_event_ledger"
            ).fetchone()["c"]
            state_n = self._conn.execute(
                "SELECT COUNT(*) AS c FROM projector_entity_state"
            ).fetchone()["c"]
        size = self.db_path.stat().st_size if self.db_path.exists() else 0
        return {
            "ledger_row_count": int(ledger_n),
            "state_row_count": int(state_n),
            "projector_sqlite_bytes": int(size),
        }
