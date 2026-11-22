"""Silver-owned known-run index and independent lifecycle polling."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

from de.silver.archive_resolver import ArchiveIntegrityError, ArchiveResolver, ArchiveUnavailable


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class LifecycleSnapshot:
    run_id: str
    generation: int
    registry_state: str
    status: str | None
    registry_updated_at: str | None
    mirror_state: str
    manifest_status: str | None
    failed_cycle_count: int | None
    manifest_file_hash: str | None
    freshness: str


class LifecycleRefresher:
    def __init__(self, state_path: Path, resolver: ArchiveResolver, interval_seconds: int = 5):
        if not 1 <= interval_seconds <= 300:
            raise ValueError("poll interval must be 1..300 seconds")
        self.path = Path(state_path)
        self.resolver = resolver
        self.interval_seconds = interval_seconds
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS silver_known_runs_v1 (
                    run_id TEXT PRIMARY KEY, generation INTEGER NOT NULL,
                    topology_digest TEXT NOT NULL, manifest_identity TEXT NOT NULL,
                    snapshot_json TEXT, last_polled_at TEXT
                );
                CREATE TABLE IF NOT EXISTS silver_lifecycle_history_v1 (
                    run_id TEXT NOT NULL, version INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL, observed_at TEXT NOT NULL,
                    projected_at TEXT,
                    PRIMARY KEY (run_id,version)
                );
                CREATE TABLE IF NOT EXISTS silver_lifecycle_poll_state_v1 (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    retry_count INTEGER NOT NULL, last_attempt_at TEXT NOT NULL,
                    last_successful_at TEXT, next_poll_at TEXT NOT NULL, last_error TEXT
                );
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(silver_lifecycle_history_v1)")}
            if "projected_at" not in columns:
                db.execute("ALTER TABLE silver_lifecycle_history_v1 ADD COLUMN projected_at TEXT")

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def latest(self, run_id: str) -> LifecycleSnapshot | None:
        """Read the deterministic latest durable projection after restart."""
        with self._connect() as db:
            row = db.execute("SELECT snapshot_json FROM silver_known_runs_v1 WHERE run_id=?",
                             (run_id,)).fetchone()
        return LifecycleSnapshot(**json.loads(row[0])) if row and row[0] else None

    def register(self, run_id: str, digest: str, generation: int) -> None:
        context = self.resolver.resolve(run_id, digest, generation)
        with self._connect() as db:
            row = db.execute("SELECT generation,topology_digest,manifest_identity "
                             "FROM silver_known_runs_v1 WHERE run_id=?", (run_id,)).fetchone()
            expected = (generation, digest, context.manifest_identity)
            if row and tuple(row) != expected:
                raise ArchiveIntegrityError("known-run immutable identity changed")
            db.execute("INSERT OR IGNORE INTO silver_known_runs_v1 "
                       "(run_id,generation,topology_digest,manifest_identity) VALUES (?,?,?,?)",
                       (run_id, *expected))

    def poll(self, limit: int = 100) -> list[LifecycleSnapshot]:
        if not 1 <= limit <= 1000:
            raise ValueError("poll batch limit must be 1..1000")
        changed = []
        with self._connect() as db:
            rows = db.execute("SELECT * FROM silver_known_runs_v1 "
                              "ORDER BY COALESCE(last_polled_at,''),run_id LIMIT ?", (limit,)).fetchall()
            for row in rows:
                old = json.loads(row["snapshot_json"]) if row["snapshot_json"] else None
                try:
                    context = self.resolver.resolve(row["run_id"], row["topology_digest"], row["generation"])
                    if context.manifest_identity != row["manifest_identity"]:
                        raise ArchiveIntegrityError("known-run manifest identity changed")
                    manifest_status = context.manifest.get("runStatus")
                    mirror = "SYNCED" if manifest_status == context.registry_status else "LAGGING"
                    count = context.manifest.get("failedCycleCount") if mirror == "SYNCED" else (
                        old["failed_cycle_count"] if old else None)
                    file_hash = context.manifest_file_hash if mirror == "SYNCED" else (
                        old["manifest_file_hash"] if old else None)
                    snapshot = LifecycleSnapshot(row["run_id"], row["generation"], "AVAILABLE",
                        context.registry_status, context.registry_updated_at, mirror,
                        manifest_status, count, file_hash, "CURRENT")
                except ArchiveUnavailable:
                    try:
                        registry = self.resolver.registry_row(row["run_id"])
                    except ArchiveUnavailable:
                        registry = None
                    if registry is not None and (registry["generation"] != row["generation"] or
                            registry["manifest_identity"] != row["manifest_identity"] or
                            registry['status'] not in {'RUNNING', 'COMPLETE', 'INCOMPLETE', 'STOPPED'}):
                        raise ArchiveIntegrityError("registry immutable identity changed")
                    snapshot = LifecycleSnapshot(row["run_id"], row["generation"],
                        "AVAILABLE" if registry else "UNAVAILABLE",
                        registry["status"] if registry else (old["status"] if old else None),
                        registry["updated_at"] if registry else (
                            old["registry_updated_at"] if old else None), "UNAVAILABLE",
                        old["manifest_status"] if old else None,
                        old["failed_cycle_count"] if old else None,
                        old["manifest_file_hash"] if old else None,
                        "CURRENT" if registry else "STALE")
                encoded = json.dumps(snapshot.__dict__, sort_keys=True, separators=(",", ":"))
                if encoded != row["snapshot_json"]:
                    version = db.execute("SELECT COALESCE(MAX(version),0)+1 FROM silver_lifecycle_history_v1 "
                                         "WHERE run_id=?", (row["run_id"],)).fetchone()[0]
                    db.execute("INSERT INTO silver_lifecycle_history_v1 "
                               "(run_id,version,snapshot_json,observed_at) VALUES (?,?,?,?)",
                               (row["run_id"], version, encoded, _now()))
                    changed.append(snapshot)
                db.execute("UPDATE silver_known_runs_v1 SET snapshot_json=?,last_polled_at=? WHERE run_id=?",
                           (encoded, _now(), row["run_id"]))
        return changed

    def project_pending(self, repository, *, replay_run_id: str | None = None,
                        limit: int = 100) -> int:
        """Reconcile durable lifecycle snapshots before acknowledging CH projection."""
        if not 1 <= limit <= 1000:
            raise ValueError("projection batch limit must be 1..1000")
        with self._connect() as db:
            rows = db.execute("SELECT h.run_id,h.version,h.snapshot_json,h.observed_at,"
                              "k.manifest_identity FROM silver_lifecycle_history_v1 h "
                              "JOIN silver_known_runs_v1 k ON k.run_id=h.run_id "
                              "WHERE h.projected_at IS NULL ORDER BY h.run_id,h.version LIMIT ?",
                              (limit,)).fetchall()
        for item in rows:
            snap = LifecycleSnapshot(**json.loads(item["snapshot_json"]))
            row = dict(simulation_run_id=snap.run_id, run_generation=snap.generation,
                       manifest_identity=item["manifest_identity"], run_status=snap.status,
                       registry_state=snap.registry_state,
                       registry_updated_at=snap.registry_updated_at,
                       manifest_run_status=snap.manifest_status,
                       failed_cycle_count=snap.failed_cycle_count,
                       manifest_file_hash=snap.manifest_file_hash,
                       manifest_mirror_state=snap.mirror_state,
                       lifecycle_freshness=snap.freshness,
                       failure_evidence_json=None, observed_at=item["observed_at"])
            target = "silver_run_lifecycle_v1"
            if not repository.reconcile_v1_row(target, row, replay_run_id=replay_run_id):
                repository.insert_v1_rows(target, [row], replay_run_id=replay_run_id)
                if not repository.reconcile_v1_row(target, row, replay_run_id=replay_run_id):
                    raise RuntimeError("lifecycle projection did not reconcile")
            with self._connect() as db:
                db.execute("UPDATE silver_lifecycle_history_v1 SET projected_at=? "
                           "WHERE run_id=? AND version=? AND projected_at IS NULL",
                           (_now(), item["run_id"], item["version"]))
        return len(rows)

    def has_pending_projection(self) -> bool:
        with self._connect() as db:
            return db.execute('SELECT 1 FROM silver_lifecycle_history_v1 WHERE projected_at IS NULL LIMIT 1').fetchone() is not None

    def poll_state(self) -> dict | None:
        with self._connect() as db:
            row = db.execute('SELECT * FROM silver_lifecycle_poll_state_v1 WHERE singleton=1').fetchone()
        return dict(row) if row else None

    def record_poll_result(self, retry_count: int, delay: float, error: str) -> None:
        now = datetime.now(timezone.utc)
        with self._connect() as db:
            db.execute('INSERT INTO silver_lifecycle_poll_state_v1 VALUES (1,?,?,?,?,?) '
                'ON CONFLICT(singleton) DO UPDATE SET retry_count=excluded.retry_count,'
                'last_attempt_at=excluded.last_attempt_at,'
                'last_successful_at=COALESCE(excluded.last_successful_at,last_successful_at),'
                'next_poll_at=excluded.next_poll_at,last_error=excluded.last_error',
                (retry_count, now.isoformat(), now.isoformat() if retry_count == 0 else None,
                 (now + timedelta(seconds=delay)).isoformat(), error[:2000] or None))

    def is_current(self, max_age_seconds: float) -> bool:
        now = datetime.now(timezone.utc)
        with self._connect() as db:
            rows = db.execute('SELECT snapshot_json,last_polled_at FROM silver_known_runs_v1').fetchall()
        return all(row['snapshot_json'] and row['last_polled_at'] and
            json.loads(row['snapshot_json'])['freshness'] == 'CURRENT' and
            (now - datetime.fromisoformat(row['last_polled_at'])).total_seconds() <= max_age_seconds
            for row in rows)

    def mark_stale(self, max_age_seconds: float) -> int:
        """Durably publish missed-poll freshness changes once, retaining authority values."""
        now = datetime.now(timezone.utc)
        changed = 0
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for row in db.execute('SELECT run_id,snapshot_json,last_polled_at FROM silver_known_runs_v1').fetchall():
                if not row['snapshot_json'] or not row['last_polled_at']:
                    continue
                snapshot = json.loads(row['snapshot_json'])
                if snapshot['freshness'] == 'STALE' or (now - datetime.fromisoformat(row['last_polled_at'])).total_seconds() <= max_age_seconds:
                    continue
                snapshot['freshness'] = 'STALE'
                encoded = json.dumps(snapshot, sort_keys=True, separators=(',', ':'))
                version = db.execute('SELECT COALESCE(MAX(version),0)+1 FROM silver_lifecycle_history_v1 WHERE run_id=?',
                                     (row['run_id'],)).fetchone()[0]
                db.execute('INSERT INTO silver_lifecycle_history_v1 (run_id,version,snapshot_json,observed_at) VALUES (?,?,?,?)',
                           (row['run_id'], version, encoded, _now()))
                db.execute('UPDATE silver_known_runs_v1 SET snapshot_json=? WHERE run_id=?', (encoded, row['run_id']))
                changed += 1
        return changed
