"""Durable V1 projection ledger and reconciliation journal."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from contracts.traffic_observation import canonical_json


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class V1Store:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY, frozen_event TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, run_id TEXT NOT NULL,
                    generation INTEGER NOT NULL, node TEXT NOT NULL,
                    cycle INTEGER NOT NULL, status TEXT NOT NULL,
                    expected_ops TEXT NOT NULL, completed_ops TEXT NOT NULL,
                    error TEXT, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS watermarks (
                    run_id TEXT NOT NULL, node TEXT NOT NULL, cycle INTEGER NOT NULL,
                    PRIMARY KEY (run_id, node)
                );
                CREATE TABLE IF NOT EXISTS outcomes (
                    topic TEXT NOT NULL, partition INTEGER NOT NULL,
                    offset INTEGER NOT NULL, event_id TEXT,
                    status TEXT NOT NULL, updated_at TEXT NOT NULL,
                    PRIMARY KEY (topic, partition, offset)
                );
                CREATE TABLE IF NOT EXISTS quarantine (
                    topic TEXT NOT NULL, partition INTEGER NOT NULL,
                    offset INTEGER NOT NULL, payload BLOB NOT NULL,
                    reason TEXT NOT NULL, updated_at TEXT NOT NULL,
                    PRIMARY KEY (topic, partition, offset)
                );
                CREATE TABLE IF NOT EXISTS reconciliations (
                    generation INTEGER PRIMARY KEY, run_id TEXT NOT NULL,
                    status TEXT NOT NULL, inventory TEXT NOT NULL,
                    error TEXT, updated_at TEXT NOT NULL,
                    plan TEXT NOT NULL DEFAULT '[]',
                    completed_ops TEXT NOT NULL DEFAULT '[]'
                );
                CREATE TABLE IF NOT EXISTS entity_attrs (
                    entity_id TEXT PRIMARY KEY, dynamic_attrs TEXT NOT NULL
                );
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(reconciliations)")}
            if "plan" not in columns:
                db.execute("ALTER TABLE reconciliations ADD COLUMN plan TEXT NOT NULL DEFAULT '[]'")
            if "completed_ops" not in columns:
                db.execute("ALTER TABLE reconciliations ADD COLUMN completed_ops TEXT NOT NULL DEFAULT '[]'")

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(str(self.path), timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def event(self, event_id: str) -> dict | None:
        with self._db() as db:
            row = db.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
            return dict(row) if row else None

    def begin(self, event: dict, generation: int, expected_ops: list[str]) -> dict:
        event_id = event["eventId"]
        frozen = canonical_json(event)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
            if row:
                if row["frozen_event"] != frozen or row["payload_hash"] != event["payloadHash"]:
                    raise ValueError("conflicting frozen event identity")
                if json.loads(row["expected_ops"]) != expected_ops:
                    raise ValueError("projection operation set changed for existing event")
                return dict(row)
            db.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
                event_id, frozen, event["payloadHash"], event["simulationRunId"], generation,
                event["intersectionId"], event["cycleSequence"], "PROCESSING",
                json.dumps(expected_ops), "[]", None, _now(),
            ))
        return self.event(event_id) or {}

    def completed_ops(self, event_id: str) -> set[str]:
        row = self.event(event_id)
        return set(json.loads(row["completed_ops"])) if row else set()

    def mark_op(self, event_id: str, operation: str) -> None:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT completed_ops, expected_ops FROM events WHERE event_id=?", (event_id,)).fetchone()
            if row is None or operation not in json.loads(row["expected_ops"]):
                raise ValueError("unknown projection operation")
            completed = set(json.loads(row["completed_ops"]))
            completed.add(operation)
            db.execute("UPDATE events SET completed_ops=?, updated_at=? WHERE event_id=?", (json.dumps(sorted(completed)), _now(), event_id))

    def watermark(self, run_id: str, node: str) -> int:
        with self._db() as db:
            row = db.execute("SELECT cycle FROM watermarks WHERE run_id=? AND node=?", (run_id, node)).fetchone()
            return int(row[0]) if row else -1

    def finish(self, event: dict, status: str, *, topic: str, partition: int, offset: int, entity_attrs: dict[str, set[str]] | None = None) -> None:
        if status not in ("APPLIED", "STALE", "FENCED"):
            raise ValueError("invalid terminal status")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if status == "APPLIED":
                row = db.execute("SELECT expected_ops, completed_ops FROM events WHERE event_id=?", (event["eventId"],)).fetchone()
                if row is None or set(json.loads(row[0])) != set(json.loads(row[1])):
                    raise ValueError("cannot mark partially projected event APPLIED")
                current = db.execute("SELECT cycle FROM watermarks WHERE run_id=? AND node=?", (event["simulationRunId"], event["intersectionId"])).fetchone()
                if not current or int(current[0]) < event["cycleSequence"]:
                    db.execute("INSERT INTO watermarks VALUES (?,?,?) ON CONFLICT(run_id,node) DO UPDATE SET cycle=excluded.cycle", (event["simulationRunId"], event["intersectionId"], event["cycleSequence"]))
                for entity_id, attrs in (entity_attrs or {}).items():
                    db.execute("INSERT INTO entity_attrs VALUES (?,?) ON CONFLICT(entity_id) DO UPDATE SET dynamic_attrs=excluded.dynamic_attrs", (entity_id, json.dumps(sorted(attrs))))
            db.execute("UPDATE events SET status=?, updated_at=? WHERE event_id=?", (status, _now(), event["eventId"]))
            db.execute("INSERT OR REPLACE INTO outcomes VALUES (?,?,?,?,?,?)", (topic, partition, offset, event.get("eventId"), status, _now()))

    def terminal_outcome(self, topic: str, partition: int, offset: int) -> str | None:
        with self._db() as db:
            row = db.execute("SELECT status FROM outcomes WHERE topic=? AND partition=? AND offset=?", (topic, partition, offset)).fetchone()
            return row[0] if row else None

    def record_outcome(self, *, topic: str, partition: int, offset: int, event_id: str | None, status: str) -> None:
        with self._db() as db:
            db.execute("INSERT OR REPLACE INTO outcomes VALUES (?,?,?,?,?,?)", (topic, partition, offset, event_id, status, _now()))

    def quarantine(self, *, topic: str, partition: int, offset: int, payload: bytes, reason: str) -> None:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT OR REPLACE INTO quarantine VALUES (?,?,?,?,?,?)", (topic, partition, offset, payload, reason, _now()))
            db.execute("INSERT OR REPLACE INTO outcomes VALUES (?,?,?,?,?,?)", (topic, partition, offset, None, "QUARANTINED", _now()))

    def start_reconciliation(self, generation: int, run_id: str, inventory: dict, plan: list[dict]) -> dict:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM reconciliations WHERE generation=?", (generation,)).fetchone()
            if row:
                if row["run_id"] != run_id:
                    raise ValueError("reconciliation generation changed run identity")
                if row["status"] == "FAULTED" and not json.loads(row["plan"]) and not json.loads(row["completed_ops"]) and plan:
                    db.execute("UPDATE reconciliations SET inventory=?,plan=?,status='IN_PROGRESS',error=NULL,updated_at=? WHERE generation=?", (json.dumps(inventory, sort_keys=True), json.dumps(plan, sort_keys=True), _now(), generation))
                else:
                    db.execute("UPDATE reconciliations SET status='IN_PROGRESS', error=NULL, updated_at=? WHERE generation=?", (_now(), generation))
                return dict(db.execute("SELECT * FROM reconciliations WHERE generation=?", (generation,)).fetchone())
            db.execute("INSERT INTO reconciliations (generation,run_id,status,inventory,error,updated_at,plan,completed_ops) VALUES (?,?,?,?,?,?,?,?)", (generation, run_id, "IN_PROGRESS", json.dumps(inventory, sort_keys=True), None, _now(), json.dumps(plan, sort_keys=True), "[]"))
        return self.reconciliation(generation) or {}

    def mark_reconciliation_op(self, generation: int, operation: str) -> None:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT plan,completed_ops FROM reconciliations WHERE generation=?", (generation,)).fetchone()
            if row is None or operation not in {item["op"] for item in json.loads(row["plan"])}:
                raise ValueError("unknown reconciliation operation")
            completed = set(json.loads(row["completed_ops"]))
            completed.add(operation)
            db.execute("UPDATE reconciliations SET completed_ops=?,updated_at=? WHERE generation=?", (json.dumps(sorted(completed)), _now(), generation))

    def reconciliation(self, generation: int) -> dict | None:
        with self._db() as db:
            row = db.execute("SELECT * FROM reconciliations WHERE generation=?", (generation,)).fetchone()
            return dict(row) if row else None

    def latest_ready(self) -> dict | None:
        with self._db() as db:
            row = db.execute("SELECT * FROM reconciliations WHERE status='READY' ORDER BY generation DESC LIMIT 1").fetchone()
            return dict(row) if row else None

    def entity_attributes(self, entity_id: str) -> set[str]:
        with self._db() as db:
            row = db.execute("SELECT dynamic_attrs FROM entity_attrs WHERE entity_id=?", (entity_id,)).fetchone()
            return set(json.loads(row[0])) if row else set()

    def set_entity_attributes(self, entity_id: str, attrs: set[str]) -> None:
        with self._db() as db:
            db.execute("INSERT INTO entity_attrs VALUES (?,?) ON CONFLICT(entity_id) DO UPDATE SET dynamic_attrs=excluded.dynamic_attrs", (entity_id, json.dumps(sorted(attrs))))

    def drop_entity_attributes(self, entity_id: str) -> None:
        with self._db() as db:
            db.execute("DELETE FROM entity_attrs WHERE entity_id=?", (entity_id,))

    def finish_reconciliation(self, generation: int, *, error: str | None = None) -> None:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT plan,completed_ops FROM reconciliations WHERE generation=?", (generation,)).fetchone()
            if not error and (row is None or {item["op"] for item in json.loads(row["plan"])} != set(json.loads(row["completed_ops"]))):
                raise ValueError("cannot finish incomplete reconciliation")
            db.execute("UPDATE reconciliations SET status=?, error=?, updated_at=? WHERE generation=?", ("FAULTED" if error else "READY", error, _now(), generation))
