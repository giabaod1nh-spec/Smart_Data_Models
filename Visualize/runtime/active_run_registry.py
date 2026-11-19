"""Persistent live-run selector for the V1 observation stream.

Only a successful launcher activation changes the selected live run. Kafka
observations and historical replay never update this registry.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def manifest_identity(manifest: dict[str, Any]) -> str:
    """Hash immutable run identity, excluding mutable run status."""
    for required in ("simulationRunId", "topologyDigest", "participatingIntersectionIds", "runGeneration"):
        if required not in manifest:
            raise ValueError(f"run manifest lacks {required}")
    identity = {name: value for name, value in manifest.items()
                if name not in {"runStatus", "failedCycleCount"}}
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class ActiveRunRegistry:
    """SQLite selector with atomic, monotonically increasing generations."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    generation INTEGER PRIMARY KEY,
                    run_id TEXT NOT NULL UNIQUE,
                    manifest_identity TEXT NOT NULL,
                    status TEXT NOT NULL,
                    activated_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_selection (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    generation INTEGER NOT NULL REFERENCES runs(generation)
                );
            """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(str(self.path), timeout=10.0, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def next_generation(self) -> int:
        with closing(self._connect()) as db:
            return int(db.execute("SELECT COALESCE(MAX(generation), 0) + 1 FROM runs").fetchone()[0])

    def activate(self, manifest: dict[str, Any]) -> dict[str, Any]:
        generation = int(manifest["runGeneration"])
        identity = manifest_identity(manifest)
        now = _now()
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                expected = int(db.execute("SELECT COALESCE(MAX(generation), 0) + 1 FROM runs").fetchone()[0])
                if generation != expected:
                    raise ValueError(f"run generation {generation} is stale; next is {expected}")
                db.execute(
                    "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?)",
                    (generation, manifest["simulationRunId"], identity, "RUNNING", now, now),
                )
                db.execute(
                    "INSERT INTO live_selection(singleton, generation) VALUES (1, ?) "
                    "ON CONFLICT(singleton) DO UPDATE SET generation=excluded.generation",
                    (generation,),
                )
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise
        return self.selected()

    def selected(self) -> dict[str, Any] | None:
        with closing(self._connect()) as db:
            row = db.execute("""
                SELECT r.generation, r.run_id, r.manifest_identity, r.status,
                       r.activated_at, r.updated_at
                FROM live_selection s JOIN runs r ON r.generation = s.generation
                WHERE s.singleton = 1
            """).fetchone()
        return dict(row) if row else None

    def assert_selected(self, manifest: dict[str, Any]) -> None:
        selected = self.selected()
        if selected is None or selected["run_id"] != manifest["simulationRunId"]:
            raise ValueError("V1 run is not the selected live run")
        if selected["generation"] != manifest["runGeneration"]:
            raise ValueError("V1 run generation is stale")
        if selected["manifest_identity"] != manifest_identity(manifest):
            raise ValueError("selected run manifest identity differs")

    def assert_publishable(self, manifest: dict[str, Any]) -> None:
        self.assert_selected(manifest)
        selected = self.selected()
        if selected["status"] != "RUNNING":
            raise ValueError(f"selected V1 run is {selected['status']}")

    def set_status(self, run_id: str, status: str) -> None:
        if status not in {"COMPLETE", "INCOMPLETE", "STOPPED"}:
            raise ValueError(f"invalid run status {status}")
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
                if row is None:
                    raise ValueError(f"unknown run {run_id}")
                if row["status"] == "INCOMPLETE" and status != "INCOMPLETE":
                    raise ValueError("incomplete run cannot be marked complete")
                db.execute(
                    "UPDATE runs SET status = ?, updated_at = ? WHERE run_id = ?",
                    (status, _now(), run_id),
                )
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    def select_replay(self, run_id: str) -> dict[str, Any]:
        """Explicit replay lookup; deliberately leaves live_selection untouched."""
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT generation, run_id, manifest_identity, status FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise ValueError(f"unknown replay run {run_id}")
        return dict(row)
