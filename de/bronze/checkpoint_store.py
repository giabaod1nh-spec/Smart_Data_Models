"""SQLite checkpoint + processing ledger (namespace-aware)."""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS bronze_instance_lock (
    lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
    holder_pid INTEGER NOT NULL,
    holder_host TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze_checkpoint (
    checkpoint_namespace TEXT NOT NULL DEFAULT 'live',
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    last_completed_offset INTEGER NOT NULL DEFAULT -1,
    source_start_offset INTEGER NOT NULL DEFAULT -1,
    start_mode TEXT NOT NULL DEFAULT 'earliest',
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    bronze_schema_version TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (checkpoint_namespace, topic, partition_id)
);

CREATE TABLE IF NOT EXISTS bronze_processing_ledger (
    checkpoint_namespace TEXT NOT NULL DEFAULT 'live',
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    raw_ingestion_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    status TEXT NOT NULL,
    bronze_ingestion_id TEXT,
    payload_hash TEXT,
    last_error TEXT,
    completed_at TEXT NOT NULL,
    PRIMARY KEY (checkpoint_namespace, topic, partition_id, offset_value),
    UNIQUE (checkpoint_namespace, raw_ingestion_id)
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class CheckpointRow:
    checkpoint_namespace: str
    topic: str
    partition_id: int
    last_completed_offset: int
    source_start_offset: int
    start_mode: str
    processor_name: str
    processor_version: str
    bronze_schema_version: str
    updated_at: str


class CheckpointStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get(
        self, namespace: str, topic: str, partition: int
    ) -> Optional[CheckpointRow]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM bronze_checkpoint
                WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                """,
                (namespace, topic, int(partition)),
            ).fetchone()
            if not row:
                return None
            return CheckpointRow(
                checkpoint_namespace=row["checkpoint_namespace"],
                topic=row["topic"],
                partition_id=int(row["partition_id"]),
                last_completed_offset=int(row["last_completed_offset"]),
                source_start_offset=int(row["source_start_offset"]),
                start_mode=row["start_mode"],
                processor_name=row["processor_name"],
                processor_version=row["processor_version"],
                bronze_schema_version=row["bronze_schema_version"],
                updated_at=row["updated_at"],
            )

    def init_checkpoint(
        self,
        *,
        namespace: str,
        topic: str,
        partition: int,
        source_start_offset: int,
        last_completed_offset: int,
        start_mode: str,
        processor_name: str,
        processor_version: str,
        bronze_schema_version: str,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO bronze_checkpoint (
                    checkpoint_namespace, topic, partition_id,
                    last_completed_offset, source_start_offset, start_mode,
                    processor_name, processor_version, bronze_schema_version, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(checkpoint_namespace, topic, partition_id) DO NOTHING
                """,
                (
                    namespace,
                    topic,
                    int(partition),
                    int(last_completed_offset),
                    int(source_start_offset),
                    start_mode,
                    processor_name,
                    processor_version,
                    bronze_schema_version,
                    now,
                ),
            )

    def advance(
        self, namespace: str, topic: str, partition: int, last_completed: int
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE bronze_checkpoint
                SET last_completed_offset=?, updated_at=?
                WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                """,
                (int(last_completed), now, namespace, topic, int(partition)),
            )

    def is_complete(
        self, namespace: str, topic: str, partition: int, offset: int
    ) -> bool:
        return offset in self.is_complete_batch(namespace, topic, partition, [offset])

    def is_complete_batch(
        self,
        namespace: str,
        topic: str,
        partition: int,
        offsets: List[int],
    ) -> set[int]:
        if not offsets:
            return set()
        with self._lock:
            placeholders = ",".join("?" * len(offsets))
            rows = self._conn.execute(
                f"""
                SELECT offset_value FROM bronze_processing_ledger
                WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                  AND offset_value IN ({placeholders})
                """,
                (namespace, topic, int(partition), *map(int, offsets)),
            ).fetchall()
            return {int(r[0]) for r in rows}

    def mark_complete(
        self,
        *,
        namespace: str,
        topic: str,
        partition: int,
        offset: int,
        raw_ingestion_id: str,
        destination: str,
        status: str,
        payload_hash: Optional[str] = None,
        bronze_ingestion_id: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO bronze_processing_ledger (
                    checkpoint_namespace, topic, partition_id, offset_value,
                    raw_ingestion_id, destination, status,
                    bronze_ingestion_id, payload_hash, last_error, completed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,NULL,?)
                ON CONFLICT(checkpoint_namespace, topic, partition_id, offset_value) DO UPDATE SET
                    raw_ingestion_id=excluded.raw_ingestion_id,
                    destination=excluded.destination,
                    status=excluded.status,
                    bronze_ingestion_id=excluded.bronze_ingestion_id,
                    payload_hash=excluded.payload_hash,
                    completed_at=excluded.completed_at
                """,
                (
                    namespace,
                    topic,
                    int(partition),
                    int(offset),
                    raw_ingestion_id,
                    destination,
                    status,
                    bronze_ingestion_id,
                    payload_hash,
                    now,
                ),
            )

    def commit_batch(
        self,
        namespace: str,
        topic: str,
        partition: int,
        entries: List[Dict[str, Any]],
        last_completed: int,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for e in entries:
                    self._conn.execute(
                        """
                        INSERT INTO bronze_processing_ledger (
                            checkpoint_namespace, topic, partition_id, offset_value,
                            raw_ingestion_id, destination, status,
                            bronze_ingestion_id, payload_hash, last_error, completed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,NULL,?)
                        ON CONFLICT(checkpoint_namespace, topic, partition_id, offset_value)
                        DO UPDATE SET
                            status=excluded.status,
                            completed_at=excluded.completed_at
                        """,
                        (
                            namespace,
                            topic,
                            int(partition),
                            int(e["offset"]),
                            e["raw_ingestion_id"],
                            e["destination"],
                            e["status"],
                            e.get("bronze_ingestion_id"),
                            e.get("payload_hash"),
                            now,
                        ),
                    )
                self._conn.execute(
                    """
                    UPDATE bronze_checkpoint
                    SET last_completed_offset=?, updated_at=?
                    WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                    """,
                    (int(last_completed), now, namespace, topic, int(partition)),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
