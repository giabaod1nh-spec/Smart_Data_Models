"""SQLite ledger — write only on STORED/QUARANTINED (no hot-path PENDING)."""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_consumer_ledger (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    raw_ingestion_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    status TEXT NOT NULL,
    event_id TEXT,
    payload_hash TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    consumed_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (topic, partition_id, offset_value),
    UNIQUE (raw_ingestion_id)
);
"""

STATUS_STORED = "STORED"
STATUS_QUARANTINED = "QUARANTINED"
STATUS_FAILED_RETRYABLE = "FAILED_RETRYABLE"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class LedgerStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get(
        self, topic: str, partition: int, offset: int
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM raw_consumer_ledger
                WHERE topic=? AND partition_id=? AND offset_value=?
                """,
                (topic, int(partition), int(offset)),
            ).fetchone()
            return dict(row) if row else None

    def get_by_ingestion_id(self, raw_ingestion_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM raw_consumer_ledger WHERE raw_ingestion_id=?",
                (raw_ingestion_id,),
            ).fetchone()
            return dict(row) if row else None

    def mark_complete(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        raw_ingestion_id: str,
        destination: str,
        status: str,
        event_id: Optional[str] = None,
        payload_hash: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO raw_consumer_ledger (
                    topic, partition_id, offset_value, raw_ingestion_id,
                    destination, status, event_id, payload_hash,
                    retry_count, last_error, consumed_at, completed_at
                ) VALUES (?,?,?,?,?,?,?,?,0,NULL,?,?)
                ON CONFLICT(topic, partition_id, offset_value) DO UPDATE SET
                    raw_ingestion_id=excluded.raw_ingestion_id,
                    destination=excluded.destination,
                    status=excluded.status,
                    event_id=excluded.event_id,
                    payload_hash=excluded.payload_hash,
                    completed_at=excluded.completed_at
                """,
                (
                    topic,
                    int(partition),
                    int(offset),
                    raw_ingestion_id,
                    destination,
                    status,
                    event_id,
                    payload_hash,
                    now,
                    now,
                ),
            )

    def is_complete(self, topic: str, partition: int, offset: int) -> bool:
        row = self.get(topic, partition, offset)
        return bool(row and row["status"] in (STATUS_STORED, STATUS_QUARANTINED))

    def max_completed_offset(self, topic: str, partition: int) -> Optional[int]:
        """Highest offset durably classified into Raw or quarantine."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT MAX(offset_value) AS max_offset
                FROM raw_consumer_ledger
                WHERE topic=? AND partition_id=? AND status IN (?, ?)
                """,
                (topic, int(partition), STATUS_STORED, STATUS_QUARANTINED),
            ).fetchone()
            value = row["max_offset"] if row else None
            return None if value is None else int(value)
