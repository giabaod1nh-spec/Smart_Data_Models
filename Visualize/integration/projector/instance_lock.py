"""Runtime exclusive lock for single production projector instance (K-5)."""
from __future__ import annotations

import atexit
import os
import socket
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class ProjectorInstanceAlreadyRunning(Exception):
    pass


class InstanceLock:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = Path(str(self.db_path) + ".lock")
        self._file_handle: Optional[object] = None
        self._conn: Optional[sqlite3.Connection] = None

    def acquire(self) -> None:
        self._acquire_file_lock()
        self._acquire_sqlite_lock()
        atexit.register(self.release)

    def release(self) -> None:
        if self._conn is not None:
            try:
                self._conn.execute("DELETE FROM projector_instance_lock WHERE lock_id=1")
                self._conn.commit()
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        if self._file_handle is not None:
            try:
                if sys.platform == "win32":
                    import msvcrt

                    msvcrt.locking(self._file_handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._file_handle.fileno(), fcntl.LOCK_UN)
                self._file_handle.close()
            except Exception:
                pass
            self._file_handle = None

    def _acquire_file_lock(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.lock_path, "a+b")
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as e:
            fh.close()
            raise ProjectorInstanceAlreadyRunning(
                "PROJECTOR_INSTANCE_ALREADY_RUNNING: file lock held"
            ) from e
        self._file_handle = fh

    def _acquire_sqlite_lock(self) -> None:
        conn = sqlite3.connect(str(self.db_path), isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS projector_instance_lock (
                lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
                holder_pid INTEGER NOT NULL,
                holder_host TEXT NOT NULL,
                acquired_at TEXT NOT NULL
            )
            """
        )
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        conn.execute("BEGIN EXCLUSIVE")
        row = conn.execute(
            "SELECT holder_pid, holder_host FROM projector_instance_lock WHERE lock_id=1"
        ).fetchone()
        if row and int(row[0]) != os.getpid():
            conn.execute("ROLLBACK")
            conn.close()
            raise ProjectorInstanceAlreadyRunning(
                f"PROJECTOR_INSTANCE_ALREADY_RUNNING: pid={row[0]} host={row[1]}"
            )
        conn.execute(
            """
            INSERT INTO projector_instance_lock (lock_id, holder_pid, holder_host, acquired_at)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(lock_id) DO UPDATE SET
                holder_pid=excluded.holder_pid,
                holder_host=excluded.holder_host,
                acquired_at=excluded.acquired_at
            """,
            (os.getpid(), socket.gethostname(), now),
        )
        conn.execute("COMMIT")
        self._conn = conn
