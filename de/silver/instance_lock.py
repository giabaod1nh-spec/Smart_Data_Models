"""Silver Plan 3 — OS file lock + SQLite audit row (namespace-scoped)."""
from __future__ import annotations

import atexit
import os
import socket
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from de.silver import PROCESSOR_VERSION


class SilverInstanceAlreadyRunning(Exception):
    pass


def _safe_lock_suffix(namespace: str) -> str:
    # live -> live; replay:run-id -> replay.run-id
    return namespace.replace(":", ".")


class InstanceLock:
    def __init__(
        self,
        checkpoint_path: Path,
        namespace: str,
        *,
        processor_version: str = PROCESSOR_VERSION,
    ) -> None:
        self.checkpoint_path = Path(checkpoint_path)
        self.namespace = namespace
        self.processor_version = processor_version
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = _safe_lock_suffix(namespace)
        self.lock_path = Path(f"{self.checkpoint_path}.{suffix}.lock")
        self._file_handle: Optional[object] = None
        self._conn: Optional[sqlite3.Connection] = None
        self._held = False

    @property
    def held(self) -> bool:
        return self._held

    def acquire(self) -> None:
        self._acquire_file_lock()
        self._acquire_sqlite_audit()
        self._held = True
        atexit.register(self.release)

    def release(self) -> None:
        if self._conn is not None:
            try:
                self._conn.execute(
                    "DELETE FROM silver_instance_lock WHERE lock_namespace=?",
                    (self.namespace,),
                )
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
        self._held = False

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
            raise SilverInstanceAlreadyRunning(
                f"SILVER_INSTANCE_ALREADY_RUNNING: file lock held for {self.namespace}"
            ) from e
        self._file_handle = fh

    def _acquire_sqlite_audit(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        conn = sqlite3.connect(str(self.checkpoint_path), isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_instance_lock (
                lock_namespace TEXT PRIMARY KEY,
                holder_pid INTEGER NOT NULL,
                holder_host TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                processor_version TEXT NOT NULL
            )
            """
        )
        # OS lock is authoritative; replace stale audit row after OS lock acquired.
        conn.execute(
            """
            INSERT INTO silver_instance_lock (
                lock_namespace, holder_pid, holder_host, acquired_at, processor_version
            ) VALUES (?,?,?,?,?)
            ON CONFLICT(lock_namespace) DO UPDATE SET
                holder_pid=excluded.holder_pid,
                holder_host=excluded.holder_host,
                acquired_at=excluded.acquired_at,
                processor_version=excluded.processor_version
            """,
            (self.namespace, os.getpid(), socket.gethostname(), now, self.processor_version),
        )
        self._conn = conn
