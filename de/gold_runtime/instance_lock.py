"""Single-writer guard: namespace-scoped OS file lock plus a SQLite lease row.

The lock is acquired before any read that can lead to a write. Losing it stops the
processor and makes readiness false. Live and replay namespaces never share a lock
path or a runtime SQLite database.
"""
from __future__ import annotations

import atexit
import os
import socket
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from de.gold_runtime import PROCESSOR_VERSION
from de.gold_runtime.checkpoint_store import GoldRuntimeStore, LeaseRow

DEFAULT_LEASE_TTL_SEC = 60.0


class GoldInstanceAlreadyRunning(Exception):
    """Another writer holds the namespace lock; start-up must exit without writes."""


class GoldLockLost(Exception):
    """The lease row was taken over by another owner."""


def _safe_suffix(namespace: str) -> str:
    return namespace.replace(":", ".")


def _utc_str(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class InstanceLock:
    def __init__(
        self,
        lock_path: Path | str,
        namespace: str,
        store: Optional[GoldRuntimeStore] = None,
        *,
        processor_version: str = PROCESSOR_VERSION,
        lease_ttl_sec: float = DEFAULT_LEASE_TTL_SEC,
    ) -> None:
        self.namespace = namespace
        self.processor_version = processor_version
        self.lease_ttl_sec = float(lease_ttl_sec)
        base = Path(lock_path)
        base.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = base.with_name(f"{base.name}.{_safe_suffix(namespace)}")
        self.owner_id = f"{socket.gethostname()}:{os.getpid()}"
        self.lease_token = uuid.uuid4().hex
        self._store = store
        self._file_handle: Optional[object] = None
        self._held = False

    @property
    def held(self) -> bool:
        return self._held

    def acquire(self) -> Optional[LeaseRow]:
        self._acquire_file_lock()
        lease = self._write_lease()
        self._held = True
        atexit.register(self.release)
        return lease

    def release(self) -> None:
        if self._store is not None and self._held:
            try:
                self._store.release_lease(self.namespace, self.lease_token)
            except Exception:
                pass
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

    def verify(self) -> bool:
        """Readiness check: the lease row must still be owned by this process."""
        if not self._held:
            return False
        if self._store is None:
            return True
        lease = self._store.get_lease(self.namespace)
        return lease is not None and lease.lease_token == self.lease_token

    def renew(self) -> Optional[LeaseRow]:
        if not self._held or self._store is None:
            return None
        lease = self._store.get_lease(self.namespace)
        if lease is not None and lease.lease_token != self.lease_token:
            raise GoldLockLost(f"lease for {self.namespace} taken over by {lease.owner_id}")
        return self._write_lease()

    def _write_lease(self) -> Optional[LeaseRow]:
        if self._store is None:
            return None
        now = datetime.now(timezone.utc)
        return self._store.write_lease(
            self.namespace,
            owner_id=self.owner_id,
            lease_token=self.lease_token,
            acquired_at=_utc_str(now),
            expires_at=_utc_str(now + timedelta(seconds=self.lease_ttl_sec)),
        )

    def _acquire_file_lock(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.lock_path, "a+b")
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            handle.close()
            raise GoldInstanceAlreadyRunning(
                f"GOLD_INSTANCE_ALREADY_RUNNING: lock held for {self.namespace}"
            ) from exc
        self._file_handle = handle
