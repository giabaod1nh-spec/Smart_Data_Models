"""Runtime instance lock — second acquire must fail."""
from __future__ import annotations

from pathlib import Path

import pytest

from de.bronze.instance_lock import BronzeInstanceAlreadyRunning, InstanceLock


def test_second_instance_lock_fails(tmp_path: Path) -> None:
    db = tmp_path / "checkpoint.sqlite3"
    lock1 = InstanceLock(db)
    lock1.acquire()
    lock2 = InstanceLock(db)
    with pytest.raises(BronzeInstanceAlreadyRunning):
        lock2.acquire()
    lock1.release()
