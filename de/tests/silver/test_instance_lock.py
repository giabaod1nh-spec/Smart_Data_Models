"""Instance lock namespace isolation tests."""
from __future__ import annotations

from pathlib import Path

import pytest

from de.silver.instance_lock import InstanceLock, SilverInstanceAlreadyRunning


def test_live_lock_exclusive(tmp_path: Path):
    db = tmp_path / "cp.sqlite3"
    a = InstanceLock(db, "live")
    b = InstanceLock(db, "live")
    a.acquire()
    try:
        with pytest.raises(SilverInstanceAlreadyRunning):
            b.acquire()
    finally:
        a.release()


def test_live_and_replay_locks_independent(tmp_path: Path):
    db = tmp_path / "cp.sqlite3"
    live = InstanceLock(db, "live")
    replay = InstanceLock(db, "replay:r1")
    live.acquire()
    try:
        replay.acquire()
        assert live.held and replay.held
    finally:
        replay.release()
        live.release()
