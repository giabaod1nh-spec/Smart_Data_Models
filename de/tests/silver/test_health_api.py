"""Health API / ready truth table tests."""
from __future__ import annotations

from de.silver.config import ProcessorState
from de.silver.health_api import app, bind_processor
from de.silver.metrics import HealthSnapshot
from fastapi.testclient import TestClient


class _FakeProc:
    def __init__(self, snap: HealthSnapshot):
        self._snap = snap

    def health_snapshot(self):
        return self._snap


def test_ready_503_when_not_ready():
    snap = HealthSnapshot(
        state=ProcessorState.STARTING.value,
        ready=False,
        worker_alive=False,
        reader_initialized=False,
        clickhouse_ok=False,
        sqlite_ok=False,
        schema_ok=False,
        lock_held=False,
        namespace="live",
        mode="main",
        shutdown_requested=False,
        snapshot_at="2099-01-01T00:00:00+00:00",
        metrics={},
    )
    bind_processor(_FakeProc(snap), max_age_sec=99999)
    tc = TestClient(app)
    assert tc.get("/ready").status_code == 503
    assert tc.get("/health").status_code == 200


def test_ready_200_when_ready_predicates_met():
    snap = HealthSnapshot(
        state=ProcessorState.READY.value,
        ready=True,
        worker_alive=True,
        reader_initialized=True,
        clickhouse_ok=True,
        sqlite_ok=True,
        schema_ok=True,
        lock_held=True,
        namespace="live",
        mode="main",
        shutdown_requested=False,
        snapshot_at="2099-01-01T00:00:00+00:00",
        metrics={},
    )
    bind_processor(_FakeProc(snap), max_age_sec=99999)
    tc = TestClient(app)
    r = tc.get("/ready")
    assert r.status_code == 200
    assert r.json()["ready"] is True


def test_ready_503_when_faulted_even_if_snapshot_recent():
    snap = HealthSnapshot(
        state=ProcessorState.FAULTED.value,
        ready=False,
        worker_alive=True,
        reader_initialized=True,
        clickhouse_ok=True,
        sqlite_ok=True,
        schema_ok=True,
        lock_held=True,
        namespace="live",
        mode="main",
        shutdown_requested=False,
        snapshot_at="2099-01-01T00:00:00+00:00",
        metrics={},
        fault_code="CAS_CONFLICT",
    )
    bind_processor(_FakeProc(snap), max_age_sec=99999)
    tc = TestClient(app)
    assert tc.get("/ready").status_code == 503
    assert tc.get("/health").status_code == 200
