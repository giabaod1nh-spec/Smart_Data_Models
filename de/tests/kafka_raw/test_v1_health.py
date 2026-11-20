"""Readiness is operational availability, not commit recency."""
from types import SimpleNamespace
import pytest
from de.tests.kafka_raw.test_v1_consumer_recovery import worker

def configured(tmp_path):
    w=worker(tmp_path,[])
    w.state="READY"
    w.broker_ok=True
    w.identity="uuid"
    w.failure_reason=None
    w.offset_snapshot=[]
    w.last_persistence=None
    w.last_commit=1
    w.repo.client=SimpleNamespace(command=lambda q:1)
    return w

def test_idle_ready_despite_old_commit(tmp_path):
    w=configured(tmp_path)
    assert w.health()["ready"]

@pytest.mark.parametrize("state",["FAULTED","DEGRADED","STOPPED"])
def test_fault_state_not_ready(tmp_path,state):
    w=configured(tmp_path)
    w.state=state
    assert not w.health()["ready"]

def test_no_assignment_or_broker_not_ready(tmp_path):
    w=configured(tmp_path)
    w.broker_ok=False
    assert not w.health()["ready"]
    w.broker_ok=True
    w.assigned.clear()
    assert not w.health()["ready"]

def test_clickhouse_failure_not_ready(tmp_path):
    w=configured(tmp_path)
    def failed(q): raise OSError("offline")
    w.repo.client.command=failed
    assert not w.health()["ready"]

def test_ledger_failure_not_ready(tmp_path):
    w=configured(tmp_path)
    w.ledger.close()
    assert not w.health()["ready"]

