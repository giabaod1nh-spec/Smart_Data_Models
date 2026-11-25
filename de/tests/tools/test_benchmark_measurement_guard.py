import json

from de.tools import gold_core_capacity
from de.tools.gold_core_capacity import MeasurementGuard


def test_silver_status_fault_and_raw_worker_liveness():
    guard = MeasurementGuard()
    result = guard.assess({"silver": {"status": "FAULTED", "ready": False}},
                          wall_utc=100, monotonic=25)
    assert result["stop_reason"] == "silver: terminal state FAULTED"
    result = guard.assess({"raw": {"state": "READY", "consumer_thread_alive": False}},
                          wall_utc=100, monotonic=26)
    assert result["invalid"] == {"raw": "worker not alive"}


def test_transient_recovery_resets_sustained_loss_budget():
    guard = MeasurementGuard(missing_budget_sec=15)
    assert not guard.assess({"bronze": None}, wall_utc=100, monotonic=0)["valid"]
    assert guard.assess({"bronze": {"state": "READY"}}, wall_utc=110, monotonic=10)["valid"]
    assert guard.assess({"bronze": None}, wall_utc=120, monotonic=20)["stop_reason"] is None
    assert guard.assess({"bronze": None}, wall_utc=134, monotonic=34)["stop_reason"] is None
    assert "endpoint unavailable" in guard.assess(
        {"bronze": None}, wall_utc=135, monotonic=35)["stop_reason"]


def test_expected_outage_still_marks_sample_invalid_and_preserves_terminal_stop():
    guard = MeasurementGuard(allowed_outages=["gold"])
    services = {"gold": None, "bronze": {"state": "READY"}}
    guard.assess(services, wall_utc=100, monotonic=0)
    result = guard.assess(services, wall_utc=300, monotonic=200)
    assert not result["valid"]
    assert result["stop_reason"] is None
    services["gold"] = {"status": "STOPPED"}
    assert guard.assess(services, wall_utc=301, monotonic=201)["stop_reason"] is None
    services["gold"]["fault_status"] = {"changes": {"retryable": False}}
    assert guard.assess(services, wall_utc=301, monotonic=201)["stop_reason"] == "gold: terminal reported fault"
    services["gold"] = {"status": "FAILED"}
    assert guard.assess(services, wall_utc=302, monotonic=202)["stop_reason"]


def test_stale_snapshots_fail_even_when_ready():
    guard = MeasurementGuard(snapshot_budget_sec=60)
    services = {"gold": {"state": "READY", "snapshot_at": "1970-01-01T00:00:10Z"},
                "bronze": {"state": "READY", "metrics_stale": True}}
    first = guard.assess(services, wall_utc=90, monotonic=5)
    assert first["invalid"] == {"gold": "snapshot too old", "bronze": "metrics flagged stale"}
    assert guard.assess(services, wall_utc=105, monotonic=20)["stop_reason"]


def test_healthy_payload_requires_a_reported_state():
    result = MeasurementGuard().assess({"bronze": {"state": None, "status": " "}},
                                       wall_utc=100, monotonic=0)
    assert result["invalid"] == {"bronze": "endpoint has no running snapshot"}


def test_sampler_persists_terminal_measurement_before_stopping(tmp_path, monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self):
            return '{"status":"FAULTED"}'

    monkeypatch.setattr(gold_core_capacity, "ENDPOINTS", {"silver": (1, "health")})
    monkeypatch.setattr(gold_core_capacity, "urlopen", lambda *_args, **_kw: Response())
    monkeypatch.setattr(gold_core_capacity, "outbox", lambda _path: None)
    assert gold_core_capacity.sample(tmp_path, 2) == 2
    row = json.loads((tmp_path / "samples.jsonl").read_text())
    assert row["measurement"]["stop_reason"] == "silver: terminal state FAULTED"
