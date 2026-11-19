"""Focused Phase 3 boundaries using the actual Phase 2 SUMO capture."""
from __future__ import annotations

import copy
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
VIS = ROOT / "Visualize"
if str(VIS) not in sys.path:
    sys.path.insert(0, str(VIS))

from contracts.topology.v1_digest import topology_digest  # noqa: E402
from integration.kafka.outbox_store import OutboxIdentityConflictError  # noqa: E402
from integration.kafka.outbox_store import KafkaOutboxStore  # noqa: E402
from integration.kafka.outbox_worker import OutboxDeliveryWorker  # noqa: E402
from integration.kafka.v1_publisher import InvalidV1CycleError, V1DurablePublisher  # noqa: E402
from observation.canonical_cycle import (  # noqa: E402
    CanonicalCycleSnapshot, CanonicalIntersection, V1CycleResult, freeze_run_catalog,
)
from runtime.active_run_registry import ActiveRunRegistry  # noqa: E402
from runtime.run_manifest import atomic_write_json, read_verified_v1_manifest  # noqa: E402
from runtime.v1_startup import validate_v1_publish_mode  # noqa: E402
from simulation.backend import SumoBackend  # noqa: E402


@pytest.fixture
def v1_package(tmp_path):
    evidence = json.loads((ROOT / "phase-2/fixtures/current-sumo-cycle.json").read_text(encoding="utf-8"))
    data = evidence["canonicalSnapshot"]
    run_id = data["simulationRunId"]
    catalog = json.loads((VIS / "generated/network_topology_catalog.json").read_text(encoding="utf-8"))
    context = freeze_run_catalog(run_id, catalog, tmp_path / "runs" / run_id / "network_topology_catalog.json")
    assert context.archive_ready and context.topology_digest == topology_digest(catalog)
    intersections = tuple(
        CanonicalIntersection(
            item["intersectionId"],
            json.dumps(item["source"]).encode("utf-8"),
            json.dumps(item["quality"]).encode("utf-8"),
        )
        for item in data["intersections"]
    )
    snapshot = CanonicalCycleSnapshot(
        run_id, data["simulationTimeSeconds"], data["simulationStep"],
        data["cycleSequence"], data["capturedAt"], data["topologyDigest"], intersections,
    )
    result = V1CycleResult(data["cycleSequence"], snapshot, tuple(copy.deepcopy(evidence["observations"])))
    manifest_path = tmp_path / "runs" / run_id / "run_manifest.json"
    manifest = {
        "simulationRunId": run_id,
        "topologyDigest": context.topology_digest,
        "topology_hash": catalog["topology_hash"],
        "participatingIntersectionIds": [item.intersection_id for item in intersections],
        "runGeneration": 1,
        "runStatus": "READY",
    }
    atomic_write_json(manifest_path, manifest)
    registry = ActiveRunRegistry(tmp_path / "runs" / "active_run.sqlite3")
    registry.activate(manifest)
    publisher = V1DurablePublisher(db_path=tmp_path / "v1-outbox.sqlite3")
    yield result, manifest_path, context, registry, publisher
    publisher.store.close()


def test_freeze_exact_v1_envelopes_and_atomic_duplicate_reuse(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    frozen = publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    assert len(frozen.rows) == 5
    assert all(row.topic == "traffic.observations.v1" for row in frozen.rows)
    assert all("TrafficSimulationRunStarted" not in row.payload_json for row in frozen.rows)
    assert all(OutboxDeliveryWorker._payload_defect(row) is None for row in frozen.rows)
    original_bytes = frozen.rows[0].payload_json.encode("utf-8")
    result.observations[0]["payload"]["scenario"] = "mutated"
    assert frozen.rows[0].payload_json.encode("utf-8") == original_bytes
    first = publisher.persist_frozen(frozen)
    assert (first.inserted, first.reused_pending, first.already_delivered) == (5, 0, 0)
    duplicate = publisher.store.append_v1_cycle(frozen.rows)
    assert (duplicate.inserted, duplicate.reused_pending) == (0, 5)
    publisher.store.mark_acked(frozen.rows[0].event_id, partition=0, offset=1)
    duplicate = publisher.store.append_v1_cycle(frozen.rows)
    assert duplicate.already_delivered == 1
    assert duplicate.reused_pending == 4
    assert publisher.store._conn.execute("SELECT COUNT(*) FROM kafka_outbox").fetchone()[0] == 5


def test_conflict_rolls_back_new_cycle_rows(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    frozen = publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    publisher.persist_frozen(frozen)
    invented = replace(frozen.rows[0], event_id="invented-new-id")
    conflict = replace(frozen.rows[1], payload_json=frozen.rows[1].payload_json + " ")
    with pytest.raises(OutboxIdentityConflictError):
        publisher.store.append_v1_cycle((invented, conflict))
    assert publisher.store._conn.execute("SELECT COUNT(*) FROM kafka_outbox").fetchone()[0] == 5


def test_archive_or_manifest_tampering_prevents_outbox_insert(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    context.archive_path.write_text("{}", encoding="utf-8")
    with pytest.raises(InvalidV1CycleError, match="archive|catalog|topology"):
        publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    assert publisher.store._conn.execute("SELECT COUNT(*) FROM kafka_outbox").fetchone()[0] == 0


def test_missing_or_mismatched_manifest_prevents_publishing(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["participatingIntersectionIds"] = ["A"]
    atomic_write_json(manifest_path, manifest)
    with pytest.raises(InvalidV1CycleError, match="manifest"):
        publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    manifest_path.unlink()
    with pytest.raises(InvalidV1CycleError, match="run package"):
        publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    assert publisher.store._conn.execute("SELECT COUNT(*) FROM kafka_outbox").fetchone()[0] == 0


def test_active_run_generation_fences_old_run(v1_package):
    result, manifest_path, context, registry, _publisher = v1_package
    first = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert registry.next_generation() == 2
    second = dict(first, simulationRunId="next-run", runGeneration=2)
    registry.activate(second)
    with pytest.raises(ValueError, match="selected"):
        registry.assert_selected(first)
    assert registry.select_replay(first["simulationRunId"])["run_id"] == first["simulationRunId"]
    assert registry.selected()["run_id"] == second["simulationRunId"]
    with pytest.raises(ValueError, match="stale"):
        registry.activate(dict(first, simulationRunId="third-run"))


def test_manifest_readback_and_startup_validation(v1_package, tmp_path):
    result, manifest_path, context, registry, _publisher = v1_package
    assert read_verified_v1_manifest(
        manifest_path, run_id=result.snapshot.simulation_run_id,
        topology_digest=context.topology_digest,
        participants=tuple(item.intersection_id for item in result.snapshot.intersections),
        generation=1,
    )["runGeneration"] == 1
    args = SimpleNamespace(v1_publish=True, v1_observations=True, no_orion=True,
                           sync_publish=False, async_publish=False)
    cfg = SimpleNamespace(V1_PUBLISH_ENABLED=False, KAFKA_PUBLISH_ENABLED=False,
                          KAFKA_OUTBOX_ENABLED=False, ORION_PUBLISH_ENABLED=True,
                          V1_KAFKA_TOPIC="traffic.observations.v1",
                          KAFKA_BOOTSTRAP_SERVERS="localhost:29092",
                          V1_OUTBOX_DB=tmp_path / "v1.sqlite3",
                          KAFKA_OUTBOX_DB=tmp_path / "v2.sqlite3")
    assert validate_v1_publish_mode(args=args, cfg=cfg)
    with pytest.raises(ValueError, match="direct Orion"):
        validate_v1_publish_mode(args=SimpleNamespace(**{**vars(args), "no_orion": False}), cfg=cfg)
    with pytest.raises(ValueError, match="KAFKA_OUTBOX_ENABLED"):
        validate_v1_publish_mode(args=args, cfg=SimpleNamespace(**{**vars(cfg), "KAFKA_OUTBOX_ENABLED": True}))


def test_worker_restarts_and_delivers_original_frozen_bytes(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    frozen = publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    publisher.persist_frozen(frozen)
    publisher.store.mark_queued_batch([frozen.rows[0].event_id])
    db_path = publisher.store.db_path
    publisher.store.close()
    store = KafkaOutboxStore(db_path)
    try:
        assert store.recover_orphaned_queued() == 1
        produced = []

        class FakeProducer:
            def produce(self, topic, *, key, value, on_delivery):
                produced.append((topic, key, value))
                on_delivery(None, SimpleNamespace(partition=lambda: 0, offset=lambda: len(produced)))

            def poll(self, _timeout):
                return 0

        worker = OutboxDeliveryWorker(store, producer=FakeProducer())
        worker._dispatch_batch(store.fetch_eligible(limit=10))
        worker._flush_status_buffers()
        assert len(produced) == 5
        assert produced[0] == (
            "traffic.observations.v1", frozen.rows[0].event_key.encode("utf-8"),
            frozen.rows[0].payload_json.encode("utf-8"),
        )
        assert store.capacity_metrics()["counts"]["ACKED"] == 5
    finally:
        store.close()


def test_failed_structural_cycle_records_incomplete_run(v1_package, tmp_path, monkeypatch):
    import configuration.config as cfg

    result, manifest_path, context, registry, publisher = v1_package
    monkeypatch.setattr(cfg, "ARTIFACTS_DIR", tmp_path)
    backend = object.__new__(SumoBackend)
    backend.v1_publishing = True
    backend.v1_run_registry = registry
    backend.simulation_run_id = result.snapshot.simulation_run_id
    backend.run_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    backend.v1_failed_cycle_count = 0
    failed = V1CycleResult(result.cycle_sequence + 1, failure_code="MISSING_TLS_STATE",
                           failure_reason="signal state absent")
    with pytest.raises(InvalidV1CycleError):
        publisher.freeze_cycle(failed, manifest_path=manifest_path, context=context, registry=registry)
    backend.set_v1_run_status("INCOMPLETE", failed_cycle=failed)
    record = json.loads((manifest_path.parent / "v1_failed_cycle.json").read_text(encoding="utf-8"))
    assert record["cycleSequence"] == failed.cycle_sequence
    assert record["failureCode"] == "MISSING_TLS_STATE"
    assert record["failedCycleCount"] == 1
    assert registry.selected()["status"] == "INCOMPLETE"
    assert publisher.store._conn.execute("SELECT COUNT(*) FROM kafka_outbox").fetchone()[0] == 0


def test_broker_failure_keeps_frozen_event_retryable(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    frozen = publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    publisher.persist_frozen(frozen)
    row = publisher.store.fetch_eligible(limit=1)[0]
    publisher.store.mark_queued_batch([row.event_id])

    class FailedProducer:
        def produce(self, _topic, *, key, value, on_delivery):
            assert key == frozen.rows[0].event_key.encode("utf-8")
            assert value == frozen.rows[0].payload_json.encode("utf-8")
            on_delivery(RuntimeError("broker unavailable"), None)

        def poll(self, _timeout):
            return 0

    worker = OutboxDeliveryWorker(publisher.store, producer=FailedProducer())
    worker._dispatch(row)
    worker._flush_status_buffers()
    stored = publisher.store._conn.execute(
        "SELECT status, payload_json FROM kafka_outbox WHERE event_id = ?", (row.event_id,),
    ).fetchone()
    assert stored["status"] == "FAILED_RETRYABLE"
    assert stored["payload_json"] == row.payload_json


def test_crash_between_broker_ack_and_sqlite_ack_resends_same_bytes(v1_package):
    result, manifest_path, context, registry, publisher = v1_package
    frozen = publisher.freeze_cycle(result, manifest_path=manifest_path, context=context, registry=registry)
    publisher.persist_frozen(frozen)
    row = publisher.store.fetch_eligible(limit=1)[0]
    publisher.store.mark_queued_batch([row.event_id])
    sent = []

    class AckProducer:
        def produce(self, topic, *, key, value, on_delivery):
            sent.append((topic, key, value))
            on_delivery(None, SimpleNamespace(partition=lambda: 0, offset=lambda: len(sent)))

        def poll(self, _timeout):
            return 0

    first_worker = OutboxDeliveryWorker(publisher.store, producer=AckProducer())
    first_worker._dispatch(row)
    # The broker callback ran, but the buffered SQLite ACK was lost on crash.
    db_path = publisher.store.db_path
    publisher.store.close()
    recovered = KafkaOutboxStore(db_path)
    try:
        assert recovered.recover_orphaned_queued() == 1
        second_worker = OutboxDeliveryWorker(recovered, producer=AckProducer())
        second_worker._dispatch_batch(recovered.fetch_eligible(limit=1))
        second_worker._flush_status_buffers()
        assert len(sent) == 2 and sent[0] == sent[1]
        assert recovered._conn.execute(
            "SELECT status FROM kafka_outbox WHERE event_id = ?", (row.event_id,),
        ).fetchone()[0] == "ACKED"
    finally:
        recovered.close()
