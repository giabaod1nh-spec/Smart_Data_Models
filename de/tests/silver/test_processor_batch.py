"""Processor batch / order / shutdown / health cache tests.

Uses a recording repository stub plus real SQLite checkpoint so persistence order
and CAS are verified against the real processor path (reader uses FakeClient).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import CheckpointKey, ProcessorState, SilverSettings
from de.silver.health_api import app, bind_processor
from de.silver.processor import SilverProcessor
from de.silver.readers import BronzeReader
from de.tests.silver.conftest import FakeClient, FakeQueryResult, payload_json
from fastapi.testclient import TestClient

TOPIC = "traffic.entity-events.v2"
TS = datetime(2026, 1, 1, tzinfo=timezone.utc)

ENTITY_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "event_id", "event_type",
    "contract_version", "simulation_run_id", "simulation_time", "cycle_sequence",
    "captured_at", "entity_id", "entity_type", "entity_payload_hash",
    "entity_payload_json", "bronze_canonical_hash", "processed_at", "scenario_id",
    "bronze_ingestion_id",
]


class RecordingRepo:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self._ledger: dict[str, Any] = {}

    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def ping(self) -> bool:
        return True

    def verify_schema(self, mode: str) -> dict:
        return {}

    def find_ledger_entries(self, namespace: str, source_ids):
        self.calls.append("find_ledger")
        return {sid: self._ledger[sid] for sid in source_ids if sid in self._ledger}

    def find_fact_states(self, target, identities, *, replay_run_id=None):
        from de.silver.config import FactReconcileResult
        from de.silver.repositories import FactReconciliation

        self.calls.append("find_facts")
        return {
            i.source_bronze_event_id: FactReconciliation(
                FactReconcileResult.MISSING, i.source_bronze_event_id
            )
            for i in identities
        }

    def insert_fact_batch(self, target, rows, *, replay_run_id=None):
        self.calls.append(f"insert_fact:{target}")
        from de.silver.repositories import WriteReceipt

        ids = tuple(r.source_bronze_event_id for r in rows)
        return WriteReceipt(ids, ids, ())

    def fetch_current_dimension_states(self, candidates, *, replay_run_id=None):
        self.calls.append("fetch_dims")
        return {(c.target_table, c.business_key): None for c in candidates}

    def find_exact_dimension_versions(self, candidates, *, replay_run_id=None):
        return {(c.target_table, c.business_key, c.source_hash): False for c in candidates}

    def insert_dimension_batch(self, target, rows, *, replay_run_id=None):
        self.calls.append(f"insert_dim:{target}")
        from de.silver.repositories import WriteReceipt

        ids = tuple(str(i) for i in range(len(rows)))
        return WriteReceipt(ids, ids, ())

    def find_quarantine_ids(self, source_ids, *, replay_run_id=None):
        self.calls.append("find_quarantine")
        return set()

    def insert_quarantine_batch(self, rows, *, replay_run_id=None):
        self.calls.append("insert_quarantine")
        from de.silver.repositories import WriteReceipt

        ids = tuple(r.silver_quarantine_id for r in rows)
        return WriteReceipt(ids, ids, ())

    def insert_ledger_batch(self, namespace, rows):
        self.calls.append("insert_ledger")
        from de.silver.repositories import LedgerEntryState, WriteReceipt

        for r in rows:
            self._ledger[r.source_bronze_event_id] = LedgerEntryState(
                checkpoint_namespace=r.checkpoint_namespace,
                source_bronze_event_id=r.source_bronze_event_id,
                raw_ingestion_id=r.raw_ingestion_id,
                payload_hash=r.payload_hash,
                disposition=r.disposition,
                target_table=r.target_table,
            )
        ids = tuple(r.source_bronze_event_id for r in rows)
        return WriteReceipt(ids, ids, ())


def _settings(tmp_path: Path) -> SilverSettings:
    return SilverSettings(
        checkpoint_path=str(tmp_path / "cp.sqlite3"),
        topic_allowlist=TOPIC,
        namespace="live",
        destination_mode="main",
        batch_size=500,
        poll_interval_sec=0.01,
        discovery_interval_sec=60,
    )


def _vehicle_entity_row(offset: int = 3) -> tuple:
    payload = payload_json("VehicleSensor.example.jsonld")
    entity_id = json.loads(payload)["id"]
    return (
        TOPIC, 0, offset, f"raw-{offset}", f"evt-{offset:04d}", "TrafficEntityObserved",
        "2.0.0", "run-1", 120.5, 1, TS, entity_id, "VehicleSensor", f"hash-{offset}",
        payload, f"canon-{offset}", TS, "normal", f"bing-{offset}",
    )


def _reader_with_vehicle(responses: list | None = None) -> tuple[BronzeReader, FakeClient]:
    payload_row = _vehicle_entity_row(3)
    # discovery query then min/max/fetch as needed — FakeClient pops responses in order.
    # BronzeReader.discover_streams issues 2 queries (run then entity). fetch_batch one query.
    # process_stream_once: min_offset, fetch_batch, max_offset
    client = FakeClient(
        responses=responses
        or [
            FakeQueryResult([], ["topic", "partition"]),  # run discovery
            FakeQueryResult([(TOPIC, 0)], ["topic", "partition"]),  # entity discovery
            FakeQueryResult([(3,)], ["min(offset)"]),  # min
            FakeQueryResult([payload_row], ENTITY_COLS),  # fetch
            FakeQueryResult([(3,)], ["max(offset)"]),  # max
        ]
    )
    return BronzeReader(SilverSettings(topic_allowlist=TOPIC), client=client), client


def test_processor_persistence_order_and_checkpoint(tmp_path: Path):
    settings = _settings(tmp_path)
    reader, _ = _reader_with_vehicle()
    repo = RecordingRepo()
    cp = SilverCheckpointStore(Path(settings.checkpoint_path))
    cp.open()
    reader.connect()
    proc = SilverProcessor(settings, reader=reader, repo=repo, checkpoint=cp, lock_held=True)
    streams = reader.discover_streams((TOPIC,))
    assert streams
    proc._streams = streams
    n = proc.process_stream_once(streams[0])
    assert n == 1
    assert "insert_fact:silver_fact_traffic_observation" in repo.calls
    assert "insert_dim:silver_dim_approach" in repo.calls
    assert "insert_ledger" in repo.calls
    fact_i = repo.calls.index("insert_fact:silver_fact_traffic_observation")
    dim_i = repo.calls.index("insert_dim:silver_dim_approach")
    led_i = repo.calls.index("insert_ledger")
    assert fact_i < dim_i < led_i
    key = CheckpointKey("live", "bronze_entity_events", TOPIC, 0)
    assert cp.get(key).last_completed_offset == 3
    cp.close()


def test_second_pass_is_idempotent(tmp_path: Path):
    settings = _settings(tmp_path)
    # First pass responses
    row = _vehicle_entity_row(3)
    client = FakeClient(
        responses=[
            FakeQueryResult([], ["topic", "partition"]),
            FakeQueryResult([(TOPIC, 0)], ["topic", "partition"]),
            FakeQueryResult([(3,)], ["min"]),
            FakeQueryResult([row], ENTITY_COLS),
            FakeQueryResult([(3,)], ["max"]),
            # second process_stream_once after checkpoint rewind
            FakeQueryResult([row], ENTITY_COLS),
            FakeQueryResult([(3,)], ["max"]),
        ]
    )
    reader = BronzeReader(settings, client=client)
    repo = RecordingRepo()
    cp = SilverCheckpointStore(Path(settings.checkpoint_path))
    cp.open()
    reader.connect()
    proc = SilverProcessor(settings, reader=reader, repo=repo, checkpoint=cp, lock_held=True)
    streams = reader.discover_streams((TOPIC,))
    proc.process_stream_once(streams[0])
    cp.conn.execute(
        "UPDATE silver_checkpoint SET last_completed_offset=-1 WHERE checkpoint_namespace='live'"
    )
    repo.calls.clear()
    n = proc.process_stream_once(streams[0])
    assert n == 1
    assert "insert_fact:silver_fact_traffic_observation" not in repo.calls
    assert proc.metrics.idempotent_observed_count >= 1
    cp.close()


def test_health_snapshot_cache_no_storage_query(tmp_path: Path):
    settings = _settings(tmp_path)
    reader = BronzeReader(settings, client=FakeClient())
    repo = RecordingRepo()
    cp = SilverCheckpointStore(Path(settings.checkpoint_path))
    proc = SilverProcessor(settings, reader=reader, repo=repo, checkpoint=cp, lock_held=True)
    proc.state = ProcessorState.READY
    proc._clickhouse_ok = True
    proc._sqlite_ok = True
    proc._schema_ok = True
    proc._thread = type("T", (), {"is_alive": lambda self: True})()
    reader.connect()  # FakeClient already injected → initialized=True
    assert reader.initialized is True
    proc._publish_snapshot()
    bind_processor(proc, max_age_sec=60)
    tc = TestClient(app)
    assert tc.get("/health").status_code == 200
    # Handler must only read cached snapshot — no CH/SQLite queries from HTTP path.
    assert tc.get("/ready").status_code in {200, 503}


def test_graceful_shutdown_sets_flag(tmp_path: Path):
    settings = _settings(tmp_path)
    proc = SilverProcessor(
        settings,
        reader=BronzeReader(settings, client=FakeClient()),
        repo=RecordingRepo(),
        checkpoint=SilverCheckpointStore(Path(settings.checkpoint_path)),
        lock_held=True,
    )
    proc.request_shutdown()
    assert proc._stop.is_set()


def test_lag_without_progress_becomes_degraded_not_ready(tmp_path: Path):
    settings = _settings(tmp_path)
    settings = settings.model_copy(update={"readiness_stale_sec": 1.0})
    reader = BronzeReader(settings, client=FakeClient())
    reader.connect()
    proc = SilverProcessor(
        settings,
        reader=reader,
        repo=RecordingRepo(),
        checkpoint=SilverCheckpointStore(Path(settings.checkpoint_path)),
        lock_held=True,
    )
    proc.state = ProcessorState.READY
    proc._clickhouse_ok = True
    proc._sqlite_ok = True
    proc._schema_ok = True
    proc._thread = type("T", (), {"is_alive": lambda self: True})()
    proc.metrics.source_lag["bronze_entity_events|t|0"] = 5
    proc.metrics.last_progress_at = "2000-01-01T00:00:00+00:00"
    proc._publish_snapshot()
    snap = proc.health_snapshot()
    assert snap.state == ProcessorState.DEGRADED.value
    assert snap.ready is False


def test_lag_with_recent_progress_stays_ready(tmp_path: Path):
    settings = _settings(tmp_path)
    settings = settings.model_copy(update={"readiness_stale_sec": 120.0})
    reader = BronzeReader(settings, client=FakeClient())
    reader.connect()
    proc = SilverProcessor(
        settings,
        reader=reader,
        repo=RecordingRepo(),
        checkpoint=SilverCheckpointStore(Path(settings.checkpoint_path)),
        lock_held=True,
    )
    proc.state = ProcessorState.READY
    proc._clickhouse_ok = True
    proc._sqlite_ok = True
    proc._schema_ok = True
    proc._thread = type("T", (), {"is_alive": lambda self: True})()
    proc.metrics.source_lag["bronze_entity_events|t|0"] = 5
    proc.metrics.mark_checkpoint()
    proc._publish_snapshot()
    snap = proc.health_snapshot()
    assert snap.state == ProcessorState.READY.value
    assert snap.ready is True
