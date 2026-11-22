"""Replay manifest validation tests."""
from __future__ import annotations

import json
import copy
from pathlib import Path

import pytest

from de.silver.config import SilverConfigError
from de.silver.replay import (SourceBoundaryChangedError, canonical_manifest_hash,
    validate_manifest, verify_v1_replay_boundary)
from de.silver.source_binding import SourceVerificationError, verify_bronze_source
from de.bronze.checkpoint_store import V1SemanticStore
from de.silver.archive_resolver import ArchiveResolver
from de.silver.config import SilverSettings
from de.silver.replay import run_replay
from de.silver.replay import build_replay_settings


def test_logical_hash_retains_immutable_identity_and_ignores_processing_metadata(tmp_path):
    from de.silver.checkpoint_store import SilverCheckpointStore
    from de.silver.replay import logical_replay_hash
    store = SilverCheckpointStore(tmp_path / 'hash.sqlite3')
    store.open()
    original = dict(simulation_run_id='run', run_generation=1, topology_digest='digest',
                    manifest_identity='a' * 64, event_id='e', vehicle_count=0,
                    created_at='first', processed_at='first', observed_at='first', replay_run_id='first')
    def digest(namespace, row):
        frozen = store.freeze_v1_obligation(namespace, 'changes', 1, 'e', 'fact', row)
        store.reconcile_v1_obligation(namespace, 'changes', 1, 'fact', frozen)
        return logical_replay_hash(store, namespace)
    try:
        baseline = digest('baseline', original)
        assert digest('timestamps', dict(original, created_at='later', processed_at='later',
            observed_at='later', replay_run_id='other')) == baseline
        for key, value in (('simulation_run_id', 'other'), ('run_generation', 2),
                           ('topology_digest', 'other'), ('manifest_identity', 'b' * 64),
                           ('vehicle_count', 1)):
            assert digest(key, dict(original, **{key: value})) != baseline
    finally:
        store.close()
from de.silver.checkpoint_store import SilverCheckpointStore
import de.silver.replay as replay_module
from de.tests.silver.test_archive_lifecycle_v1 import _fixture
from de.tests.silver.v1_fixtures import snapshot_v1 as _snapshot
from contracts.traffic_observation import build_observation


@pytest.mark.parametrize('change', ['source', 'archive', 'unavailable'])
@pytest.mark.parametrize('prior_status', ['RUNNING', 'COMPLETE'])
def test_resume_preflight_persists_abort_only_for_interrupted_matching_manifest(tmp_path, monkeypatch, change, prior_status):
    manifest, folder, _ = _fixture(tmp_path / 'archive')
    archive = ArchiveResolver(tmp_path / 'archive')
    context = archive.resolve(manifest['simulationRunId'], manifest['topologyDigest'])
    bronze_path = tmp_path / 'bronze.sqlite3'
    bronze = V1SemanticStore(bronze_path)
    binding = verify_bronze_source(bronze_path, 'live')
    doc = dict(manifest_version='silver-replay-v1', replay_run_id='interrupted',
        source_namespace='live', source_binding=binding, change_high_water=0,
        invalidation_high_water=0, simulation_run_id=context.run_id, run_generation=context.generation,
        topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
        manifest_file_hash=context.manifest_file_hash, catalog_file_hash=context.catalog_file_hash,
        reader_version='1', processor_version='1', schema_version='008')
    manifest_path = tmp_path / 'replay.json'
    manifest_path.write_text(json.dumps(doc))
    base = SilverSettings(checkpoint_path=str(tmp_path / 'silver' / 'checkpoint.sqlite3'),
        v1_bronze_path=str(bronze_path), v1_archive_root=str(tmp_path / 'archive'))
    settings = build_replay_settings(base, 'interrupted')
    state = SilverCheckpointStore(Path(settings.checkpoint_path))
    state.open()
    state.conn.execute('INSERT INTO silver_v1_replay_state VALUES (?,?,?,?,?)',
        ('interrupted', canonical_manifest_hash(doc), prior_status, 'historical-hash', 'before'))
    state.close()
    if change == 'source':
        event = build_observation(_snapshot(), cycle_sequence=4, simulation_step=400)
        intent = bronze.prepare('live', dict(raw_ingestion_id='new', topic='traffic.observations.v1',
            partition=0, offset=0, topic_identity='topic'), event, 'envelope', 'content', lambda *args: [])
        bronze.complete('live', 'new', intent['expected_hash'])
    elif change == 'archive':
        manifest['failedCycleCount'] += 1
        (folder / 'run_manifest.json').write_text(json.dumps(manifest))
    bronze.close()
    if change == 'unavailable':
        bronze_path.rename(tmp_path / 'temporarily-offline.sqlite3')
    with monkeypatch.context() as scoped:
        def unexpected_client(*args, **kwargs):
            pytest.fail('changed preflight must fail before creating a CH repository')
        scoped.setattr(replay_module, 'SilverClickHouseRepository', unexpected_client)
        with pytest.raises(SourceVerificationError if change == 'unavailable' else SourceBoundaryChangedError) as error:
            run_replay(manifest_path, 'interrupted', resume=True, base_settings=base)
        assert error.value.code == ('AUTHORITY_UNAVAILABLE' if change == 'unavailable' else 'SOURCE_BOUNDARY_CHANGED')
    state.open()
    row = state.conn.execute('SELECT status,logical_hash FROM silver_v1_replay_state').fetchone()
    expected_status = 'ABORTED' if prior_status == 'RUNNING' and change != 'unavailable' else prior_status
    assert tuple(row) == (expected_status, 'historical-hash')
    state.close()
    assert not Path(base.checkpoint_path).exists()
    if expected_status == 'ABORTED':
        monkeypatch.setattr(replay_module, 'verify_v1_replay_boundary', lambda *args: binding)
        monkeypatch.setattr(replay_module.ArchiveResolver, 'resolve', lambda *args: context)
        with pytest.raises(SilverConfigError, match='cannot resume'):
            run_replay(manifest_path, 'interrupted', resume=True, base_settings=base)


def test_manifest_validation(tmp_path: Path):
    path = tmp_path / "bronze.sqlite3"
    store = V1SemanticStore(path)
    store.close()
    binding = verify_bronze_source(path, "live")
    doc = {
        "manifest_version": "silver-replay-v1",
        "replay_run_id": "r1",
        "source_namespace": "live", "source_binding": binding,
        "change_high_water": 0, "invalidation_high_water": 0,
        "simulation_run_id": "run", "run_generation": 1,
        "topology_digest": "v1:sha256:" + "d" * 64, "manifest_identity": "a" * 64,
        "manifest_file_hash": "b" * 64, "catalog_file_hash": "c" * 64,
        "reader_version": "1", "processor_version": "1", "schema_version": "008",
    }
    h = validate_manifest(doc, "r1")
    assert h == canonical_manifest_hash(doc)
    for key, invalid in (('change_high_water', True), ('invalidation_high_water', -1),
            ('run_generation', True), ('run_generation', 0), ('schema_version', '004'),
            ('reader_version', '99'), ('processor_version', '99'),
            ('topology_digest', 'digest'), ('manifest_identity', 'not-a-hash'),
            ('simulation_run_id', '../escape'), ('source_binding', {})):
        with pytest.raises(SilverConfigError):
            validate_manifest(dict(doc, **{key: invalid}), 'r1')
    with pytest.raises(SilverConfigError):
        validate_manifest(doc, "other")
    assert verify_v1_replay_boundary(doc, path) == binding
    doc["change_high_water"] = 1
    with pytest.raises(SourceBoundaryChangedError):
        verify_v1_replay_boundary(doc, path)


def test_empty_v1_replay_uses_isolated_state_and_is_idempotent(tmp_path, monkeypatch):
    manifest, _, _ = _fixture(tmp_path / "archive")
    archive_root = tmp_path / "archive"
    context = ArchiveResolver(archive_root).resolve(
        manifest["simulationRunId"], manifest["topologyDigest"])
    bronze_path = tmp_path / "bronze_v1" / "checkpoint.sqlite3"
    bronze = V1SemanticStore(bronze_path)
    bronze.close()
    binding = verify_bronze_source(bronze_path, "live")
    doc = dict(manifest_version="silver-replay-v1", replay_run_id="replay-one",
        source_namespace="live", source_binding=binding,
        change_high_water=0, invalidation_high_water=0,
        simulation_run_id=context.run_id, run_generation=context.generation,
        topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
        manifest_file_hash=context.manifest_file_hash,
        catalog_file_hash=context.catalog_file_hash, reader_version="1",
        processor_version="1", schema_version="008")
    manifest_path = tmp_path / "replay.json"
    manifest_path.write_text(json.dumps(doc))
    base = SilverSettings(checkpoint_path=str(tmp_path / "silver_v1" / "checkpoint.sqlite3"),
        v1_bronze_path=str(bronze_path), v1_archive_root=str(archive_root))

    class FakeRepo:
        def __init__(self, settings):
            pass

        def connect(self):
            pass

        def verify_v1_schema(self, **kwargs):
            pass

        def close(self):
            pass

    monkeypatch.setattr(replay_module, "SilverClickHouseRepository", FakeRepo)
    monkeypatch.setattr(replay_module, "BronzeClickHouseRepository", FakeRepo)
    first = run_replay(manifest_path, "replay-one", base_settings=base)
    assert first["status"] == "COMPLETE" and first["source_entries_processed"] == 0
    second = run_replay(manifest_path, "replay-one", resume=True, base_settings=base)
    assert second["idempotent"] is True
    assert (tmp_path / "silver_v1" / "replay" / "replay-one" / "checkpoint.sqlite3").is_file()
    assert not (tmp_path / "silver_v1" / "checkpoint.sqlite3").exists()
    doc["replay_run_id"] = "replay-abort"
    manifest_path.write_text(json.dumps(doc))
    def changed(*args, **kwargs):
        raise SourceBoundaryChangedError()
    monkeypatch.setattr(replay_module.SilverProcessor, "run_v1_journal_pass", changed)
    with pytest.raises(SourceBoundaryChangedError):
        run_replay(manifest_path, "replay-abort", base_settings=base)
    with pytest.raises(SilverConfigError, match="cannot resume"):
        run_replay(manifest_path, "replay-abort", resume=True, base_settings=base)


def test_v1_replay_materializes_only_manifest_run(tmp_path, monkeypatch):
    manifest, _, _ = _fixture(tmp_path / "archive")
    archive_root = tmp_path / "archive"
    context = ArchiveResolver(archive_root).resolve(manifest["simulationRunId"],
                                                    manifest["topologyDigest"])
    bronze_path = tmp_path / "bronze_v1" / "checkpoint.sqlite3"
    bronze = V1SemanticStore(bronze_path)
    events = {}
    for cycle, run in ((4, context.run_id), (5, "unrelated-run")):
        snapshot = _snapshot()
        snapshot["simulation_run_id"] = run
        snapshot["topology_digest"] = context.topology_digest
        original = next(iter(snapshot["directions"].values()))
        snapshot["directions"] = {name: copy.deepcopy(original)
                                  for name in context.directions["A"]}
        snapshot["colors"] = {name: "green" for name in context.directions["A"]}
        event = build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 100)
        delivery = dict(raw_ingestion_id=f"raw-{cycle}", topic="traffic.observations.v1",
            partition=0, offset=cycle, physical_record_hash="d" * 64,
            source_byte_hash="e" * 64)
        expected = json.dumps([{"table": "bronze_delivery_dispositions_v1", "row": delivery}])
        intent = bronze.prepare("live", dict(raw_ingestion_id=f"raw-{cycle}",
            topic="traffic.observations.v1", partition=0, offset=cycle, topic_identity="topic"),
            event, "envelope", "f" * 64, lambda *args, expected=expected: json.loads(expected))
        bronze.complete("live", f"raw-{cycle}", intent["expected_hash"])
        events[event["eventId"]] = dict(event=event, semantic_revision=1,
            bronze_canonical_hash="f" * 64, primary_delivery={"expected_json": expected})
    bronze.close()
    binding = verify_bronze_source(bronze_path, "live")
    doc = dict(manifest_version="silver-replay-v1", replay_run_id="replay-events",
        source_namespace="live", source_binding=binding,
        change_high_water=2, invalidation_high_water=0,
        simulation_run_id=context.run_id, run_generation=context.generation,
        topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
        manifest_file_hash=context.manifest_file_hash, catalog_file_hash=context.catalog_file_hash,
        reader_version="1", processor_version="1", schema_version="008")
    manifest_path = tmp_path / "replay-events.json"
    manifest_path.write_text(json.dumps(doc))
    base = SilverSettings(checkpoint_path=str(tmp_path / "silver_v1" / "checkpoint.sqlite3"),
        v1_bronze_path=str(bronze_path), v1_archive_root=str(archive_root))
    writes = {}
    lost_response = {"armed": True}

    class FakeSilverRepo:
        def __init__(self, settings):
            pass
        def connect(self):
            pass
        def close(self):
            pass
        def verify_v1_schema(self, **kwargs):
            pass
        def reconcile_v1_row(self, table, row, replay_run_id=None):
            return (replay_run_id, table, json.dumps(row, sort_keys=True)) in writes
        def insert_v1_rows(self, table, rows, replay_run_id=None):
            assert replay_run_id in {"replay-events", "replay-events-two"}
            for row in rows:
                writes[replay_run_id, table, json.dumps(row, sort_keys=True)] = row
            if replay_run_id == "replay-events-two" and lost_response["armed"]:
                lost_response["armed"] = False
                raise RuntimeError("insert response lost after commit")

    class FakeBronzeRepo:
        def __init__(self, settings):
            pass
        def connect(self):
            pass
        def close(self):
            pass
        def read_valid_observation(self, namespace, event_id, revision, **kwargs):
            assert namespace == "live" and revision == 1
            return events[event_id]

    monkeypatch.setattr(replay_module, "SilverClickHouseRepository", FakeSilverRepo)
    monkeypatch.setattr(replay_module, "BronzeClickHouseRepository", FakeBronzeRepo)
    result = run_replay(manifest_path, "replay-events", base_settings=base)
    assert result["status"] == "COMPLETE" and result["change_cursor"] == 2
    assert len(writes) == 2 * len(context.directions["A"]) + 1 + 3 + 1 + 1
    assert any(table == "silver_run_lifecycle_v1" for _, table, _ in writes)
    assert all(row.get("simulation_run_id", context.run_id) == context.run_id for row in writes.values())
    assert not (tmp_path / "silver_v1" / "checkpoint.sqlite3").exists()
    assert run_replay(manifest_path, "replay-events", resume=True,
                      base_settings=base)["logical_hash"] == result["logical_hash"]
    doc["replay_run_id"] = "replay-events-two"
    manifest_path.write_text(json.dumps(doc))
    with pytest.raises(RuntimeError, match="response lost"):
        run_replay(manifest_path, "replay-events-two", base_settings=base)
    second = run_replay(manifest_path, "replay-events-two", resume=True, base_settings=base)
    assert second["logical_hash"] == result["logical_hash"]
