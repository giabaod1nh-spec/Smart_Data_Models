import json
import sqlite3
from copy import deepcopy
from pathlib import Path

import pytest

from de.silver.archive_resolver import ArchiveIntegrityError, ArchiveResolver, ArchiveUnavailable, manifest_identity
from de.silver.lifecycle_refresher import LifecycleRefresher
from de.silver.dimension_builders import build_v1_dimensions
from contracts.topology.v1_digest import topology_digest


def _fixture(tmp_path):
    source = Path(__file__).resolve().parents[3] / "Visualize/artifacts/runs/phase1-raw-20261119-smoke01"
    manifest = json.loads((source / "run_manifest.json").read_text())
    run_id = manifest["simulationRunId"]
    folder = tmp_path / "runs" / run_id
    folder.mkdir(parents=True)
    (folder / "run_manifest.json").write_text(json.dumps(manifest))
    (folder / "network_topology_catalog.json").write_bytes(
        (source / "network_topology_catalog.json").read_bytes())
    registry_path = tmp_path / "runs" / "active_run.sqlite3"
    with sqlite3.connect(registry_path) as db:
        db.execute("CREATE TABLE runs (generation INTEGER,run_id TEXT,manifest_identity TEXT,"
                   "status TEXT,activated_at TEXT,updated_at TEXT)")
        db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?)",
                   (manifest["runGeneration"], run_id, manifest_identity(manifest),
                    "RUNNING", "t1", "t1"))
    return manifest, folder, registry_path


def test_archived_context_and_lifecycle_outages(tmp_path, monkeypatch):
    manifest, folder, registry_path = _fixture(tmp_path)
    resolver = ArchiveResolver(tmp_path)
    run_id, digest = manifest["simulationRunId"], manifest["topologyDigest"]
    context = resolver.resolve(run_id, digest, manifest["runGeneration"])
    assert context.registry_status == "RUNNING"
    assert set(context.directions) == set(manifest["participatingIntersectionIds"])
    dimensions = build_v1_dimensions(context, str(tmp_path))
    assert len(dimensions) == 3
    assert "runStatus" not in dimensions["silver_run_dimension_v1"]["manifest_metadata_json"]
    assert dimensions["silver_run_topology_binding_v1"]["catalog_hash"] == context.catalog_file_hash
    poller = LifecycleRefresher(tmp_path / "silver.sqlite3", resolver)
    poller.register(run_id, digest, manifest["runGeneration"])
    assert poller.poll()[0].mirror_state == "LAGGING"
    assert poller.poll() == []
    (folder / "run_manifest.json").rename(folder / "manifest.offline")
    assert poller.poll()[0].mirror_state == "UNAVAILABLE"
    def unavailable(_run_id):
        raise ArchiveUnavailable("registry offline")
    monkeypatch.setattr(resolver, "registry_row", unavailable)
    stale = poller.poll()[0]
    assert stale.freshness == "STALE" and stale.status == "RUNNING"
    assert poller.poll() == []
    restarted = LifecycleRefresher(tmp_path / "silver.sqlite3", resolver)
    assert restarted.latest(run_id) == stale
    monkeypatch.undo()
    (folder / "manifest.offline").rename(folder / "run_manifest.json")
    assert restarted.poll()[0].freshness == "CURRENT"
    assert restarted.poll() == []


def test_subset_run_keeps_topology_dimension_catalog_complete(tmp_path):
    manifest, folder, registry_path = _fixture(tmp_path)
    resolver = ArchiveResolver(tmp_path)
    run_id, digest = manifest["simulationRunId"], manifest["topologyDigest"]
    full = resolver.resolve(run_id, digest)
    full_topology = build_v1_dimensions(full, str(tmp_path))["silver_topology_dimension_v1"]

    manifest["participatingIntersectionIds"] = manifest["participatingIntersectionIds"][:1]
    (folder / "run_manifest.json").write_text(json.dumps(manifest))
    with sqlite3.connect(registry_path) as db:
        db.execute("UPDATE runs SET manifest_identity=?", (manifest_identity(manifest),))

    subset = resolver.resolve(run_id, digest)
    subset_dimensions = build_v1_dimensions(subset, str(tmp_path))
    assert subset.participants == tuple(manifest["participatingIntersectionIds"])
    assert set(subset.directions) == set(subset.participants)
    assert subset_dimensions["silver_topology_dimension_v1"] == full_topology
    assert json.loads(subset_dimensions["silver_run_topology_binding_v1"]["participant_ids_json"]) == list(subset.participants)


def test_archive_digest_and_registry_identity_mismatch(tmp_path):
    manifest, folder, registry_path = _fixture(tmp_path)
    resolver = ArchiveResolver(tmp_path)
    run_id, digest = manifest["simulationRunId"], manifest["topologyDigest"]
    with sqlite3.connect(registry_path) as db:
        db.execute("UPDATE runs SET manifest_identity='bad'")
    with pytest.raises(ArchiveIntegrityError):
        resolver.resolve(run_id, digest)
    with sqlite3.connect(registry_path) as db:
        db.execute("UPDATE runs SET manifest_identity=?", (manifest_identity(manifest),))
    catalog_path = folder / "network_topology_catalog.json"
    catalog = json.loads(catalog_path.read_text())
    catalog["approach_edges"] = {}
    catalog_path.write_text(json.dumps(catalog))
    with pytest.raises(ArchiveIntegrityError):
        resolver.resolve(run_id, digest)


def test_subset_cannot_hide_malformed_nonparticipant_catalog_node(tmp_path):
    manifest, folder, registry_path = _fixture(tmp_path)
    catalog_path = folder / "network_topology_catalog.json"
    catalog = json.loads(catalog_path.read_text())
    participant, other = list(catalog["node_to_tls"])[:2]
    manifest["participatingIntersectionIds"] = [participant]
    catalog["approach_edges"][catalog["node_to_tls"][other]] = {}
    manifest["topologyDigest"] = topology_digest(catalog)
    catalog_path.write_text(json.dumps(catalog))
    (folder / "run_manifest.json").write_text(json.dumps(manifest))
    with sqlite3.connect(registry_path) as db:
        db.execute("UPDATE runs SET manifest_identity=?", (manifest_identity(manifest),))
    with pytest.raises(ArchiveIntegrityError, match="invalid archived approaches"):
        ArchiveResolver(tmp_path).resolve(manifest["simulationRunId"], manifest["topologyDigest"])


@pytest.mark.parametrize('filename', ['run_manifest.json', 'network_topology_catalog.json'])
@pytest.mark.parametrize('damage', ['missing', 'malformed', 'non-object'])
def test_archive_file_failures_have_explicit_retryability(tmp_path, filename, damage):
    manifest, folder, _ = _fixture(tmp_path)
    path = folder / filename
    if damage == 'missing':
        path.rename(folder / (filename + '.offline'))
    elif damage == 'malformed':
        path.write_text('{broken json')
    else:
        path.write_text('[]')
    expected = ArchiveUnavailable if damage == 'missing' else ArchiveIntegrityError
    resolver = ArchiveResolver(tmp_path)
    with pytest.raises(expected):
        resolver.resolve(manifest['simulationRunId'], manifest['topologyDigest'])
    # The same historical path is retried, with no alternate active catalog.
    with pytest.raises(expected):
        resolver.resolve(manifest['simulationRunId'], manifest['topologyDigest'])


def test_historical_run_generations_resolve_distinct_archived_digests(tmp_path):
    first, folder, registry = _fixture(tmp_path)
    second = deepcopy(first)
    second.update(simulationRunId=first['simulationRunId'] + '-next',
                  runGeneration=first['runGeneration'] + 1)
    catalog = json.loads((folder / 'network_topology_catalog.json').read_text())
    catalog['signal_program_hash'] = 'f' * 64
    second['topologyDigest'] = topology_digest(catalog)
    second_folder = folder.parent / second['simulationRunId']
    second_folder.mkdir()
    (second_folder / 'run_manifest.json').write_text(json.dumps(second))
    (second_folder / 'network_topology_catalog.json').write_text(json.dumps(catalog))
    with sqlite3.connect(registry) as db:
        db.execute('INSERT INTO runs VALUES (?,?,?,?,?,?)',
            (second['runGeneration'], second['simulationRunId'], manifest_identity(second),
             'RUNNING', 't2', 't2'))
    resolver = ArchiveResolver(tmp_path)
    old = resolver.resolve(first['simulationRunId'], first['topologyDigest'], first['runGeneration'])
    new = resolver.resolve(second['simulationRunId'], second['topologyDigest'], second['runGeneration'])
    assert old.generation + 1 == new.generation and old.topology_digest != new.topology_digest
    assert old.catalog_file_hash != new.catalog_file_hash
    assert old.manifest_identity != new.manifest_identity
    assert build_v1_dimensions(old, str(tmp_path))['silver_run_topology_binding_v1'] != build_v1_dimensions(
        new, str(tmp_path))['silver_run_topology_binding_v1']
    with pytest.raises(ArchiveIntegrityError):
        resolver.resolve(first['simulationRunId'], new.topology_digest)
    poller = LifecycleRefresher(tmp_path / 'silver.sqlite3', resolver)
    for context in (old, new):
        poller.register(context.run_id, context.topology_digest, context.generation)
    assert {snapshot.run_id for snapshot in poller.poll()} == {old.run_id, new.run_id}
    assert poller.poll() == []


def test_lifecycle_projection_reconciles_lost_insert_response(tmp_path):
    manifest, _, _ = _fixture(tmp_path)
    resolver = ArchiveResolver(tmp_path)
    poller = LifecycleRefresher(tmp_path / "silver.sqlite3", resolver)
    poller.register(manifest["simulationRunId"], manifest["topologyDigest"],
                    manifest["runGeneration"])
    poller.poll()
    rows = {}

    class LostResponseRepository:
        fail_once = True

        def reconcile_v1_row(self, table, row, **kwargs):
            return (table, row["observed_at"]) in rows

        def insert_v1_rows(self, table, batch, **kwargs):
            for row in batch:
                rows[table, row["observed_at"]] = row
            if self.fail_once:
                self.fail_once = False
                raise ConnectionError("response lost after commit")

    repo = LostResponseRepository()
    with pytest.raises(ConnectionError, match="response lost"):
        poller.project_pending(repo)
    assert poller.project_pending(repo) == 1
    assert poller.project_pending(repo) == 0
    assert len(rows) == 1
    row = next(iter(rows.values()))
    assert row["run_status"] == "RUNNING"
    assert row["manifest_mirror_state"] == "LAGGING"
    assert row["failed_cycle_count"] is None


@pytest.mark.parametrize('terminal', ['COMPLETE', 'INCOMPLETE', 'STOPPED'])
def test_lifecycle_terminal_without_observations_preserves_confirmed_mirror(tmp_path, monkeypatch, terminal):
    manifest, folder, registry = _fixture(tmp_path)
    manifest.update(runStatus='RUNNING', failedCycleCount=7)
    manifest_path = folder / 'run_manifest.json'
    manifest_path.write_text(json.dumps(manifest))
    resolver = ArchiveResolver(tmp_path)
    poller = LifecycleRefresher(tmp_path / 'silver.sqlite3', resolver)
    run_id = manifest['simulationRunId']
    poller.register(run_id, manifest['topologyDigest'], manifest['runGeneration'])
    synced = poller.poll()[0]
    assert synced.mirror_state == 'SYNCED' and synced.failed_cycle_count == 7
    assert poller.poll() == []
    with sqlite3.connect(registry) as db:
        db.execute('UPDATE runs SET status=?,updated_at=?', (terminal, 't2'))
    lagging = poller.poll()[0]
    assert lagging.status == terminal and lagging.mirror_state == 'LAGGING'
    assert lagging.failed_cycle_count == 7 and lagging.manifest_file_hash == synced.manifest_file_hash
    manifest_path.rename(folder / 'offline')
    unavailable = poller.poll()[0]
    assert unavailable.mirror_state == 'UNAVAILABLE' and unavailable.failed_cycle_count == 7
    (folder / 'offline').rename(manifest_path)
    manifest.update(runStatus=terminal, failedCycleCount=0 if terminal == 'COMPLETE' else 9)
    manifest_path.write_text(json.dumps(manifest))
    recovered = poller.poll()[0]
    assert recovered.mirror_state == 'SYNCED'
    assert recovered.failed_cycle_count == manifest['failedCycleCount']
    with monkeypatch.context() as scoped:
        def offline(*args):
            raise ArchiveUnavailable('registry temporarily offline')
        scoped.setattr(resolver, 'registry_row', offline)
        stale = poller.poll()[0]
        assert stale.registry_state == 'UNAVAILABLE' and stale.freshness == 'STALE'
        assert stale.status == terminal and stale.failed_cycle_count == recovered.failed_cycle_count
        assert stale.manifest_file_hash == recovered.manifest_file_hash
        assert poller.poll() == []
    restarted = LifecycleRefresher(tmp_path / 'silver.sqlite3', resolver)
    assert restarted.latest(run_id) == stale
    assert restarted.poll()[0] == recovered
    assert restarted.poll() == []


def test_manifest_outage_does_not_allow_invalid_registry_status(tmp_path):
    manifest, folder, registry = _fixture(tmp_path)
    poller = LifecycleRefresher(tmp_path / 'silver.sqlite3', ArchiveResolver(tmp_path))
    poller.register(manifest['simulationRunId'], manifest['topologyDigest'], manifest['runGeneration'])
    (folder / 'run_manifest.json').rename(folder / 'offline')
    with sqlite3.connect(registry) as db:
        db.execute("UPDATE runs SET status='UNSUPPORTED'")
    with pytest.raises(ArchiveIntegrityError):
        poller.poll()


def test_missed_poll_publishes_stale_once_and_recovers(tmp_path):
    manifest, _, _ = _fixture(tmp_path)
    path = tmp_path / 'silver.sqlite3'
    poller = LifecycleRefresher(path, ArchiveResolver(tmp_path))
    run_id = manifest['simulationRunId']
    poller.register(run_id, manifest['topologyDigest'], manifest['runGeneration'])
    original = poller.poll()[0]
    assert poller.is_current(15)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE silver_known_runs_v1 SET last_polled_at=?', ('2000-01-01T00:00:00+00:00',))
    assert not poller.is_current(15)
    assert poller.mark_stale(15) == 1
    assert poller.mark_stale(15) == 0
    poller.record_poll_result(2, 2.0, 'temporarily offline')
    saved = LifecycleRefresher(path, ArchiveResolver(tmp_path)).poll_state()
    assert saved['retry_count'] == 2 and saved['last_error'] == 'temporarily offline'
    assert saved['last_successful_at'] is None
    poller.record_poll_result(0, 5.0, '')
    assert poller.poll_state()['last_successful_at'] is not None
    stale = poller.latest(run_id)
    assert stale.freshness == 'STALE' and stale.status == original.status
    assert stale.failed_cycle_count == original.failed_cycle_count
    assert poller.poll()[0] == original
    assert poller.is_current(15)
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM silver_lifecycle_history_v1').fetchone()[0] == 3
