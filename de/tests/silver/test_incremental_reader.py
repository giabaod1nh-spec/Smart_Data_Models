"""Stage A scenarios against SQLite authorities and an isolated ClickHouse database."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from contracts.topology.v1_digest import topology_digest
from contracts.traffic_observation import build_observation, canonical_json
from de.bronze.checkpoint_store import V1SemanticStore
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.bronze.processor import BronzeProcessor
from de.kafka_raw.clickhouse_repository import _split
from de.silver.archive_resolver import ArchiveResolver, manifest_identity
from de.silver.canonical_reader import CanonicalSilverReader, SnapshotStaleError
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import SilverSettings
from de.silver.incremental_contract import (
    CanonicalChangePageRequest, CanonicalFeedError, CanonicalInvalidationPageRequest,
    FeedLimits, SourceDiscoveryRequest, canonical_bytes,
)
from de.silver.processor import SilverProcessor
from de.silver.repositories import SilverClickHouseRepository
from de.silver.source_binding import verify_bronze_source
from de.tests.silver.feed_consumer_probe import acknowledge, initialize
from de.tests.silver.test_archive_lifecycle_v1 import _fixture
from de.tests.silver.v1_fixtures import snapshot_v1

NS = "live"
pytestmark = pytest.mark.skipif(
    os.environ.get("SILVER_LIVE_ACCEPTANCE") != "1",
    reason="Stage A requires real, isolated ClickHouse",
)


class AuthorityFixture:
    def __init__(self, path, archive, bronze, silver, bronze_repo, silver_repo, settings):
        self.path, self.archive, self.bronze, self.silver = path, archive, bronze, silver
        self.bronze_repo = bronze_repo
        self.reader = CanonicalSilverReader(bronze.path, Path(settings.checkpoint_path),
                                            silver_repo, bronze_repo, archive)
        self.worker = object.__new__(SilverProcessor)
        self.worker.settings = settings
        self.worker.checkpoint = silver
        self.worker.repo = silver_repo
        self.lifecycle = SimpleNamespace(register=lambda *args: None)
        self.binding = self.reader.discover_canonical_source(SourceDiscoveryRequest(NS)).source_binding_id
        self.offset = 0

    def event(self, cycle, run, conflict=False, node="A"):
        context = self.archive.resolve(run["simulationRunId"], run["topologyDigest"])
        snapshot = snapshot_v1()
        measured = next(iter(snapshot["directions"].values()))
        directions = {name: dict(measured) for name in context.directions[node]}
        if conflict:
            next(iter(directions.values()))["vehicle_count"] += 1
        snapshot.update(node_id=node, simulation_run_id=context.run_id, topology_digest=context.topology_digest,
                        directions=directions, colors={name: "green" for name in directions})
        event = build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 100)
        return self.publish(event, conflict=conflict)

    def publish(self, event, *, conflict=False):
        self.offset += 1
        raw_id = hashlib.sha256(f"{self.path.name}:{self.offset}".encode()).hexdigest()
        raw = dict(raw_ingestion_id=raw_id, topic="traffic.observations.v1", topic_identity="test-topic",
                   partition=0, offset=self.offset, physical_record_hash=raw_id,
                   payload_bytes_hash=hashlib.sha256(canonical_json(event).encode()).hexdigest())
        result = BronzeProcessor.materialize_v1_delivery(self.bronze, self.bronze_repo, NS, raw, event)
        assert result == ("SEMANTIC_CONFLICT" if conflict else "ACCEPTED_NEW")
        return event["eventId"]

    def apply(self, limit=100):
        SilverProcessor.run_v1_journal_pass(
            self.worker, self.reader.bronze, self.bronze_repo, self.archive, self.lifecycle,
            batch_size=min(limit, 100), max_entries=limit)

    def changes(self, after=0, **kwargs):
        return self.reader.read_canonical_changes_page(CanonicalChangePageRequest(
            namespace=NS, expected_source_binding_id=self.binding, after_change_sequence=after, **kwargs))

    def invalidations(self, after, through, **kwargs):
        return self.reader.read_canonical_invalidations_page(CanonicalInvalidationPageRequest(
            namespace=NS, expected_source_binding_id=self.binding, after_invalidation_sequence=after,
            through_invalidation_sequence=through, **kwargs))

    def assert_oracle(self, page):
        change = hasattr(page, "through_change_sequence")
        sequence = "change_sequence" if change else "invalidation_sequence"
        table = "v1_changes" if change else "v1_invalidations"
        after, through = (getattr(page, prefix + sequence) for prefix in ("after_", "through_"))
        rows = self.bronze.db.execute(f"SELECT * FROM {table} WHERE namespace=? AND {sequence}>? "
                                      f"AND {sequence}<=? ORDER BY {sequence}", (NS, after, through))
        expected = [(row[sequence], row["event_id"], row["revision"]) for row in rows]
        assert expected == [(getattr(entry, sequence), entry.event_id, entry.semantic_revision)
                            for entry in page.entries]
        assert len(canonical_bytes(page)) == page.encoded_bytes
        assert page.entry_count == through - after
        if change:
            for entry in page.entries:
                identity = self.bronze.current(NS, entry.event_id)
                if entry.change_type == "CONFLICTED":
                    assert entry.effective_disposition == "CONFLICTED"
                elif identity["state"] == "CONFLICTED":
                    assert entry.effective_disposition == "SUPERSEDED_BY_CONFLICT"
                    assert entry.superseding_change_sequence == identity["change_sequence"]
                else:
                    assert entry.effective_disposition == "BUNDLE_VALID"
                    expected_facts = [json.loads(row[0]) for row in self.silver.conn.execute(
                        "SELECT expected_json FROM silver_v1_obligation WHERE source_kind='changes' "
                        "AND namespace=? AND source_sequence=? AND target_table LIKE '%fact_v1'",
                        (NS, entry.change_sequence))]
                    bundle = entry.canonical_bundle
                    actual = [dict(row) for row in (bundle.intersection, *bundle.traffic, *bundle.signal)]
                    assert sorted(map(canonical_json, actual)) == sorted(map(canonical_json, expected_facts))


@pytest.fixture
def authority(tmp_path):
    manifest, folder, registry = _fixture(tmp_path / "archive")
    catalog = json.loads((folder / "network_topology_catalog.json").read_text(encoding="utf-8"))
    catalog["node_locations"]["A"]["x"] += 1
    second = dict(manifest, simulationRunId="stage-a-late-run", runGeneration=manifest["runGeneration"] + 1,
                  topologyDigest=topology_digest(catalog))
    second_folder = folder.parent / second["simulationRunId"]
    second_folder.mkdir()
    (second_folder / "run_manifest.json").write_text(json.dumps(second), encoding="utf-8")
    (second_folder / "network_topology_catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    with sqlite3.connect(registry) as db:
        db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?)", (
            second["runGeneration"], second["simulationRunId"], manifest_identity(second),
            "RUNNING", "t1", "t1"))
    database = "p4pre_" + uuid4().hex[:16]
    base = SilverSettings()
    admin = SilverClickHouseRepository(base)
    admin.connect()
    admin.client.command(f"CREATE DATABASE {database}")
    bronze = silver = bronze_repo = silver_repo = fixture = None
    try:
        migrations = Path(__file__).resolve().parents[2] / "migrations"
        for prefix in ("006_", "007_", "008_"):
            sql = next(migrations.glob(prefix + "*.sql")).read_text(encoding="utf-8").replace("smart_traffic", database)
            for statement in _split(sql):
                admin.client.command(statement)
        bronze_path, silver_path = tmp_path / "bronze_v1/state.sqlite3", tmp_path / "silver_v1/state.sqlite3"
        settings = base.model_copy(update=dict(clickhouse_database=database, checkpoint_path=str(silver_path),
            v1_bronze_path=str(bronze_path), v1_archive_root=str(tmp_path / "archive"), namespace=NS,
            destination_mode="main", replay_run_id=""))
        bronze = V1SemanticStore(bronze_path)
        silver = SilverCheckpointStore(silver_path)
        silver.open()
        silver.bind_v1_source(NS, verify_bronze_source(bronze_path, NS))
        bronze_repo = BronzeClickHouseRepository(ObservationSettings(
            checkpoint_path=str(bronze_path), clickhouse_database=database,
            clickhouse_host=base.clickhouse_host, clickhouse_port=base.clickhouse_port,
            clickhouse_user=base.clickhouse_user, clickhouse_password=base.clickhouse_password))
        bronze_repo.connect()
        silver_repo = SilverClickHouseRepository(settings)
        silver_repo.connect()
        fixture = AuthorityFixture(tmp_path, ArchiveResolver(tmp_path / "archive"),
                                  bronze, silver, bronze_repo, silver_repo, settings)
        yield fixture, manifest, second
    finally:
        for resource in (fixture.reader if fixture else None, silver_repo, bronze_repo, silver, bronze):
            if resource is not None:
                resource.close()
        admin.client.command(f"DROP DATABASE {database}")
        admin.close()


def expect(code, call, *args, **kwargs):
    with pytest.raises(CanonicalFeedError) as result:
        call(*args, **kwargs)
    assert result.value.code == code, str(result.value)
    assert result.value.receipt is None and result.value.cursor_advance is False
    return result.value


def test_live_prefix_receipts_late_multi_run_and_byte_limits(authority, monkeypatch):
    f, first, second = authority
    assert f.changes().status == "EMPTY"
    expect("INVALID_REQUEST", f.changes, True)
    expect("INVALID_REQUEST", f.changes, max_entries=101)
    expect("UNSUPPORTED_CONTRACT_VERSION", f.changes, contract_version="2")
    first_id = f.event(6, first)
    expect("SOURCE_LAG", f.changes)
    second_id = f.event(3, second)
    late_id = f.event(2, first)
    f.apply(limit=2)
    captured_discovery = f.reader.discover_canonical_source(SourceDiscoveryRequest(NS, f.binding))
    assert (captured_discovery.verified_change_prefix, captured_discovery.bronze_change_high_water) == (2, 3)
    assert not captured_discovery.bootstrap_required
    original = f.reader._verified_bundle
    appended = []

    def append(*args):
        if not appended:
            appended.append(f.event(4, second))
        return original(*args)

    monkeypatch.setattr(f.reader, "_verified_bundle", append)
    first_page = f.changes(max_entries=1)
    assert first_page.has_more and first_page.captured_bronze_change_high_water == 3
    one = f.changes(max_encoded_bytes=first_page.encoded_bytes)
    assert one.through_change_sequence == 1 and one.entries[0].event_id == first_id
    with pytest.raises(TypeError):
        one.entries[0].canonical_bundle.intersection["event_id"] = "changed"
    f.assert_oracle(one)
    next_page = f.changes(1)
    assert not next_page.has_more and next_page.entries[0].event_id == second_id
    assert next_page.entries[0].run_generation != one.entries[0].run_generation
    assert next_page.entries[0].topology_digest != one.entries[0].topology_digest
    expect("SOURCE_LAG", f.changes, 2)
    expect("INVALID_REQUEST", f.changes, max_encoded_bytes=100)
    exact_entry_bytes = len(canonical_bytes(one.entries[0]))
    f.reader.feed_limits = FeedLimits(max_single_entry_bytes=exact_entry_bytes - 1)
    assert expect("RECORD_TOO_LARGE", f.changes).details["observed_bytes"] == exact_entry_bytes
    f.reader.feed_limits = FeedLimits()
    f.apply()
    last = f.changes(2)
    assert [entry.event_id for entry in last.entries] == [late_id, appended[0]]
    f.assert_oracle(last)
    assert f.changes(4).status == "EMPTY"


def test_conflict_blocks_then_converges_and_acknowledges_independently(authority):
    f, first, _ = authority
    event_id = f.event(1, first)
    f.apply()
    old = f.changes()
    old_bytes = canonical_bytes(old)
    f.event(1, first, conflict=True)
    assert expect("PREFIX_BLOCKED", f.changes).retryable
    expect("INVALIDATION_LAG", f.invalidations, 0, 1)
    f.apply()
    now = f.changes()
    assert [entry.effective_disposition for entry in now.entries] == ["SUPERSEDED_BY_CONFLICT", "CONFLICTED"]
    assert now.entries[0].source_transition_hash == old.entries[0].source_transition_hash
    assert canonical_bytes(old) == old_bytes and now.entries[0].canonical_bundle is None
    f.assert_oracle(now)
    f.silver.conn.execute("UPDATE silver_v1_obligation SET status='PENDING' WHERE source_kind='invalidations'")
    f.reader.silver_repo.client.command(
        f"TRUNCATE TABLE {f.worker.settings.clickhouse_database}.silver_invalidation_v1")
    inv = f.invalidations(0, 2)
    assert inv.entries[0].event_id == event_id and inv.entries[0].mirror_status == "PENDING"
    assert not inv.has_more and inv.through_invalidation_sequence == 1
    f.assert_oracle(inv)
    expect("INVALIDATION_LAG", f.invalidations, 1, 2)
    assert f.invalidations(1, 1).status == "EMPTY"
    probe = f.path / "probe.db"
    initialize(probe, f.binding)
    with pytest.raises(ValueError, match="watermark"):
        acknowledge(probe, canonical_bytes(now))
    for boundary, exit_code in (("before", 73), ("after", 74)):
        result = subprocess.run([sys.executable, "-m", "de.tests.silver.feed_consumer_probe",
                                 str(probe), boundary], input=canonical_bytes(inv), timeout=30)
        assert result.returncode == exit_code
        with sqlite3.connect(probe) as db:
            assert dict(db.execute("SELECT kind,sequence FROM cursor_state")) == {
                "CHANGE": 0, "INVALIDATION": int(boundary == "after")}
    acknowledge(probe, canonical_bytes(inv))
    acknowledge(probe, canonical_bytes(now))
    acknowledge(probe, canonical_bytes(now))
    with sqlite3.connect(probe) as db:
        assert dict(db.execute("SELECT kind,sequence FROM cursor_state")) == {"CHANGE": 2, "INVALIDATION": 1}
        assert db.execute("SELECT count(*) FROM transitions").fetchone()[0] == 3
    with pytest.raises(ValueError, match="binding"):
        initialize(probe, "changed")
    # A once-valid receipt may still be acknowledged at its recorded boundary
    # when the later conflict has not been consumed. Independent invalidation
    # catch-up then permits acknowledgment of the distinct conflict sequence.
    prior_probe = f.path / "old-receipt-probe.db"
    initialize(prior_probe, f.binding)
    for edge, exit_code in (("before", 73), ("after", 74)):
        result = subprocess.run([sys.executable, "-m", "de.tests.silver.feed_consumer_probe",
                                 str(prior_probe), edge], input=old_bytes, timeout=30)
        assert result.returncode == exit_code
        with sqlite3.connect(prior_probe) as db:
            assert dict(db.execute("SELECT kind,sequence FROM cursor_state")) == {
                "CHANGE": int(edge == "after"), "INVALIDATION": 0}
    acknowledge(prior_probe, canonical_bytes(inv))
    acknowledge(prior_probe, canonical_bytes(f.changes(1)))
    with sqlite3.connect(prior_probe) as db:
        assert dict(db.execute("SELECT kind, sequence FROM cursor_state")) == {"INVALIDATION": 1, "CHANGE": 2}
        assert db.execute("SELECT transition_hash FROM transitions WHERE kind='CHANGE' AND sequence=1").fetchone() == (
            old.entries[0].source_transition_hash,)


def test_conflict_during_read_and_binding_gaps(authority, monkeypatch):
    f, first, _ = authority
    f.event(1, first)
    f.apply()
    original = f.reader._verified_bundle

    def conflict(*args):
        result = original(*args)
        f.event(1, first, conflict=True)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(f.reader, "_verified_bundle", conflict)
        assert expect("BOUNDARY_STALE", f.changes).retryable
    expect("SOURCE_BINDING_CHANGED", f.reader.discover_canonical_source, SourceDiscoveryRequest(NS, "other"))
    f.bronze.db.execute("DELETE FROM v1_changes WHERE change_sequence=1")
    expect("RETENTION_GAP", f.changes)
    expect("RETENTION_GAP", f.reader.discover_canonical_source, SourceDiscoveryRequest(NS))


def test_invalidation_continues_under_unrelated_append(authority, monkeypatch):
    f, first, _ = authority
    for cycle in (1, 2, 3):
        f.event(cycle, first)
    f.event(1, first, conflict=True)
    f.apply()
    proof = f.reader._invalidation_proof
    appended = []

    def append(ns, row):
        if not appended:
            appended.append(f.event(2, first, conflict=True))
        return proof(ns, row)

    with monkeypatch.context() as patch:
        patch.setattr(f.reader, "_invalidation_proof", append)
        page = f.invalidations(0, 3)
        assert page.captured_bronze_invalidation_high_water == page.through_invalidation_sequence == 1
    f.apply()
    f.assert_oracle(f.invalidations(1, 2))
    original = f.reader.bronze.current
    legacy_appended = []

    def append_legacy(*args):
        if not legacy_appended:
            legacy_appended.append(f.event(3, first, conflict=True))
        return original(*args)

    monkeypatch.setattr(f.reader.bronze, "current", append_legacy)
    with pytest.raises(SnapshotStaleError):
        f.reader.read_invalidation_page(limit=1)


def test_entry_timeouts_and_authority_faults_produce_no_receipt(authority, monkeypatch):
    from de.silver import incremental_reader

    f, first, _ = authority
    f.event(1, first)
    f.event(2, first)
    f.apply()
    original = f.reader._verified_bundle
    now = [0.0]
    with monkeypatch.context() as patch:
        patch.setattr(incremental_reader, "time", SimpleNamespace(monotonic=lambda: now[0]))
        patch.setattr(incremental_reader, "feed_query_budget", lambda *_: nullcontext())

        def slow(*args):
            if args[2] == 2:
                now[0] += 0.020
            return original(*args)

        patch.setattr(f.reader, "_verified_bundle", slow)
        assert expect("ENTRY_TIMEOUT", f.changes, max_entry_processing_ms=10).details["sequence"] == 2

    def unavailable(*args):
        raise ConnectionError("fixture transport down")

    with monkeypatch.context() as patch:
        patch.setattr(f.reader, "_verified_bundle", unavailable)
        expect("CLICKHOUSE_UNAVAILABLE", f.changes)
    with monkeypatch.context() as patch:
        def final_unavailable(*args):
            raise sqlite3.OperationalError("isolated final authority unavailable")

        patch.setattr(f.reader, "_check_feed_end", final_unavailable)
        expect("AUTHORITY_UNAVAILABLE", f.changes)
    f.silver.conn.execute("UPDATE silver_v1_obligation SET expected_hash='corrupt' WHERE source_sequence=2")
    expect("SOURCE_INTEGRITY_ERROR", f.changes)


def test_retained_source_replacement_rewind_and_resume(authority, monkeypatch):
    from de.silver import incremental_reader

    f, first, _ = authority
    f.event(1, first)
    f.apply()
    page = f.changes()
    f.assert_oracle(page)
    replacement_path = f.path / "replacement.db"
    with sqlite3.connect(replacement_path) as replacement:
        f.bronze.db.backup(replacement)
    with CanonicalSilverReader(replacement_path, Path(f.worker.settings.checkpoint_path),
                               f.reader.silver_repo, f.bronze_repo, f.archive) as other:
        expect("SOURCE_BINDING_CHANGED", other.read_canonical_changes_page, CanonicalChangePageRequest(
            namespace=NS, expected_source_binding_id=f.binding, after_change_sequence=1))
    verification = incremental_reader.verify_bronze_source
    checked = []

    def trace(*args, full_history, **kwargs):
        checked.append(full_history)
        return verification(*args, full_history=full_history, **kwargs)

    monkeypatch.setattr(incremental_reader, "verify_bronze_source", trace)
    with CanonicalSilverReader(f.bronze.path, Path(f.worker.settings.checkpoint_path),
                               f.reader.silver_repo, f.bronze_repo, f.archive) as reopened:
        discovery = reopened.discover_canonical_source(SourceDiscoveryRequest(NS, f.binding))
        assert discovery.source_binding_id == f.binding
        assert not discovery.bootstrap_required and checked == [False]
    f.bronze.db.execute("UPDATE v1_sequence SET next_change=1 WHERE namespace=?", (NS,))
    expect("SOURCE_REWOUND", f.changes, 1)


def test_missing_clickhouse_fact_illegal_transition_and_archive_fault(authority, monkeypatch):
    f, first, _ = authority
    event = f.event(1, first)
    f.apply()
    assert f.changes().entries[0].event_id == event
    f.bronze.db.execute("UPDATE v1_changes SET previous_state='ACCEPTED' WHERE change_sequence=1")
    expect("INVALID_TRANSITION", f.changes)
    f.bronze.db.execute("UPDATE v1_changes SET previous_state='PENDING' WHERE change_sequence=1")
    manifest = f.archive.root / "runs" / first["simulationRunId"] / "run_manifest.json"
    good = manifest.read_bytes()
    manifest.write_text("{}", encoding="utf-8")
    try:
        expect("SOURCE_INTEGRITY_ERROR", f.changes)
    finally:
        manifest.write_bytes(good)
    database = f.worker.settings.clickhouse_database
    f.reader.silver_repo.client.command(f"TRUNCATE TABLE {database}.silver_signal_state_fact_v1")
    expect("CANONICAL_EVIDENCE_MISSING", f.changes)


def test_conflict_before_materialization_and_invalidation_lag(authority):
    f, first, _ = authority
    unaffected = f.event(1, first)
    affected = f.event(2, first)
    f.event(2, first, conflict=True)
    f.apply()
    cold = f.reader.discover_canonical_source(SourceDiscoveryRequest(NS))
    assert cold.bootstrap_required and cold.next_change_sequence == cold.next_invalidation_sequence == 0
    page = f.changes()
    assert [(entry.event_id, entry.effective_disposition) for entry in page.entries] == [
        (unaffected, "BUNDLE_VALID"), (affected, "SUPERSEDED_BY_CONFLICT"), (affected, "CONFLICTED"),
    ]
    f.assert_oracle(page)
    # Simulate a capture that sees the applied change cursor while the durable
    # invalidation cursor sample trails. Change acknowledgment is still safe.
    f.silver.conn.execute("UPDATE silver_v1_source_binding SET invalidation_cursor=0 WHERE namespace=?", (NS,))
    assert f.changes().through_change_sequence == 1
    expect("PREFIX_BLOCKED", f.changes, 1)
    expect("INVALIDATION_LAG", f.invalidations, 0, 1)


def test_normal_page_deadline_can_return_only_the_verified_prefix(authority, monkeypatch):
    from de.silver import incremental_reader

    f, first, _ = authority
    f.event(1, first)
    f.event(2, first)
    f.apply()
    original = f.reader._verified_bundle
    tick = [0.0]
    monkeypatch.setattr(incremental_reader, "time", SimpleNamespace(monotonic=lambda: tick[0]))
    monkeypatch.setattr(incremental_reader, "feed_query_budget", lambda *_: nullcontext())

    def delayed_second(*args):
        result = original(*args)
        if args[2] == 2:
            tick[0] += 0.095
        return result

    monkeypatch.setattr(f.reader, "_verified_bundle", delayed_second)
    prefix = f.changes(max_processing_ms=100, max_entry_processing_ms=200)
    assert prefix.has_more and prefix.through_change_sequence == 1
    f.assert_oracle(prefix)
    expect("DEADLINE_EXCEEDED", f.changes, 1, max_processing_ms=100)


def test_invalidation_link_and_final_identity_faults_fail_closed(authority, monkeypatch):
    f, first, _ = authority
    f.event(1, first)
    f.event(1, first, conflict=True)
    f.apply()
    saved = f.invalidations(0, 1)
    original = f.reader._check_feed_end

    def corrupt_after_proof(*args):
        f.bronze.db.execute("UPDATE v1_identity SET content_hash='altered' WHERE namespace=?", (NS,))
        return original(*args)

    with monkeypatch.context() as patch:
        patch.setattr(f.reader, "_check_feed_end", corrupt_after_proof)
        expect("BOUNDARY_STALE", f.invalidations, 0, 1)
    expect("INVALIDATION_LINK_MISMATCH", f.invalidations, 0, 1)
    assert len(canonical_bytes(saved)) == saved.encoded_bytes


@pytest.mark.skipif(os.environ.get("SILVER_FEED_CAPACITY") != "1",
                    reason="17 minute focused capacity profile")
def test_focused_feed_capacity(authority):
    """Replay exact retained five-intersection observation cycles at a fixed wall cadence."""
    import platform

    import psutil

    f, _, _ = authority
    root = Path(os.environ["SILVER_FEED_EVIDENCE"]).resolve()
    root.mkdir(parents=True, exist_ok=True)
    artifacts = Path("Visualize/artifacts")
    outbox = (artifacts / "kafka_outbox/v1-outbox.sqlite3").resolve(strict=True)
    cycle_interval = int(os.environ.get("SILVER_FEED_CYCLE_SEC", "20"))
    steady_duration, burst_duration, recovery_duration = 600, 120, 300
    assert all(duration % cycle_interval == 0 for duration in (steady_duration, burst_duration, recovery_duration))
    cycles = (steady_duration + burst_duration + recovery_duration) // cycle_interval
    # The Phase 3 A01 capture has 120 complete cycles. Higher-rate experiments
    # use a second verified capture while retaining the original event IDs.
    first_run_cycles = 120
    run_ids = ("phase3-silver-20261122-a01", "phase1-raw-20261119-long01")
    archive_registry = f.archive.root / "runs/active_run.sqlite3"
    with sqlite3.connect(archive_registry) as registry:
        for run_id in run_ids:
            target = f.archive.root / "runs" / run_id
            shutil.copytree(artifacts / "runs" / run_id, target)
            manifest = json.loads((target / "run_manifest.json").read_text(encoding="utf-8"))
            assert len(manifest["participatingIntersectionIds"]) == 5
            registry.execute("INSERT INTO runs VALUES (?,?,?,?,?,?)", (
                manifest["runGeneration"], run_id, manifest_identity(manifest), "COMPLETE", "t1", "t1"))

    process = psutil.Process()
    report = dict(
        workload="Genuine V1 observation envelopes read-only from retained producer Outbox",
        replay_selection=[dict(run_id=run_ids[0], cycles=min(cycles, first_run_cycles)),
                          dict(run_id=run_ids[1], cycles=max(0, cycles - first_run_cycles))],
        durations_sec=dict(steady=steady_duration, burst=burst_duration, recovery=recovery_duration),
        wall_cycle_interval_sec=cycle_interval, intersections_per_cycle=5, offered_events=cycles * 5,
        reductions=f"Paced replay, one capture cycle per {cycle_interval} wall seconds. "
                   "Initial five-second cadence exceeded measured replay/apply capacity. Kafka and Raw excluded.",
        consumer_peak_rss_limit_bytes=512 * 2**20, page_entries=50, page_encoded_bytes_limit=4 * 2**20,
        page_processing_limit_ms=30_000, per_entry_limit_ms=10_000,
        python=sys.version, platform=platform.platform(), cpu_count=psutil.cpu_count(),
        memory_bytes=psutil.virtual_memory().total, baseline=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True).strip(),
    )
    samples, schedule, ready = (root / name for name in ("consumer.jsonl", "schedule.json", "ready"))
    with (root / "consumer.log").open("w", encoding="utf-8") as log:
        consumer = subprocess.Popen(
            [sys.executable, "-m", "de.tests.silver.feed_capacity_probe",
             str(samples), str(f.path / "capacity-probe.db"), str(ready), str(schedule)],
            stdin=subprocess.PIPE, stdout=log, stderr=log, text=True,
        )
        try:
            consumer.stdin.write(f.worker.settings.model_dump_json())
            consumer.stdin.close()
            until = time.monotonic() + 30
            while not ready.exists() and consumer.poll() is None and time.monotonic() < until:
                time.sleep(0.1)
            assert ready.exists() and consumer.poll() is None, "Consumer did not start; see consumer.log"
            begin = time.time() + 2
            steady = begin + steady_duration
            burst = steady + burst_duration
            end = burst + recovery_duration
            pending = schedule.with_suffix(".tmp")
            pending.write_text(json.dumps([begin, steady, burst, end]), encoding="utf-8")
            pending.replace(schedule)
            arrival_log = root / "applied.jsonl"
            with sqlite3.connect(outbox.as_uri() + "?mode=ro", uri=True) as capture, arrival_log.open(
                "w", encoding="utf-8", buffering=1
            ) as arrivals:
                for index in range(cycles):
                    delay = begin + index * cycle_interval - time.time()
                    if delay > 0:
                        time.sleep(delay)
                    assert consumer.poll() is None
                    run_id, cycle = ((run_ids[0], index + 1) if index < first_run_cycles
                                     else (run_ids[1], index - first_run_cycles + 1))
                    rows = capture.execute(
                        "SELECT payload_json FROM kafka_outbox WHERE simulation_run_id=? AND cycle_sequence=? "
                        "AND event_kind='observation_v1' AND status='ACKED' ORDER BY outbox_sequence",
                        (run_id, cycle)).fetchall()
                    assert len(rows) == 5
                    events = [json.loads(payload) for (payload,) in rows]
                    assert len({event["intersectionId"] for event in events}) == 5
                    for event in events:
                        f.publish(event)
                    f.apply(limit=10)
                    state = f.silver.v1_cursors(NS)
                    assert state["change_cursor"] == (index + 1) * 5
                    arrivals.write(json.dumps(dict(time=time.time(), change=state["change_cursor"],
                        invalidation=state["invalidation_cursor"], producer_rss=process.memory_info().rss)) + "\n")
            assert consumer.wait(timeout=max(30, end + 30 - time.time())) == 0
        finally:
            if consumer.poll() is None:
                consumer.terminate()
                consumer.wait(timeout=10)
    applied = [json.loads(line) for line in arrival_log.read_text().splitlines()]
    stats = [json.loads(line) for line in samples.read_text().splitlines()]
    report["schedule_utc_epoch"] = [begin, steady, burst, end]

    def before(rows, key, stamp):
        return max((row[key] for row in rows if row["time"] < stamp), default=0)

    progress = [row for row in stats if row["kind"] in {"sample", "page", "end"}]
    steady_applied = before(applied, "change", steady)
    steady_acked = before(progress, "cursor", steady)
    # Samples after a publication include up to one normal five-event release.
    # Compare that lag early and late instead of treating a page in flight as loss.
    steady_lags = [
        before(applied, "change", row["time"]) - row["cursor"] for row in progress
        if row["kind"] == "sample" and begin + 30 <= row["time"] < steady
    ]
    early_max, late_max = max(steady_lags[:60]), max(steady_lags[-60:])
    burst_backlog = before(applied, "change", burst) - before(progress, "cursor", burst)
    catchup = next((row for row in progress if row["time"] >= burst and
                    row["cursor"] >= before(applied, "change", row["time"])), None)
    peak_rss = max(row["peak_rss"] for row in stats if row["kind"] == "sample")
    from de.tests.silver.feed_capacity_oracle import verify

    oracle = verify(f.path)
    page_records = [row for row in stats if row["kind"] == "page"]
    latencies = sorted(row["latency_ms"] for row in page_records)
    last_stats = [row for row in stats if row["kind"] == "sample"][-1]
    report.update(dict(
        applied_steady=steady_applied, acked_steady=steady_acked,
        steady_applied_per_second=steady_applied / steady_duration,
        steady_acked_per_second=steady_acked / steady_duration,
        early_steady_backlog_max=early_max, late_steady_backlog_max=late_max,
        burst_backlog=burst_backlog, recovery_sec=catchup["time"] - burst if catchup else None,
        consumer_peak_rss_bytes=peak_rss, oracle_match=oracle["exact_oracle_match"],
        oracle=oracle, consumer_final=stats[-1],
        final_resource_sample=last_stats, producer_peak_sampled_rss_bytes=max(r["producer_rss"] for r in applied),
        latency_ms={f"p{p}": latencies[int((len(latencies) - 1) * p / 100)] for p in (50, 95, 99)},
        max_page_bytes=max(row["bytes"] for row in page_records),
        encoded_bytes_per_second=stats[-1]["encoded_bytes"] / (end - begin),
        clickhouse_version=f.reader.silver_repo.client.server_version,
        clickhouse_memory_tracking_bytes=f.reader.silver_repo.client.query(
            "SELECT value FROM system.metrics WHERE metric='MemoryTracking'").result_rows[0][0],
    ))
    offered_steady = steady_duration // cycle_interval * 5
    offered_burst = burst_duration // cycle_interval * 5
    passed = (oracle["exact_oracle_match"] and steady_applied == steady_acked == offered_steady and
              late_max <= early_max + 5 and burst_backlog == offered_burst and catchup is not None and
              peak_rss <= report["consumer_peak_rss_limit_bytes"])
    report["capacity_verdict"] = "SUSTAINABLE AT TESTED WORKLOAD" if passed else "CAPACITY INSUFFICIENT"
    (root / "capacity-result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    assert passed, f"See {root / 'capacity-result.json'}"
