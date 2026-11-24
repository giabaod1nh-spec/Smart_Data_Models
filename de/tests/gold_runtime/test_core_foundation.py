"""Core SQLite, receipt, membership and recovery tests, independent of Docker."""
from dataclasses import asdict, replace
from hashlib import sha256
from math import nextafter
from pathlib import Path
from types import SimpleNamespace

import pytest

from de.gold.windowing import tumbling_bounds
from de.gold_runtime.checkpoint_store import CoreStateFault, GoldCoreStore, GoldRuntimeStore
from de.gold_runtime.config import GoldCoreSettings
from de.gold_runtime.instance_lock import GoldInstanceAlreadyRunning
from de.gold_runtime.main import build_processor
from de.gold_runtime.processing_ledger import core_run_identity
from de.gold_runtime.processor import CoreWorkChanged
from de.gold_runtime.silver_readers import CoreReceiptFault, verify_core_page
from de.silver.canonical_reader import ObservationBundle
from de.silver.config import SilverSettings
from de.silver.incremental_contract import (
    CanonicalBatchReceipt, CanonicalChangeEntry, CanonicalChangePage,
    CanonicalFeedError, CanonicalInvalidationEntry, CanonicalInvalidationPage,
    CanonicalInvalidationReceipt, SourceDiscovery, batch_digest, canonical_bytes, canonical_hash,
)


def digest(value):
    return sha256(str(value).encode()).hexdigest()


class FixtureSource:
    """Public protocol fixture. Mutable lists stand in for immutable journals."""

    binding = dict(namespace="live", source_path="test/authority", file_identity=(1, 2),
                   schema_fingerprint="fixture", bronze_version="fixture1")
    binding_id = "sha256:" + canonical_hash(dict(namespace="live", canonical_source_path="test/authority",
        file_identity=(1, 2), schema_fingerprint="fixture", bronze_version="fixture1"))

    def __init__(self):
        self.changes = []
        self.invalidations = []
        self.requests = []
        self.block = None

    def close(self):
        pass

    def _match(self, request):
        assert request.namespace == "live"
        if request.expected_source_binding_id not in (None, self.binding_id):
            raise CanonicalFeedError("SOURCE_BINDING_CHANGED", "fixture replacement")

    def discover_canonical_source(self, request):
        self._match(request)
        self.requests.append(("discover", request.expected_source_binding_id))
        hc, hi = len(self.changes), len(self.invalidations)
        return SourceDiscovery(namespace="live", source_binding_id=self.binding_id,
            source_binding=self.binding, bronze_change_high_water=hc, bronze_invalidation_high_water=hi,
            silver_change_applied_sequence=hc, silver_invalidation_applied_sequence=hi,
            verified_change_prefix=hc, verified_invalidation_prefix=hi,
            bootstrap_required=request.expected_source_binding_id is None)

    def event(self, name, tick, *, run="r1", scenario="NORMAL"):
        seq = len(self.changes) + 1
        run_values = dict(run_id=run, run_generation=1, topology_digest="t-" + run,
                          manifest_identity="m-" + run)
        fact = dict(simulation_run_id=run, run_generation=1, topology_digest="t-" + run,
                    manifest_identity="m-" + run, event_id=name, bronze_semantic_revision=1,
                    silver_schema_version="v1", simulation_time_seconds=tick, scenario_id=scenario,
                    intersection_id="X", cycle_sequence=seq, captured_at="2026-01-01T00:00:00Z",
                    source_topic="fixture", source_partition=0, source_offset=seq,
                    source_payload_hash=digest("payload"), source_raw_ingestion_id=digest("raw"),
                    source_physical_record_hash=digest("physical"), source_byte_hash=digest("byte"),
                    bronze_canonical_hash=digest("bronze"), value=None)
        bundle = ObservationBundle(name, fact, (), ())
        result = CanonicalChangeEntry(**run_values, change_sequence=seq, event_id=name,
            semantic_revision=1, change_type="ACCEPTED", previous_state=None, new_state="ACCEPTED",
            source_transition_hash=digest(seq), source_invalidation_id=None,
            effective_disposition="BUNDLE_VALID", superseding_change_sequence=None,
            invalidation_sequence=None, canonical_bundle=bundle, bundle_hash=canonical_hash(bundle))
        self.changes.append(result)
        return result

    def conflict(self, entry):
        seq = len(self.changes) + 1
        inv = len(self.invalidations) + 1
        node = entry.canonical_bundle.intersection
        self.invalidations.append(CanonicalInvalidationEntry(
            run_id=entry.run_id, run_generation=entry.run_generation, topology_digest=entry.topology_digest,
            manifest_identity=entry.manifest_identity, invalidation_sequence=inv, invalidation_id=f"i{inv}",
            event_id=entry.event_id, semantic_revision=2, previous_immutable_envelope_hash=digest("prior"),
            conflicting_immutable_envelope_hash=digest("conflict"), source_transition_hash=digest(seq),
            mirror_status="PENDING", intersection_id=node["intersection_id"],
            cycle_sequence=node["cycle_sequence"], simulation_time_seconds=node["simulation_time_seconds"]))
        self.changes[entry.change_sequence - 1] = replace(entry, effective_disposition="SUPERSEDED_BY_CONFLICT",
            superseding_change_sequence=seq, invalidation_sequence=inv, canonical_bundle=None, bundle_hash=None)
        self.changes.append(replace(entry, change_sequence=seq, semantic_revision=2, change_type="CONFLICTED",
            previous_state="ACCEPTED", new_state="CONFLICTED", source_transition_hash=digest(seq),
            source_invalidation_id=f"i{inv}", effective_disposition="CONFLICTED",
            invalidation_sequence=inv, canonical_bundle=None, bundle_hash=None))

    def _page(self, request, kind):
        self._match(request)
        change = kind == "CHANGE"
        hc, hi = len(self.changes), len(self.invalidations)
        after = request.after_change_sequence if change else request.after_invalidation_sequence
        ceiling = hc if change else min(hi, request.through_invalidation_sequence)
        self.requests.append((kind, after, ceiling, request.max_entries))
        if change and self.block:
            raise CanonicalFeedError(self.block, "fixture block")
        entries = tuple((self.changes if change else self.invalidations)[after:ceiling][:request.max_entries])
        count = len(entries)
        through = after + count
        bounds = dict(captured_bronze_change_high_water=hc, captured_bronze_invalidation_high_water=hi,
                      captured_silver_change_applied_sequence=hc, captured_silver_invalidation_applied_sequence=hi)
        common = dict(namespace="live", source_binding_id=self.binding_id)
        boundary = "sha256:" + canonical_hash(dict(bounds, contract_version="1", source_binding_id=self.binding_id))
        header = dict(bounds, **common, requested_after_sequence=after, returned_after_sequence=after,
                      returned_through_sequence=through, receipt_kind=kind, entry_count=count,
                      safe_invalidation_through_sequence=hi if change else None)
        receipt = None
        if count:
            receipt = (CanonicalBatchReceipt(**header) if change else CanonicalInvalidationReceipt(
                **header, requested_through_invalidation_sequence=request.through_invalidation_sequence))
            hashed = batch_digest(receipt, entries)
            receipt = replace(receipt, batch_digest=hashed, receipt_id="sha256:" + hashed)
        envelope = dict(common, entries=entries, receipt=receipt, boundary_id=boundary,
                        entry_count=count, has_more=through < ceiling, status="OK" if count else "EMPTY")
        if change:
            page = CanonicalChangePage(**envelope, **bounds, after_change_sequence=after,
                through_change_sequence=through, verified_change_prefix=hc, safe_invalidation_through_sequence=hi)
        else:
            page = CanonicalInvalidationPage(**envelope, after_invalidation_sequence=after,
                through_invalidation_sequence=through, verified_invalidation_prefix=ceiling,
                captured_bronze_invalidation_high_water=hi, captured_silver_invalidation_applied_sequence=hi,
                requested_through_invalidation_sequence=request.through_invalidation_sequence)
        return size(page)

    def read_canonical_changes_page(self, request):
        return self._page(request, "CHANGE")

    def read_canonical_invalidations_page(self, request):
        return self._page(request, "INVALIDATION")


def size(page):
    while len(canonical_bytes(page)) != page.encoded_bytes:
        size = len(canonical_bytes(page))
        page = replace(page, encoded_bytes=size, receipt=replace(page.receipt, encoded_bytes=size)
                       if page.receipt else None)
    return page


@pytest.fixture
def local(tmp_path):
    settings = GoldCoreSettings(checkpoint_path=tmp_path / "gold_v1/checkpoint.sqlite3",
        instance_lock_path=tmp_path / "gold_v1/instance.lock", replay_root=tmp_path / "replays",
        min_free_bytes=0, max_entries=3, retry_initial_sec=0.001, retry_max_sec=0.01)
    return settings, FixtureSource()


def open_core(local, *, silver=None):
    settings, fake = local
    p, lock = build_processor(settings, silver=silver or SilverSettings())
    p.session_factory = lambda _settings: fake
    return p, lock


def release(p, lock):
    p.close()
    lock.release()
    p.store.close()


def drain(p, rounds=150):
    for _ in range(rounds):
        if not p.run_cycle():
            break
    else:
        pytest.fail("Foundation did not reach an idle cycle")


def test_fractional_boundaries_and_malformed_times():
    assert tumbling_bounds(nextafter(60., 0.), 60) == (0, 60)
    assert tumbling_bounds(60., 60) == (60, 120)
    assert tumbling_bounds(nextafter(300., 0.), 300) == (0, 300)
    assert tumbling_bounds(135., 60) == (120, 180)
    assert tumbling_bounds(135., 300) == (0, 300)
    for invalid in (True, -0.001, float("nan"), float("inf"), 1 << 63):
        with pytest.raises(ValueError):
            tumbling_bounds(invalid, 60)


def test_cold_resume_materialization_late_invalidation_and_scope(local):
    _, fake = local
    first = fake.event("z-first", 135)
    fake.event("other-run", 136, run="r2")
    fake.event("tick", 360)
    p, lock = open_core(local)
    drain(p)
    db = p.store.conn
    expected = [(60, 120, 180), (300, 0, 300)]
    assert [tuple(row) for row in db.execute("""SELECT duration,start,end FROM core_window w
        JOIN core_membership m ON m.window_id=w.id WHERE m.event_id='z-first' ORDER BY duration""")] == expected
    target = db.execute("SELECT id FROM core_window WHERE duration=60 AND start=120 AND eligible=1").fetchone()[0]
    work = db.execute("SELECT * FROM core_work WHERE window_id=? AND state='COMPLETE'", (target,)).fetchone()
    stream = p.materialize(work)
    assert tuple(x.event_id for x in stream) == ("z-first",)
    initial = stream.input_hash
    assert work["input_hash"] == initial
    assert p.store.cursors() == {"CHANGE": 3, "INVALIDATION": 0}
    release(p, lock)

    fake.requests.clear()
    p, lock = open_core(local)
    try:
        drain(p)
        assert fake.requests[0] == ("discover", fake.binding_id)
        stream = p.materialize(work)
        assert [x.event_id for x in stream] == ["z-first"]
        assert stream.input_hash == initial
        fake.event("a-late", 136.25, scenario="PEAK")
        drain(p)
        db = p.store.conn
        complete = db.execute("""SELECT * FROM core_work WHERE window_id=? AND state='COMPLETE'
                              ORDER BY generation DESC LIMIT 1""", (target,)).fetchone()
        assert complete["generation"] > work["generation"]
        stream = p.materialize(complete)
        assert [x.event_id for x in stream] == ["a-late", "z-first"]
        assert stream.input_hash != initial and complete["input_count"] == 2
        fake.conflict(first)
        with pytest.raises(CoreWorkChanged):
            list(p.materialize(complete))
        drain(p)
        assert p.store.cursors() == {"CHANGE": 5, "INVALIDATION": 1}
        assert db.execute("SELECT count(*) FROM core_membership WHERE event_id='z-first'").fetchone()[0] == 0
        latest = db.execute("""SELECT * FROM core_work WHERE window_id=? AND state='COMPLETE'
                            ORDER BY generation DESC LIMIT 1""", (target,)).fetchone()
        assert [x.event_id for x in p.materialize(latest)] == ["a-late"]
        assert {tuple(row) for row in db.execute("SELECT DISTINCT completeness,finality FROM core_window")} == {
            ("UNKNOWN", "DISABLED")}
        assert db.execute("SELECT disposition FROM core_transition WHERE kind='CHANGE' AND sequence=1").fetchone()[0] == "BUNDLE_VALID"
    finally:
        release(p, lock)


@pytest.mark.parametrize("reason", ("PREFIX_BLOCKED", "SOURCE_LAG"))
def test_independent_invalidations_advance_under_change_block(local, reason):
    _, fake = local
    event = fake.event("already-invalid", 135)
    fake.conflict(event)
    fake.block = reason
    p, lock = open_core(local)
    try:
        assert p.run_cycle()
        assert p.run_cycle()
        assert p.store.cursors() == {"CHANGE": 0, "INVALIDATION": 1}
        assert p._faults["change"]["code"] == reason
        assert p.store.conn.execute("SELECT count(*) FROM core_suppression").fetchone()[0] == 1
        fake.block = None
        p._retry.clear()
        fake.requests.clear()
        drain(p)
        assert p.store.cursors() == {"CHANGE": 2, "INVALIDATION": 1}
        assert p.store.conn.execute("SELECT count(*) FROM core_membership").fetchone()[0] == 0
        assert not any(request[0] == "INVALIDATION" for request in fake.requests)
    finally:
        release(p, lock)


def test_fixed_pi_rollback_and_duplicate(local):
    _, fake = local
    event = fake.event("invalid", 135)
    fake.conflict(event)
    p, lock = open_core(local)
    try:
        p.run_cycle()
        page = p.source.changes(p.binding_id, 0)
        with pytest.raises(CoreStateFault, match="precedes"):
            p.ledger.acknowledge(page)
        assert p.store.cursors() == {"CHANGE": 0, "INVALIDATION": 0}
        p.ledger.acknowledge(p.source.invalidations(p.binding_id, 0, 1))
        assert p.ledger.acknowledge(page)
        assert not p.ledger.acknowledge(page)
        changed = size(replace(page, entries=(replace(page.entries[0], source_transition_hash="0"*64), *page.entries[1:])))
        with pytest.raises(CoreStateFault, match="immutable"):
            p.ledger.acknowledge(changed)
        assert p.store.cursors() == {"CHANGE": 2, "INVALIDATION": 1}
    finally:
        release(p, lock)


def test_receipt_fault_has_no_progress(local):
    _, fake = local
    fake.event("x", 0)
    p, lock = open_core(local)
    try:
        p.run_cycle()
        page = p.source.changes(p.binding_id, 0)
        malformed = size(replace(page, receipt=replace(page.receipt, batch_digest="0" * 64,
                                                       receipt_id="sha256:" + "0" * 64)))
        fake.read_canonical_changes_page = lambda _request: malformed
        with pytest.raises(CoreReceiptFault, match="digest"):
            p.run_cycle()
        assert p.store.cursors() == {"CHANGE": 0, "INVALIDATION": 0}
        assert p.store.conn.execute("SELECT count(*) FROM core_receipt").fetchone()[0] == 0
    finally:
        release(p, lock)


def test_v2_store_isolation_and_os_lock(local, tmp_path):
    legacy_path = tmp_path / "legacy.sqlite3"
    legacy = GoldRuntimeStore(legacy_path)
    legacy.open()
    legacy.close()
    before = legacy_path.read_bytes()
    with pytest.raises(CoreStateFault, match="non-Core"):
        GoldCoreStore(legacy_path).open()
    assert legacy_path.read_bytes() == before
    p, lock = open_core(local)
    try:
        assert lock.lock_path == local[0].instance_lock_path
        with pytest.raises(GoldInstanceAlreadyRunning):
            open_core(local)
        drain(p)
        assert legacy_path.read_bytes() == before
    finally:
        release(p, lock)



def test_fixed_pending_page_watermark_does_not_follow_new_invalidations(local):
    settings, source = local
    settings = settings.model_copy(update=dict(max_entries=1))
    events = [source.event(f"evt-{n}", n) for n in range(5)]
    for event in events[:3]:
        source.conflict(event)
    p, lock = open_core((settings, source))
    try:
        p.run_cycle()
        assert p.run_cycle()
        assert p.pending_change.receipt.safe_invalidation_through_sequence == 3
        assert p.store.cursors() == {"CHANGE": 0, "INVALIDATION": 1}
        source.conflict(events[3])
        source.conflict(events[4])
        p.run_cycle()
        p.run_cycle()
        assert p.store.cursors() == {"CHANGE": 1, "INVALIDATION": 3}
        assert [(req[1], req[2]) for req in source.requests if req[0] == "INVALIDATION"] == [
            (0, 3), (1, 3), (2, 3)]
        drain(p)
        assert p.store.cursors() == {"CHANGE": 10, "INVALIDATION": 5}
    finally:
        release(p, lock)


def test_atomic_storage_failure_and_retry(local):
    import sqlite3

    _, source = local
    source.event("window-transaction", 135)
    p, lock = open_core(local)
    try:
        p.run_cycle()
        db = p.store.conn
        db.execute("""CREATE TRIGGER test_disk_failure BEFORE INSERT ON core_membership BEGIN
                   SELECT RAISE(ABORT,'injected membership write failure'); END""")
        with pytest.raises(sqlite3.IntegrityError, match="injected"):
            p.run_cycle()
        assert p.store.cursors() == {"CHANGE": 0, "INVALIDATION": 0}
        for name in ("core_receipt", "core_transition", "core_membership", "core_event", "core_work"):
            assert db.execute(f"SELECT count(*) FROM {name}").fetchone()[0] == 0
        db.execute("DROP TRIGGER test_disk_failure")
        drain(p)
        assert p.store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}
        assert db.execute("SELECT count(*) FROM core_membership").fetchone()[0] == 2
    finally:
        release(p, lock)


def test_replacement_and_graceful_shutdown_preserve_state(local):
    _, source = local
    source.event("original-source", 135)
    p, lock = open_core(local)
    try:
        drain(p)
        p.request_shutdown()
        assert p.run() == 0
        assert p.health_snapshot()["state"] == "STOPPED"
        assert p.store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}
    finally:
        release(p, lock)
    source.binding_id = "sha256:" + "f" * 64
    p, lock = open_core(local)
    try:
        with pytest.raises(CanonicalFeedError) as failure:
            p.run_cycle()
        assert failure.value.code == "SOURCE_BINDING_CHANGED"
        assert p.store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}
    finally:
        release(p, lock)


def test_active_replay_aborts_on_boundary_change_without_touching_live(local, tmp_path):
    import json
    from de.gold_runtime.replay import CoreReplayManifest

    settings, source = local
    source.event("scope", 135)
    live, owner = open_core(local)
    drain(live)
    release(live, owner)
    before = settings.checkpoint_path.read_bytes()
    manifest = CoreReplayManifest(source.binding_id, (1, 0, 1, 0), "r1", 1, "t-r1", "m-r1", 0, 360)
    manifest_path = tmp_path / "replay.json"
    manifest_path.write_text(json.dumps(asdict(manifest)), encoding="utf-8")
    replay, replay_owner = open_core((settings.model_copy(update=dict(replay_manifest=manifest_path)), source))
    try:
        assert replay.run_cycle()
        source.event("new-boundary", 480)
        assert replay.run() == 2
        assert replay.store.conn.execute("SELECT status FROM core_replay").fetchone()[0] == "ABORTED"
        assert replay.store.cursors() == {"CHANGE": 0, "INVALIDATION": 0}
        assert settings.checkpoint_path.read_bytes() == before
    finally:
        release(replay, replay_owner)


@pytest.mark.parametrize("error_code", ["AUTHORITY_UNAVAILABLE", "SOURCE_BINDING_CHANGED"])
def test_replay_final_discovery_fault(local, tmp_path, monkeypatch, error_code):
    import json
    import time
    from de.gold_runtime.replay import CoreReplayManifest

    settings, source = local
    source.event("scope", 135)
    live, owner = open_core(local)
    drain(live)
    release(live, owner)
    before = settings.checkpoint_path.read_bytes()
    manifest = CoreReplayManifest(source.binding_id, (1, 0, 1, 0), "r1", 1, "t-r1", "m-r1", 0, 360)
    path = tmp_path / "replay.json"
    path.write_text(json.dumps(asdict(manifest)), encoding="utf-8")
    replay, owner = open_core((settings.model_copy(update=dict(replay_manifest=path)), source))
    try:
        assert replay.run_cycle()
        assert replay.run_cycle()
        discover = replay.discover
        calls = 0

        def transient_at_finish():
            nonlocal calls
            calls += 1
            # Cycle discovery, empty change page validation, then completion.
            if calls == 3:
                raise CanonicalFeedError(error_code, "final validation")
            return discover()

        monkeypatch.setattr(replay, "discover", transient_at_finish)
        if error_code == "AUTHORITY_UNAVAILABLE":
            replay.run_cycle()
            assert calls == 3
            assert replay.store.conn.execute("SELECT status FROM core_replay").fetchone()[0] == "RUNNING"
            assert replay._faults["replay_completion"]["retryable"]
            time.sleep(settings.retry_max_sec)
            replay.run_cycle()
            assert replay.store.conn.execute("SELECT status FROM core_replay").fetchone()[0] == "COMPLETE"
            assert "replay_completion" not in replay._faults
        else:
            assert replay.run() == 2
            assert calls == 3
            assert replay.store.conn.execute("SELECT status FROM core_replay").fetchone()[0] == "ABORTED"
        assert replay.store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}
        assert settings.checkpoint_path.read_bytes() == before
    finally:
        release(replay, owner)


def test_lease_loss_keeps_durable_cursors(local):
    _, source = local
    source.event("before-lease-loss", 135)
    p, lock = open_core(local)
    try:
        drain(p)
        source.event("after-lease-loss", 350)
        p.store.conn.execute("UPDATE gold_runtime_lease SET lease_token='test-other-owner'")
        assert p.run() == 2
        assert p.store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}
        assert p.health_snapshot()["foundation_ready"] is False
    finally:
        release(p, lock)

