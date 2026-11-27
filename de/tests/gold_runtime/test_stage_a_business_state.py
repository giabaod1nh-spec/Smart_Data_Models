"""D-A14 disposable v1→v2 rehearsal and publication failure fixtures."""

from contextlib import contextmanager
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from de.gold_runtime.business_publication import BusinessPublisher, DivergentResult, PhysicalRow, result_rows
from de.gold_runtime.business_metadata import BusinessMetadata
from de.gold.business import Claim, DirectionSample, compute_fixture_window
from de.gold_runtime.business_materialization import applied_window_coverage, compute_business_once, seal_business_work
from de.gold_runtime.business_state import BusinessState, IsolatedBusinessStore
from de.gold_runtime.checkpoint_store import CoreStateFault, GoldCoreStore
from de.gold_runtime.instance_lock import InstanceLock
from de.gold_runtime.processing_ledger import CoreWorkLedger, canonical_run_identity
from de.gold_runtime.silver_readers import CanonicalGoldSource
from de.silver.incremental_contract import canonical_bytes, canonical_hash, freeze
from de.silver.observation_contract import ObservationMetadataCursor, VerifiedObservationMetadata
from contracts.observation_schedule import ObservationSchedule


@contextmanager
def opened(root, cls):
    store = cls(root / "checkpoint.sqlite3")
    lock = InstanceLock(root / "scope.lock", "live", store, exact_path=True)
    lock.acquire(open_store=True)
    try:
        ledger = CoreWorkLedger(store, "live", lock.lease_token)
        yield store, ledger
    finally:
        lock.release()
        store.close()


@pytest.fixture
def staged_v1(tmp_path):
    root = IsolatedBusinessStore.allocate_checkpoint(tmp_path).parent
    with opened(root, GoldCoreStore) as (store, ledger):
        store.initialize_binding("live", ledger.token, ("binding", b"{}", "1", "v1", "gold-core-v1"))
        with store.transaction("live", ledger.token) as db:
            db.execute("INSERT INTO core_receipt VALUES ('receipt','CHANGE',0,1,?,0)", (b"{}",))
            db.execute("UPDATE core_cursor SET sequence=1 WHERE kind='CHANGE'")
            db.execute("INSERT INTO core_run(id,identity) VALUES ('r',?)", (b"{}",))
            for key, start in (("w1", 0), ("w2", 60)):
                db.execute("""
                    INSERT INTO core_window(id,run_key,spec,duration,start,end,eligible,generation)
                    VALUES (?,'r','tumbling',60,?,?,1,1)
                """, (key, start, start + 60))
                db.execute("""
                    INSERT INTO core_work(window_id,generation,state,source_receipt,created_at,eligible_at)
                    VALUES (?,1,'PENDING','receipt',0,?)
                """, (key, start))
    return root


def test_isolated_business_store_refuses_unmarked_core_before_any_write(tmp_path):
    root = tmp_path / "stage-a-gold-lookalike"
    root.mkdir()
    with opened(root, GoldCoreStore):
        pass
    before = {path.name: path.read_bytes() for path in root.iterdir()}
    with pytest.raises(CoreStateFault, match="identity is unavailable"):
        IsolatedBusinessStore(root / "checkpoint.sqlite3")
    assert {path.name: path.read_bytes() for path in root.iterdir()} == before


def commit_projection(state, ledger, work, *, barrier=None):
    return state.finish(ledger, work, "proof-hash", 60, canonical_bytes({"projection": [1, 2]}),
                        definition_set="fixture", source_semantics="fixture-source",
                        coverage_policy="fixture-v1", population_digest="population",
                        metadata_digest="fixture-boundary", barrier=barrier)


def test_isolated_migration_lease_restart_and_atomic_rollback(staged_v1):
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == 2
        assert store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}
        state = BusinessState(store, "live", ledger.token)
        work = state.reserve_next()
        assert work["window_id"] == "w1" and state.reserve_next() is None

        def kill(_):
            raise RuntimeError("failure after seal before commit")

        with pytest.raises(RuntimeError, match="failure after seal"):
            commit_projection(state, ledger, work, barrier=kill)
        assert store.conn.execute("SELECT state FROM core_work WHERE window_id='w1'").fetchone()[0] == "RUNNING"
        assert store.conn.execute("SELECT count(*) FROM business_obligation").fetchone()[0] == 0
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        assert state.recover_reservations() == 1
        work = state.reserve_next()
        assert commit_projection(state, ledger, work)
        assert not commit_projection(state, ledger, work)
        assert store.conn.execute("PRAGMA foreign_key_check").fetchall() == []
    with pytest.raises(CoreStateFault, match="migration version"):
        with opened(staged_v1, GoldCoreStore):
            pass
    with opened(staged_v1, IsolatedBusinessStore) as (store, _):
        joined = store.conn.execute("""
            SELECT w.state,o.state,w.work_id=o.work_id,w.input_hash=o.input_hash
            FROM core_work w JOIN business_obligation o ON w.window_id=o.window_id
        """).fetchone()
        assert tuple(joined) == ("COMPLETE", "PENDING", 1, 1)


def test_hold_does_not_monopolize_traversal_and_stale_generation_cannot_seal(staged_v1):
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        work = state.reserve_next()
        state.hold_unsealed(work)
        following = state.reserve_next()
        assert following["window_id"] == "w2"
        assert store.conn.execute("SELECT reason FROM business_hold").fetchone()[0] == "RESOURCE_LIMIT"
        with store.transaction("live", ledger.token) as db:
            ledger._advance_work(db, following["window_id"], "receipt")
        assert not commit_projection(state, ledger, following)
        assert state.recover_reservations() == 1
        assert state.reserve_next()["generation"] == 2


def test_over_budget_membership_is_held_before_claim_and_next_window_is_admitted(staged_v1):
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        with store.transaction("live", ledger.token) as db:
            db.execute("""
                WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<3001)
                INSERT INTO core_event SELECT 'member-'||n,'r',n,1,'transition','bundle','normal','A',n,1 FROM ids
            """)
            db.execute("INSERT INTO core_membership SELECT 'w1',id FROM core_event")
        assert state.reserve_next()["window_id"] == "w2"
        assert tuple(store.conn.execute("SELECT window_id,reason FROM business_hold").fetchone()) == ("w1", "MEMBER_LIMIT")
        assert tuple(store.conn.execute("SELECT window_id FROM business_reservation").fetchone()) == ("w2",)
        assert store.conn.execute("SELECT state FROM core_work WHERE window_id='w1'").fetchone()[0] == "PENDING"


class DisposableRows:
    def __init__(self):
        self.rows = {}
        self.ambiguous = False
        self.read_count = 0
        self.insert_count = 0

    def read(self, result_id, *, limit):
        self.read_count += 1
        return tuple(self.rows.get(result_id, [])[:limit])

    def insert(self, result_id, rows):
        self.insert_count += 1
        if self.ambiguous:
            self.rows.setdefault(result_id, []).append(rows[0])
            self.ambiguous = False
            raise TimeoutError("partial commit")
        self.rows.setdefault(result_id, []).extend(rows)


def result_payload():
    return canonical_bytes({"rows": [
        {"grain": "direction", "intersection": "A", "direction": "east", "value": 20},
        {"grain": "intersection", "intersection": "A", "direction": None, "value": 20},
        {"grain": "network", "intersection": None, "direction": None, "value": 20},
    ]})


def prepare_result(state, ledger, publisher):
    assert commit_projection(state, ledger, state.reserve_next())
    obligation = state.store.conn.execute(
        "SELECT * FROM business_obligation WHERE state='PENDING' ORDER BY created_at LIMIT 1"
    ).fetchone()
    identifier = publisher.save_computed(obligation["id"], obligation["projection_hash"], result_payload())
    assert identifier == publisher.save_computed(obligation["id"], obligation["projection_hash"], result_payload())
    return identifier


def test_retry_reuses_payload_revision_and_repairs_partial_physical_rows(staged_v1):
    target = DisposableRows()
    target.ambiguous = True
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        writer = BusinessPublisher(state, target, clock=lambda: 0)
        identifier = prepare_result(state, ledger, writer)
        assert writer.publish_once() == "PENDING"
        assert store.conn.execute("SELECT result_id FROM business_scope").fetchone()[0] is None
        assert len(target.rows[identifier]) == 1
        assert writer.publish_once() == "IDLE"
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target, clock=lambda: 1)
        assert writer.publish_once() == "PUBLISHED"
        assert store.conn.execute("SELECT id,revision FROM business_result").fetchone()[0:2] == (identifier, 1)
        assert len(target.rows[identifier]) == 3
        scope = store.conn.execute("SELECT id FROM business_scope").fetchone()[0]
        assert writer.effective_pointer(scope) == (identifier, 1)
        with store.transaction("live", ledger.token) as db:
            ledger._advance_work(db, "w1", "receipt")
        assert writer.effective_pointer(scope) is None
        assert tuple(store.conn.execute("SELECT result_id,epoch FROM business_scope").fetchone()) == (None, 2)
        assert store.conn.execute("SELECT state FROM business_obligation").fetchone()[0] == "SUPERSEDED"
        assert len(target.rows[identifier]) == 3


@pytest.mark.parametrize("invalidate", (False, True))
def test_verified_result_holds_exact_manifest_across_restart_and_fences_generation(staged_v1, invalidate):
    target = DisposableRows()
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        writer = BusinessPublisher(state, target, clock=lambda: 1)
        identifier = prepare_result(state, ledger, writer)

        def crash():
            raise RuntimeError("interrupted after durable verification")

        with pytest.raises(RuntimeError, match="after durable verification"):
            writer.publish_once(after_verify=crash)
        scope = store.conn.execute("SELECT id FROM business_scope").fetchone()[0]
        assert writer.effective_pointer(scope) is None
        assert store.conn.execute("""
            SELECT v.manifest_hash=r.manifest_hash AND v.claim_token=p.claim_token
            FROM business_publication_verification v JOIN business_result r ON r.id=v.result_id
            JOIN business_publication p ON p.result_id=v.result_id
        """).fetchone()[0] == 1
        assert (target.read_count, target.insert_count) == (2, 1)

    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target, clock=lambda: 5)
        assert writer.recover() == 0
        if invalidate:
            with store.transaction("live", ledger.token) as db:
                ledger._advance_work(db, "w1", "receipt")
        assert writer.publish_once() == ("INVALIDATED" if invalidate else "PUBLISHED")
        assert writer.effective_pointer(scope) == (None if invalidate else (identifier, 1))
        assert tuple(store.conn.execute("SELECT id,revision FROM business_result").fetchone()) == (identifier, 1)
        assert store.conn.execute("SELECT count(*) FROM business_publication_verification").fetchone()[0] == 1
        assert (target.read_count, target.insert_count) == (2, 1)


def test_reopening_earlier_disposable_v2_preserves_immutable_results(staged_v1):
    target = DisposableRows()
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target)
        identifier = prepare_result(writer.state, ledger, writer)
        before = tuple(store.conn.execute("SELECT * FROM business_result WHERE id=?", (identifier,)).fetchone())
        with store.transaction("live", ledger.token) as db:
            # Reconstruct the previously shipped v2 fixture shape before reopening.
            db.execute("DROP TRIGGER business_core_generation_fence")
            db.execute("DROP TABLE business_publication_verification")
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == 2
        assert tuple(store.conn.execute("SELECT * FROM business_result WHERE id=?", (identifier,)).fetchone()) == before
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target)
        assert writer.publish_once() == "PUBLISHED"
        assert store.conn.execute("SELECT result_id FROM business_publication_verification").fetchone()[0] == identifier


def test_exhausted_write_budget_uses_last_claim_only_to_reconcile(staged_v1):
    class LateVisibility(DisposableRows):
        def insert(self, result_id, rows):
            self.insert_count += 1
            if self.insert_count == 3:
                self.rows[result_id] = list(rows)
            raise TimeoutError("ambiguous insert")

    target = LateVisibility()
    now = [0]
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target, clock=lambda: now[0])
        identifier = prepare_result(writer.state, ledger, writer)
        for _ in range(3):
            assert writer.publish_once() == "PENDING"
            now[0] += 2
        assert writer.publish_once() == "PUBLISHED"
        assert (target.insert_count, target.read_count) == (3, 4)
        assert tuple(store.conn.execute("SELECT result_id,attempts FROM business_publication").fetchone()) == (identifier, 4)


def test_computed_families_cannot_drop_a_sealed_approach(staged_v1):
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        writer = BusinessPublisher(state, DisposableRows())
        assert state.finish(ledger, state.reserve_next(), "proof", 60, canonical_bytes({"projection": []}),
            definition_set="fixture", source_semantics="silver_observation_metadata_v1", coverage_policy="fixture-v1",
            population_digest=canonical_hash([["A", ["east", "west"]]]), metadata_digest="fixture-boundary")
        obligation = store.conn.execute("SELECT * FROM business_obligation").fetchone()
        with pytest.raises(DivergentResult, match="sealed approach population"):
            writer.save_computed(obligation["id"], obligation["projection_hash"], result_payload())
        assert store.conn.execute("SELECT count(*) FROM business_result").fetchone()[0] == 0


@pytest.mark.parametrize("divergent", (False, True))
def test_physical_same_key_duplicates_are_reconciled_by_exact_bytes(staged_v1, divergent):
    target = DisposableRows()
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target)
        identifier = prepare_result(writer.state, ledger, writer)
        expected = result_rows(result_payload())
        duplicate = replace(expected[0], payload=b"{}") if divergent else expected[0]
        target.rows[identifier] = [*expected, duplicate]
        assert writer.publish_once() == ("BLOCKED" if divergent else "PUBLISHED")
        assert target.read_count == 1 and target.insert_count == 0


def test_stale_generation_and_divergent_duplicate_never_set_pointer(staged_v1):
    target = DisposableRows()
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        writer = BusinessPublisher(state, target, clock=lambda: 0)
        identifier = prepare_result(state, ledger, writer)

        def late():
            with store.transaction("live", ledger.token) as db:
                ledger._advance_work(db, "w1", "receipt")

        assert writer.publish_once(barrier=late) == "INVALIDATED"
        assert len(target.rows[identifier]) == 3
        assert store.conn.execute("SELECT result_id FROM business_scope").fetchone()[0] is None
        second = prepare_result(state, ledger, writer)
        target.rows[second] = [PhysicalRow("network", "wrong", b"{}")]
        assert writer.publish_once() == "BLOCKED"
        assert store.conn.execute("SELECT count(*) FROM business_scope WHERE result_id IS NOT NULL").fetchone()[0] == 0


def test_metadata_requires_both_core_watermarks_and_atomically_fences_correction(staged_v1):
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        metadata = BusinessMetadata(state)
        writer = BusinessPublisher(state, DisposableRows(), clock=lambda: 0)
        identity = dict(run_id="isolated", run_generation=1, topology_digest="v1:sha256:" + "a" * 64,
                        manifest_identity="b" * 64)
        core_identity = canonical_run_identity(*identity.values())
        run_key = canonical_hash(core_identity)
        eid = "e" * 64
        with store.transaction("live", ledger.token) as db:
            db.execute("INSERT INTO core_run(id,identity) VALUES (?,?)", (run_key, canonical_bytes(core_identity)))
            db.execute("UPDATE core_window SET run_key=? WHERE id='w1'", (run_key,))
            db.execute("INSERT INTO core_event VALUES (?,?,1,1,'transition','bundle','normal','A',1,0.01)",
                       (eid, run_key))
            db.execute("INSERT INTO core_transition VALUES ('CHANGE',1,?,'transition','BUNDLE_VALID','bundle',"
                       "'receipt',?)", (eid, canonical_bytes({"source_payload_hash": "d" * 64})))
            db.execute("INSERT INTO core_membership VALUES ('w1',?)", (eid,))
        genesis = ObservationMetadataCursor(stream="MEASUREMENT")
        first_cursor = replace(genesis, sequence=1, digest="1" * 64)
        anchor = {"runId": "isolated", "generation": 1, "manifestIdentity": "b" * 64,
                  "topologyDigest": identity["topology_digest"], "eventId": eid, "payloadHash": "d" * 64,
                  "cycle": 1, "intersectionId": "A", "simulationTimeSeconds": 0.01}
        first = VerifiedObservationMetadata(**identity, source_binding_id="binding", schedule_id="a" * 64,
            archive_head_digest="c" * 64, captured_through_sequence=2, before=genesis, after=first_cursor,
            required_change_sequence=2, required_invalidation_sequence=1, status="VERIFIED",
            payload=freeze({"objectDigest": "f" * 64, "revision": 1, "previousDigest": None,
                            "canonicalDisposition": "BUNDLE_VALID", "anchor": anchor, "measurements": {}}))

        class PublicSilverSession:
            def read_observation_metadata(self, request):
                assert request.run_id == "isolated" and request.cursor == genesis
                return first

        source = CanonicalGoldSource(PublicSilverSession())
        first = source.observation_metadata("binding", "isolated", 1, identity["topology_digest"], genesis)
        assert metadata.apply(first) == "WAITING"
        with store.transaction("live", ledger.token) as db:
            db.execute("UPDATE core_cursor SET sequence=2 WHERE kind='CHANGE'")
        assert metadata.apply(first) == "WAITING"
        with store.transaction("live", ledger.token) as db:
            db.execute("UPDATE core_cursor SET sequence=1 WHERE kind='INVALIDATION'")

        def fail():
            raise RuntimeError("crash before metadata commit")

        with pytest.raises(RuntimeError, match="crash before metadata commit"):
            metadata.apply(first, barrier=fail)
        assert metadata.cursor(*identity.values(), "MEASUREMENT") == genesis
        assert store.conn.execute("SELECT count(*) FROM business_measurement").fetchone()[0] == 0
        assert metadata.apply(first) == "APPLIED" and metadata.apply(first) == "REUSED"
        identifier = prepare_result(state, ledger, writer)
        assert writer.publish_once() == "PUBLISHED"
        scope = store.conn.execute("SELECT id FROM business_scope").fetchone()[0]
        assert writer.effective_pointer(scope) == (identifier, 1)
        next_cursor = replace(first_cursor, sequence=2, digest="2" * 64)
        correction = replace(first, before=first_cursor, after=next_cursor, payload=freeze({
            "objectDigest": "3" * 64, "revision": 2, "previousDigest": "f" * 64, "anchor": anchor,
            "canonicalDisposition": "BUNDLE_VALID", "measurements": None}))
        assert metadata.apply(correction) == "APPLIED"
        assert writer.effective_pointer(scope) is None
        assert store.conn.execute("SELECT generation FROM core_window WHERE id='w1'").fetchone()[0] == 1
        assert store.conn.execute("SELECT window_id FROM business_metadata_dirty").fetchone()[0] == "w1"
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        metadata = BusinessMetadata(BusinessState(store, "live", ledger.token))
        assert metadata.cursor(*identity.values(), "MEASUREMENT") == next_cursor
        assert metadata.apply(correction) == "REUSED"


def _schedule_fixture_receipt(schedule, cursor, slots=(), *, lifecycle="RUNNING", tick=0, cutoff=None):
    after = replace(cursor, sequence=cursor.sequence + 1, digest=canonical_hash(["segment", cursor.sequence + 1]),
                    settled_ordinal=cursor.settled_ordinal + len(slots))
    return VerifiedObservationMetadata(run_id=schedule.run_id, run_generation=schedule.generation,
        topology_digest=schedule.topology_digest, manifest_identity=schedule.manifest_digest,
        source_binding_id="binding", schedule_id=schedule.schedule_id, archive_head_digest="e" * 64,
        captured_through_sequence=after.sequence, before=cursor, after=after, required_change_sequence=0,
        required_invalidation_sequence=0, status="VERIFIED", payload=freeze({
            "schedule": json.loads(schedule.canonical_bytes()), "slots": slots,
            "lifecycle": lifecycle, "lifecycleTick": tick, "terminalCutoffTick": cutoff}))


@pytest.mark.parametrize("duration", (60, 300))
@pytest.mark.parametrize("terminal_partial", (False, True))
def test_metadata_only_close_distinguishes_absent_window_from_partial_after_restart(staged_v1, duration, terminal_partial):
    schedule = ObservationSchedule("metadata-only", 1, "b" * 64, "c" * 64, "v1:sha256:" + "a" * 64,
                                  (("A", ("East",)),), 10_000, 1_000_000, 10_000, 1)
    run_key = canonical_hash(canonical_run_identity(schedule.run_id, schedule.generation,
        schedule.topology_digest, schedule.manifest_digest))
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        metadata = BusinessMetadata(state)
        cursor = ObservationMetadataCursor(stream="SCHEDULE")
        assert applied_window_coverage(state, run_key, 0, duration)[0] == "UNSUPPORTED_SCHEDULE"
        registered = _schedule_fixture_receipt(schedule, cursor, lifecycle="REGISTERED")
        assert metadata.apply(registered) == "APPLIED"
        started = _schedule_fixture_receipt(schedule, registered.after)
        assert metadata.apply(started) == "APPLIED"
        cursor = started.after
        for first in range(0, duration, 16):
            slots = [{"ordinal": ordinal, "outcome": "MISSED_TICK", "events": []}
                     for ordinal in range(first, min(first + 16, duration))]
            page = _schedule_fixture_receipt(schedule, cursor, slots)
            assert metadata.apply(page) == "APPLIED"
            cursor = page.after
        assert applied_window_coverage(state, run_key, 0, duration)[0] == "WAITING"
        cutoff = duration * 1_000_000 - (500_000 if terminal_partial else 0)
        terminal = _schedule_fixture_receipt(schedule, cursor, lifecycle="ENDED", tick=cutoff, cutoff=cutoff)
        assert metadata.apply(terminal) == "APPLIED"

    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        expected = "TERMINAL_PARTIAL" if terminal_partial else "MISSING_WINDOW"
        assert applied_window_coverage(state, run_key, 0, duration) == (expected, None)
        assert applied_window_coverage(state, run_key, duration, duration) == ("OUTSIDE_RUN", None)
        assert store.conn.execute("SELECT count(*) FROM core_window WHERE run_key=?", (run_key,)).fetchone()[0] == 0
        assert store.conn.execute("SELECT count(*) FROM business_result").fetchone()[0] == 0
        assert store.cursors() == {"CHANGE": 1, "INVALIDATION": 0}


def test_verified_and_late_conflicts_never_become_missing_or_add_metric_credit(staged_v1):
    schedule = ObservationSchedule("conflicts", 1, "b" * 64, "c" * 64, "v1:sha256:" + "a" * 64,
                                  (("A", ("A-East",)), ("B", ("B-East",))), 10_000, 1_000_000, 10_000, 1)
    identity = dict(run_id=schedule.run_id, run_generation=schedule.generation,
        topology_digest=schedule.topology_digest, manifest_identity=schedule.manifest_digest)
    core_identity = canonical_run_identity(*identity.values())
    run_key = canonical_hash(core_identity)
    e1, e2 = schedule.event_ids(0)
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        metadata = BusinessMetadata(state)
        cursor = ObservationMetadataCursor(stream="SCHEDULE")
        started = _schedule_fixture_receipt(schedule, cursor)
        assert metadata.apply(started) == "APPLIED"
        cursor = started.after
        for first in range(0, 60, 16):
            slots = [{"ordinal": ordinal, "outcome": "MISSED_TICK", "events": []}
                     for ordinal in range(first, min(first + 16, 60))]
            if first == 0:
                slots[0] = {"ordinal": 0, "outcome": "DELIVERED", "events": [
                    {"eventId": e1, "disposition": "CONFLICTED"}, {"eventId": e2, "disposition": "BUNDLE_VALID"}]}
            page = _schedule_fixture_receipt(schedule, cursor, slots)
            assert metadata.apply(page) == "APPLIED"
            cursor = page.after
        ended = _schedule_fixture_receipt(schedule, cursor, lifecycle="ENDED", tick=60_000_000, cutoff=60_000_000)
        assert metadata.apply(ended) == "APPLIED"
        status, (spec, due, _) = applied_window_coverage(state, run_key, 0, 60)
        assert status == "READY" and due == {e2: 10_000}
        result = compute_fixture_window(spec, [Claim(10_000, 10_000, "B", e2, (DirectionSample("B-East", 0, 0, 0, 0),))])
        assert result.slot_states == (("A", 0, 59, 0, 1, 0), ("B", 1, 59, 0, 0, 0))
        assert result.rows[-1].observed_load.value is None
        with store.transaction("live", ledger.token) as db:
            db.execute("INSERT INTO core_run(id,identity) VALUES (?,?)", (run_key, canonical_bytes(core_identity)))
            db.execute("INSERT INTO core_suppression VALUES (?,1,'invalidation',?,0.01,'B',1,2,'proof')", (e2, run_key))
        assert applied_window_coverage(state, run_key, 0, 60) == ("CONFLICTED_WINDOW", None)
        assert store.conn.execute("SELECT count(*) FROM business_result").fetchone()[0] == 0


def test_verified_schedule_wakes_isolated_work_and_materializes_same_core_proof(staged_v1):
    schedule = ObservationSchedule("isolated", 1, "b" * 64, "c" * 64, "v1:sha256:" + "a" * 64,
                                  (("A", ("East",)),), 10_000, 1_000_000, 10_000, 1)
    identity = dict(run_id=schedule.run_id, run_generation=1, topology_digest=schedule.topology_digest,
                    manifest_identity=schedule.manifest_digest)
    core_identity = canonical_run_identity(*identity.values())
    run_key = canonical_hash(core_identity)
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        metadata = BusinessMetadata(state)
        eid = schedule.event_ids(0)[0]
        with store.transaction("live", ledger.token) as db:
            db.execute("INSERT INTO core_run(id,identity) VALUES (?,?)", (run_key, canonical_bytes(core_identity)))
            db.execute("UPDATE core_window SET run_key=? WHERE id='w1'", (run_key,))
            db.execute("INSERT INTO core_event VALUES (?,?,1,1,'transition','bundle','normal','A',1,0.01)",
                       (eid, run_key))
            db.execute("INSERT INTO core_transition VALUES ('CHANGE',1,?,'transition','BUNDLE_VALID','bundle',"
                       "'receipt',?)", (eid, canonical_bytes({"source_payload_hash": "d" * 64})))
            db.execute("INSERT INTO core_membership VALUES ('w1',?)", (eid,))
        work = state.reserve_next()
        assert seal_business_work(state, ledger, work, lambda _: pytest.fail("unverified schedule traversal")) == "UNSUPPORTED_SCHEDULE"
        cursor = ObservationMetadataCursor(stream="SCHEDULE")
        for sequence, first in enumerate(range(0, 60, 15), 1):
            slots = [{"ordinal": i, "outcome": "DELIVERED", "events": [{"eventId": schedule.event_ids(i)[0],
                      "disposition": "BUNDLE_VALID"}]} for i in range(first, first + 15)]
            after = replace(cursor, sequence=sequence, digest=str(sequence) * 64, settled_ordinal=first + 14)
            receipt = VerifiedObservationMetadata(**identity, source_binding_id="binding", schedule_id=schedule.schedule_id,
                archive_head_digest="e" * 64, captured_through_sequence=4, before=cursor, after=after,
                required_change_sequence=1, required_invalidation_sequence=0, status="VERIFIED", payload=freeze({
                    "schedule": json.loads(schedule.canonical_bytes()), "slots": slots,
                    "lifecycle": "RUNNING", "lifecycleTick": 0, "terminalCutoffTick": None}))
            assert metadata.apply(receipt) == "APPLIED"
            cursor = after
            assert state.recheck_waiting_schedule() == int(sequence == 4)

        class VerifiedStream:
            input_hash = "same-core-proof"
            count = 60
            bytes = 1024

            def __iter__(self):
                for ordinal in range(60):
                    yield SimpleNamespace(event_id=schedule.event_ids(ordinal)[0],
                        intersection={"intersection_id": "A", "simulation_time_seconds": ordinal + 0.01},
                        traffic=({"source_direction": "East", "vehicle_count": 2, "waiting_vehicle_count": 1,
                                  "average_speed_kmh": 20.0, "queue_length_m": 999.0},))

        work = state.reserve_next()
        assert work["window_id"] == "w1"
        assert seal_business_work(state, ledger, work, lambda _: VerifiedStream()) == "SEALED"
        proof = store.conn.execute("SELECT input_hash FROM core_work WHERE window_id='w1'").fetchone()[0]
        assert proof == "same-core-proof"
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        target = DisposableRows()
        writer = BusinessPublisher(BusinessState(store, "live", ledger.token), target, clock=lambda: 0)
        result_id = compute_business_once(writer)
        assert result_id and compute_business_once(writer) is None and writer.publish_once() == "PUBLISHED"
        network = next(json.loads(r.payload) for r in target.rows[result_id] if r.grain == "network")
        assert network["moving_speed"]["value"] == 20
        assert network["stopped_ratio"]["value"] == 50
        assert network["observed_load"]["value"] == 2
        assert network["queue_p95"]["value"] is None  # No Silver-qualified corrected queue for these events.
        scope = store.conn.execute("SELECT id FROM business_scope").fetchone()[0]
        assert writer.effective_pointer(scope) == (result_id, 1)
        metadata = BusinessMetadata(writer.state)
        cursor = ObservationMetadataCursor(stream="MEASUREMENT")
        receipt = VerifiedObservationMetadata(**identity, source_binding_id="binding", schedule_id=schedule.schedule_id,
            archive_head_digest="e" * 64, before=cursor, after=replace(cursor, sequence=1, digest="c" * 64),
            captured_through_sequence=1, required_change_sequence=1, required_invalidation_sequence=0,
            status="VERIFIED", payload=freeze({"anchor": {
                "eventId": eid, "payloadHash": "d" * 64, "runId": "isolated", "generation": 1,
                "manifestIdentity": schedule.manifest_digest, "topologyDigest": schedule.topology_digest,
                "cycle": 1, "intersectionId": "A", "simulationTimeSeconds": 0.01},
                "previousDigest": None, "objectDigest": "a" * 64, "revision": 1, "canonicalDisposition": "BUNDLE_VALID",
                "measurements": {"directions": {"East": {"queue": {"method": "route_stopped_length_v1",
                    "scopeDigest": "b" * 64, "value": 4}}}}}))
        assert metadata.apply(receipt) == "APPLIED" and writer.effective_pointer(scope) is None
        writer.state.hold_unsealed(writer.state.reserve_next(), reason="UNSUPPORTED_SCHEDULE")  # w2 has no schedule.
        next_work = writer.state.reserve_next()
        assert (next_work["window_id"], next_work["state"]) == ("w1", "COMPLETE")
        assert seal_business_work(writer.state, ledger, next_work, lambda _: VerifiedStream()) == "SEALED"
        revision = compute_business_once(writer)
        assert revision != result_id and writer.publish_once() == "PUBLISHED"
        assert writer.effective_pointer(scope) == (revision, 3)
        assert store.conn.execute("SELECT input_hash,generation FROM core_work WHERE window_id='w1'").fetchone()[:] == (proof, 1)
