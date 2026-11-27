"""Stage A bounded proof, terminal eligibility, and Reader regression cases."""

from dataclasses import replace
from contextlib import closing
import json
import sqlite3
import time

import pytest

from contracts.observation_schedule import ObservationSchedule
from de.gold.business import Claim, DirectionSample, VERIFIED_DEFINITION, WindowSpec, compute_fixture_window
from de.gold_runtime.business_experiments import validate_pair
from de.gold_runtime.business_metadata import BusinessMetadata
from de.gold_runtime.business_publication import BusinessPublisher, DivergentResult
from de.gold_runtime.business_reader import GoldReader, historical_trend, overview
from de.gold_runtime.business_state import BusinessState, IsolatedBusinessStore
from de.gold_runtime.config import GoldCoreSettings
from de.gold_runtime.instance_lock import InstanceLock
from de.gold_runtime.processing_ledger import canonical_run_identity
from de.gold_runtime.processor import CoreInputStream, CoreWorkChanged
from de.gold_runtime.replay import CoreReplayManifest
from de.gold_runtime.stage_a import BatchedStageAInput, StageABusinessProcessor
from de.gold_runtime.stage_a_oracle import verify_source_lineage
from de.silver.config import SilverSettings
from de.silver.incremental_contract import canonical_bytes, canonical_hash
from de.silver.observation_contract import ObservationMetadataCursor
from de.tests.gold_runtime.test_core_foundation import FixtureSource, local, open_core, release
from de.tests.gold_runtime.test_stage_a_business_state import (
    DisposableRows, _schedule_fixture_receipt, opened, staged_v1,
)


def test_batched_public_reads_keep_identical_core_proof_in_event_id_order(local):
    _, source = local
    for index in range(12):
        source.event(f"event-{11 - index:02}", index + 0.01)
    later = source.event("outside", 60)
    processor, lock = open_core(local)
    try:
        assert processor._bootstrap()
        processor.discover()
        while processor._changes():
            pass
        processor.ledger.promote(10)
        work = processor.ledger.claim()
        assert work is not None
        normal = CoreInputStream(processor, work)
        expected = {bundle.event_id for bundle in normal}
        assert len(expected) == 12
        source.requests.clear()
        batch = BatchedStageAInput(processor, work)
        assert {bundle.event_id for bundle in batch} == expected
        assert batch.input_hash == normal.input_hash and batch.count == normal.count
        requests = [r for r in source.requests if r[0] == "CHANGE"]
        assert len(requests) == 4  # Existing public-source cap of three entries.
        assert batch.bytes < normal.bytes
        batch = BatchedStageAInput(processor, work)
        next(batch)
        source.conflict(later)
        with pytest.raises(CoreWorkChanged):
            list(batch)
    finally:
        release(processor, lock)


def test_terminal_cutoff_and_physical_reader_fence(staged_v1):
    schedule = ObservationSchedule("closed", 1, "b" * 64, "c" * 64, "v1:sha256:" + "a" * 64,
                                  (("A", ("East",)),), 10_000, 1_000_000, 10_000, 1)
    identity = canonical_run_identity(schedule.run_id, 1, schedule.topology_digest, schedule.manifest_digest)
    run_key = canonical_hash(identity)
    target = DisposableRows()
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger):
        state = BusinessState(store, "live", ledger.token)
        with state._tx() as db:
            db.execute("INSERT INTO core_run(id,identity) VALUES (?,?)", (run_key, canonical_bytes(identity)))
            db.execute("UPDATE core_window SET run_key=?,eligible=0 WHERE id IN ('w1','w2')", (run_key,))
            db.execute("UPDATE core_work SET state='OPEN',eligible_at=NULL")
            db.execute("INSERT INTO core_event VALUES ('event',?,1,1,'transition','bundle','normal','A',1,0.01)",
                       (run_key,))
            db.execute("INSERT INTO core_membership VALUES ('w1','event'),('w2','event')")
        metadata = BusinessMetadata(state)
        cursor = ObservationMetadataCursor(stream="SCHEDULE")
        started = _schedule_fixture_receipt(schedule, cursor)
        assert metadata.apply(started) == "APPLIED"
        assert state.promote_terminal_windows(run_key, limit=10) == 0
        terminal = _schedule_fixture_receipt(schedule, started.after, lifecycle="ENDED",
                                            tick=60_000_000, cutoff=60_000_000)
        assert metadata.apply(terminal) == "APPLIED"
        assert state.promote_terminal_windows(run_key, limit=10) == 1
        work = state.reserve_next()
        assert work["window_id"] == "w1"
        assert state.finish(ledger, work, "proof", 60, canonical_bytes({}), definition_set=VERIFIED_DEFINITION,
            source_semantics="silver_observation_metadata_v1", coverage_policy="isolated_strict_v1",
            population_digest=canonical_hash(schedule.participants), metadata_digest="metadata")
        publisher = BusinessPublisher(state, target)
        row = store.conn.execute("SELECT id,projection_hash FROM business_obligation").fetchone()
        spec = WindowSpec(0, 60, tuple(schedule.due_tick(i) for i in range(60)), schedule.participants,
                          schedule.due_tick(59))
        claims = [Claim(tick, tick, "A", str(index), (DirectionSample("East", 10, 2, 30, 8, index),))
                  for index, tick in enumerate(spec.slots_us)]
        result = publisher.save_computed(row["id"], row["projection_hash"],
            compute_fixture_window(spec, claims, definition_set=VERIFIED_DEFINITION).encode())
        assert publisher.publish_once() == "PUBLISHED"
        reader = GoldReader(store.db_path, target)
        selection = reader.windows(run_key, 60, 0, 120)
        assert overview(selection)["kpis"]["moving_speed"]["value"] == 30
        assert historical_trend(selection, metric="occupancy_mean", grain="direction", intersection="A",
                                direction="East") == [
            dict(start=0, expected=60, valid=60, value=29.5, status="OK"),
            dict(start=60, value=None, status="OUTSIDE_RUN")]
        saved = target.rows[result]
        target.rows[result] = saved[1:]
        with pytest.raises(DivergentResult, match="incomplete"):
            reader.windows(run_key, 60, 0, 60)
        target.rows[result] = saved
        with state._tx() as db:
            db.execute("UPDATE core_window SET generation=generation+1 WHERE id='w1'")
        assert reader.windows(run_key, 60)["state"] == "NO_PUBLISHED_RESULT"


def test_experiment_qualification_does_not_infer_comparability_from_modes():
    fixed = {"control_mode": "FIXED", "sumo_seed": 42}
    adaptive = {"control_mode": "ADAPTIVE", "sumo_seed": 42}
    assert validate_pair(fixed, adaptive)["status"] == "UNKNOWN"
    assert validate_pair(fixed, dict(adaptive, sumo_seed=7))["status"] == "INCOMPATIBLE"


def test_disposable_coordinator_drains_terminal_window_and_restores_published_revision(tmp_path):
    schedule = ObservationSchedule("integrated", 1, "b" * 64, "c" * 64, "v1:sha256:" + "a" * 64,
                                  (("X", ("East",)),), 10_000, 1_000_000, 10_000, 1)
    source = FixtureSource()
    for ordinal in range(60):
        entry = source.event(schedule.event_ids(ordinal)[0], ordinal + 0.01, run=schedule.run_id)
        bundle = entry.canonical_bundle
        intersection = dict(bundle.intersection, topology_digest=schedule.topology_digest,
            manifest_identity=schedule.manifest_digest, control_mode="FIXED", incidents_json="[]")
        bundle = replace(bundle, intersection=intersection, traffic=(
            dict(source_direction="East", vehicle_count=4, waiting_vehicle_count=1, average_speed_kmh=30),))
        source.changes[-1] = replace(entry, topology_digest=schedule.topology_digest,
            manifest_identity=schedule.manifest_digest, canonical_bundle=bundle, bundle_hash=canonical_hash(bundle))
    cursor = ObservationMetadataCursor(stream="SCHEDULE")
    segments = {_schedule_fixture_receipt(schedule, cursor).before.sequence:
                _schedule_fixture_receipt(schedule, cursor)}
    cursor = segments[0].after
    for first in range(0, 60, 15):
        slots = [{"ordinal": i, "outcome": "DELIVERED", "events": [
            {"eventId": schedule.event_ids(i)[0], "disposition": "BUNDLE_VALID"}]} for i in range(first, first + 15)]
        receipt = _schedule_fixture_receipt(schedule, cursor, slots)
        segments[cursor.sequence] = replace(receipt, required_change_sequence=first + 15)
        cursor = receipt.after
    terminal = _schedule_fixture_receipt(schedule, cursor, lifecycle="ENDED", tick=60_000_000, cutoff=60_000_000)
    segments[cursor.sequence] = terminal

    def metadata(request):
        before = request.cursor
        if before.stream == "SCHEDULE" and before.sequence in segments:
            return replace(segments[before.sequence], source_binding_id=source.binding_id)
        return replace(terminal, source_binding_id=source.binding_id, before=before, after=before,
                       required_change_sequence=0, payload=None, status="EMPTY")

    source.read_observation_metadata = metadata
    coords = schedule.run_id, 1, schedule.topology_digest, schedule.manifest_digest
    target = DisposableRows()
    live_checkpoint = IsolatedBusinessStore.allocate_checkpoint(tmp_path)
    replay_checkpoint = IsolatedBusinessStore.allocate_checkpoint(tmp_path)
    replay = CoreReplayManifest(source.binding_id, (60, 0, 60, 0), *coords, 0, 60)
    retained = tmp_path / "source.sqlite3"
    with closing(sqlite3.connect(retained)) as db:
        db.execute("CREATE TABLE kafka_outbox(event_id,payload_hash,payload_json)")
        db.executemany("INSERT INTO kafka_outbox VALUES (?,?,?)", [
            (entry.event_id, entry.canonical_bundle.intersection["source_payload_hash"],
             json.dumps({"simulationTimeSeconds": entry.canonical_bundle.intersection["simulation_time_seconds"]}))
            for entry in source.changes])
        db.commit()
    live_result = None
    for checkpoint, execution in ((live_checkpoint, None), (live_checkpoint, None), (replay_checkpoint, replay)):
        namespace = execution.namespace if execution else "live"
        settings = GoldCoreSettings(checkpoint_path=checkpoint, min_free_bytes=0)
        store = IsolatedBusinessStore(checkpoint)
        lock = InstanceLock(checkpoint.with_suffix(".lock"), namespace, store, exact_path=True)
        lock.acquire(open_store=True)
        processor = StageABusinessProcessor(settings, SilverSettings(), store, lock, run=coords,
            target=target, deadline=time.monotonic()+10, session_factory=lambda _: source,
            replay=execution, archive_head="e" * 64 if execution else None)
        try:
            for _ in range(60):
                if not processor.run_cycle() and processor.drained(processor.status()):
                    break
            else:
                pytest.fail(f"Stage A fixture did not drain: {processor.status()}")
            assert processor.status()["verified_members"] == 60
            assert processor.status()["published_rows"] == 3
            assert processor.confirm_archive() == "e" * 64
            assert verify_source_lineage(retained, checkpoint, processor.run_key) == dict(
                events=60, verified_members_by_duration={60: 60})
            window = GoldReader(checkpoint, target, namespace=namespace).windows(processor.run_key, 60)["windows"][0]
            assert window["lineage"]["revision"] == 1
            if execution:
                assert processor._replay_terminal and len(target.rows) == 2
                assert window["lineage"]["result_id"] != live_result["lineage"]["result_id"]
                assert window["lineage"]["input_hash"] == live_result["lineage"]["input_hash"]
                assert window["rows"] == live_result["rows"]
            else:
                assert len(target.rows) == 1
                live_result = window
        finally:
            release(processor, lock)
