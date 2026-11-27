"""Isolated schedule clock and durable producer-prefix acceptance fixtures."""

import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from contracts.observation_schedule import ObservationSchedule, seconds_to_ticks
from contracts.observation_measurements import original_anchor
from contracts.traffic_observation import TOPIC, build_observation, canonical_json, observation_key
from de.silver.archive_resolver import ArchiveIntegrityError, ArchiveUnavailable, VerifiedRunContext
from de.silver.observation_archive import VerifiedObservationArchive
from de.silver.incremental_contract import CanonicalFeedError, CapturedBoundary
from de.silver.incremental_reader import IncrementalSilverSource
from de.silver.observation_contract import ObservationMetadataCursor, ObservationMetadataRequest
from de.tests.silver.v1_fixtures import snapshot_v1


@pytest.fixture
def outbox(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[3] / "Visualize"))
    from integration.kafka.outbox_schema import EVENT_KIND_OBSERVATION_V1
    from integration.kafka.outbox_store import OutboxIdentityConflictError, OutboxRow
    from integration.kafka.scheduled_outbox import IsolatedScheduledOutbox

    snapshot = snapshot_v1()
    participants = tuple((node, tuple(sorted(snapshot["directions"]))) for node in ("X", "Y"))
    schedule = ObservationSchedule(
        "isolated-schedule-1", 1, "a" * 64, "b" * 64, snapshot["topology_digest"],
        participants, 10_000, 1_000_000, 10_000, 1,
    )

    def frozen_rows(ordinal, *, late=False):
        tick = schedule.due_tick(ordinal)
        rows = []
        for i, (node, _) in enumerate(participants):
            snapshot.update(simulation_run_id=schedule.run_id, node_id=node,
                            simulation_time_sec=(tick + int(late)) / 1_000_000)
            event = build_observation(snapshot, cycle_sequence=schedule.cycle(ordinal),
                                      simulation_step=tick // schedule.step_ticks)
            rows.append(OutboxRow(
                event["eventId"], schedule.run_id, event["cycleSequence"], i,
                observation_key(event), TOPIC, canonical_json(event), event["payloadHash"],
                EVENT_KIND_OBSERVATION_V1,
            ))
        return tuple(rows)

    return schedule, frozen_rows, IsolatedScheduledOutbox, OutboxIdentityConflictError, tmp_path


def test_fractional_lattice_preserves_identity_through_window_edges(outbox):
    schedule, rows, *_ = outbox
    assert schedule.due_tick(59) == 59_010_000
    assert schedule.due_tick(60) == 60_010_000
    assert schedule.due_tick(299) == 299_010_000
    assert schedule.due_tick(300) == 300_010_000
    assert tuple(row.event_id for row in rows(300)) == schedule.event_ids(300)
    assert replace(schedule, period_ticks=2_000_000).schedule_id != schedule.schedule_id
    assert sha256(schedule.canonical_bytes()).hexdigest() == schedule.schedule_id
    with pytest.raises(ValueError, match="lattice"):
        replace(schedule, period_ticks=1_000_001)
    with pytest.raises(ValueError, match="clock"):
        schedule.verify_capture(60, 60.04, 6004)
    with pytest.raises(ValueError, match="clock"):
        schedule.verify_capture(60, 60.01, 6000)
    assert seconds_to_ticks("300.010000") == 300_010_000
    with pytest.raises(ValueError, match="lattice"):
        seconds_to_ticks("300.01000000000001")


def test_slot_and_frozen_rows_commit_or_roll_back_together(outbox):
    schedule, rows, store_cls, conflict, scratch = outbox
    store = store_cls.create(scratch, schedule)
    path = store.db_path
    store.set_lifecycle("RUNNING", tick=0)
    with pytest.raises(conflict, match="every scheduled participant"):
        store.append_scheduled_cycle(0, rows(0)[:1])
    with pytest.raises(conflict, match="contiguous prefix"):
        store.append_scheduled_cycle(1, rows(1))
    assert all(store.get(row.event_id) is None for row in rows(1))
    assert store.checkpoint() == -1
    with pytest.raises(ValueError, match="clock"):
        store.append_scheduled_cycle(0, rows(0, late=True))
    assert all(store.get(row.event_id) is None for row in rows(0))
    assert store.append_scheduled_cycle(0, rows(0)).inserted == 2
    store.close()

    reopened = store_cls(path, schedule)
    try:
        assert reopened.append_scheduled_cycle(0, rows(0)).reused_pending == 2
        assert tuple(reopened.get(row.event_id).payload_json for row in rows(0)) == tuple(
            row.payload_json for row in rows(0)
        )
        reopened.record_failure(1, "CLOCK_MISMATCH")
        reopened.record_failure(1, "CLOCK_MISMATCH")
        with pytest.raises(conflict, match="replace a failure"):
            reopened.append_scheduled_cycle(1, rows(1))
        assert all(reopened.get(row.event_id) is None for row in rows(1))
        assert reopened.append_scheduled_cycle(2, rows(2)).inserted == 2
        assert reopened.checkpoint() == 2
        previous = None
        for sequence, digest, segment in reopened.read_journal():
            record = json.loads(segment)
            assert record["previousDigest"] == previous
            assert record["sequence"] == sequence
            assert sha256(segment).hexdigest() == digest
            previous = digest
        assert record["defaultSuccessRange"] == [0, 2]
        assert record["sparseFailures"] == [{"ordinal": 1, "reason": "CLOCK_MISMATCH"}]
    finally:
        reopened.close()


def test_pause_resume_and_terminal_metadata_never_settle_future_slots(outbox):
    schedule, _, store_cls, conflict, scratch = outbox
    store = store_cls.create(scratch, schedule)
    try:
        store.set_lifecycle("RUNNING", tick=0)
        store.record_failure(0, "CAPTURE_FAILED")
        tick = 120_000
        store.set_lifecycle("PAUSED", tick=tick)
        with pytest.raises(conflict, match="running contiguous prefix"):
            store.record_failure(1, "MISSED_TICK")
        with pytest.raises(conflict, match="clock is inconsistent"):
            store.set_lifecycle("RUNNING", tick=tick + schedule.step_ticks)
        store.set_lifecycle("RUNNING", tick=tick)
        with pytest.raises(conflict, match="would skip"):
            store.set_lifecycle("ABORTED", tick=schedule.due_tick(1))
        store.set_lifecycle("ABORTED", tick=tick)
        terminal = json.loads(store.read_journal()[-1][2])
        assert terminal["settledOrdinal"] == 0
        assert terminal["terminalCutoffTick"] == tick
        assert terminal["defaultSuccessRange"] is None
        with pytest.raises(conflict, match="invalid schedule lifecycle"):
            store.set_lifecycle("RUNNING", tick=tick)
    finally:
        store.close()


def test_archive_head_follows_only_durable_segments_and_retries_byte_exact(outbox, monkeypatch):
    from integration.kafka import scheduled_outbox as module

    schedule, _, store_cls, conflict, scratch = outbox
    store = store_cls.create(scratch, schedule)
    archive = store.run_archive / "observation_schedule_v1"
    try:
        store.set_lifecycle("RUNNING", tick=0)
        assert store.export_checkpoint() == 2
        first_head = (archive / "head.json").read_bytes()
        store.record_failure(0, "CAPTURE_FAILED")
        assert store.checkpoint() == 0
        sequence, digest, segment = store.read_journal()[-1]
        install = module._install_archive_file

        def fail_head(path, content, *, immutable):
            assert not store._conn.in_transaction
            # The export can stall on filesystem I/O without owning the writer gate.
            assert store._write_gate._writer.acquire(blocking=False)
            store._write_gate._writer.release()
            if not immutable:
                raise OSError("interrupted after segments but before head")
            return install(path, content, immutable=immutable)

        with monkeypatch.context() as patch:
            patch.setattr(module, "_install_archive_file", fail_head)
            with pytest.raises(OSError, match="interrupted"):
                store.export_checkpoint()
        assert (archive / "head.json").read_bytes() == first_head
        assert (archive / f"schedule-{sequence}.json").read_bytes() == segment
        db_path = store.db_path
        store.close()
        store = store_cls(db_path, schedule)
        assert store.export_checkpoint() == sequence
        assert store.export_checkpoint() == sequence
        head = json.loads((archive / "head.json").read_bytes())
        assert head["segmentDigest"] == digest
        assert (archive / (schedule.schedule_id + ".json")).read_bytes() == schedule.canonical_bytes()
        (archive / f"schedule-{sequence}.json").write_bytes(b"{}")
        with pytest.raises(conflict, match="head failed validation"):
            store.export_checkpoint()
    finally:
        store.close()


def test_unmarked_outbox_refused_before_wal_or_schema_mutation(outbox):
    from integration.kafka.outbox_store import KafkaOutboxStore

    schedule, _, store_cls, conflict, scratch = outbox
    folder = scratch / "stage-a-schedule-lookalike"
    folder.mkdir()
    path = folder / "scheduled-outbox.sqlite3"
    live = KafkaOutboxStore(path)
    live.close()
    before = {p.name: p.read_bytes() for p in folder.iterdir()}
    with pytest.raises(conflict, match="application identity"):
        store_cls(path, schedule)
    assert {p.name: p.read_bytes() for p in folder.iterdir()} == before

    isolated = store_cls.create(scratch, schedule)
    try:
        with pytest.raises(conflict, match="already has a writer"):
            store_cls(isolated.db_path, schedule)
    finally:
        isolated.close()
    snapshot = {p.name: p.read_bytes() for p in isolated.db_path.parent.iterdir() if p.is_file()}
    with pytest.raises(conflict, match="another immutable schedule"):
        store_cls(isolated.db_path, replace(schedule, generation=2))
    assert {p.name: p.read_bytes() for p in isolated.db_path.parent.iterdir() if p.is_file()} == snapshot


def test_measurement_linkage_revision_withdrawal_and_verified_prefix(outbox):
    schedule, rows, store_cls, conflict, scratch = outbox
    store = store_cls.create(scratch, schedule)
    first = rows(0)[0]
    event = json.loads(first.payload_json)
    directions = dict(schedule.participants)
    context = VerifiedRunContext(schedule.run_id, schedule.generation, schedule.topology_digest,
        schedule.manifest_digest, tuple(directions), directions, {"config_hash": schedule.config_digest},
        {}, "c" * 64, "d" * 64, "RUNNING", None)
    anchor = original_anchor(event, context.generation, context.manifest_identity)
    base = {"directions": {"East": {"occupancy": {"method": "lane_percent_v2", "percent": [0.8, 1.2]}}},
            "spillback": None}
    corrected = {"directions": {"East": {"occupancy": {"method": "lane_percent_v2", "percent": [0.8, 1.6]}}},
                 "spillback": None}

    def load_anchor(event_id):
        assert event_id == first.event_id
        return anchor, directions[event["intersectionId"]]

    try:
        store.set_lifecycle("RUNNING", tick=0)
        with pytest.raises(conflict, match="contiguous prefix"):
            store.append_scheduled_cycle(1, rows(1), measurement_evidence={rows(1)[0].event_id: base})
        assert store._conn.execute("SELECT count(*) FROM observation_measurement_revision").fetchone()[0] == 0
        store.append_scheduled_cycle(0, rows(0), measurement_evidence={first.event_id: base})
        with pytest.raises(conflict, match="settled measurement"):
            store.append_scheduled_cycle(0, rows(0), measurement_evidence={first.event_id: corrected})
        assert store.get(first.event_id).payload_json == first.payload_json
        assert store.checkpoint() == 0
        store.export_checkpoint()
        archive = VerifiedObservationArchive(store.db_path.parent, context)
        initial_index, original, record = archive.measurement(1, None, load_anchor=load_anchor)
        occupancy = record["measurements"]["directions"]["East"]["occupancy"]
        assert occupancy["value"] == 1.0
        assert record["anchor"]["payloadHash"] == first.payload_hash
        assert archive.measurement(2, initial_index, load_anchor=load_anchor) is None
        assert sha256((archive.directory / (original + ".json")).read_bytes()).hexdigest() == original

        previous = (None, "REGISTERED", 0, -1)
        for sequence in range(1, 4):
            segment_hash, segment = archive.schedule_segment(sequence, previous)
            previous = (segment_hash, segment["lifecycle"], segment["lifecycleTick"], segment["settledOrdinal"])
        assert previous[3] == 0

        replacement = store.correct_measurement(first.event_id, previous_digest=original, evidence=corrected)
        assert store.correct_measurement(first.event_id, previous_digest=original, evidence=corrected) == replacement
        withdrawal = store.correct_measurement(first.event_id, previous_digest=replacement, evidence=None)
        with pytest.raises(conflict, match="stale or withdrawn"):
            store.correct_measurement(first.event_id, previous_digest=original, evidence=base)
        store.export_checkpoint()
        with pytest.raises(ArchiveUnavailable, match="advanced"):
            archive.revalidate()
        archive = VerifiedObservationArchive(store.db_path.parent, context)
        next_index, observed_replacement, record = archive.measurement(2, initial_index, load_anchor=load_anchor)
        assert observed_replacement == replacement and record["previousDigest"] == original
        result = record["measurements"]["directions"]["East"]["occupancy"]
        assert result["value"] == 1.2 and result["measurementId"] == occupancy["measurementId"]
        _, observed_withdrawal, record = archive.measurement(3, next_index, load_anchor=load_anchor)
        assert observed_withdrawal == withdrawal and record["measurements"] is None
        with pytest.raises(ArchiveIntegrityError, match="verified original"):
            archive.measurement(3, next_index,
                load_anchor=lambda _: ({**anchor, "payloadHash": "0" * 64}, directions[event["intersectionId"]]))
        (archive.directory / (replacement + ".json")).write_bytes(b"{}")
        with pytest.raises(ArchiveIntegrityError, match="differs from its index"):
            archive.measurement(2, initial_index, load_anchor=load_anchor)
    finally:
        store.close()


def test_public_silver_receipts_block_delayed_observations_and_bind_original_evidence(outbox):
    schedule, rows, store_cls, _, scratch = outbox
    store = store_cls.create(scratch, schedule)
    run_id = schedule.run_id
    directions = dict(schedule.participants)
    context = VerifiedRunContext(run_id, 1, schedule.topology_digest, schedule.manifest_digest,
        tuple(directions), directions, {"config_hash": schedule.config_digest}, {}, "c" * 64,
        "d" * 64, "RUNNING", None)
    events = {}
    identities = {}

    class Reader(IncrementalSilverSource):
        archive_resolver = SimpleNamespace(root=store.db_path.parent, resolve=lambda *_: context)
        bronze = SimpleNamespace(current=lambda _, eid: identities.get(eid))
        bronze_repo = SimpleNamespace(read_valid_observation=lambda _, eid, __, **kw:
            {"event": events[eid], "change_sequence": identities[eid]["change_sequence"]})
        bronze_path = store.db_path.parent / "canonical.sqlite3"

        def _capture_feed(self, namespace, expected, **_):
            assert namespace == "live" and expected == "fixture-binding"
            return expected, {}, CapturedBoundary(captured_bronze_change_high_water=9,
                captured_silver_change_applied_sequence=9, captured_bronze_invalidation_high_water=2,
                captured_silver_invalidation_applied_sequence=2)

        def _read_change_entry(self, ns, sequence, boundary, *, original_out):
            identity = next(row for row in identities.values() if row["change_sequence"] == sequence)
            conflict = identity["state"] == "CONFLICTED"
            if not conflict:
                original_out["accepted"] = self.bronze_repo.read_valid_observation(ns, identity["event_id"], 1)
            return SimpleNamespace(event_id=identity["event_id"], run_id=run_id, run_generation=1,
                topology_digest=schedule.topology_digest, manifest_identity=schedule.manifest_digest,
                effective_disposition="CONFLICTED" if conflict else "BUNDLE_VALID", semantic_revision=2 if conflict else 1,
                invalidation_sequence=2 if conflict else None, canonical_bundle=SimpleNamespace(
                    intersection={"source_payload_hash": identity["payload_hash"]})), (None, identity)

        def _check_feed_end(self, namespace, binding, boundary, proofs):
            assert all(identities[eid] == proof for eid, proof in proofs)

    reader = Reader()

    def read(cursor, **kwargs):
        return reader.read_observation_metadata(ObservationMetadataRequest(
            source_binding_id="fixture-binding", run_id=run_id, run_generation=1,
            topology_digest=schedule.topology_digest, cursor=cursor, **kwargs))

    def arrive(row, seq):
        event = json.loads(row.payload_json)
        events[row.event_id] = event
        identities[row.event_id] = {"state": "ACCEPTED", "event_id": row.event_id, "change_sequence": seq,
            "intersection_id": event["intersectionId"], "cycle_sequence": event["cycleSequence"],
            "simulation_time_seconds": event["simulationTimeSeconds"], "payload_hash": row.payload_hash}

    first, second = rows(0)
    evidence = {"directions": {"East": {"occupancy": {"method": "lane_percent_v2", "percent": [0.8]}}},
                "spillback": None}
    try:
        store.set_lifecycle("RUNNING", tick=0)
        store.append_scheduled_cycle(0, (first, second), measurement_evidence={first.event_id: evidence})
        store.record_failure(1, "MISSED_TICK")
        assert store.checkpoint() == 1
        store.export_checkpoint()
        genesis = read(ObservationMetadataCursor(stream="SCHEDULE"))
        started = read(genesis.after)
        assert genesis.payload["lifecycle"] == "REGISTERED" and started.payload["lifecycle"] == "RUNNING"
        arrive(first, 1)
        waiting = read(started.after)
        assert waiting.status == "WAITING" and waiting.after == started.after
        arrive(second, 3)
        partial = read(started.after, max_slots=1)
        assert partial.after.sequence == started.after.sequence and partial.after.settled_ordinal == 0
        assert partial.required_change_sequence == 3 and partial.payload["slots"][0]["outcome"] == "DELIVERED"
        finished = read(partial.after)
        assert finished.required_change_sequence == 0 and finished.payload["slots"][0]["outcome"] == "MISSED_TICK"
        assert read(finished.after).status == "EMPTY"
        measurement = read(ObservationMetadataCursor(stream="MEASUREMENT"))
        assert measurement.required_change_sequence == 1
        assert measurement.payload["measurements"]["directions"]["East"]["occupancy"]["value"] == 0.8
        identities[first.event_id] = {**identities[first.event_id], "state": "CONFLICTED", "change_sequence": 9}
        suppressed = read(measurement.before)
        assert (suppressed.required_change_sequence, suppressed.required_invalidation_sequence) == (9, 2)
        assert suppressed.payload["measurements"] is None
        with pytest.raises(CanonicalFeedError, match="rewound or replaced"):
            read(replace(measurement.after, digest="f" * 64))
    finally:
        store.close()


def test_public_measurement_reuses_verified_bronze_read_and_rejects_changed_silver_fact(outbox):
    from de.bronze.checkpoint_store import V1SemanticStore
    from de.silver.canonical_reader import CanonicalSilverReader
    from de.silver.checkpoint_store import SilverCheckpointStore
    from de.silver.fact_builders import build_v1_fact_rows
    from de.silver.incremental_contract import SourceDiscoveryRequest
    from de.silver.source_binding import verify_bronze_source

    schedule, rows, store_cls, _, scratch = outbox
    store = store_cls.create(scratch, schedule)
    bronze = V1SemanticStore(scratch / "bronze.sqlite3")
    silver_path = scratch / "silver.sqlite3"
    silver = SilverCheckpointStore(silver_path)
    silver.open()
    try:
        first = rows(0)[0]
        event = json.loads(first.payload_json)
        directions = dict(schedule.participants)
        context = VerifiedRunContext(schedule.run_id, 1, schedule.topology_digest, schedule.manifest_digest,
            tuple(directions), directions, {"config_hash": schedule.config_digest}, {}, "b" * 64,
            "c" * 64, "RUNNING", None)
        store.set_lifecycle("RUNNING", tick=0)
        evidence = {"directions": {"East": {"occupancy": {"method": "lane_percent_v2", "percent": [0.8]}}},
                    "spillback": None}
        store.append_scheduled_cycle(0, rows(0), measurement_evidence={first.event_id: evidence})
        store.export_checkpoint()

        transport = dict(raw_ingestion_id="raw", topic=TOPIC, topic_identity="fixture", partition=0, offset=0)
        intent = bronze.prepare("live", transport, event, "envelope", "d" * 64, lambda *_: [])
        bronze.complete("live", "raw", intent["expected_hash"])
        silver.bind_v1_source("live", verify_bronze_source(bronze.path, "live"))
        provenance = dict(bronze_semantic_revision=1, bronze_canonical_hash="d" * 64,
            source_raw_ingestion_id="raw", source_topic=TOPIC, source_partition=0, source_offset=0,
            source_physical_record_hash="e" * 64, source_byte_hash="f" * 64)
        facts = build_v1_fact_rows(event, context, provenance)
        physical = {f"silver_{name}_fact_v1": facts[key] for name, key in (
            ("intersection_state", "intersection"), ("traffic_observation", "traffic"), ("signal_state", "signal"))}
        marker = dict(namespace="live", event_id=first.event_id, simulation_run_id=schedule.run_id,
            topology_digest=schedule.topology_digest, semantic_revision=1, bronze_change_sequence=1,
            invalidation_id=None, validity_state="VALID", reason="ACCEPTED")
        obligations = [(table, row) for table, records in physical.items() for row in records]
        silver.freeze_v1_bundle("live", "changes", 1, first.event_id, 1,
                               obligations + [("silver_event_validity_v1", marker)])
        for item in silver.pending_v1_obligations("live"):
            silver.reconcile_v1_obligation("live", "changes", 1, item["target_table"], item["expected_hash"])
        silver.publish_v1_valid("live", first.event_id, 1, 1)
        silver.advance_v1_cursor("live", "changes", 1, 0)
        calls = []

        def bronze_fetch(*args, **kwargs):
            calls.append(args)
            return {"event": event, "change_sequence": 1}

        with CanonicalSilverReader(bronze.path, silver_path,
                SimpleNamespace(fetch_v1_event_rows=lambda table, *_: physical[table]),
                SimpleNamespace(read_valid_observation=bronze_fetch),
                SimpleNamespace(root=store.db_path.parent, resolve=lambda *_: context)) as reader:
            binding = reader.discover_canonical_source(SourceDiscoveryRequest("live")).source_binding_id
            request = ObservationMetadataRequest(source_binding_id=binding, run_id=schedule.run_id,
                run_generation=1, topology_digest=schedule.topology_digest,
                cursor=ObservationMetadataCursor(stream="MEASUREMENT"))
            receipt = reader.read_observation_metadata(request)
            assert len(calls) == 1
            assert receipt.payload["anchor"]["payloadHash"] == event["payloadHash"]
            assert receipt.payload["measurements"]["directions"]["East"]["occupancy"]["value"] == 0.8
            assert physical["silver_traffic_observation_fact_v1"][0]["occupancy_pct"] == 0.0
            physical["silver_traffic_observation_fact_v1"][0]["occupancy_pct"] = 12.0
            with pytest.raises(CanonicalFeedError, match="same-key content differs") as failure:
                reader.read_observation_metadata(request)
            assert failure.value.code == "SOURCE_INTEGRITY_ERROR" and not failure.value.cursor_advance
    finally:
        silver.close()
        bronze.close()
        store.close()
