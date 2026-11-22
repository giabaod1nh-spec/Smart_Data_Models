import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from de.tests.silver.v1_fixtures import snapshot_v1 as _snapshot
from contracts.traffic_observation import build_observation
from de.bronze.checkpoint_store import V1SemanticStore
from de.silver.archive_resolver import VerifiedRunContext
from de.silver.canonical_reader import (CanonicalReadQuery, CanonicalSilverReader,
    SnapshotStaleError, SourceLagError, SourceChurnError, AuthorityUnavailableError,
    CanonicalContentConflict, ValidityUnknownError)
from de.silver.canonical_reader import SourceBindingChangedError
from de.silver.canonical_reader import InvalidationLagError
from de.silver.repositories import FactBusinessKeyConflictError
import de.silver.canonical_reader as canonical_module
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.source_binding import verify_bronze_source


@pytest.mark.parametrize('change_cursor,invalidation_cursor,error', [
    (1, 1, SourceLagError), (2, 0, InvalidationLagError),
    (1, 0, SourceLagError), (2, 1, None),
])
def test_reader_gates_each_applied_cursor_before_any_fact_query(monkeypatch, change_cursor,
                                                              invalidation_cursor, error):
    reader = object.__new__(CanonicalSilverReader)
    source = dict(change_high_water=2, invalidation_high_water=1)
    applied = dict(change_cursor=change_cursor, invalidation_cursor=invalidation_cursor)
    monkeypatch.setattr(reader, '_bound_source', lambda _: (source, applied))
    if error is not None:
        with pytest.raises(error) as result:
            reader.read_valid_page(CanonicalReadQuery('live', 'r', 'digest', 0, 10))
        assert result.value.retryable
        assert result.value.code == ('SOURCE_LAG' if error is SourceLagError else 'INVALIDATION_LAG')
    else:
        assert reader._boundary('live') == (source, applied)


@pytest.mark.parametrize('error', [AuthorityUnavailableError, ValidityUnknownError])
def test_complete_scan_stops_after_four_transient_attempts(monkeypatch, error):
    reader = object.__new__(CanonicalSilverReader)
    attempts = []
    def fail(*args):
        attempts.append(args)
        raise error('temporarily unavailable')
    monkeypatch.setattr(reader, 'read_valid_page', fail)
    monkeypatch.setattr(canonical_module.time, 'sleep', lambda _: None)
    with pytest.raises(error):
        reader.read_complete_scan(CanonicalReadQuery('live', 'run', 'digest', 0, 10))
    assert len(attempts) == 4


def test_complete_scan_discards_pages_before_stale_restart(monkeypatch):
    reader = object.__new__(CanonicalSilverReader)
    keys = []
    def page(query, key, limit, token):
        keys.append(key)
        if len(keys) == 1:
            return SimpleNamespace(bundles=('stale-page',), has_more=True,
                                   next_key=('run', 'digest', 'e'), token='old')
        if len(keys) == 2:
            raise SnapshotStaleError('boundary advanced')
        return SimpleNamespace(bundles=('fresh-result',), has_more=False)
    monkeypatch.setattr(reader, 'read_valid_page', page)
    monkeypatch.setattr(canonical_module.time, 'sleep', lambda _: None)
    result = reader.read_complete_scan(CanonicalReadQuery('live', 'run', 'digest', 0, 10))
    assert result == ('fresh-result',)
    assert keys == [None, ('run', 'digest', 'e'), None]


def test_missing_authority_is_typed_and_does_not_create_files(tmp_path):
    bronze = tmp_path / 'missing-bronze.sqlite3'
    silver = tmp_path / 'missing-silver.sqlite3'
    with pytest.raises(AuthorityUnavailableError):
        CanonicalSilverReader(bronze, silver, None, None, None)
    assert not bronze.exists() and not silver.exists()


def test_whole_event_pages_and_stale_authority_token(tmp_path, monkeypatch):
    bronze_path = tmp_path / "bronze_v1" / "state.sqlite3"
    store = V1SemanticStore(bronze_path)
    events = {}

    def publish(cycle):
        snapshot = _snapshot()
        event = build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 100)
        raw = dict(raw_ingestion_id=f"raw-{cycle}", topic="traffic.observations.v1",
                   partition=0, offset=cycle, topic_identity="topic")
        intent = store.prepare("live", raw, event, "envelope", "content", lambda *args: [])
        store.complete("live", raw["raw_ingestion_id"], intent["expected_hash"])
        events[event["eventId"]] = event
        return event

    publish(4)
    publish(5)
    silver_path = tmp_path / "silver_v1" / "state.sqlite3"
    silver = SilverCheckpointStore(silver_path)
    silver.open()
    silver.bind_v1_source("live", verify_bronze_source(bronze_path, "live"))
    facts = {}
    directions = tuple(_snapshot()["directions"])
    for event in events.values():
        sequence = store.current("live", event["eventId"])["change_sequence"]
        rows = []
        for family, table in (("traffic", "silver_traffic_observation_fact_v1"),
                              ("signal", "silver_signal_state_fact_v1"),
                              ("intersection", "silver_intersection_state_fact_v1")):
            for direction in (directions if family != "intersection" else (None,)):
                row = dict(event_id=event["eventId"], simulation_run_id=event["simulationRunId"],
                    topology_digest=event["topologyDigest"], simulation_time_seconds=event["simulationTimeSeconds"])
                if direction:
                    row["source_direction"] = direction
                rows.append((table, row))
                facts.setdefault((table, event["eventId"]), []).append(row)
        silver.freeze_v1_bundle("live", "changes", sequence, event["eventId"], 1, rows)
        for obligation in silver.pending_v1_obligations("live"):
            silver.reconcile_v1_obligation("live", "changes", sequence,
                obligation["target_table"], obligation["expected_hash"])
        silver.publish_v1_valid("live", event["eventId"], 1, sequence)
        silver.advance_v1_cursor("live", "changes", sequence, sequence - 1)

    class SilverRepo:
        def fetch_v1_event_rows(self, table, run, digest, event_id):
            return facts[(table, event_id)]

        def reconcile_v1_row(self, table, row):
            return row in facts[(table, row["event_id"])]

    context = VerifiedRunContext("run-contract", 1, next(iter(events.values()))["topologyDigest"],
        "a" * 64, ("A",), {"A": directions}, {}, {}, "b" * 64, "c" * 64, "RUNNING", "t1")
    bronze_repo = SimpleNamespace(read_valid_observation=lambda ns, eid, rev, **kwargs:
                                  dict(event=events[eid]))
    archive = SimpleNamespace(resolve=lambda *args: context)
    query = CanonicalReadQuery("live", "run-contract", context.topology_digest, 0, 100)
    try:
        with CanonicalSilverReader(bronze_path, silver_path, SilverRepo(), bronze_repo, archive) as reader:
            for invalid_limit in (0, 101, True, 1.5):
                with pytest.raises(ValueError):
                    reader.read_valid_page(query, limit=invalid_limit)
            first = reader.read_valid_page(query, limit=1)
            assert len(first.bundles) == 1 and first.has_more and len(first.bundles[0].traffic) == len(directions)
            claims = reader._decode(first.token)
            for name, expected in (("reader_version", 1), ("schema_version", "008"),
                                   ("ordering_key_version", 1)):
                assert claims[name] == expected
                changed = dict(claims, **{name: "unsupported"})
                with pytest.raises(SnapshotStaleError):
                    reader.read_valid_page(query, first.next_key, 1, reader._token(changed))
                missing = dict(claims)
                del missing[name]
                with pytest.raises(SnapshotStaleError):
                    reader.read_valid_page(query, first.next_key, 1, reader._token(missing))
            second = reader.read_valid_page(query, first.next_key, 1, first.token)
            assert len(second.bundles) == 1 and not second.has_more
            with pytest.raises(SnapshotStaleError):
                reader.read_valid_page(query, first.next_key, 2, first.token)
            for changed_query in (
                CanonicalReadQuery('live', query.run_id, query.topology_digest, 1, 100),
                CanonicalReadQuery('live', query.run_id, query.topology_digest, 0, 101),
            ):
                with pytest.raises(SnapshotStaleError):
                    reader.read_valid_page(changed_query, first.next_key, 1, first.token)
            with pytest.raises(SnapshotStaleError):
                reader.read_valid_page(query, (query.run_id, query.topology_digest, 'different-key'), 1, first.token)
            all_events = reader.read_valid_page(query, limit=100)
            assert len(all_events.bundles) == 2 and not all_events.has_more
            assert len(reader.read_complete_scan(query, page_size=1)) == 2
            store.db.execute('UPDATE v1_sequence SET next_change=2 WHERE namespace=?', ('live',))
            with pytest.raises(SourceBindingChangedError):
                reader.read_valid_page(query, limit=1)
            store.db.execute('UPDATE v1_sequence SET next_change=3 WHERE namespace=?', ('live',))
            for invalid_token in ('not-json', reader._token([]), reader._token(None)):
                with pytest.raises(SnapshotStaleError):
                    reader.read_valid_page(query, first.next_key, 1, invalid_token)
            with pytest.raises(SnapshotStaleError):
                reader.read_valid_page(query, ('wrong-run', query.topology_digest, 'e'), 1, first.token)
            original_bundle = reader._bundle
            for fault, expected in ((ConnectionError('CH offline'), AuthorityUnavailableError),
                                    (FactBusinessKeyConflictError('conflicting row'), CanonicalContentConflict)):
                def fail(*args, fault=fault):
                    raise fault
                monkeypatch.setattr(reader, '_bundle', fail)
                with pytest.raises(expected):
                    reader.read_valid_page(query, limit=1)
            monkeypatch.setattr(reader, '_bundle', original_bundle)
            publish(6)
            with pytest.raises(SnapshotStaleError):
                reader.read_valid_page(query, first.next_key, 1, first.token)
            with pytest.raises(SourceLagError):
                reader.read_valid_page(query, limit=1)
            monkeypatch.setattr(canonical_module.time, "sleep", lambda _: None)
            monkeypatch.setattr(reader, "read_valid_page", lambda *args, **kwargs:
                                (_ for _ in ()).throw(SnapshotStaleError("changed")))
            with pytest.raises(SourceChurnError):
                reader.read_complete_scan(query, page_size=1)
    finally:
        silver.close()
        store.close()
