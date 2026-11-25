import json
from dataclasses import replace
import pytest
from pathlib import Path
from types import SimpleNamespace

from de.tests.silver.v1_fixtures import snapshot_v1 as _snapshot
from contracts.traffic_observation import build_observation
from de.silver.archive_resolver import ArchiveIntegrityError, VerifiedRunContext
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.metrics import Metrics
from de.silver.processor import SilverProcessor
from de.silver.source_binding import SourceVerificationError
import de.silver.processor as processor_module


@pytest.mark.parametrize('sequences', [[1, 3], [1, 2], [2, 3], []])
def test_gapped_page_is_rejected_before_any_materialization(monkeypatch, sequences):
    monkeypatch.setattr(processor_module, 'verify_bronze_source', lambda *args, **kwargs:
        dict(namespace='live', source_path='bronze', schema_fingerprint='s', bronze_version='v1',
             file_identity=[1, 2], change_high_water=3, invalidation_high_water=0))
    checkpoint = SimpleNamespace(bind_v1_source=lambda *args: None,
        v1_cursors=lambda *args: dict(change_cursor=0, invalidation_cursor=0),
        pending_v1_obligations=lambda *args: [])
    worker = SimpleNamespace(metrics=Metrics(), settings=SimpleNamespace(namespace='live', v1_source_namespace='live',
        v1_bronze_path='bronze', v1_replay_run_filter=''), checkpoint=checkpoint,
        process_v1_invalidation=lambda *args: pytest.fail('invalid page was consumed'),
        process_v1_accepted_change=lambda *args: pytest.fail('invalid page was consumed'),
        process_v1_conflicted_change=lambda *args: pytest.fail('invalid page was consumed'))
    reader = SimpleNamespace(read_page=lambda *args, **kwargs: dict(rows=[
        dict(change_sequence=value, change_type='ACCEPTED') for value in sequences]))
    with pytest.raises(SourceVerificationError) as result:
        SilverProcessor.run_v1_journal_pass(worker, reader, None, None, None)
    assert result.value.code == 'CURSOR_GAP'


@pytest.mark.parametrize('topology_case', ['valid', 'missing', 'unexpected', 'digest', 'nonparticipant'])
def test_accepted_journal_entry_materializes_complete_bundle(tmp_path, topology_case):
    snapshot = _snapshot()
    event = build_observation(snapshot, cycle_sequence=4, simulation_step=1200)
    directions = tuple(snapshot["directions"])
    context = VerifiedRunContext(event["simulationRunId"], 1, event["topologyDigest"],
        "a" * 64, ("A",), {"A": directions}, {"simulationRunId": event["simulationRunId"]},
        {"node_to_tls": {"A": "tls", "B": "tls-b"},
         "approach_edges": {"tls": {key: [] for key in directions}, "tls-b": {"North": []}}},
        "b" * 64, "c" * 64, "RUNNING", "t1")
    if topology_case == 'missing':
        context = replace(context, directions={'A': directions + ('North',)})
    elif topology_case == 'unexpected':
        context = replace(context, directions={'A': directions[:-1]})
    elif topology_case == 'digest':
        context = replace(context, topology_digest='v1:sha256:' + '9' * 64)
    elif topology_case == 'nonparticipant':
        context = replace(context, participants=('B',), directions={'B': ('North',)})
    delivery = dict(raw_ingestion_id="raw", topic="traffic.observations.v1",
        partition=0, offset=4, physical_record_hash="d" * 64, source_byte_hash="e" * 64)
    current = dict(event=event, semantic_revision=1, bronze_canonical_hash="f" * 64,
        primary_delivery={"expected_json": json.dumps([
            {"table": "bronze_delivery_dispositions_v1", "row": delivery}])})
    bronze = SimpleNamespace(read_valid_observation=lambda *args, **kwargs: current)
    archive = SimpleNamespace(resolve=lambda *args: context)
    registered = []
    lifecycle = SimpleNamespace(register_verified=registered.append)
    checkpoint = SilverCheckpointStore(Path(tmp_path) / "silver.sqlite3")
    checkpoint.open()
    checkpoint.bind_v1_source("live", dict(namespace="live", source_path="bronze",
        schema_fingerprint="schema", bronze_version="v1", file_identity=[1, 2]))

    class Repo:
        def __init__(self):
            self.rows = {}

        def reconcile_v1_row(self, table, row, **kwargs):
            return (table, json.dumps(row, sort_keys=True)) in self.rows

        def insert_v1_rows(self, table, rows, **kwargs):
            for row in rows:
                self.rows[table, json.dumps(row, sort_keys=True)] = row

    repo = Repo()
    worker = SimpleNamespace(metrics=Metrics(), settings=SimpleNamespace(namespace="live", v1_bronze_path="bronze",
        v1_source_namespace="live", v1_archive_root="archive", destination_mode="main", replay_run_id=""),
        checkpoint=checkpoint, repo=repo)
    worker.recover_v1_obligations = lambda limit: SilverProcessor.recover_v1_obligations(worker, limit)
    try:
        if topology_case != 'valid':
            with pytest.raises(ArchiveIntegrityError) as error:
                SilverProcessor.process_v1_accepted_change(worker, dict(change_type='ACCEPTED',
                    change_sequence=1, event_id=event['eventId'], revision=1), bronze, archive, lifecycle)
            if topology_case == 'missing':
                assert "missing=['North'] unexpected=[]" in str(error.value)
            elif topology_case == 'unexpected':
                assert f"missing=[] unexpected=['{directions[-1]}']" in str(error.value)
            assert repo.rows == {}
            assert checkpoint.v1_cursors('live')['change_cursor'] == 0
            assert checkpoint.conn.execute('SELECT count(*) FROM silver_v1_event_validity').fetchone()[0] == 0
            assert checkpoint.pending_v1_obligations('live') == []
            assert registered == []
            return
        SilverProcessor.process_v1_accepted_change(worker, dict(change_type="ACCEPTED",
            change_sequence=1, event_id=event["eventId"], revision=1), bronze, archive, lifecycle)
        assert checkpoint.v1_cursors("live")["change_cursor"] == 1
        assert checkpoint.conn.execute("SELECT state FROM silver_v1_event_validity").fetchone()[0] == "VALID"
        assert len(repo.rows) == 2 * len(directions) + 1 + 3 + 1
        topology = next(row for (table, _), row in repo.rows.items()
                        if table == "silver_topology_dimension_v1")
        assert json.loads(topology["intersection_ids_json"]) == ["A", "B"]
        assert json.loads(topology["approaches_by_intersection_json"])["B"] == ["North"]
        traffic = [row for (table, _), row in repo.rows.items()
                   if table == "silver_traffic_observation_fact_v1"]
        assert {row["intersection_id"] for row in traffic} == {"A"}
        assert {row["source_direction"] for row in traffic} == set(directions)
        assert registered == [context]
        # Journal order is authoritative even when simulation cycles move back.
        expected_ids = {event['eventId']}
        for sequence, cycle in ((2, 9), (3, 2)):
            late = build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 100)
            current['event'] = late
            expected_ids.add(late['eventId'])
            SilverProcessor.process_v1_accepted_change(worker, dict(change_type='ACCEPTED',
                change_sequence=sequence, event_id=late['eventId'], revision=1), bronze, archive, lifecycle)
            assert checkpoint.v1_cursors('live')['change_cursor'] == sequence
        validity = checkpoint.conn.execute('SELECT event_id,state,change_sequence '
                                           'FROM silver_v1_event_validity').fetchall()
        assert {row[0] for row in validity} == expected_ids
        assert all(row[1] == 'VALID' for row in validity)
        facts = [row for (table, _), row in repo.rows.items()
                 if table == 'silver_intersection_state_fact_v1']
        assert {row['event_id'] for row in facts} == expected_ids
        assert {row['cycle_sequence'] for row in facts} == {4, 9, 2}
    finally:
        checkpoint.close()


def test_invalidation_suppresses_before_cursor_ack(tmp_path):
    checkpoint = SilverCheckpointStore(Path(tmp_path) / "silver.sqlite3")
    checkpoint.open()
    checkpoint.bind_v1_source("live", dict(namespace="live", source_path="bronze",
        schema_fingerprint="schema", bronze_version="v1", file_identity=[1, 2]))
    entry = dict(invalidation_id="i" * 64, invalidation_sequence=1,
        event_id="e" * 64, revision=2, previous_hash="a" * 64,
        conflicting_hash="b" * 64, first_raw_id="c" * 64,
        conflicting_raw_id="d" * 64)
    provenance = dict(entry, simulation_run_id="r", topology_digest="digest",
        intersection_id="A", cycle_sequence=4, simulation_time_seconds=12.0,
        change_sequence=2, original_delivery={"raw": "c"},
        conflicting_delivery={"raw": "d"})
    source = SimpleNamespace(read_invalidation_provenance=lambda *args: provenance)
    archive = SimpleNamespace(resolve=lambda *args: SimpleNamespace(generation=1))

    class Repo:
        def __init__(self):
            self.rows = {}

        def reconcile_v1_row(self, table, row, **kwargs):
            return (table, json.dumps(row, sort_keys=True)) in self.rows

        def insert_v1_rows(self, table, rows, **kwargs):
            assert checkpoint.conn.execute("SELECT state FROM silver_v1_event_validity").fetchone()[0] == "INVALIDATED"
            for row in rows:
                self.rows[table, json.dumps(row, sort_keys=True)] = row

    repo = Repo()
    worker = SimpleNamespace(metrics=Metrics(), settings=SimpleNamespace(namespace="live", destination_mode="main",
        v1_source_namespace="live", replay_run_id=""), checkpoint=checkpoint, repo=repo)
    worker.recover_v1_obligations = lambda limit: SilverProcessor.recover_v1_obligations(worker, limit)
    try:
        SilverProcessor.process_v1_invalidation(worker, entry, source, archive)
        assert checkpoint.v1_cursors("live")["invalidation_cursor"] == 1
        assert len(repo.rows) == 1
    finally:
        checkpoint.close()


@pytest.mark.parametrize('max_entries,expected_calls', [
    (None, ['invalidations', 'accepted', 'conflicted']),
    (1, ['invalidations']), (2, ['invalidations', 'accepted']),
])
def test_journal_pass_handles_invalidation_before_changes(monkeypatch, max_entries, expected_calls):
    monkeypatch.setattr(processor_module, "verify_bronze_source", lambda *args, **kwargs:
        dict(namespace="live", source_path="bronze", schema_fingerprint="s",
             bronze_version="v1", file_identity=[1, 2],
             change_high_water=2, invalidation_high_water=1))
    calls = []
    cursors = {"change_cursor": 0, "invalidation_cursor": 0}
    checkpoint = SimpleNamespace(bind_v1_source=lambda *args: None,
        pending_v1_obligations=lambda *args: [], v1_cursors=lambda *args: cursors)

    def page(namespace, kind, after, limit, high_water):
        if kind == "invalidations":
            return {"rows": [{"invalidation_sequence": 1}]}
        return {"rows": [{"change_sequence": 1, "change_type": "ACCEPTED"},
                         {"change_sequence": 2, "change_type": "CONFLICTED"}]}

    def mark(kind, entry, *args):
        calls.append(kind)
        cursors["invalidation_cursor" if kind == "invalidations" else "change_cursor"] = (
            entry["invalidation_sequence"] if kind == "invalidations" else entry["change_sequence"])

    worker = SimpleNamespace(metrics=Metrics(), settings=SimpleNamespace(namespace="live", v1_bronze_path="bronze",
        v1_source_namespace="live", v1_replay_run_filter=""),
        checkpoint=checkpoint, recover_v1_obligations=lambda limit: 0,
        process_v1_invalidation=lambda entry, *args: mark("invalidations", entry),
        process_v1_accepted_change=lambda entry, *args: mark("accepted", entry),
        process_v1_conflicted_change=lambda entry, *args: mark("conflicted", entry))
    reader = SimpleNamespace(read_page=page)
    assert SilverProcessor.run_v1_journal_pass(worker, reader, None, None, None,
                                              max_entries=max_entries) == len(expected_calls)
    assert calls == expected_calls


def test_replay_filters_other_run_with_durable_skip(tmp_path, monkeypatch):
    monkeypatch.setattr(processor_module, "verify_bronze_source", lambda *args, **kwargs:
        dict(namespace="live", source_path="bronze", schema_fingerprint="s",
             bronze_version="v1", file_identity=[1, 2],
             change_high_water=1, invalidation_high_water=0))
    checkpoint = SilverCheckpointStore(Path(tmp_path) / "replay.sqlite3")
    checkpoint.open()
    worker = SimpleNamespace(metrics=Metrics(), settings=SimpleNamespace(namespace="replay:job",
        v1_source_namespace="live", v1_bronze_path="bronze",
        v1_replay_run_filter="target"), checkpoint=checkpoint,
        recover_v1_obligations=lambda limit: 0,
        process_v1_invalidation=lambda *args: pytest.fail("wrong run"),
        process_v1_accepted_change=lambda *args: pytest.fail("wrong run"),
        process_v1_conflicted_change=lambda *args: pytest.fail("wrong run"))
    source = SimpleNamespace(current=lambda *args: {"simulation_run_id": "other"},
        read_page=lambda *args, **kwargs: {"rows": [{"event_id": "e",
            "change_sequence": 1, "change_type": "ACCEPTED"}]})
    try:
        assert SilverProcessor.run_v1_journal_pass(worker, source, None, None, None) == 1
        assert checkpoint.v1_cursors("replay:job")["change_cursor"] == 1
        assert checkpoint.conn.execute("SELECT target_table FROM silver_v1_obligation").fetchone()[0] == "__LOCAL_SKIP__"
    finally:
        checkpoint.close()
