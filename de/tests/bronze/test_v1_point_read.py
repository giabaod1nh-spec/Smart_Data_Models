import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from contracts.traffic_observation import canonical_json
from de.bronze.checkpoint_store import V1SemanticStore
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.kafka_raw.v1_ledger_store import IntegrityFault


def test_exact_current_accepted_point_read_and_fail_closed(tmp_path):
    event = json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    content_hash = hashlib.sha256(canonical_json(dict(
        observation=event, normalization_version='v1', bronze_schema_version='v1')).encode()).hexdigest()
    state = tmp_path / 'bronze.sqlite'
    store = V1SemanticStore(state)
    raw = dict(raw_ingestion_id='primary', topic='traffic.observations.v1', partition=0,
               offset=1, topic_identity='topic-id')
    row = dict(namespace='live', event_id=event['eventId'], semantic_revision=1,
               observation_envelope_json=json.dumps(event), normalization_version='v1',
               bronze_schema_version='v1', bronze_canonical_hash=content_hash)
    lineage = dict(namespace='live', raw_ingestion_id='primary', event_id=event['eventId'])
    intent = store.prepare('live', raw, event, 'envelope', content_hash, lambda *args: [
        dict(table='bronze_observations_v1', row=row), dict(table='bronze_delivery_dispositions_v1', row=lineage),
    ])
    store.complete('live', 'primary', intent['expected_hash'])
    repo = BronzeClickHouseRepository(ObservationSettings())
    class Client:
        missing_lineage = False
        corrupt_duplicate = False

        def query(self, sql, parameters):
            if 'bronze_observations_v1' in sql:
                assert parameters == dict(ns='live', eid=event['eventId'], rev=1)
                mirrors = [list(row.values())]
                if self.corrupt_duplicate:
                    mirrors.append(list(dict(row, bronze_canonical_hash='wrong').values()))
                return SimpleNamespace(column_names=list(row), result_rows=mirrors)
            assert 'bronze_delivery_dispositions_v1' in sql
            assert parameters == dict(p0='live', p1='primary')
            return SimpleNamespace(column_names=list(lineage),
                                   result_rows=[] if self.missing_lineage else [list(lineage.values())])

    client = repo._client = Client()
    valid = repo.read_valid_observation('live', event['eventId'], 1, state_path=state)
    assert valid['event'] == event and valid['primary_delivery']['raw_id'] == 'primary'
    assert repo.read_valid_observation('live', event['eventId'], 2, state_path=state) is None
    assert repo.read_valid_observation('other', event['eventId'], 1, state_path=state) is None
    client.missing_lineage = True
    with pytest.raises(RuntimeError, match='mirror'):
        repo.read_valid_observation('live', event['eventId'], 1, state_path=state)
    client.missing_lineage = False
    client.corrupt_duplicate = True
    with pytest.raises(IntegrityFault, match='bronze_canonical_hash'):
        repo.read_valid_observation('live', event['eventId'], 1, state_path=state)
    store.db.execute("UPDATE v1_identity SET state='CONFLICTED',revision=2 WHERE namespace='live'")
    assert repo.read_valid_observation('live', event['eventId'], 1, state_path=state) is None
    store.close()


def test_point_read_rejects_transition_during_clickhouse_query(tmp_path):
    event = json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    digest = hashlib.sha256(canonical_json(dict(
        observation=event, normalization_version='v1', bronze_schema_version='v1')).encode()).hexdigest()
    state = tmp_path / 'bronze.sqlite'
    store = V1SemanticStore(state)
    intent = store.prepare('live', dict(raw_ingestion_id='primary',
        topic='traffic.observations.v1', partition=0, offset=1, topic_identity='topic-id'),
        event, 'envelope', digest, lambda *args: [])
    store.complete('live', 'primary', intent['expected_hash'])
    repo = BronzeClickHouseRepository(ObservationSettings())
    row = dict(observation_envelope_json=json.dumps(event), normalization_version='v1',
               bronze_schema_version='v1', bronze_canonical_hash=digest)

    class RacingClient:
        def query(self, sql, parameters):
            store.db.execute("UPDATE v1_identity SET state='CONFLICTED',revision=2 "
                             "WHERE namespace='live' AND event_id=?", (event['eventId'],))
            return SimpleNamespace(column_names=list(row), result_rows=[list(row.values())])

    repo._client = RacingClient()
    repo.reconcile_v1_outputs = lambda _: []
    assert repo.read_valid_observation('live', event['eventId'], 1, state_path=state) is None
    store.close()
