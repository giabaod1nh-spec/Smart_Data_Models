import json
from pathlib import Path

import pytest

from de.bronze.checkpoint_store import V1SemanticStore
from de.bronze.processor import BronzeProcessor
from de.bronze.payload_codec import decode_v1_observation
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository


class Repository:
    def __init__(self):
        self.outputs = []
        self.fail = False

    def persist_v1_outputs(self, obligations):
        if self.fail:
            raise RuntimeError('ClickHouse unavailable')
        for obligation in obligations:
            if obligation not in self.outputs:
                self.outputs.append(obligation)


def test_materialization_restart_and_conflict_mirror_outage(tmp_path):
    event = json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    def raw(offset):
        record = V1Record('traffic.observations.v1',0,offset,
            f"{event['simulationRunId']}:{event['intersectionId']}".encode(),
            json.dumps(event).encode(),topic_identity='uuid')
        return V1ClickHouseRepository(None).materialize(V1RawValidator().classify(record))
    store = V1SemanticStore(tmp_path/'state.sqlite')
    repository = Repository()
    first = raw(0)
    assert BronzeProcessor.materialize_v1_delivery(store,repository,'live',first,event) == 'ACCEPTED_NEW'
    assert store.current('live',event['eventId'])['state'] == 'ACCEPTED'
    assert BronzeProcessor.materialize_v1_delivery(store,repository,'live',raw(1),event) == 'SEMANTIC_DUPLICATE'
    event['capturedAt'] = '2026-11-20T00:00:00Z'
    conflict = raw(2)
    decode_v1_observation(conflict)
    repository.fail = True
    with pytest.raises(RuntimeError,match='unavailable'):
        BronzeProcessor.materialize_v1_delivery(store,repository,'live',conflict,event)
    assert store.current('live',event['eventId'])['state'] == 'CONFLICTED'
    assert len(store.pending('live')) == 1
    store.close()
    store = V1SemanticStore(tmp_path/'state.sqlite')
    repository.fail = False
    BronzeProcessor.materialize_v1_delivery(store,repository,'live',conflict,event)
    assert not store.pending('live')
    assert store.db.execute('SELECT count(*) FROM v1_changes').fetchone()[0] == 2
    assert len([o for o in repository.outputs if o['table']=='bronze_observations_v1']) == 1
    assert len([o for o in repository.outputs if o['table']=='bronze_invalidations_v1']) == 1
    store.close()
