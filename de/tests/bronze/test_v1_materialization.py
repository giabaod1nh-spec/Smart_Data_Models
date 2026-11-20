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


class CountingRepository(Repository):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def persist_v1_outputs(self, obligations):
        self.calls += 1
        return super().persist_v1_outputs(obligations)


def _event_and_raw(offset=0):
    event = json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    record = V1Record('traffic.observations.v1',0,offset,
        f"{event['simulationRunId']}:{event['intersectionId']}".encode(),
        json.dumps(event).encode(),topic_identity='uuid')
    raw = V1ClickHouseRepository(None).materialize(V1RawValidator().classify(record))
    return event, raw


def _pending(tmp_path):
    event, raw = _event_and_raw()
    store = V1SemanticStore(tmp_path/'state.sqlite')
    repo = CountingRepository(); repo.fail = True
    with pytest.raises(RuntimeError,match='unavailable'):
        BronzeProcessor.materialize_v1_delivery(store,repo,'live',raw,event)
    repo.fail = False; repo.calls = 0
    return store, repo, event, raw


def test_recovery_rejects_corrupt_and_failed_integrity_without_writes(tmp_path):
    from de.kafka_raw.v1_ledger_store import IntegrityFault
    store,repo,event,raw=_pending(tmp_path)
    store.db.execute("UPDATE v1_intents SET expected_json=expected_json||' ' WHERE namespace='live'")
    with pytest.raises(IntegrityFault,match='hash'):
        BronzeProcessor.recover_v1_intent(store,repo,'live',raw['raw_ingestion_id'])
    assert repo.calls==0
    intent=store.pending('live')[0]
    assert intent['processing_status']=='FAILED_INTEGRITY'
    attempts=intent['attempt_count']
    with pytest.raises(IntegrityFault,match='FAILED_INTEGRITY'):
        BronzeProcessor.recover_v1_intent(store,repo,'live',raw['raw_ingestion_id'])
    assert repo.calls==0 and store.pending('live')[0]['attempt_count']==attempts
    store.close()


@pytest.mark.parametrize('mutation',["namespace","destination","identity"])
def test_recovery_rejects_invalid_frozen_obligation_before_persistence(tmp_path,mutation):
    import hashlib
    from de.kafka_raw.v1_ledger_store import IntegrityFault
    store,repo,event,raw=_pending(tmp_path)
    intent=store.pending('live')[0]; obligations=json.loads(intent['expected_json'])
    if mutation=='namespace': obligations[0]['row']['namespace']='other'
    elif mutation=='destination': obligations[0]['table']='unapproved_table'
    else: obligations[0]['row']['raw_ingestion_id']='wrong'
    frozen=json.dumps(obligations,separators=(',',':'),sort_keys=True)
    store.db.execute('UPDATE v1_intents SET expected_json=?,expected_hash=? WHERE namespace=? AND raw_id=?',
        (frozen,hashlib.sha256(frozen.encode()).hexdigest(),'live',raw['raw_ingestion_id']))
    with pytest.raises(IntegrityFault,match='frozen'):
        BronzeProcessor.recover_v1_intent(store,repo,'live',raw['raw_ingestion_id'])
    assert repo.calls==0 and store.pending('live')[0]['processing_status']=='FAILED_INTEGRITY'
    store.close()


def test_recovery_counts_each_attempt_once_and_preserves_frozen_obligations(tmp_path):
    store,repo,event,raw=_pending(tmp_path)
    before=store.pending('live')[0]
    assert before['attempt_count']==1
    repo.fail=True
    with pytest.raises(RuntimeError,match='unavailable'):
        BronzeProcessor.recover_v1_intent(store,repo,'live',raw['raw_ingestion_id'])
    failed=store.pending('live')[0]
    assert failed['attempt_count']==2
    assert failed['expected_json']==before['expected_json'] and failed['expected_hash']==before['expected_hash']
    repo.fail=False
    BronzeProcessor.recover_v1_intent(store,repo,'live',raw['raw_ingestion_id'])
    completed=store.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND raw_id=?',('live',raw['raw_ingestion_id'])).fetchone()
    assert completed['processing_status']=='COMPLETED' and completed['attempt_count']==3
    assert store.current('live',event['eventId'])['state']=='ACCEPTED'
    store.close()


def test_worker_and_materialization_use_shared_recovery_guards(tmp_path):
    from de.kafka_raw.v1_ledger_store import IntegrityFault
    store,repo,event,raw=_pending(tmp_path)
    store.db.execute("UPDATE v1_intents SET processing_status='FAILED_INTEGRITY' WHERE namespace='live'")
    worker=object.__new__(__import__('de.bronze.processor',fromlist=['ObservationWorker']).ObservationWorker)
    worker.store,worker.repo,worker.recovery_attempts=store,repo,0
    with pytest.raises(IntegrityFault): worker.recover()
    with pytest.raises(IntegrityFault): BronzeProcessor.materialize_v1_delivery(store,repo,'live',raw,event)
    assert repo.calls==0
    store.close()


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
