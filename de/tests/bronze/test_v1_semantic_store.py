import sqlite3

from de.bronze.checkpoint_store import V1SemanticStore
import pytest


def test_new_duplicate_late_conflict_and_restart(tmp_path):
    path = tmp_path / 'state.sqlite'
    store = V1SemanticStore(path)
    event = dict(eventId='event',payloadHash='payload',simulationRunId='run',intersectionId='A',
                 cycleSequence=1,simulationTimeSeconds=1.,topologyDigest='topology')
    def raw(n):
        return dict(raw_ingestion_id=str(n),topic='topic',partition=0,offset=n,topic_identity='uuid')
    def outputs(disposition,revision,conflict,invalidation):
        return dict(disposition=disposition,revision=revision,conflict=conflict,invalidation=invalidation)
    first = store.prepare('live',raw(0),event,'envelope','content',outputs)
    assert store.current('live','event')['state'] == 'PENDING'
    with pytest.raises(RuntimeError,match='earlier semantic intent'):
        store.prepare('live',raw(1),event,'changed','changed-content',outputs)
    store.close()
    store = V1SemanticStore(path)
    assert store.pending('live')[0]['expected_json'] == first['expected_json']
    store.complete('live','0',first['expected_hash'])
    duplicate = store.prepare('live',raw(1),event,'envelope','content',outputs)
    assert duplicate['disposition'] == 'SEMANTIC_DUPLICATE'
    store.complete('live','1',duplicate['expected_hash'])
    conflict = store.prepare('live',raw(2),event,'changed','changed-content',outputs)
    assert store.current('live','event')['state'] == 'CONFLICTED'
    assert store.db.execute('SELECT count(*) FROM v1_changes').fetchone()[0] == 2
    assert store.db.execute('SELECT count(*) FROM v1_invalidations').fetchone()[0] == 1
    store.close()
    store = V1SemanticStore(path)
    store.complete('live','2',conflict['expected_hash'])
    retry = store.prepare('live',raw(3),event,'envelope','content',outputs)
    assert retry['disposition'] == 'ALREADY_CONFLICTED'
    assert store.db.execute('SELECT count(*) FROM v1_changes').fetchone()[0] == 2
    store.close()


def test_sequence_allocation_is_transactional_and_pending_has_no_publication(tmp_path):
    store = V1SemanticStore(tmp_path / "bronze-v1.sqlite3")
    store.begin()
    one = store.next_sequence("live", "change")
    store.rollback()
    store.begin()
    assert store.next_sequence("live", "change") == one == 1
    assert store.next_sequence("live", "change") == 2
    assert store.next_sequence("live", "invalidation") == 1
    store.commit()
    store.db.execute("""INSERT INTO v1_identity VALUES
      ('live','event-1','PENDING',1,'e','p','c','raw','run','node',1,1.0,'digest',NULL)""")
    assert store.current("live", "event-1")["change_sequence"] is None
    store.close()
    reopened = V1SemanticStore(tmp_path / "bronze-v1.sqlite3")
    with pytest.raises(RuntimeError,match='transaction'):
        reopened.next_sequence('live','change')
    reopened.begin()
    assert reopened.next_sequence("live", "change") == 3
    reopened.commit()
    assert reopened.current("live", "event-1")["state"] == "PENDING"
    reopened.close()


def test_cas_rejects_stale_generation_and_pending_obligations(tmp_path):
    store = V1SemanticStore(tmp_path/'state.sqlite')
    store.db.execute("INSERT INTO v1_checkpoint VALUES ('live','topic',0,'uuid','binding',100,110,0)")
    def intent(raw_id,offset,status='PREPARED'):
        store.db.execute('''INSERT INTO v1_intents
          (namespace,raw_id,topic,partition_id,offset_value,topic_identity,event_id,
           disposition,processing_status,expected_json,expected_hash,revision,prepared_at)
          VALUES ('live',?,'topic',0,?,'uuid',?,'ACCEPTED_NEW',?,'[]','hash',1,'now')''',
          (raw_id,offset,'event-'+raw_id,status))
    def checkpoint():
        return tuple(store.db.execute("SELECT next_source_offset,safe_next_offset,generation FROM v1_checkpoint WHERE namespace='live'").fetchone())
    original=checkpoint()
    intent('inside',105)
    with pytest.raises(RuntimeError,match='incomplete'):
        store.advance_v1('live','topic',0,'uuid','binding',0,100,110,110)
    assert checkpoint()==original
    store.db.execute("UPDATE v1_intents SET processing_status='FAILED_RETRYABLE' WHERE raw_id='inside'")
    with pytest.raises(RuntimeError,match='incomplete'):
        store.advance_v1('live','topic',0,'uuid','binding',0,100,110,110)
    assert checkpoint()==original
    store.db.execute("UPDATE v1_intents SET processing_status='COMPLETED' WHERE raw_id='inside'")
    intent('exclusive-end',110)
    store.advance_v1('live','topic',0,'uuid','binding',0,100,110,110)
    assert checkpoint()==(110,110,1)
    # The exclusive-end intent did not block the previous range, but blocks the next.
    with pytest.raises(RuntimeError,match='incomplete'):
        store.advance_v1('live','topic',0,'uuid','binding',1,110,120,120)
    assert checkpoint()==(110,110,1)
    store.db.execute("UPDATE v1_intents SET processing_status='COMPLETED' WHERE raw_id='exclusive-end'")
    for args in [
        ('wrong','binding',1,110,120,120),
        ('uuid','wrong',1,110,120,120),
        ('uuid','binding',0,110,120,120),
        ('uuid','binding',1,109,120,120),
    ]:
        with pytest.raises(RuntimeError,match='CAS'):
            store.advance_v1('live','topic',0,*args)
        assert checkpoint()==(110,110,1)
    with pytest.raises(RuntimeError,match='boundary'):
        store.advance_v1('live','topic',0,'uuid','binding',1,110,120,119)
    assert checkpoint()==(110,110,1)
    # Sparse offsets are valid once all actually delivered intents in the range complete.
    intent('sparse',117,'COMPLETED')
    store.close()
    store=V1SemanticStore(tmp_path/'state.sqlite')
    store.advance_v1('live','topic',0,'uuid','binding',1,110,120,120)
    assert tuple(store.db.execute("SELECT next_source_offset,safe_next_offset,generation FROM v1_checkpoint WHERE namespace='live'").fetchone())==(120,120,2)
    store.close()
