import sqlite3
import pytest
from de.bronze.checkpoint_store import V1SemanticStore,V1SemanticReader


def test_readonly_journal_pages_keep_history_and_equal_timestamps(tmp_path):
    path=tmp_path/'state.sqlite'
    writer=V1SemanticStore(path)
    for seq,eid,rev,kind in [(1,'A',1,'ACCEPTED'),(2,'B',1,'ACCEPTED'),(3,'A',2,'CONFLICTED')]:
        writer.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
            ('live',seq,eid,rev,kind,None,kind,'hash',None,'same-time'))
    reader=V1SemanticReader(path)
    first=reader.read_page('live','changes',limit=2)
    assert first['high_water']==3 and first['next_cursor']==2
    reader.close()
    reader=V1SemanticReader(path)
    second=reader.read_page('live','changes',after=first['next_cursor'],high_water=first['high_water'])
    assert [r['change_type'] for r in second['rows']]==['CONFLICTED']
    with pytest.raises(sqlite3.OperationalError):
        reader.db.execute('DELETE FROM v1_changes')
    reader.close()
    writer.close()


def test_downstream_restart_discovers_conflict_and_complete_invalidation_provenance(tmp_path):
    path=tmp_path/'state.sqlite';cursor_path=tmp_path/'silver-cursor.sqlite'
    writer=V1SemanticStore(path)
    event=dict(eventId='event',payloadHash='payload',simulationRunId='historical-run',intersectionId='A',
        cycleSequence=7,simulationTimeSeconds=3.5,topologyDigest='v1:sha256:'+'a'*64)
    def raw(n): return dict(raw_ingestion_id='raw-'+str(n),topic='topic',partition=0,offset=n,topic_identity='uuid')
    def outputs(disposition,revision,conflict,invalidation):
        return [dict(table='audit',row=dict(disposition=disposition,revision=revision))]
    accepted=writer.prepare('live',raw(1),event,'envelope','content',outputs)
    writer.complete('live','raw-1',accepted['expected_hash'])
    reader=V1SemanticReader(path)
    page=reader.read_page('live','changes',after=0,limit=10)
    assert [r['change_type'] for r in page['rows']]==['ACCEPTED']
    consumer=sqlite3.connect(cursor_path);consumer.execute('CREATE TABLE cursor(value INTEGER)')
    consumer.execute('INSERT INTO cursor VALUES (?)',(page['next_cursor'],));consumer.commit();consumer.close();reader.close()
    conflict=dict(event);conflict['payloadHash']='changed'
    pending=writer.prepare('live',raw(2),conflict,'changed-envelope','changed-content',outputs)
    # Authoritative conflict/invalidation is visible even while its mirror is pending.
    assert writer.current('live','event')['state']=='CONFLICTED'
    writer.close()
    consumer=sqlite3.connect(cursor_path);after=consumer.execute('SELECT value FROM cursor').fetchone()[0];consumer.close()
    reader=V1SemanticReader(path)
    changes=reader.read_page('live','changes',after=after,limit=10)
    assert [r['change_type'] for r in changes['rows']]==['CONFLICTED']
    inv=reader.read_page('live','invalidations',after=0,limit=10)
    assert len(inv['rows'])==1 and inv['rows'][0]['mirror_status']=='PENDING'
    provenance=reader.read_invalidation_provenance('live',inv['rows'][0]['invalidation_id'])
    assert provenance['simulation_run_id']=='historical-run'
    assert provenance['intersection_id']=='A' and provenance['cycle_sequence']==7
    assert provenance['simulation_time_seconds']==3.5 and provenance['topology_digest']==event['topologyDigest']
    assert provenance['original_delivery']['raw_id']=='raw-1'
    assert provenance['conflicting_delivery']['raw_id']=='raw-2'
    assert reader.current('live','event')['state']=='CONFLICTED'
    reader.close()
