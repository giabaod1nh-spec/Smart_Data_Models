"""Opt-in real isolated Kafka/Raw/Bronze acceptance, never the live topic."""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import pytest

pytestmark = pytest.mark.skipif(os.getenv('BRONZE_V1_ACCEPTANCE') != '1',reason='requires isolated phase2acceptance stack')


def state_query(sql):
    code = "import sqlite3,json; d=sqlite3.connect('file:/app/de/artifacts/bronze_v1/checkpoint.sqlite3?mode=ro',uri=True); print(json.dumps(d.execute("+repr(sql)+").fetchall()))"
    return json.loads(subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',code],text=True))


def frozen_live_inventory():
    script='''
import sqlite3,json,hashlib
from de.bronze.config import ObservationSettings
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
s=ObservationSettings();r=BronzeClickHouseRepository(s);r.connect();out={}
for path,names,prefix in [(s.checkpoint_path,['v1_identity','v1_checkpoint','v1_intents','v1_changes','v1_invalidations'],'bronze'),(s.raw_ledger_path,['deliveries','partition_progress','batches','integrity_faults'],'raw')]:
 d=sqlite3.connect('file:'+path+'?mode=ro',uri=True)
 for name in names:
  rows=d.execute('SELECT * FROM '+name).fetchall()
  out[prefix+'.'+name]=hashlib.sha256(json.dumps(sorted(rows,key=repr),default=str).encode()).hexdigest()
 d.close()
for name in ['bronze_observations_v1','bronze_delivery_dispositions_v1','bronze_invalidations_v1','v1_raw_events','v1_quarantine_events','bronze_entity_events']:
 where=" WHERE namespace='live'" if name.startswith('bronze_') and name.endswith('_v1') else ''
 rows=r.client.query('SELECT * FROM smart_traffic.'+name+where).result_rows
 out['ch.'+name]=hashlib.sha256(json.dumps(sorted(rows,key=repr),default=str).encode()).hexdigest()
print(json.dumps(out));r.close()
'''
    inventory=json.loads(subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',script],text=True))
    inventory['broker']=subprocess.check_output(['docker','exec','phase2acceptance-kafka-1','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:9092','--describe','--group','de-kafka-raw-v1'],text=True)
    return inventory


def test_canonical_and_readonly_interfaces_in_separate_process():
    script = '''
import sqlite3,tempfile,json
from pathlib import Path
from types import SimpleNamespace
from de.bronze.config import ObservationSettings
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.checkpoint_store import V1SemanticReader
s=ObservationSettings();r=BronzeClickHouseRepository(s);r.connect()
reader=V1SemanticReader(s.checkpoint_path)
assert reader.db.execute('PRAGMA query_only').fetchone()[0]==1
try: reader.db.execute('DELETE FROM v1_changes');raise AssertionError('write allowed')
except sqlite3.OperationalError: pass
page=reader.read_page('live','changes',limit=2)
following=reader.read_page('live','changes',after=page['next_cursor'],limit=2,high_water=page['high_water'])
assert all(x['change_sequence']>page['next_cursor'] for x in following['rows'])
assert reader.read_page('live','invalidations',limit=1)['rows']
eid=reader.db.execute("SELECT event_id FROM v1_identity WHERE state='CONFLICTED' LIMIT 1").fetchone()[0]
assert reader.read_delivery_lineage('live',eid)
allrows=[];cursor=None
while True:
 p=r.read_valid_observations(s.checkpoint_path,'live',after=cursor,limit=100)
 allrows.extend(p['rows'])
 if not p['rows']:break
 cursor=p['next_cursor']
assert eid not in [x['event']['eventId'] for x in allrows]
assert len(allrows)==reader.db.execute("SELECT count(*) FROM v1_identity WHERE state='ACCEPTED'").fetchone()[0]
with tempfile.TemporaryDirectory() as directory:
 clone=Path(directory)/'clone.sqlite';db=sqlite3.connect(clone);reader.db.backup(db)
 db.execute("UPDATE v1_identity SET state='PENDING' WHERE state='ACCEPTED'");db.commit();db.close()
 assert r.read_valid_observations(clone,'live')['rows']==[]
 try:r.read_valid_observations(Path(directory)/'missing.sqlite','live');raise AssertionError('authority fallback')
 except sqlite3.OperationalError:pass
client=r._client
class Missing:
 def query(self,sql,**kwargs):
  if 'SELECT * FROM smart_traffic.bronze_observations_v1' in sql:return SimpleNamespace(result_rows=[],column_names=[])
  return client.query(sql,**kwargs)
r._client=Missing()
try:r.read_valid_observations(s.checkpoint_path,'live',limit=1);raise AssertionError('missing mirror accepted')
except RuntimeError as e:assert 'missing' in str(e)
class Corrupt:
 def query(self,sql,**kwargs):
  q=client.query(sql,**kwargs)
  if 'SELECT * FROM smart_traffic.bronze_observations_v1' in sql:
   rows=[list(x) for x in q.result_rows];rows[0][q.column_names.index('observation_envelope_json')]='{}'
   return SimpleNamespace(result_rows=rows,column_names=q.column_names)
  return q
r._client=Corrupt()
try:r.read_valid_observations(s.checkpoint_path,'live',limit=1);raise AssertionError('corrupt mirror accepted')
except (RuntimeError,ValueError):pass
r._client=client;r.close();reader.close()
print(json.dumps({'valid_count':len(allrows),'conflicted_excluded':eid,'readonly':True}))
'''
    result=json.loads(subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',script],text=True))
    assert result['readonly'] and result['valid_count']>=700


@pytest.mark.parametrize('failure',['lost_response','partial_outputs','finalization','conflict_mirror','pending_outage'])
def test_real_persistence_fault_boundaries(tmp_path,failure):
    from de.bronze.checkpoint_store import V1SemanticStore
    from de.bronze.clickhouse_repository import BronzeClickHouseRepository
    from de.bronze.config import ObservationSettings
    from de.bronze.processor import BronzeProcessor
    from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
    from de.kafka_raw.v1_models import V1Record
    from de.kafka_raw.v1_validator import V1RawValidator
    settings=ObservationSettings(clickhouse_port=18124,clickhouse_query_timeout=2)
    repo=BronzeClickHouseRepository(settings);repo.connect()
    namespace='replay:fault-'+uuid.uuid4().hex[:12]
    store=V1SemanticStore(tmp_path/'state.sqlite')
    event=json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    def raw(offset):
        return V1ClickHouseRepository(None).materialize(V1RawValidator().classify(V1Record(
            'traffic.observations.v1',0,offset,f"{event['simulationRunId']}:{event['intersectionId']}".encode(),json.dumps(event).encode(),topic_identity='fault-test')))
    offset=0
    if failure=='conflict_mirror':
        BronzeProcessor.materialize_v1_delivery(store,repo,namespace,raw(0),event)
        event['capturedAt']='2026-11-20T00:00:00Z';offset=1
    frozen_raw=raw(offset)
    original_persist=repo.persist_v1_outputs
    original_complete=store.complete
    if failure=='lost_response':
        client=repo._client
        class LostResponse:
            def query(self,*args,**kwargs):return client.query(*args,**kwargs)
            def insert(self,*args,**kwargs):
                client.insert(*args,**kwargs)
                raise ConnectionError('lost insert response after server persistence')
        repo._client=LostResponse()
    elif failure in ('partial_outputs','conflict_mirror'):
        def partial(obligations):
            original_persist(obligations[:1])
            raise ConnectionError('remaining output unavailable')
        repo.persist_v1_outputs=partial
    elif failure=='finalization':
        store.complete=lambda *args: (_ for _ in ()).throw(sqlite_failure())
    elif failure=='pending_outage':
        def outage(obligations):
            assert store.pending(namespace), 'fault must occur after durable preparation'
            subprocess.check_call(['docker','pause','phase2acceptance-clickhouse-1'])
            try: original_persist(obligations)
            finally: subprocess.check_call(['docker','unpause','phase2acceptance-clickhouse-1'])
        repo.persist_v1_outputs=outage
    try:
        if failure=='lost_response':
            BronzeProcessor.materialize_v1_delivery(store,repo,namespace,frozen_raw,event)
            assert not store.pending(namespace)
        else:
            with pytest.raises(Exception):
                BronzeProcessor.materialize_v1_delivery(store,repo,namespace,frozen_raw,event)
            pending=store.pending(namespace)
            assert len(pending)==1 and pending[0]['processing_status']=='FAILED_RETRYABLE'
            assert pending[0]['attempt_count']>=1
            assert store.current(namespace,event['eventId'])['state']==('CONFLICTED' if failure=='conflict_mirror' else 'PENDING')
            frozen=pending[0]['expected_json']
            store.close();store=V1SemanticStore(tmp_path/'state.sqlite')
            assert store.pending(namespace)[0]['expected_json']==frozen
            repo.persist_v1_outputs=original_persist
            BronzeProcessor.materialize_v1_delivery(store,repo,namespace,frozen_raw,event)
            assert not store.pending(namespace)
            assert store.current(namespace,event['eventId'])['state']==('CONFLICTED' if failure=='conflict_mirror' else 'ACCEPTED')
    finally:
        if failure=='lost_response':repo._client=client
        repo.close();store.close()


def sqlite_failure():
    return RuntimeError('injected SQLite finalization failure after persistence')


def test_additive_upgrade_preserves_v2_sentinel_and_raw_schema():
    import clickhouse_connect
    from de.kafka_raw.config import KafkaRawSettings
    from de.kafka_raw.clickhouse_repository import run_migration_file
    settings=KafkaRawSettings(clickhouse_port=18124,clickhouse_password='')
    run_migration_file(settings,Path('de/migrations/003_create_bronze_v2.sql'))
    client=clickhouse_connect.get_client(host='localhost',port=18124)
    sentinel=hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    client.command("INSERT INTO smart_traffic.bronze_entity_events (raw_ingestion_id,event_id) VALUES ('"+sentinel+"','"+sentinel+"')")
    tables=['bronze_entity_events','bronze_run_events','bronze_quarantine','v1_raw_events','v1_quarantine_events']
    before={t:client.command('SHOW CREATE TABLE smart_traffic.'+t) for t in tables}
    raw_counts={t:client.command('SELECT count() FROM smart_traffic.'+t) for t in ('v1_raw_events','v1_quarantine_events')}
    for _ in range(2):run_migration_file(settings,Path('de/migrations/007_create_bronze_v1.sql'))
    assert before=={t:client.command('SHOW CREATE TABLE smart_traffic.'+t) for t in tables}
    assert raw_counts=={t:client.command('SELECT count() FROM smart_traffic.'+t) for t in raw_counts}
    assert client.command('SELECT count() FROM smart_traffic.bronze_entity_events WHERE raw_ingestion_id={id:String}',parameters={'id':sentinel})==1
    assert client.command("SELECT count() FROM system.tables WHERE database='smart_traffic' AND name IN ('bronze_observations_v1','bronze_delivery_dispositions_v1','bronze_conflict_evidence_v1','bronze_quarantine_v1','bronze_invalidations_v1')")==5
    client.close()


def test_full_sumo_source_output_reconciliation():
    script='''
import json,sqlite3,hashlib,base64
from de.bronze.config import ObservationSettings
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.payload_codec import decode_v1_observation
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_ledger_store import V1LedgerReader
s=ObservationSettings();repo=BronzeClickHouseRepository(s);repo.connect()
rawrepo=V1ClickHouseRepository(repo.client);ledger=V1LedgerReader(s.raw_ledger_path)
d=sqlite3.connect('file:'+s.checkpoint_path+'?mode=ro',uri=True)
reports=[]
for run,expected in [('phase2-smoke-20261120-01',100),('phase2-sustained-20261120-01',600)]:
 rows=repo.client.query('SELECT * FROM smart_traffic.v1_raw_events WHERE simulation_run_id={run:String}',parameters={'run':run})
 source={}
 for vals in rows.result_rows:
  raw=dict(zip(rows.column_names,vals))
  for k in ('raw_ingestion_id','physical_record_hash','payload_bytes_hash'):
   if isinstance(raw[k],bytes):raw[k]=raw[k].decode('ascii')
  event,original=decode_v1_observation(raw)
  assert hashlib.sha256(original).hexdigest()==raw['payload_bytes_hash']
  source[event['eventId']]=event
 valid=[];cursor=None
 while True:
  page=repo.read_valid_observations(s.checkpoint_path,'live',after=cursor,limit=100,simulation_run_id=run)
  valid.extend(page['rows'])
  if not page['rows']:break
  cursor=page['next_cursor']
 assert len(source)==len(valid)==expected
 assert all(source[x['event']['eventId']]==x['event'] for x in valid)
 lineage=d.execute("SELECT count(*) FROM v1_intents WHERE namespace='live' AND event_id IN (SELECT event_id FROM v1_identity WHERE simulation_run_id=?) AND disposition='ACCEPTED_NEW' AND processing_status='COMPLETED'",(run,)).fetchone()[0]
 assert lineage==expected
 reports.append({'run':run,'raw_physical':len(rows.result_rows),'distinct_events':len(source),'canonical_valid':len(valid),'completed_lineage':lineage})
assert d.execute("SELECT count(*) FROM v1_intents WHERE namespace='live' AND processing_status!='COMPLETED'").fetchone()[0]==0
for p,n in d.execute("SELECT partition_id,next_source_offset FROM v1_checkpoint WHERE namespace='live'"):
 assert n==ledger.db.execute('SELECT safe_next_offset FROM partition_progress WHERE partition_id=?',(p,)).fetchone()[0]
print(json.dumps(reports));repo.close();ledger.close();d.close()
'''
    reports=json.loads(subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',script],text=True))
    assert sum(r['canonical_valid'] for r in reports)==700


def test_actual_raw_replay_source_and_interrupted_bronze_resume():
    run='rawmode-'+uuid.uuid4().hex[:10]
    subprocess.check_call(['docker','exec','phase2acceptance-raw-1','python','-m','de.kafka_raw.v1_replay',
        '--topic','traffic.observations.v1','--partition','1','--start-offset','2','--end-offset','22',
        '--replay-run-id',run,'--bootstrap-servers','kafka:9092'])
    script='''
import json,sqlite3
from pathlib import Path
from de.bronze.config import ObservationSettings
from de.bronze.replay import observation_replay
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
s=ObservationSettings(raw_ledger_path='/raw/kafka_raw_v1/replay/'+RUN+'/ledger.sqlite3',batch_size=5)
d=sqlite3.connect('file:'+s.raw_ledger_path+'?mode=ro',uri=True)
identity=d.execute('SELECT topic_identity FROM partition_progress').fetchone()[0];d.close()
m={'source_mode':'raw-replay-range','raw_replay_run_id':RUN,'context_complete':True,'topic':'traffic.observations.v1','partitions':[{'partition':1,'topic_identity':identity,'start_offset':2,'end_offset':22}]}
p=Path('/tmp/'+RUN+'.json');p.write_text(json.dumps(m))
original=BronzeClickHouseRepository.persist_v1_outputs
calls=[0]
def interrupt(self,obligations):
 calls[0]+=1
 if calls[0]==8:raise RuntimeError('injected interrupted replay with prepared intent')
 return original(self,obligations)
BronzeClickHouseRepository.persist_v1_outputs=interrupt
try:observation_replay(p,RUN,s);raise AssertionError('interruption absent')
except RuntimeError as e:assert 'interrupted' in str(e)
state=Path(s.checkpoint_path).parent/'replay'/RUN/'checkpoint.sqlite3'
d=sqlite3.connect(state)
cursor=d.execute('SELECT next_source_offset FROM v1_checkpoint').fetchone()[0]
assert cursor==7
assert d.execute("SELECT count(*) FROM v1_intents WHERE processing_status!='COMPLETED'").fetchone()[0]==1
d.close()
BronzeClickHouseRepository.persist_v1_outputs=original
r=observation_replay(p,RUN,s)
d=sqlite3.connect(state)
assert d.execute('SELECT next_source_offset FROM v1_checkpoint').fetchone()[0]==22
assert d.execute('SELECT count(*) FROM v1_intents').fetchone()[0]==20
assert not r['pending']
m['partitions'][0]['end_offset']=21;p.write_text(json.dumps(m))
try:observation_replay(p,RUN,s);raise AssertionError('changed manifest accepted')
except ValueError as e:assert 'manifest' in str(e)
print(json.dumps({'cursor_before':cursor,'cursor_after':22,'deliveries':20}));d.close()
'''.replace('RUN',repr(run))
    result=json.loads(subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',script],text=True))
    assert result['deliveries']==20


def test_wire_duplicate_conflicts_and_monotonic_retry():
    from confluent_kafka import Producer
    from contracts.traffic_observation import canonical_json,validate_observation
    run = 'phase2-semantic-'+uuid.uuid4().hex[:12]
    original = json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    original['simulationRunId'] = run
    original['payload']['simulation_run_id'] = run
    original['payloadHash'] = hashlib.sha256(canonical_json(original['payload']).encode()).hexdigest()
    # Shared identity is schema/run/intersection/cycle; payload retains its own
    # required run context as defined by the fixture.
    original['eventId'] = hashlib.sha256(f"1|{run}|{original['intersectionId']}|{original['cycleSequence']}".encode()).hexdigest()
    validate_observation(original)
    second = copy.deepcopy(original)
    second['cycleSequence'] += 1
    second['eventId'] = hashlib.sha256(f"1|{run}|{second['intersectionId']}|{second['cycleSequence']}".encode()).hexdigest()
    validate_observation(second)
    payload_conflict = copy.deepcopy(original)
    payload_conflict['scenarioId'] = 'bronze-conflict-test'
    payload_conflict['payload']['scenario'] = 'bronze-conflict-test'
    payload_conflict['payloadHash'] = hashlib.sha256(canonical_json(payload_conflict['payload']).encode()).hexdigest()
    metadata_conflict = copy.deepcopy(second)
    metadata_conflict['capturedAt'] = '2026-11-20T00:00:00Z'
    events = [original,original,payload_conflict,original,second,metadata_conflict]
    producer = Producer({'bootstrap.servers':'localhost:39092','acks':'all'})
    errors = []
    for event in events:
        validate_observation(event)
        producer.produce('traffic.observations.v1',key=f"{run}:{event['intersectionId']}".encode(),
            value=canonical_json(event).encode(),on_delivery=lambda err,msg: errors.append(err) if err else None)
        producer.flush(10)
    assert not errors
    quoted = "'"+run+"'"
    deadline = time.monotonic()+60
    while time.monotonic()<deadline:
        identities = state_query('SELECT state FROM v1_identity WHERE simulation_run_id='+quoted)
        pending = state_query('SELECT count(*) FROM v1_intents WHERE event_id IN (SELECT event_id FROM v1_identity WHERE simulation_run_id='+quoted+") AND processing_status!='COMPLETED'")[0][0]
        if len(identities)==2 and all(r[0]=='CONFLICTED' for r in identities) and pending==0:
            break
        time.sleep(.5)
    assert identities == [['CONFLICTED'],['CONFLICTED']]
    assert pending == 0
    dispositions = dict(state_query('SELECT disposition,count(*) FROM v1_intents WHERE event_id IN (SELECT event_id FROM v1_identity WHERE simulation_run_id='+quoted+') GROUP BY disposition'))
    assert dispositions == {'ACCEPTED_NEW':2,'SEMANTIC_DUPLICATE':1,'SEMANTIC_CONFLICT':2,'ALREADY_CONFLICTED':1}
    assert state_query('SELECT count(*) FROM v1_changes WHERE event_id IN (SELECT event_id FROM v1_identity WHERE simulation_run_id='+quoted+')')[0][0]==4
    assert state_query('SELECT count(*) FROM v1_invalidations WHERE event_id IN (SELECT event_id FROM v1_identity WHERE simulation_run_id='+quoted+')')[0][0]==2


def test_historical_out_of_order_and_availability_roundtrip():
    from confluent_kafka import Producer
    from contracts.traffic_observation import canonical_json,observation_event_id,validate_observation
    run='phase2-history-'+uuid.uuid4().hex[:12]
    base=json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    unavailable=json.loads(Path('contracts/observation/fixtures/unavailable_synthetic.json').read_text())
    events=[]
    # Deliberately publish cycle 9 before cycle 2 and cycle 5 in one historical run.
    for cycle,scenario,source in [(9,'scenario-late',unavailable),(2,'scenario-early',base),(5,'scenario-middle',base)]:
        event=copy.deepcopy(source)
        event['simulationRunId']=run;event['intersectionId']='A';event['cycleSequence']=cycle
        event['eventId']=observation_event_id(run,'A',cycle)
        event['simulationTimeSeconds']=float(cycle);event['simulationStep']=cycle*100
        event['capturedAt']=f'2024-01-01T00:00:{cycle:02d}Z';event['scenarioId']=scenario
        payload=event['payload'];payload['simulation_run_id']=run;payload['simulation_time_sec']=float(cycle)
        payload['scenario']=scenario
        if cycle==5:
            direction=payload['directions']['East']
            direction['occupancy_pct']=None
            direction['metric_availability']={'occupancy_pct':'NOT_APPLICABLE'}
            direction.pop('vehicle_class_composition',None)
        event['payloadHash']=hashlib.sha256(canonical_json(payload).encode()).hexdigest()
        validate_observation(event);events.append(event)
    producer=Producer({'bootstrap.servers':'localhost:39092','acks':'all'})
    for event in events:
        producer.produce('traffic.observations.v1',key=f"{run}:A".encode(),value=canonical_json(event).encode())
        producer.flush(10)
    wanted={event['eventId']:event for event in events}
    deadline=time.monotonic()+60
    rows=[]
    while time.monotonic()<deadline:
        script='''
import json
from de.bronze.config import ObservationSettings
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
s=ObservationSettings();r=BronzeClickHouseRepository(s);r.connect();out=[];cursor=None
while True:
 p=r.read_valid_observations(s.checkpoint_path,'live',after=cursor,limit=100,simulation_run_id=RUN)
 out.extend(x['event'] for x in p['rows'])
 if not p['rows']:break
 cursor=p['next_cursor']
print(json.dumps(out));r.close()
'''.replace('RUN',repr(run))
        rows=json.loads(subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',script],text=True))
        if {event['eventId'] for event in rows}==set(wanted):break
        time.sleep(.5)
    assert {event['eventId'] for event in rows}==set(wanted)
    actual={event['eventId']:event for event in rows}
    assert actual==wanted
    assert [actual[event['eventId']]['cycleSequence'] for event in events]==[9,2,5]
    assert actual[events[0]['eventId']]['payload']['directions']['East']['occupancy_pct'] is None
    assert actual[events[0]['eventId']]['payload']['directions']['East']['metric_availability']=={'occupancy_pct':'UNAVAILABLE'}
    assert actual[events[2]['eventId']]['payload']['directions']['East']['metric_availability']=={'occupancy_pct':'NOT_APPLICABLE'}
    assert 'vehicle_class_composition' not in actual[events[2]['eventId']]['payload']['directions']['East']
    assert actual[events[1]['eventId']]['payload']['directions']['East']['vehicle_count']==0
    assert state_query("SELECT count(*) FROM v1_intents WHERE event_id IN (SELECT event_id FROM v1_identity WHERE simulation_run_id='"+run+"') AND disposition='ACCEPTED_NEW'")[0][0]==3


def test_replay_cursor_resume_and_live_state_invariance():
    # Frozen source is quiescent for this acceptance case. Manifest captures all
    # three partitions, including both accepted and conflicted semantic context.
    inventory_before=frozen_live_inventory()
    before = state_query('SELECT event_id,state,revision,change_sequence FROM v1_identity ORDER BY event_id')
    run_id = 'acceptance-'+uuid.uuid4().hex[:10]
    script = '''
import json,sqlite3
from pathlib import Path
from de.bronze.config import ObservationSettings
from de.bronze.replay import observation_replay
s=ObservationSettings()
d=sqlite3.connect('file:'+s.raw_ledger_path+'?mode=ro',uri=True)
m={'source_mode':'live-raw-range','context_complete':True,'topic':'traffic.observations.v1','partitions':[
 {'partition':p,'topic_identity':u,'start_offset':0,'end_offset':end}
 for p,u,end in d.execute('SELECT partition_id,topic_identity,safe_next_offset FROM partition_progress ORDER BY partition_id')]}
d.close()
p=Path('/tmp/bronze-replay-manifest.json');p.write_text(json.dumps(m))
first=observation_replay(p,RUN_ID,s)
second=observation_replay(p,RUN_ID,s)
r=sqlite3.connect(first['state_path'])
live=sqlite3.connect('file:'+s.checkpoint_path+'?mode=ro',uri=True)
assert r.execute('SELECT event_id,state FROM v1_identity ORDER BY event_id').fetchall()==live.execute('SELECT event_id,state FROM v1_identity ORDER BY event_id').fetchall()
assert all(n==e for n,e in r.execute('SELECT next_source_offset,safe_next_offset FROM v1_checkpoint'))
assert not first['pending'] and not second['pending']
print(json.dumps({'replay':first,'checkpoints':r.execute('SELECT partition_id,next_source_offset FROM v1_checkpoint').fetchall()}))
'''.replace('RUN_ID',repr(run_id))
    output = subprocess.check_output(['docker','exec','phase2acceptance-bronze-1','python','-c',script],text=True)
    assert json.loads(output)['replay']['pending']==0
    assert before == state_query('SELECT event_id,state,revision,change_sequence FROM v1_identity ORDER BY event_id')
    assert inventory_before==frozen_live_inventory()


def test_worker_restart_preserves_semantics_and_recovers_idle_readiness():
    import urllib.request
    before=state_query('SELECT event_id,state,revision,change_sequence FROM v1_identity ORDER BY event_id')
    subprocess.check_call(['docker','restart','phase2acceptance-bronze-1'])
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen('http://localhost:18098/ready',timeout=3) as response:
                health=json.load(response)
            if health['ready']:
                break
        except (OSError,ValueError):
            pass
        time.sleep(.5)
    else:
        pytest.fail('Bronze failed to regain readiness after restart')
    assert all(p['offset_distance']==0 for p in health['partitions'].values())
    assert before==state_query('SELECT event_id,state,revision,change_sequence FROM v1_identity ORDER BY event_id')


def test_isolated_clickhouse_outage_and_automatic_recovery():
    import urllib.request
    import urllib.error
    before=state_query('SELECT event_id,state,revision,change_sequence FROM v1_identity ORDER BY event_id')
    checkpoints=state_query('SELECT partition_id,next_source_offset FROM v1_checkpoint ORDER BY partition_id')
    subprocess.check_call(['docker','pause','phase2acceptance-clickhouse-1'])
    degraded=False
    try:
        deadline=time.monotonic()+50
        while time.monotonic()<deadline:
            try:
                urllib.request.urlopen('http://localhost:18098/ready',timeout=3).close()
            except urllib.error.HTTPError as exc:
                if exc.code==503:
                    degraded=True
                    break
            time.sleep(.5)
        assert degraded,'Bronze did not expose dependency unavailability'
        assert checkpoints==state_query('SELECT partition_id,next_source_offset FROM v1_checkpoint ORDER BY partition_id')
    finally:
        subprocess.check_call(['docker','unpause','phase2acceptance-clickhouse-1'])
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen('http://localhost:18098/ready',timeout=3) as r:
                if json.load(r)['ready']:
                    break
        except OSError:
            pass
        time.sleep(.5)
    else:
        pytest.fail('Bronze failed to recover after isolated dependency outage')
    assert before==state_query('SELECT event_id,state,revision,change_sequence FROM v1_identity ORDER BY event_id')
