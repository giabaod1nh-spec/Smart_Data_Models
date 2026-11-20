"""Reproducible clean isolated Phase 2 hardening acceptance runner."""
import hashlib,json,os,subprocess,sys,time,urllib.request,uuid
from pathlib import Path


def run(command, *, env=None, output=None):
    started=time.monotonic()
    completed=subprocess.run(command,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if output: Path(output).write_text(completed.stdout,encoding='utf-8')
    return dict(command=subprocess.list2cmdline(command),exit_code=completed.returncode,duration=time.monotonic()-started,output=completed.stdout[-4000:])


def create_conflict_fixture(bootstrap):
    from confluent_kafka import Producer
    from contracts.traffic_observation import canonical_json,observation_event_id,validate_observation
    run_id='clean-conflict-'+uuid.uuid4().hex[:8]
    event=json.loads(Path('contracts/observation/fixtures/valid_code_derived.json').read_text())
    event['simulationRunId']=run_id;event['payload']['simulation_run_id']=run_id
    event['eventId']=observation_event_id(run_id,event['intersectionId'],event['cycleSequence'])
    event['payloadHash']=hashlib.sha256(canonical_json(event['payload']).encode()).hexdigest();validate_observation(event)
    changed=json.loads(json.dumps(event));changed['capturedAt']='2026-11-20T00:00:00Z';validate_observation(changed)
    producer=Producer({'bootstrap.servers':bootstrap,'acks':'all'})
    for item in (event,changed):
        producer.produce('traffic.observations.v1',key=f"{run_id}:{item['intersectionId']}".encode(),value=canonical_json(item).encode())
        producer.flush(10)
    return run_id


def main():
    execution='hardening-'+uuid.uuid4().hex[:8]
    project='phase2'+execution.replace('-','')
    root=Path('de/artifacts/bronze_v1/acceptance/independent-hardening')/execution
    root.mkdir(parents=True,exist_ok=False)
    compose=['docker','compose','-p',project,'-f','docker-compose.v1-acceptance.yml','-f','docker-compose.v1-hardening.yml']
    commands=[]
    up=run(compose+['up','-d','--build']);commands.append(up)
    if up['exit_code']: raise SystemExit(up['output'])
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen('http://localhost:19098/ready',timeout=3) as response:
                if json.load(response).get('ready'):break
        except OSError:pass
        time.sleep(1)
    else:raise RuntimeError('clean Bronze did not become ready')
    workload='clean-'+uuid.uuid4().hex[:10]
    producer_root=root/'producer'
    producer=run([sys.executable,'-m','de.tools.v1_raw_sumo_acceptance','--run-id',workload,
        '--duration','20','--step-length','0.1','--bootstrap','localhost:49092','--artifact-root',str(producer_root)],output=root/'producer.log')
    commands.append(producer)
    if producer['exit_code']:raise RuntimeError('clean source failed')
    import clickhouse_connect
    client=clickhouse_connect.get_client(host='localhost',port=19124)
    deadline=time.monotonic()+90
    reconciliation={}
    while time.monotonic()<deadline:
        raw=int(client.query('SELECT count() FROM smart_traffic.v1_raw_events WHERE simulation_run_id={r:String}',parameters={'r':workload}).result_rows[0][0])
        bronze=int(client.query("SELECT count() FROM smart_traffic.bronze_observations_v1 WHERE namespace='live' AND simulation_run_id={r:String}",parameters={'r':workload}).result_rows[0][0])
        with urllib.request.urlopen('http://localhost:19098/ready',timeout=3) as response:health=json.load(response)
        reconciliation=dict(run_id=workload,expected=100,raw=raw,bronze=bronze,health=health)
        if raw==bronze==100 and health.get('ready') and health.get('pending_intents')==0:break
        time.sleep(1)
    client.close()
    (root/'clean-start.json').write_text(json.dumps(reconciliation,indent=2),encoding='utf-8')
    if reconciliation.get('raw')!=100 or reconciliation.get('bronze')!=100:raise RuntimeError('clean source reconciliation failed')
    conflict_run=create_conflict_fixture('localhost:49092')
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        client=clickhouse_connect.get_client(host='localhost',port=19124)
        count=int(client.query("SELECT count() FROM smart_traffic.bronze_invalidations_v1 WHERE namespace='live' AND simulation_run_id={r:String}",parameters={'r':conflict_run}).result_rows[0][0]);client.close()
        if count==1:break
        time.sleep(.5)
    if count!=1:raise RuntimeError('clean conflict fixture was not materialized')
    env=dict(os.environ,BRONZE_V1_ACCEPTANCE='1',BRONZE_V1_STACK=project,
        BRONZE_V1_CLICKHOUSE_PORT='19124',BRONZE_V1_HEALTH_PORT='19098',
        BRONZE_V1_BOOTSTRAP='localhost:49092',BRONZE_V1_SELECTED_RUNS=json.dumps([[workload,100]]),BRONZE_V1_MIN_VALID='100')
    tests=run([sys.executable,'-m','pytest','de/tests/integration/test_bronze_v1_acceptance.py','-q',
        '--basetemp',str(root/'pytest-temp'),'--junitxml',str(root/'runtime.xml')],env=env,output=root/'runtime.log')
    commands.append(tests)
    diff=subprocess.check_output(['git','diff','--binary'],text=False)
    images={service:subprocess.check_output(['docker','image','inspect',f'{project}-{service}:latest','--format','{{.Id}}'],text=True).strip()
        for service in ('raw','bronze')}
    (root/'source-images.json').write_text(json.dumps(dict(head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        diff_sha256=hashlib.sha256(diff).hexdigest(),images=images,project=project),indent=2),encoding='utf-8')
    (root/'commands.json').write_text(json.dumps(commands,indent=2),encoding='utf-8')
    (root/'result.json').write_text(json.dumps(dict(result='PASS' if tests['exit_code']==0 else 'FAIL',execution=execution,
        project=project,workload=workload,runtime_exit_code=tests['exit_code']),indent=2),encoding='utf-8')
    print(json.dumps(dict(result='PASS' if tests['exit_code']==0 else 'FAIL',evidence=str(root),project=project)))
    return tests['exit_code']


if __name__=='__main__':raise SystemExit(main())
