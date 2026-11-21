"""Verify the actual main Compose Bronze service against an isolated source stack."""
import json,subprocess,sys,time,urllib.request,uuid
from pathlib import Path


def main():
    output=Path('de/artifacts/bronze_v1/acceptance/independent-hardening/main-deployment')
    output.mkdir(parents=True,exist_ok=True)
    run_id='phase2-main-hardening-'+uuid.uuid4().hex[:10]
    deadline=time.monotonic()+90
    converged=False
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen('http://localhost:19198/ready',timeout=3) as response:
                if json.load(response).get('ready'):break
        except OSError:pass
        time.sleep(.5)
    else:raise RuntimeError('main Bronze did not become ready')
    command=[sys.executable,'-m','de.tools.v1_raw_sumo_acceptance','--run-id',run_id,'--duration','20',
        '--step-length','0.1','--bootstrap','localhost:49092','--artifact-root',str(output/'producer')]
    with (output/'producer.log').open('w',encoding='utf-8') as log:
        result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
    if result.returncode:raise RuntimeError('main SUMO source failed')
    import clickhouse_connect
    client=clickhouse_connect.get_client(host='localhost',port=19124)
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        raw=int(client.query('SELECT count() FROM smart_traffic.v1_raw_events WHERE simulation_run_id={r:String}',parameters={'r':run_id}).result_rows[0][0])
        bronze=int(client.query("SELECT count() FROM smart_traffic.bronze_observations_v1 WHERE namespace='live' AND simulation_run_id={r:String}",parameters={'r':run_id}).result_rows[0][0])
        lineage=int(client.query("SELECT count() FROM smart_traffic.bronze_delivery_dispositions_v1 WHERE namespace='live' AND disposition='ACCEPTED_NEW' AND event_id IN (SELECT event_id FROM smart_traffic.bronze_observations_v1 WHERE namespace='live' AND simulation_run_id={r:String})",parameters={'r':run_id}).result_rows[0][0])
        with urllib.request.urlopen('http://localhost:19198/ready',timeout=3) as response:health=json.load(response)
        converged=(raw==bronze==lineage==100 and health.get('ready') is True
            and health.get('pending_intents')==0 and health.get('pending_reconciliation')==0
            and health.get('integrity_faults')==0 and bool(health.get('partitions'))
            and all(p.get('offset_distance')==0 for p in health['partitions'].values()))
        if converged:
            reconciled_at=time.monotonic()
            break
        time.sleep(.5)
    client.close()
    inspect=json.loads(subprocess.check_output(['docker','inspect','phase2mainhardening-de-bronze-v1-1'],text=True))[0]
    mounts={m['Destination']:m['RW'] for m in inspect['Mounts']}
    report=dict(result='PASS',run_id=run_id,raw_rows=raw,canonical_rows=bronze,lineage_rows=lineage,
        health=health,container_command=inspect['Config']['Cmd'],mounts=mounts,
        differences_from_standard=dict(network='isolated hardening network',raw_storage='external named acceptance volume',bronze_storage='external named acceptance volume',health_host_port=19198))
    if not converged or reconciled_at>deadline or mounts.get('/raw') is not False or mounts.get('/app/de/artifacts') is not True:
        report['result']='FAIL'
    (output/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))
    return 0 if report['result']=='PASS' else 1


if __name__=='__main__':raise SystemExit(main())
