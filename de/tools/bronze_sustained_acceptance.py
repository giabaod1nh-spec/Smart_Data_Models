"""Measured isolated SUMO workload with bounded post-source catch-up."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import uuid


def completion_failures(snapshot, *, source_completed_at, now, deadline_seconds=60):
    """Return every unmet final-runtime condition; an empty list means PASS."""
    failures=[]
    if not snapshot.get('source_process_ok'): failures.append('source process incomplete')
    expected=int(snapshot.get('expected_events') or 0)
    participants=snapshot.get('participants') or []
    cycles=int(snapshot.get('cycles') or 0)
    if expected<=0 or not participants or cycles<=0 or expected!=len(participants)*cycles:
        failures.append('empty or inconsistent expected inventory')
    if snapshot.get('outbox_rows')!=expected or snapshot.get('outbox_acked')!=expected:
        failures.append('selected-run Outbox incomplete')
    if snapshot.get('raw_rows')!=expected: failures.append('selected-run Raw incomplete')
    if snapshot.get('raw_group_lag')!=0: failures.append('Raw consumer lag')
    partitions=snapshot.get('bronze_partitions') or {}
    if not partitions: failures.append('empty Bronze partition inventory')
    elif any(p.get('offset_distance')!=0 for p in partitions.values()): failures.append('Bronze checkpoint lag')
    if snapshot.get('canonical_rows')!=expected: failures.append('selected-run canonical output incomplete')
    if snapshot.get('lineage_rows')!=expected: failures.append('selected-run lineage incomplete')
    if snapshot.get('pending_intents')!=0: failures.append('pending persistence')
    if snapshot.get('pending_reconciliation')!=0: failures.append('pending reconciliation')
    if snapshot.get('integrity_faults')!=0: failures.append('integrity faults')
    if snapshot.get('ready') is not True: failures.append('unhealthy readiness')
    observed=snapshot.get('observed_at')
    if source_completed_at is None or observed is None or observed<source_completed_at:
        failures.append('stale pre-completion metrics')
    if source_completed_at is not None and now-source_completed_at>deadline_seconds:
        failures.append('catch-up deadline exceeded')
    return failures


def selected_run_snapshot(run, artifact_root, metrics, observed_at):
    import sqlite3
    import clickhouse_connect
    root=Path(artifact_root)
    db=sqlite3.connect((root/'kafka_outbox/v1-outbox.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
    rows=db.execute('SELECT payload_json,status,cycle_sequence FROM kafka_outbox WHERE simulation_run_id=?',(run,)).fetchall();db.close()
    events=[json.loads(row[0]) for row in rows]
    participants=sorted({event['intersectionId'] for event in events})
    cycles=len({row[2] for row in rows})
    client=clickhouse_connect.get_client(host='localhost',port=int(os.getenv('BRONZE_V1_CLICKHOUSE_PORT','18124')))
    raw=int(client.query('SELECT count() FROM smart_traffic.v1_raw_events WHERE simulation_run_id={run:String}',parameters={'run':run}).result_rows[0][0])
    canonical=int(client.query("SELECT count() FROM smart_traffic.bronze_observations_v1 WHERE namespace='live' AND simulation_run_id={run:String}",parameters={'run':run}).result_rows[0][0])
    lineage=int(client.query("SELECT count() FROM smart_traffic.bronze_delivery_dispositions_v1 WHERE namespace='live' AND disposition='ACCEPTED_NEW' AND event_id IN (SELECT event_id FROM smart_traffic.bronze_observations_v1 WHERE namespace='live' AND simulation_run_id={run:String})",parameters={'run':run}).result_rows[0][0]);client.close()
    stack=os.getenv('BRONZE_V1_STACK','phase2acceptance')
    group=subprocess.check_output(['docker','exec',stack+'-kafka-1','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:9092','--describe','--group','de-kafka-raw-v1'],text=True)
    lag=[]
    for line in group.splitlines():
        fields=line.split()
        if len(fields)>=6 and fields[0]=='de-kafka-raw-v1' and fields[1]=='traffic.observations.v1':
            lag.append(int(fields[5]) if fields[5]!='-' else 0)
    return dict(source_process_ok=True,expected_events=len(rows),outbox_rows=len(rows),
        outbox_acked=sum(row[1]=='ACKED' for row in rows),participants=participants,cycles=cycles,
        raw_rows=raw,raw_group_lag=sum(lag) if lag else None,canonical_rows=canonical,lineage_rows=lineage,
        bronze_partitions=metrics.get('partitions'),pending_intents=metrics.get('pending_intents'),
        pending_reconciliation=metrics.get('pending_reconciliation'),integrity_faults=metrics.get('integrity_faults'),
        ready=metrics.get('ready'),observed_at=observed_at)


def main():
    run='phase2-measured-'+uuid.uuid4().hex[:10]
    evidence_root=Path(os.getenv('BRONZE_V1_EVIDENCE_ROOT','de/artifacts/bronze_v1/acceptance/independent-hardening'))
    producer_root=os.getenv('BRONZE_V1_PRODUCER_ROOT','de/artifacts/bronze_v1/producer-isolated')
    bootstrap=os.getenv('BRONZE_V1_BOOTSTRAP','localhost:39092')
    health_port=os.getenv('BRONZE_V1_HEALTH_PORT','18098');raw_port=os.getenv('BRONZE_V1_RAW_HEALTH_PORT','18097')
    stack=os.getenv('BRONZE_V1_STACK','phase2acceptance')
    output=evidence_root/run
    output.mkdir(parents=True,exist_ok=True)
    # Representative five-participant workload; catch up within60 wall seconds
    # after source exit. This is an observed local workload, not a capacity test.
    command=[sys.executable,'-m','de.tools.v1_raw_sumo_acceptance','--run-id',run,
        '--duration','120','--step-length','0.1','--bootstrap',bootstrap,'--artifact-root',producer_root]
    with (output/'producer.log').open('w',encoding='utf-8') as log:
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        samples=[];source_stopped=None;started=time.monotonic();final_snapshot=None
        while True:
            with urllib.request.urlopen('http://localhost:'+health_port+'/metrics',timeout=5) as response:
                metrics=json.load(response)
            with urllib.request.urlopen('http://localhost:'+raw_port+'/metrics',timeout=5) as response:
                raw=json.load(response)
            stats=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',
                stack+'-bronze-1',stack+'-raw-1',stack+'-clickhouse-1'],text=True)
            samples.append(dict(elapsed=time.monotonic()-started,bronze=metrics,raw=raw,resources=stats))
            (output/'samples.json').write_text(json.dumps(samples,indent=2),encoding='utf-8')
            if process.poll() is not None:
                assert process.returncode==0,'source failed'
                if source_stopped is None:source_stopped=time.monotonic()
                observed=time.monotonic()
                final_snapshot=selected_run_snapshot(run,producer_root,metrics,observed)
                failures=completion_failures(final_snapshot,source_completed_at=source_stopped,now=observed)
                if not failures:
                    break
                assert observed-source_stopped<=60,'; '.join(failures)
            time.sleep(1)
    report=dict(run_id=run,simulation_seconds=120,catchup_seconds=time.monotonic()-source_stopped,
                elapsed=time.monotonic()-started,samples=len(samples),result='PASS',verification=final_snapshot)
    (output/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))


def inventory():
    import sqlite3
    root=Path('de/artifacts/bronze_v1/producer-isolated')
    db=sqlite3.connect((root/'kafka_outbox/v1-outbox.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
    reports=[]
    for manifest in sorted((root/'runs').glob('*/run_manifest.json')):
        run=manifest.parent.name
        rows=db.execute('SELECT payload_json,status,cycle_sequence FROM kafka_outbox WHERE simulation_run_id=?',(run,)).fetchall()
        if not rows:continue
        events=[json.loads(r[0]) for r in rows]
        assert all(r[1]=='ACKED' for r in rows)
        participants=sorted({e['intersectionId'] for e in events})
        cycles={r[2] for r in rows}
        assert len(rows)==len(cycles)*len(participants)
        reports.append(dict(run=run,outbox=len(rows),cycles=len(cycles),participants=participants,all_acked=True))
    db.close()
    path=Path('de/artifacts/bronze_v1/acceptance/outbox-inventory.json')
    path.write_text(json.dumps(reports,indent=2),encoding='utf-8')
    print(json.dumps(reports))


if __name__=='__main__':
    inventory() if '--inventory' in sys.argv else main()
