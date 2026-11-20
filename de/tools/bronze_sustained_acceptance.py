"""Measured isolated SUMO workload with bounded post-source catch-up."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
import uuid


def main():
    run='phase2-measured-'+uuid.uuid4().hex[:10]
    output=Path('de/artifacts/bronze_v1/acceptance')/run
    output.mkdir(parents=True,exist_ok=True)
    # Representative five-participant workload; catch up within60 wall seconds
    # after source exit. This is an observed local workload, not a capacity test.
    command=[sys.executable,'-m','de.tools.v1_raw_sumo_acceptance','--run-id',run,
        '--duration','120','--step-length','0.1','--artifact-root','de/artifacts/bronze_v1/producer-isolated']
    with (output/'producer.log').open('w',encoding='utf-8') as log:
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        samples=[];source_stopped=None;started=time.monotonic()
        while True:
            with urllib.request.urlopen('http://localhost:18098/metrics',timeout=5) as response:
                metrics=json.load(response)
            with urllib.request.urlopen('http://localhost:18097/metrics',timeout=5) as response:
                raw=json.load(response)
            stats=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',
                'phase2acceptance-bronze-1','phase2acceptance-raw-1','phase2acceptance-clickhouse-1'],text=True)
            samples.append(dict(elapsed=time.monotonic()-started,bronze=metrics,raw=raw,resources=stats))
            (output/'samples.json').write_text(json.dumps(samples,indent=2),encoding='utf-8')
            if process.poll() is not None:
                assert process.returncode==0,'source failed'
                if source_stopped is None:source_stopped=time.monotonic()
                caught=all(v['offset_distance']==0 for v in metrics['partitions'].values())
                if caught and metrics['pending_intents']==0 and metrics['integrity_faults']==0:
                    break
                assert time.monotonic()-source_stopped<60,'catch-up deadline exceeded'
            time.sleep(1)
    report=dict(run_id=run,simulation_seconds=120,catchup_seconds=time.monotonic()-source_stopped,
                elapsed=time.monotonic()-started,samples=len(samples),result='PASS')
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
