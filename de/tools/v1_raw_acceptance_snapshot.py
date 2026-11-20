"""Persist reproducible local acceptance snapshots without deleting data."""
import argparse
import base64
import hashlib
import json
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen
import clickhouse_connect

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--run-id",required=True)
    p.add_argument("--clickhouse-port",type=int,default=8123)
    p.add_argument("--health-url",default="http://localhost:8097/health")
    p.add_argument("--outbox",default="Visualize/artifacts/kafka_outbox/v1-outbox.sqlite3")
    p.add_argument("--ledger",default="de/artifacts/kafka_raw_v1/ledger.sqlite3")
    p.add_argument("--bootstrap")
    a=p.parse_args()
    output=Path("de/artifacts/v1_raw/acceptance")/a.run_id
    output.mkdir(parents=True,exist_ok=True)
    def save(name,value):
        (output/name).write_text(json.dumps(value,indent=2,default=str),encoding="utf-8")
    client=clickhouse_connect.get_client(port=a.clickhouse_port)
    rows=client.query("SELECT * FROM smart_traffic.v1_raw_events WHERE simulation_run_id={run:String}",
                      parameters={"run":a.run_id})
    raw=[dict(zip(rows.column_names,r)) for r in rows.result_rows]
    outbox=sqlite3.connect(Path(a.outbox).resolve().as_uri()+"?mode=ro",uri=True)
    producer=outbox.execute("SELECT event_id,payload_json,status,kafka_partition,kafka_offset FROM kafka_outbox WHERE simulation_run_id=?",
                           (a.run_id,)).fetchall()
    by_event={}
    for row in raw:
        by_event.setdefault(row["event_id"],[]).append(row)
    matches=all(event in by_event and all(base64.b64decode(r["value_bytes_base64"])==text.encode()
                   for r in by_event[event]) for event,text,*_ in producer)
    kafka_checks=[]
    if a.bootstrap:
        from confluent_kafka import Consumer,TopicPartition
        broker=Consumer({"bootstrap.servers":a.bootstrap,"group.id":"phase1-audit-"+uuid.uuid4().hex,
                         "enable.auto.commit":False,"enable.auto.offset.store":False})
        expected={(r["topic"],r["partition"],r["offset"]):r for r in raw}
        starts={}
        for topic,partition,offset in expected:
            starts[(topic,partition)]=min(starts.get((topic,partition),offset),offset)
        broker.assign([TopicPartition(t,p,o) for (t,p),o in starts.items()])
        deadline=time.monotonic()+90
        while expected and time.monotonic()<deadline:
            msg=broker.poll(.5)
            if msg is None: continue
            if msg.error(): raise RuntimeError(str(msg.error()))
            identity=(msg.topic(),msg.partition(),msg.offset())
            row=expected.pop(identity,None)
            if row is None: continue
            expected_hash=row["payload_bytes_hash"]
            if isinstance(expected_hash,bytes): expected_hash=expected_hash.decode()
            actual=hashlib.sha256(msg.value() or b"").hexdigest()
            assert actual==expected_hash,"Kafka-to-Raw byte mismatch"
            kafka_checks.append(dict(topic=identity[0],partition=identity[1],offset=identity[2],sha256=actual))
        broker.close()
        assert not expected,"Kafka audit did not reach every run delivery"
        save("kafka-byte-audit.json",kafka_checks)
    ledger=sqlite3.connect(Path(a.ledger).resolve().as_uri()+"?mode=ro",uri=True)
    ledger_rows=ledger.execute("SELECT partition_id,state,count(*) FROM deliveries GROUP BY partition_id,state").fetchall()
    report=dict(run_id=a.run_id,producer_rows=len(producer),raw_physical_rows=len(raw),
        kafka_deliveries_verified=len(kafka_checks),
        complete_cycles=outbox.execute("SELECT count(DISTINCT cycle_sequence) FROM kafka_outbox WHERE simulation_run_id=?",(a.run_id,)).fetchone()[0],
        processing_interval=client.query("SELECT min(consumed_at),max(consumed_at),uniqExact(raw_ingestion_id) FROM smart_traffic.v1_raw_events WHERE simulation_run_id={run:String}",parameters={"run":a.run_id}).result_rows,
        distinct_event_ids=len(by_event),byte_match=matches,producer_statuses=sorted(set(r[2] for r in producer)),
        ledger_states=ledger_rows,partition_progress=ledger.execute("SELECT * FROM partition_progress").fetchall(),
        health=json.load(urlopen(a.health_url)),
        baseline=subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
        working_tree=subprocess.check_output(["git","status","--short"],text=True))
    save("reconciliation.json",report)
    save("schema.json",{name:client.query("SHOW CREATE TABLE smart_traffic."+name).result_rows
         for name in ("v1_raw_events","v1_quarantine_events","v1_raw_events_replay","v1_quarantine_events_replay")})
    save("raw-lineage.json",[{k:r[k] for k in ("event_id","topic","partition","offset","payload_bytes_hash")}
                           for r in raw])
    print(json.dumps(report,indent=2,default=str))
    ledger.close()
    outbox.close()
    client.close()

if __name__=="__main__":
    main()

