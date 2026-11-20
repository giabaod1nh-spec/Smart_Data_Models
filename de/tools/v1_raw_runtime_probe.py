"""Isolated full-worker failure probe. Never touches project workload containers."""
import json
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen
from urllib.error import HTTPError
from confluent_kafka import Consumer, Producer, TopicPartition

def health():
    return json.load(urlopen("http://localhost:18097/health",timeout=20))

def main():
    path=Path("de/artifacts/v1_raw/acceptance/isolated-probe")
    path.mkdir(parents=True,exist_ok=True)
    samples=[]
    for _ in range(4):
        samples.append(dict(at=time.time(),health=health()))
        time.sleep(5)
    broker=Consumer({"bootstrap.servers":"localhost:39092","group.id":"de-kafka-raw-v1",
                     "enable.auto.commit":False})
    producer=Producer({"bootstrap.servers":"localhost:39092"})
    delivered=[]
    container="phase1acceptance-clickhouse-1"
    subprocess.run(["docker","pause",container],check=True,capture_output=True)
    try:
        for _ in range(6):
            producer.produce("traffic.observations.v1",key=b"probe",value=bytes([0,255]),
                on_delivery=lambda e,m:delivered.append((e,m.partition(),m.offset())))
        assert producer.flush(15)==0
        assert all(e is None for e,_,_ in delivered)
        time.sleep(3)
        offsets=broker.committed([TopicPartition("traffic.observations.v1",i) for i in range(3)],timeout=10)
        for error,partition,offset in delivered:
            current=next(p.offset for p in offsets if p.partition==partition)
            assert current<=offset,"committed past unpersisted probe record"
        try:
            urlopen("http://localhost:18097/ready",timeout=20)
            raise AssertionError("ready during persistence outage")
        except HTTPError as e:
            assert e.code==503
        degraded=health()
    finally:
        subprocess.run(["docker","unpause",container],check=True,capture_output=True)
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        state=health()
        if state["ready"] and state["buffered_records"]==0:
            break
        time.sleep(1)
    assert state["ready"]
    (path/"probe.json").write_text(json.dumps(dict(samples=samples,produced=[p[1:] for p in delivered],
                    degraded=degraded,recovered=state),indent=2),encoding="utf-8")
    print(json.dumps(dict(samples=len(samples),outage_not_ready=True,offset_safety=True,recovered=state),indent=2))
    broker.close()

if __name__=="__main__":
    main()

