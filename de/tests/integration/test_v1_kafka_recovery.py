"""Real Kafka/ClickHouse recovery in the dedicated acceptance stack."""
import json
import os
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
import pytest
from confluent_kafka import Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from de.kafka_raw.v1_consumer import V1RawKafkaConsumer
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_ledger_store import V1LedgerStore

pytestmark=pytest.mark.skipif(os.getenv("V1_RAW_KAFKA_INTEGRATION")!="1",reason="requires dedicated acceptance stack")

@pytest.mark.parametrize("failure_state",["PERSISTENCE_CONFIRMED","COMMITTED"])
def test_actual_broker_commit_and_ledger_failure(tmp_path,failure_state):
    import clickhouse_connect
    suffix=uuid.uuid4().hex
    topic="phase1-fault-"+suffix
    bootstrap="localhost:39092"
    admin=AdminClient({"bootstrap.servers":bootstrap})
    admin.create_topics([NewTopic(topic,1,1)])[topic].result()
    event=json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    payload=json.dumps(event).encode()
    key=f"{event['simulationRunId']}:{event['intersectionId']}".encode()
    producer=Producer({"bootstrap.servers":bootstrap})
    for value in (payload,bytes([0,255]),payload):
        producer.produce(topic,key=key,value=value,headers=[("dup",b"x"),("dup",None)])
    assert producer.flush(20)==0
    settings=SimpleNamespace(bootstrap_servers=bootstrap,topic=topic,group_id="phase1-group-"+suffix)
    client=clickhouse_connect.get_client(port=18124)
    ledger=V1LedgerStore(str(tmp_path/"ledger.sqlite"))
    repo=V1ClickHouseRepository(client,"fault-"+suffix)
    w=V1RawKafkaConsumer(settings,repo,ledger)
    w.start()
    deadline=time.monotonic()+30
    while len(w.pending)<3 and time.monotonic()<deadline:
        msg=w.consumer.poll(.2)
        if msg is not None: w._handle_msg(msg)
    assert len(w.pending)==3
    original=ledger.transition
    injected=[]
    def fail_once(ids,state,error=None):
        if state==failure_state and not injected:
            injected.append(True)
            raise OSError("controlled ledger failure "+state)
        return original(ids,state,error)
    ledger.transition=fail_once
    with pytest.raises(OSError): w.flush()
    committed=w.consumer.committed([TopicPartition(topic,0)],timeout=10)[0].offset
    if failure_state=="COMMITTED":
        assert committed==3
    else:
        assert committed<0
    # Simulate lost ownership: do not flush on close; durable evidence survives.
    w.on_lost(w.consumer,[TopicPartition(topic,0)])
    w.consumer.close()
    ledger.transition=original
    recovered=V1RawKafkaConsumer(settings,repo,ledger)
    recovered.start()
    deadline=time.monotonic()+30
    while not recovered.assigned and time.monotonic()<deadline:
        msg=recovered.consumer.poll(.2)
        if msg is not None: recovered._handle_msg(msg)
    if failure_state=="PERSISTENCE_CONFIRMED":
        while len(recovered.pending)<3 and time.monotonic()<deadline:
            msg=recovered.consumer.poll(.2)
            if msg is not None: recovered._handle_msg(msg)
        recovered.flush()
    else:
        assert ledger.get(topic,0,0)["state"]=="COMMITTED"
    assert recovered.consumer.committed([TopicPartition(topic,0)],timeout=10)[0].offset==3
    rows=repo.logical_read(ledger,topic,0,0,3)
    assert len(rows)==3
    assert [r["status"] for r in rows]==["CONFIRMED_RAW","CONFIRMED_QUARANTINE","CONFIRMED_RAW"]
    assert all(r["safe_next_offset"]==3 for r in rows)
    recovered.on_lost(recovered.consumer,[TopicPartition(topic,0)])
    recovered.consumer.close()
    client.close()
    ledger.close()

def test_actual_disposable_topic_recreation_is_rejected(tmp_path):
    import clickhouse_connect
    from de.kafka_raw.v1_ledger_store import IntegrityFault
    suffix=uuid.uuid4().hex
    topic="phase1-disposable-"+suffix
    settings=SimpleNamespace(bootstrap_servers="localhost:39092",topic=topic,group_id="phase1-group-"+suffix)
    admin=AdminClient({"bootstrap.servers":settings.bootstrap_servers})
    admin.create_topics([NewTopic(topic,1,1)])[topic].result()
    client=clickhouse_connect.get_client(port=18124)
    ledger=V1LedgerStore(str(tmp_path/"identity.sqlite"))
    repo=V1ClickHouseRepository(client,"identity-"+suffix)
    first=V1RawKafkaConsumer(settings,repo,ledger)
    first.start()
    old_identity=first.identity
    first.consumer.close()
    ledger.db.execute("INSERT INTO partition_progress VALUES(?,0,?,0,0)",(topic,old_identity))
    # Only this empty, randomly named test-owned topic is recreated.
    admin.delete_topics([topic])[topic].result()
    deadline=time.monotonic()+20
    while topic in admin.list_topics(timeout=5).topics and time.monotonic()<deadline:
        time.sleep(.2)
    admin.create_topics([NewTopic(topic,1,1)])[topic].result()
    second=V1RawKafkaConsumer(settings,repo,ledger)
    second.start()
    assert second.identity!=old_identity
    with pytest.raises(IntegrityFault):
        second.on_assign(second.consumer,[TopicPartition(topic,0)])
    second.consumer.close()
    assert ledger.db.execute("SELECT topic_identity FROM partition_progress").fetchone()[0]==old_identity
    ledger.close()
    client.close()

