"""Final Phase 2 read boundary and live/replay isolation on real services."""
import hashlib
import os
import uuid
from pathlib import Path
import pytest
from confluent_kafka import Consumer,TopicPartition
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_ledger_store import V1LedgerReader
from de.kafka_raw.v1_replay import main as replay

pytestmark=pytest.mark.skipif(os.getenv("V1_RAW_FINAL")!="1",reason="requires completed isolated SUMO run")

def test_readonly_handover_and_replay_preserve_live_state(monkeypatch):
    import clickhouse_connect
    client=clickhouse_connect.get_client(port=18124)
    path=Path("de/artifacts/v1_raw/isolated-ledger-final.sqlite")
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    reader=V1LedgerReader(path)
    repository=V1ClickHouseRepository(client)
    logical=[]
    for partition in range(3):
        logical+=repository.logical_read(reader,"traffic.observations.v1",partition,0)
    assert sum(row["status"]=="CONFIRMED_RAW" for row in logical)==6000
    expected_quarantine=reader.db.execute("SELECT count(*) FROM deliveries WHERE destination='QUARANTINE'").fetchone()[0]
    assert expected_quarantine>=6
    assert sum(row["status"]=="CONFIRMED_QUARANTINE" for row in logical)==expected_quarantine
    assert all(row["safe_next_offset"]>row["identity"][2] for row in logical)
    assert all(row["record"] and "value_bytes_base64" in row["record"] for row in logical)
    assert len(repository.logical_read(reader,"traffic.observations.v1",1,1,3))==2
    broker=Consumer({"bootstrap.servers":"localhost:39092","group.id":"de-kafka-raw-v1","enable.auto.commit":False})
    def offsets():
        return [(p.partition,p.offset) for p in broker.committed(
            [TopicPartition("traffic.observations.v1",p) for p in range(3)],timeout=10)]
    def inventory():
        return [client.query("SELECT count(),uniqExact(raw_ingestion_id),groupBitXor(cityHash64(physical_record_hash)) FROM smart_traffic."+name).result_rows
                for name in ("v1_raw_events","v1_quarantine_events")]
    before=inventory()
    committed=offsets()
    monkeypatch.setenv("DE_V1_RAW_CLICKHOUSE_PORT","18124")
    replay(["--topic","traffic.observations.v1","--partition","1",
            "--start-offset","0","--end-offset","20",
            "--replay-run-id","final-"+uuid.uuid4().hex,"--bootstrap-servers","localhost:39092"])
    assert inventory()==before
    assert offsets()==committed
    assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
    reader.close()
    broker.close()
    client.close()

