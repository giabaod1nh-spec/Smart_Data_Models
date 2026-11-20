"""Actual ClickHouse migration/binary/reconciliation acceptance. Explicit opt-in."""
import base64
import os
import sqlite3
from pathlib import Path
import pytest
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_ledger_store import V1LedgerStore, IntegrityFault

pytestmark=pytest.mark.skipif(os.getenv("V1_RAW_INTEGRATION")!="1",reason="requires local ClickHouse")

def test_actual_clickhouse_binary_and_logical_read(tmp_path):
    import clickhouse_connect
    client=clickhouse_connect.get_client(host="localhost",port=int(os.getenv("V1_RAW_TEST_PORT","8123")))
    sql=Path("de/migrations/006_create_kafka_raw_v1.sql").read_text()
    for _ in range(2):
        for statement in sql.split(";"):
            if statement.strip(): client.command(statement)
    import uuid
    replay=uuid.uuid4().hex
    repo=V1ClickHouseRepository(client,replay)
    ledger=V1LedgerStore(str(tmp_path/"ledger.db"))
    records=[V1RawValidator().classify(V1Record("test-binary",0,i,None,value,
                 (("duplicate",bytes([0,255])),("duplicate",None)),topic_identity="isolated-test"))
             for i,value in enumerate((None,b"",bytes([0,255,128])))]
    ledger.receive_batch(records,"binary")
    repo.insert(records)
    repo.insert(records) # intentional identical physical retry
    assert len(repo.reconcile(records))==3
    ledger.transition([r.row["raw_ingestion_id"] for r in records],"PERSISTENCE_CONFIRMED")
    rows=repo.logical_read(ledger,"test-binary",0,0,3)
    assert len(rows)==3
    assert all(r["status"]=="CONFIRMED_QUARANTINE" and r["physical_rows"]==2 for r in rows)
    assert base64.b64decode(rows[2]["record"]["value_bytes_base64"])==bytes([0,255,128])
    assert rows[0]["record"]["value_is_null"]==1
    assert rows[1]["record"]["value_is_null"]==0
    assert len(repo.logical_read(ledger,"test-binary",0,1,2))==1
    from de.kafka_raw.v1_models import V1ClassifiedRecord
    conflicting=V1ClassifiedRecord("RAW",records[0].row)
    repo.insert([conflicting])
    with pytest.raises(IntegrityFault):
        repo.reconcile(records)
    assert repo.logical_read(ledger,"test-binary",0,0,1)[0]["status"]=="CONFLICT"
    client.close()
    ledger.close()

def test_actual_availability_and_absence_roundtrip():
    import copy,json,uuid,clickhouse_connect
    from contracts.traffic_observation import canonical_json
    from de.kafka_raw.v1_models import digest
    event=json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    direction=next(iter(event["payload"]["directions"].values()))
    direction["vehicle_count"]=0
    direction["average_speed_kmh"]=None
    direction["queue_length_m"]=None
    direction["metric_availability"]={"average_speed_kmh":"UNAVAILABLE","queue_length_m":"NOT_APPLICABLE"}
    direction.pop("arrival_rate_pcu_per_sec",None)
    event["payloadHash"]=digest(canonical_json(event["payload"]).encode())
    value=json.dumps(event).encode()
    record=V1Record("metric-test",0,0,f"{event['simulationRunId']}:{event['intersectionId']}".encode(),value,topic_identity="isolated-metric-test")
    classified=V1RawValidator().classify(record)
    assert classified.destination=="RAW",classified.error_detail
    client=clickhouse_connect.get_client(port=int(os.getenv("V1_RAW_TEST_PORT","8123")))
    repo=V1ClickHouseRepository(client,"metrics-"+uuid.uuid4().hex)
    repo.insert([classified])
    stored=repo.evidence([record.raw_ingestion_id])[record.raw_ingestion_id][0]
    restored=json.loads(base64.b64decode(stored["value_bytes_base64"]))
    assert restored==event
    restored_direction=next(iter(restored["payload"]["directions"].values()))
    assert restored_direction["vehicle_count"]==0
    assert restored_direction["average_speed_kmh"] is None
    assert restored_direction["metric_availability"]["queue_length_m"]=="NOT_APPLICABLE"
    assert "arrival_rate_pcu_per_sec" not in restored_direction
    client.close()

