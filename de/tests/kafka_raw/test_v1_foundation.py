import base64
import json
from pathlib import Path
import pytest
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator
from de.kafka_raw.v1_ledger_store import V1LedgerStore, IntegrityFault

def record(value=b"{}", key=b"k", headers=(), offset=0):
    return V1Record("traffic.observations.v1",0,offset,key,value,headers,topic_identity="test")

@pytest.mark.parametrize("value,code", [(None,"NULL_VALUE"),(b"","INVALID_JSON"),(bytes([255]),"NON_UTF8"),(b"[]","INVALID_JSON")])
def test_bad_wire_preserves_bytes(value,code):
    result=V1RawValidator().classify(record(value))
    assert result.destination=="QUARANTINE"
    assert result.error_code==code
    assert base64.b64decode(result.row["value"])==(value or b"")

def test_transport_integrity_includes_nulls_keys_headers_and_timestamp():
    original=record()
    variants=[record(None),record(b"",None),record(key=b"x"),
              record(headers=(("x",b"a"),("x",None))),
              V1Record(original.topic,0,0,b"k",b"{}",broker_timestamp_ms=1)]
    assert all(v.physical_record_hash!=original.physical_record_hash for v in variants)
    assert record(None).physical_record_hash!=record(b"").physical_record_hash
    assert record(headers=(("x",b"a"),("x",b"b"))).physical_record_hash!=record(headers=(("x",b"b"),("x",b"a"))).physical_record_hash

def test_valid_fixture_uses_shared_contract():
    event=json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    raw=json.dumps(event).encode()
    r=record(raw,f"{event['simulationRunId']}:{event['intersectionId']}".encode())
    result=V1RawValidator().classify(r)
    assert result.destination=="RAW",result.error_detail
    assert base64.b64decode(result.row["value"])==raw
    assert result.row["event_id"]==event["eventId"]

def test_ledger_never_commits_unconfirmed(tmp_path):
    ledger=V1LedgerStore(str(tmp_path/"ledger.db"))
    result=V1RawValidator().classify(record())
    ledger.receive_batch([result],"b")
    rid=result.row["raw_ingestion_id"]
    with pytest.raises(IntegrityFault):
        ledger.transition([rid],"COMMITTED")
    ledger.transition([rid],"PERSISTENCE_ATTEMPTED")
    ledger.transition([rid],"PERSISTENCE_CONFIRMED")
    ledger.transition([rid],"COMMIT_ELIGIBLE")
    ledger.transition([rid],"COMMITTED")
    assert ledger.get(record().topic,0,0)["state"]=="COMMITTED"
    ledger.close()

def test_ledger_rejects_same_offset_transport_change(tmp_path):
    ledger=V1LedgerStore(str(tmp_path/"ledger.db"))
    validator=V1RawValidator()
    ledger.receive_batch([validator.classify(record())],"b")
    with pytest.raises(IntegrityFault):
        ledger.receive_batch([validator.classify(record(key=b"changed"))],"c")
    ledger.close()

@pytest.mark.parametrize("change", [
    lambda e:e.update(eventType="TrafficSimulationRunStarted"),
    lambda e:e.update(schemaVersion=2),
    lambda e:e.update(eventId="0"*64),
    lambda e:e.update(payloadHash="0"*64),
    lambda e:e.update(capturedAt="bad"),
    lambda e:e.pop("scenarioId"),
])
def test_invalid_envelope_quarantined(change):
    event=json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    change(event)
    result=V1RawValidator().classify(record(json.dumps(event).encode()))
    assert result.destination=="QUARANTINE"

def test_same_semantic_event_different_offsets_preserved():
    event=json.loads(Path("contracts/observation/fixtures/valid_code_derived.json").read_text())
    key=f"{event['simulationRunId']}:{event['intersectionId']}".encode()
    a=V1RawValidator().classify(record(json.dumps(event).encode(),key,offset=0))
    event["capturedAt"]="2026-11-19T00:00:00Z"
    b=V1RawValidator().classify(record(json.dumps(event).encode(),key,offset=1))
    assert a.destination==b.destination=="RAW"
    assert a.row["raw_ingestion_id"]!=b.row["raw_ingestion_id"]
    assert a.row["event_id"]==b.row["event_id"]

def test_readonly_ledger_cannot_mutate(tmp_path):
    from de.kafka_raw.v1_ledger_store import V1LedgerReader
    import sqlite3
    path=tmp_path/"ledger.db"
    writer=V1LedgerStore(str(path))
    reader=V1LedgerReader(path)
    assert reader.db.execute("SELECT count(*) FROM deliveries").fetchone()[0]==0
    with pytest.raises(sqlite3.OperationalError):
        reader.db.execute("DELETE FROM deliveries")
    reader.close()
    writer.close()

