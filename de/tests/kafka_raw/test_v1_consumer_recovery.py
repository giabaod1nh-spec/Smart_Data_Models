"""Worker safety tests using explicit Kafka and persistence failures."""
from types import SimpleNamespace
import pytest
from de.kafka_raw.v1_consumer import V1RawKafkaConsumer
from de.kafka_raw.v1_ledger_store import V1LedgerStore, IntegrityFault
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator

class Repo:
    def __init__(self):
        self.rows={}
        self.partial=False
        self.lose_response=False
    def reconcile(self,records):
        return [r for r in records if r.row["raw_ingestion_id"] in self.rows]
    def insert(self,records):
        for r in (records[:1] if self.partial else records):
            self.rows[r.row["raw_ingestion_id"]]=r
        if self.lose_response: raise OSError("lost response")

class Broker:
    def __init__(self):
        self.commits=[]
        self.fail=False
    def commit(self,offsets,asynchronous):
        if self.fail: raise OSError("commit failed")
        self.commits.extend(offsets)
        return [SimpleNamespace(error=None) for _ in offsets]

def worker(tmp_path, offsets):
    w=object.__new__(V1RawKafkaConsumer)
    w.repo=Repo()
    w.ledger=V1LedgerStore(str(tmp_path/"ledger.sqlite"))
    w.consumer=Broker()
    w.assigned={("test",0)}
    w.pending=[V1RawValidator().classify(V1Record("test",0,o,b"k",b"{}",topic_identity="uuid")) for o in offsets]
    w.counts=dict(retries=0,persisted=0)
    return w

def test_valid_broker_offset_gaps_progress(tmp_path):
    w=worker(tmp_path,[0,3,9])
    w.flush()
    assert w.consumer.commits[0].offset==10
    assert not w.pending

def test_partial_insert_blocks_unresolved_delivery(tmp_path):
    w=worker(tmp_path,[0,3,9])
    w.repo.partial=True
    with pytest.raises(RuntimeError): w.flush()
    assert w.consumer.commits[0].offset==1
    assert [r.row["offset"] for r in w.pending]==[3,9]
    w.repo.partial=False
    w.flush()
    assert w.consumer.commits[-1].offset==10

def test_lost_response_reconciles(tmp_path):
    w=worker(tmp_path,[4,8])
    w.repo.lose_response=True
    w.flush()
    assert w.consumer.commits[-1].offset==9

def test_commit_failure_preserves_confirmed_rows(tmp_path):
    w=worker(tmp_path,[0])
    w.consumer.fail=True
    with pytest.raises(OSError): w.flush()
    assert w.ledger.get("test",0,0)["state"]=="COMMIT_ELIGIBLE"
    assert len(w.pending)==1
    w.consumer.fail=False
    w.flush()
    assert w.consumer.commits[-1].offset==1

def test_lost_ownership_forbids_commit(tmp_path):
    w=worker(tmp_path,[0])
    w.assigned.clear()
    with pytest.raises(RuntimeError): w.flush()
    assert not w.consumer.commits

def assignment_broker(committed, low=0, high=20):
    return SimpleNamespace(get_watermark_offsets=lambda *a,**k:(low,high),
        committed=lambda *a,**k:[SimpleNamespace(offset=committed)],
        assign=lambda p:None)

def test_broker_commit_success_before_ledger_ack_recovers(tmp_path):
    w=worker(tmp_path,[0,3])
    w.identity="uuid"
    w.ledger.db.execute("INSERT INTO partition_progress VALUES('test',0,'uuid',0,0)")
    original=w.ledger.transition
    def crash(ids,state,error=None):
        if state=="COMMITTED": raise OSError("crash after Kafka ack")
        original(ids,state,error)
    w.ledger.transition=crash
    with pytest.raises(OSError): w.flush()
    assert w.consumer.commits[-1].offset==4
    assert w.ledger.get("test",0,0)["state"]=="COMMIT_ELIGIBLE"
    w.ledger.transition=original
    w.repo.evidence=lambda ids:{rid:[dict(physical_record_hash=w.repo.rows[rid].row["physical_record_hash"],
        destination=w.repo.rows[rid].destination)] for rid in ids}
    w.on_assign(assignment_broker(4),[SimpleNamespace(topic="test",partition=0)])
    assert w.ledger.get("test",0,0)["state"]=="COMMITTED"
    assert w.ledger.get("test",0,3)["state"]=="COMMITTED"

@pytest.mark.parametrize("committed,low,old_uuid",[(0,0,"changed"),(0,5,"uuid"),(21,0,"uuid")])
def test_startup_rejects_identity_retention_and_out_of_range(tmp_path,committed,low,old_uuid):
    w=worker(tmp_path,[])
    w.identity="uuid"
    w.ledger.db.execute("INSERT INTO partition_progress VALUES('test',0,?,0,0)",(old_uuid,))
    with pytest.raises(IntegrityFault):
        w.on_assign(assignment_broker(committed,low),[SimpleNamespace(topic="test",partition=0)])

def test_startup_refuses_broker_history_without_ledger(tmp_path):
    w=worker(tmp_path,[])
    w.identity="uuid"
    with pytest.raises(IntegrityFault):
        w.on_assign(assignment_broker(5),[SimpleNamespace(topic="test",partition=0)])

def test_revoke_and_lost_clear_ownership(tmp_path):
    w=worker(tmp_path,[0])
    w.on_revoke(w.consumer,[SimpleNamespace(topic="test",partition=0)])
    assert not w.assigned and not w.pending
    w.assigned.add(("test",0))
    w.on_lost(w.consumer,[SimpleNamespace(topic="test",partition=0)])
    assert not w.assigned

