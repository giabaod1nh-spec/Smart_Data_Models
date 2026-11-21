"""Bounded V1 Raw source contract without a second physical-resolution engine."""
from types import SimpleNamespace

import pytest

from de.bronze.lineage_resolver import LineageResolver
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository
from de.kafka_raw.v1_ledger_store import V1LedgerStore, IntegrityFault
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator


class Client:
    def __init__(self, rows, orphan=False):
        self.rows = rows
        self.orphan = orphan

    def query(self, sql, parameters=None):
        if "SELECT DISTINCT" in sql:
            dest = "QUARANTINE" if "quarantine" in sql else "RAW"
            ids = [(r["raw_ingestion_id"],) for r in self.rows
                   if r["destination"] == dest and
                   parameters["start"] <= r["offset"] < parameters["end"]]
            if self.orphan and dest == "RAW":
                ids.append(("orphan",))
            return SimpleNamespace(result_rows=ids)
        raise AssertionError(sql)


def test_sparse_and_empty_verified_ranges(tmp_path):
    topic = "traffic.observations.v1"
    path = tmp_path / "ledger.sqlite3"
    ledger = V1LedgerStore(str(path))
    validator = V1RawValidator()
    classified = [validator.classify(V1Record(topic, 0, off, None, None, topic_identity="uuid"))
                  for off in (100, 101, 104, 105)]
    ledger.receive_batch(classified, "batch")
    ledger.transition([c.row["raw_ingestion_id"] for c in classified], "PERSISTENCE_CONFIRMED")
    ledger.db.execute("INSERT INTO partition_progress VALUES (?,?,?,?,?)",
                      (topic, 0, "uuid", 110, 110))
    repo = V1ClickHouseRepository(Client([]))
    physical = [repo.materialize(c) for c in classified]
    repo.client.rows = physical
    repo.evidence = lambda ids: {rid: [r for r in physical if r["raw_ingestion_id"] == rid]
                                 for rid in ids}
    rows, end, identity, safe = LineageResolver.read_v1_batch(repo, str(path), topic, 0, 100, 10)
    assert [r["identity"][2] for r in rows] == [100, 101, 104, 105]
    assert (end, identity, safe) == (110, "uuid", 110)
    assert LineageResolver.read_v1_batch(repo, str(path), topic, 0, 106, 4)[0] == []
    repo.client.orphan = True
    with pytest.raises(IntegrityFault, match="lacks confirmed ledger"):
        LineageResolver.read_v1_batch(repo, str(path), topic, 0, 100, 10)
    ledger.close()


def test_concurrent_safe_boundary_advance_keeps_batch_snapshot_consistent(tmp_path):
    topic,path,ledger,repo,physical=_one_delivery(tmp_path)
    original=repo.logical_read
    def advance_during_read(reader, *args):
        # A separate writer commits a newer safe boundary after the reader's BEGIN.
        ledger.db.execute('UPDATE partition_progress SET safe_next_offset=20, committed_next_offset=20 WHERE topic=? AND partition_id=0',(topic,))
        return original(reader,*args)
    repo.logical_read=advance_during_read
    rows,end,identity,safe=LineageResolver.read_v1_batch(repo,str(path),topic,0,10,1)
    assert (len(rows),end,identity,safe)==(1,11,'uuid',11)
    assert ledger.db.execute('SELECT safe_next_offset FROM partition_progress').fetchone()[0]==20
    ledger.close()


def _one_delivery(tmp_path, *, value=b'{}'):
    topic='traffic.observations.v1';path=tmp_path/'ledger.sqlite3';ledger=V1LedgerStore(str(path))
    classified=V1RawValidator().classify(V1Record(topic,0,10,None,value,topic_identity='uuid'))
    ledger.receive_batch([classified],'batch')
    ledger.transition([classified.row['raw_ingestion_id']],'PERSISTENCE_CONFIRMED')
    ledger.db.execute('INSERT INTO partition_progress VALUES (?,?,?,?,?)',(topic,0,'uuid',11,11))
    repo=V1ClickHouseRepository(Client([]));physical=repo.materialize(classified);repo.client.rows=[physical]
    repo.evidence=lambda ids:{physical['raw_ingestion_id']:[physical]}
    return topic,path,ledger,repo,physical


def test_missing_unresolved_identity_boundary_and_raw_fault_block_source(tmp_path):
    topic,path,ledger,repo,physical=_one_delivery(tmp_path)
    repo.evidence=lambda ids:{physical['raw_ingestion_id']:[]}
    with pytest.raises(IntegrityFault,match='unresolved'):
        LineageResolver.read_v1_batch(repo,str(path),topic,0,10,1)
    repo.evidence=lambda ids:{physical['raw_ingestion_id']:[physical]}
    changed=dict(physical,topic_identity='different')
    repo.evidence=lambda ids:{physical['raw_ingestion_id']:[changed]}
    with pytest.raises(IntegrityFault,match='physical conflict|identity'):
        LineageResolver.read_v1_batch(repo,str(path),topic,0,10,1)
    with pytest.raises(IntegrityFault,match='exceeds'):
        LineageResolver.read_v1_batch(repo,str(path),topic,0,12,1)
    ledger.db.execute("INSERT INTO integrity_faults VALUES ('fault','detail','now')")
    with pytest.raises(IntegrityFault,match='integrity fault'):
        LineageResolver.read_v1_batch(repo,str(path),topic,0,10,1)
    ledger.close()


def test_confirmed_raw_quarantine_becomes_skip_without_observation(tmp_path):
    from de.bronze.checkpoint_store import V1SemanticStore
    from de.bronze.processor import BronzeProcessor
    class Destination:
        def __init__(self): self.outputs=[]
        def persist_v1_outputs(self,outputs): self.outputs.extend(outputs)
    topic,path,ledger,raw_repo,physical=_one_delivery(tmp_path,value=b'not-json')
    assert physical['destination']=='QUARANTINE'
    rows,end,_,_=LineageResolver.read_v1_batch(raw_repo,str(path),topic,0,10,1)
    assert rows[0]['status']=='CONFIRMED_QUARANTINE'
    store=V1SemanticStore(tmp_path/'bronze.sqlite');destination=Destination()
    disposition=BronzeProcessor.materialize_v1_delivery(store,destination,'live',rows[0]['record'],None)
    assert disposition=='RAW_QUARANTINE_SKIPPED'
    assert store.db.execute('SELECT count(*) FROM v1_identity').fetchone()[0]==0
    assert store.db.execute("SELECT count(*) FROM v1_intents WHERE disposition='RAW_QUARANTINE_SKIPPED' AND processing_status='COMPLETED'").fetchone()[0]==1
    store.close();ledger.close()
