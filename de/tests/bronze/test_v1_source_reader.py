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
    rows, end, identity = LineageResolver.read_v1_batch(repo, str(path), topic, 0, 100, 10)
    assert [r["identity"][2] for r in rows] == [100, 101, 104, 105]
    assert (end, identity) == (110, "uuid")
    assert LineageResolver.read_v1_batch(repo, str(path), topic, 0, 106, 4)[0] == []
    repo.client.orphan = True
    with pytest.raises(IntegrityFault, match="lacks confirmed ledger"):
        LineageResolver.read_v1_batch(repo, str(path), topic, 0, 100, 10)
    ledger.close()
