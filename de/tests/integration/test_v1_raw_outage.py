"""Isolated ClickHouse outage recovery with real inserts and controlled Kafka commit."""
import os
import subprocess
from types import SimpleNamespace
import pytest
from de.tests.kafka_raw.test_v1_consumer_recovery import worker
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository

pytestmark=pytest.mark.skipif(os.getenv("V1_RAW_ISOLATED_OUTAGE")!="1",reason="requires isolated ClickHouse container")
def test_real_outage_holds_commit_then_recovers(tmp_path):
    import clickhouse_connect
    from pathlib import Path
    import uuid
    client=clickhouse_connect.get_client(port=18123,connect_timeout=1,send_receive_timeout=2)
    for statement in Path("de/migrations/006_create_kafka_raw_v1.sql").read_text().split(";"):
        if statement.strip(): client.command(statement)
    w=worker(tmp_path,[0,3])
    w.repo=V1ClickHouseRepository(client,"outage-"+uuid.uuid4().hex)
    subprocess.run(["docker","pause","de-v1-raw-acceptance-clickhouse"],check=True,capture_output=True)
    try:
        with pytest.raises(Exception):
            w.flush()
        assert not w.consumer.commits
        assert w.ledger.get("test",0,0)["state"]=="PERSISTENCE_ATTEMPTED"
    finally:
        subprocess.run(["docker","unpause","de-v1-raw-acceptance-clickhouse"],check=True,capture_output=True)
    w.flush()
    assert w.consumer.commits[-1].offset==4
    assert not w.pending
    w.ledger.close()
    client.close()

