"""Additive upgrade proof on isolated ClickHouse with actual V2 sentinel data."""
import os
from pathlib import Path
import uuid
import pytest

pytestmark=pytest.mark.skipif(os.getenv("V1_RAW_UPGRADE")!="1",reason="requires isolated ClickHouse")
def test_v2_history_unchanged_by_v1_migration():
    import clickhouse_connect
    client=clickhouse_connect.get_client(port=18124)
    historical=Path("de/migrations/002_create_kafka_raw_events.sql").read_text()
    for statement in historical.split(";"):
        statement="\n".join(line for line in statement.splitlines() if not line.strip().startswith("--"))
        if statement.strip(): client.command(statement)
    sentinel="phase1-v2-preservation-"+uuid.uuid4().hex
    client.command("INSERT INTO smart_traffic.kafka_raw_events (topic,partition,offset) VALUES ({t:String},0,123)",
                   parameters={"t":sentinel})
    schema=client.query("SHOW CREATE TABLE smart_traffic.kafka_raw_events").result_rows
    before=client.query("SELECT * FROM smart_traffic.kafka_raw_events WHERE topic={t:String}",
                        parameters={"t":sentinel}).result_rows
    assert len(before)==1
    for _ in range(2):
        for statement in Path("de/migrations/006_create_kafka_raw_v1.sql").read_text().split(";"):
            if statement.strip(): client.command(statement)
    assert client.query("SHOW CREATE TABLE smart_traffic.kafka_raw_events").result_rows==schema
    assert client.query("SELECT * FROM smart_traffic.kafka_raw_events WHERE topic={t:String}",
                         parameters={"t":sentinel}).result_rows==before
    client.close()

