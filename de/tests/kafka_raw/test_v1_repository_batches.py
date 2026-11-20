from types import SimpleNamespace
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository

def test_large_reconciliation_uses_bounded_http_parameters():
    sizes=[]
    def query(sql,parameters):
        sizes.append(len(parameters["ids"]))
        return SimpleNamespace(result_rows=[],column_names=[])
    repo=V1ClickHouseRepository(SimpleNamespace(query=query))
    assert not repo.evidence([str(i) for i in range(1501)])
    assert max(sizes)==500
    assert len(sizes)==8

def test_logical_read_rejects_same_hash_with_different_content():
    import sqlite3
    from types import SimpleNamespace
    from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE deliveries (topic TEXT, partition_id INTEGER, offset_value INTEGER, raw_ingestion_id TEXT, state TEXT, destination TEXT, physical_record_hash TEXT)")
    db.execute("CREATE TABLE partition_progress (topic TEXT, partition_id INTEGER, safe_next_offset INTEGER)")
    db.execute("INSERT INTO deliveries VALUES ('traffic.observations.v1',0,4,'rid','COMMITTED','RAW','hash')")
    db.execute("INSERT INTO partition_progress VALUES ('traffic.observations.v1',0,5)")
    repo = V1ClickHouseRepository(None)
    repo.evidence = lambda ids: {'rid': [
        {'physical_record_hash': 'hash', 'destination': 'RAW', 'value_bytes_base64': 'YQ=='},
        {'physical_record_hash': 'hash', 'destination': 'RAW', 'value_bytes_base64': 'Yg=='},
    ]}
    rows = repo.logical_read(SimpleNamespace(db=db), 'traffic.observations.v1', 0, 4, 5)
    assert rows[0]['status'] == 'CONFLICT'
    assert rows[0]['record'] is None
