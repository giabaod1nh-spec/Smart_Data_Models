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

