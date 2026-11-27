"""Three-grain ClickHouse adapter tests with an opt-in physical probe."""

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from de.gold_runtime.business_clickhouse import IsolatedClickHouseRows
from de.gold_runtime.business_publication import DivergentResult, PhysicalRow


_ROWS = (PhysicalRow("direction", "a" * 64, b'{"payload":"direction"}'),
         PhysicalRow("intersection", "b" * 64, b'{"payload":"intersection"}'),
         PhysicalRow("network", "c" * 64, b'{"payload":"network"}'))


class FixtureClient:
    def __init__(self):
        self.commands = []
        self.tables = {}
        self.interrupt_after_write = False

    def command(self, sql, **_):
        self.commands.append(sql)

    def query(self, sql, *, parameters, settings):
        assert 1 <= settings["max_execution_time"] <= 5
        if "system.tables" in sql:
            return SimpleNamespace(result_rows=[(name.split(".")[1],) for name in self.tables])
        table = sql.split("FROM ")[1].split()[0]
        values = [row for row in self.tables.get(table, []) if row[0] == parameters["rid"]]
        return SimpleNamespace(result_rows=[(key, digest, len(body.encode()), body)
                                             for _, key, digest, body in values[:parameters["n"]]])

    def insert(self, table, rows, *, column_names, settings):
        assert tuple(column_names) == ("result_id", "row_key", "row_hash", "payload_json")
        assert settings["async_insert"] == 0 and settings["wait_end_of_query"] == 1
        self.tables.setdefault(table, []).extend(rows)
        if self.interrupt_after_write:
            self.interrupt_after_write = False
            raise OperationalError("fixture disconnect after one physical table write")


def test_adapter_limits_isolation_partial_write_and_exact_row_hash():
    client = FixtureClient()
    with pytest.raises(ValueError, match="dedicated"):
        IsolatedClickHouseRows(client, "smart_traffic")
    target = IsolatedClickHouseRows(client, "stage_a_probe_" + "0" * 16)
    target.initialize()
    assert len(client.commands) == 3 and all("MergeTree" in sql for sql in client.commands)
    rid = "f" * 64
    client.interrupt_after_write = True
    with pytest.raises(ConnectionError, match="uncertain"):
        target.insert(rid, _ROWS)
    assert target.read(rid, limit=47) == _ROWS[:1]
    target.insert(rid, _ROWS[1:])
    assert target.read(rid, limit=47) == _ROWS
    key = next(iter(client.tables))
    client.tables[key][0] = (rid, "a" * 64, "b" * 64, "{}")
    with pytest.raises(DivergentResult, match="length or hash"):
        target.read(rid, limit=47)


@pytest.mark.skipif(os.environ.get("STAGE_A_CH_PROBE") != "1",
                    reason="requires an explicitly allocated disposable ClickHouse endpoint and database")
def test_physical_three_grain_restart_and_exact_readback():
    import clickhouse_connect

    database = os.environ["STAGE_A_CH_PROBE_DATABASE"]

    def connect():
        return clickhouse_connect.get_client(host=os.environ["STAGE_A_CH_PROBE_HOST"],
            port=int(os.environ["STAGE_A_CH_PROBE_PORT"]), username=os.environ["STAGE_A_CH_PROBE_USER"],
            password=os.environ["STAGE_A_CH_PROBE_PASSWORD"], database=database,
            connect_timeout=2, send_receive_timeout=5, query_retries=0)

    result_id = uuid4().hex + uuid4().hex
    with connect() as client:
        target = IsolatedClickHouseRows(client, database)
        target.initialize()
        target.insert(result_id, _ROWS[:1])
    with connect() as client:
        target = IsolatedClickHouseRows(client, database)
        assert target.read(result_id, limit=47) == _ROWS[:1]
        target.insert(result_id, _ROWS[1:])
        assert target.read(result_id, limit=47) == _ROWS
