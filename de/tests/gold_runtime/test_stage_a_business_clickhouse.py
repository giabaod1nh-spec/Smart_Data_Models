"""Three-grain ClickHouse adapter tests with an opt-in physical probe."""

import os
import shutil
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from de.gold_runtime.business_clickhouse import IsolatedClickHouseRows
from de.gold_runtime.business_publication import BusinessPublisher, DivergentResult, PhysicalRow, result_rows
from de.gold_runtime.business_state import BusinessState, IsolatedBusinessStore
from de.gold_runtime.stage_a import open_clickhouse_target
from de.silver.incremental_contract import canonical_bytes
from de.tests.gold_runtime.test_stage_a_business_state import opened, result_payload, staged_v1


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


def test_loopback_disposable_target_supports_windows_empty_environment_value(monkeypatch):
    import clickhouse_connect

    calls = []
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kwargs: calls.append(kwargs))
    for key, value in {"": "1", "_HOST": "127.0.0.1", "_PORT": "18124", "_USER": "default",
                       "_DATABASE": "stage_a_probe_" + "0" * 16}.items():
        monkeypatch.setenv("STAGE_A_CH_PROBE" + key, value)
    monkeypatch.delenv("STAGE_A_CH_PROBE_PASSWORD", raising=False)
    open_clickhouse_target()
    assert calls[0]["password"] == ""
    monkeypatch.setenv("STAGE_A_CH_PROBE_HOST", "other-host")
    with pytest.raises(ValueError, match="PASSWORD"):
        open_clickhouse_target()


@pytest.mark.skipif(os.environ.get("STAGE_A_CH_PROBE") != "1",
                    reason="requires an explicitly allocated disposable ClickHouse endpoint and database")
def test_physical_three_grain_restart_and_exact_readback(staged_v1):
    import clickhouse_connect
    import psutil

    database = os.environ["STAGE_A_CH_PROBE_DATABASE"]
    deadline = time.monotonic() + 60

    def check_guards(*, startup=False):
        floor = 2_500_000_000 if startup else 1024**3
        assert psutil.virtual_memory().available >= floor, f"Host available memory fell below {floor} bytes"
        assert shutil.disk_usage(staged_v1).free >= 512 * 1024**2, "Scratch space fell below 512 MiB"
        assert time.monotonic() < deadline, "Isolated publication exceeded its wall budget"

    def connect():
        check_guards()
        return clickhouse_connect.get_client(host=os.environ["STAGE_A_CH_PROBE_HOST"],
            port=int(os.environ["STAGE_A_CH_PROBE_PORT"]), username=os.environ["STAGE_A_CH_PROBE_USER"],
            password=os.environ["STAGE_A_CH_PROBE_PASSWORD"], database=database,
            connect_timeout=2, send_receive_timeout=2, query_retries=0)

    check_guards(startup=True)

    class GuardedRows(IsolatedClickHouseRows):
        def read(self, result_id, *, limit):
            check_guards()
            return super().read(result_id, limit=limit)

        def insert(self, result_id, rows):
            check_guards()
            return super().insert(result_id, rows)

    class InterruptAfterFirstGrain(GuardedRows):
        def insert(self, result_id, rows):
            super().insert(result_id, rows[:1])
            raise TimeoutError("probe interruption after first ClickHouse INSERT")

    payload = result_payload()
    wanted = result_rows(payload)
    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger), connect() as client:
        target = InterruptAfterFirstGrain(client, database, max_query_seconds=2)
        target.initialize()
        state = BusinessState(store, "live", ledger.token)
        publisher = BusinessPublisher(state, target, clock=lambda: 0)
        assert state.finish(ledger, state.reserve_next(), "physical-proof", 60, canonical_bytes({}),
            definition_set="physical-probe-" + uuid4().hex, source_semantics="fixture-source",
            coverage_policy="fixture", population_digest="population", metadata_digest="physical-boundary")
        obligation = store.conn.execute("SELECT id,projection_hash FROM business_obligation").fetchone()
        result_id = publisher.save_computed(obligation["id"], obligation["projection_hash"], payload)
        scope = store.conn.execute("SELECT id FROM business_scope").fetchone()[0]
        assert publisher.publish_once() == "PENDING"
        assert target.read(result_id, limit=47) == wanted[:1] and publisher.effective_pointer(scope) is None

    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger), connect() as client:
        target = GuardedRows(client, database, max_query_seconds=2)
        publisher = BusinessPublisher(BusinessState(store, "live", ledger.token), target, clock=lambda: 5)

        def interrupt():
            raise RuntimeError("probe interruption after durable VERIFIED state")

        with pytest.raises(RuntimeError, match="durable VERIFIED"):
            publisher.publish_once(after_verify=interrupt)
        assert publisher.effective_pointer(scope) is None
        assert target.read(result_id, limit=47) == wanted
        assert target.verify_typed(result_id) == len(wanted)

    with opened(staged_v1, IsolatedBusinessStore) as (store, ledger), connect() as client:
        target = GuardedRows(client, database, max_query_seconds=2)
        publisher = BusinessPublisher(BusinessState(store, "live", ledger.token), target, clock=lambda: 10)
        assert publisher.recover() == 0 and publisher.publish_once() == "PUBLISHED"
        assert publisher.effective_pointer(scope) == (result_id, 1)
        assert target.read(result_id, limit=47) == wanted
    check_guards()
