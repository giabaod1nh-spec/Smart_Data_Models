from types import SimpleNamespace
from datetime import datetime
from pathlib import Path
import re

import pytest

from de.silver.config import SilverSettings
from de.silver.repositories import (InvalidTargetTableError, ReplayModeGuardError,
    SchemaMismatchError, SilverClickHouseRepository, V1_TABLES)


class Client:
    def __init__(self):
        self.writes = []

    def query(self, sql, parameters):
        if "SELECT *" in sql:
            return SimpleNamespace(column_names=["simulation_run_id", "run_generation"],
                                   result_rows=self.read_rows)
        if "system.columns" in sql:
            ddl = (Path(__file__).resolve().parents[2] / "migrations" /
                   "008_create_silver_v1.sql").read_text()
            rows = []
            for match in re.finditer(
                    r"CREATE TABLE IF NOT EXISTS smart_traffic\.(\w+)\s*\(\s*(.*?)\s*\)\s*ENGINE",
                    ddl, re.S):
                table, body = match.groups()
                for line in body.splitlines():
                    name, kind = line.strip().rstrip(",").split(None, 1)
                    rows.append((table, name, kind))
            if getattr(self, "corrupt_schema", False):
                rows[0] = (rows[0][0], rows[0][1], "UInt8")
            return SimpleNamespace(result_rows=rows)
        return SimpleNamespace(result_rows=[(name,) for name in parameters["names"]])

    def insert(self, table, data, column_names):
        self.writes.append((table, data, column_names))


def test_v1_static_targets_and_replay_isolation():
    repo = SilverClickHouseRepository(SilverSettings())
    repo._client = Client()
    repo.verify_v1_schema()
    repo.client.corrupt_schema = True
    with pytest.raises(SchemaMismatchError, match="column/type mismatch"):
        repo.verify_v1_schema()
    repo.client.corrupt_schema = False
    assert len(V1_TABLES) == 10
    repo.insert_v1_rows("silver_run_dimension_v1", [{"simulation_run_id": "r"}])
    assert repo.client.writes[0][0] == "smart_traffic.silver_run_dimension_v1"
    repo.insert_v1_rows("silver_run_dimension_v1", [{"simulation_run_id": "r",
        "created_at": "2026-01-01T00:00:00Z"}])
    assert isinstance(repo.client.writes[1][1][0][1], datetime)
    repo.client.read_rows = [("r", 1), ("r", 1)]
    assert repo.reconcile_v1_row("silver_run_dimension_v1",
        {"simulation_run_id": "r", "run_generation": 1})
    repo.client.read_rows = [("r", 2)]
    with pytest.raises(Exception, match="content differs"):
        repo.reconcile_v1_row("silver_run_dimension_v1",
            {"simulation_run_id": "r", "run_generation": 1})
    with pytest.raises(InvalidTargetTableError):
        repo.insert_v1_rows("silver_dim_run", [{"simulation_run_id": "r"}])
    with pytest.raises(ReplayModeGuardError):
        repo.insert_v1_rows("silver_run_dimension_v1", [{"simulation_run_id": "r"}], replay_run_id="x")
    replay = SilverClickHouseRepository(SilverSettings(destination_mode="replay", namespace="replay:x", replay_run_id="x"))
    replay._client = Client()
    replay.insert_v1_rows("silver_run_dimension_v1", [{"simulation_run_id": "r"}], replay_run_id="x")
    assert replay.client.writes[0][0] == "smart_traffic.silver_run_dimension_replay_v1"
    assert replay.client.writes[0][2][0] == "replay_run_id"


def test_immutable_dimension_reconciliation_ignores_only_processing_time():
    class DimensionClient:
        def query(self, sql, parameters):
            return SimpleNamespace(column_names=["simulation_run_id", "run_generation",
                "manifest_identity", "created_at"], result_rows=[("r", 1, "hash",
                datetime.fromisoformat("2026-01-01T00:00:00"))])

    repo = SilverClickHouseRepository(SilverSettings(), client=DimensionClient())
    row = dict(simulation_run_id="r", run_generation=1, manifest_identity="hash",
               created_at="2026-01-02T00:00:00Z")
    assert repo.reconcile_v1_row("silver_run_dimension_v1", row)
    with pytest.raises(Exception, match="manifest_identity"):
        repo.reconcile_v1_row("silver_run_dimension_v1",
                              dict(row, manifest_identity="changed"))


def test_shared_topology_retains_first_archive_locator_but_checks_hash():
    class TopologyClient:
        def query(self, sql, parameters):
            return SimpleNamespace(column_names=["topology_digest", "catalog_path", "catalog_hash"],
                                   result_rows=[("digest", "/runs/first/catalog.json", "hash")])

    repo = SilverClickHouseRepository(SilverSettings(), client=TopologyClient())
    row = dict(topology_digest="digest", catalog_path="/runs/second/catalog.json", catalog_hash="hash")
    assert repo.reconcile_v1_row("silver_topology_dimension_v1", row)
    with pytest.raises(Exception, match="catalog_hash"):
        repo.reconcile_v1_row("silver_topology_dimension_v1", dict(row, catalog_hash="changed"))
