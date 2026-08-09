"""Shared fixtures/helpers for Silver Plan 2 unit tests."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from de.silver.input_models import BronzeEntityInputRecord, BronzeRunInputRecord

REPO = Path(__file__).resolve().parents[3]
PAYLOAD_DIR = REPO / "contracts" / "entity" / "payloads"
EVENT_DIR = REPO / "contracts" / "events" / "examples"

FIXED_TS = datetime(2026, 7, 24, 7, 56, 9, tzinfo=timezone.utc)


def load_payload(name: str) -> dict:
    return json.loads((PAYLOAD_DIR / name).read_text(encoding="utf-8"))


def payload_json(name: str) -> str:
    return (PAYLOAD_DIR / name).read_text(encoding="utf-8")


def make_entity(
    *,
    entity_type: str,
    payload_name: str | None = None,
    payload_json_str: str | None = None,
    entity_id: str = "urn:ngsi-ld:VehicleSensor:A:NORTHBOUND",
    event_id: str = "evt-" + "a" * 60,
    scenario_id: str = "normal",
    simulation_run_id: str = "run-001",
    cycle_sequence: int = 1,
    simulation_time: float = 120.5,
    **overrides,
) -> BronzeEntityInputRecord:
    if payload_json_str is None:
        assert payload_name is not None
        payload_json_str = payload_json(payload_name)
    base = dict(
        topic="traffic.entity.observed",
        partition=0,
        offset=10,
        raw_ingestion_id="raw-" + "b" * 60,
        event_id=event_id,
        event_type="TrafficEntityObserved",
        contract_version="2.0.0",
        simulation_run_id=simulation_run_id,
        simulation_time=simulation_time,
        cycle_sequence=cycle_sequence,
        captured_at=FIXED_TS,
        entity_id=entity_id,
        entity_type=entity_type,
        entity_payload_hash="hash-" + "c" * 59,
        entity_payload_json=payload_json_str,
        bronze_canonical_hash="canon-" + "d" * 58,
        processed_at=FIXED_TS,
        scenario_id=scenario_id,
    )
    base.update(overrides)
    return BronzeEntityInputRecord(**base)


def make_run(**overrides) -> BronzeRunInputRecord:
    payload = (EVENT_DIR / "run-started-event.json").read_text(encoding="utf-8")
    base = dict(
        topic="traffic.simulation.run",
        partition=0,
        offset=1,
        raw_ingestion_id="raw-run-" + "e" * 55,
        event_type="TrafficSimulationRunStarted",
        contract_version="2.0.0",
        source="sumo",
        producer_id="visualize-traci",
        producer_session_id="session-example-001",
        simulation_run_id="00000000-0000-4000-8000-000000000001",
        started_at=FIXED_TS,
        scenario_id="normal",
        event_payload_json=payload,
        bronze_canonical_hash="canon-run-" + "f" * 54,
        processed_at=FIXED_TS,
    )
    base.update(overrides)
    return BronzeRunInputRecord(**base)


@pytest.fixture
def engine():
    from de.silver.engine import TransformationEngine

    return TransformationEngine()


class FakeQueryResult:
    """Minimal stand-in for a clickhouse_connect QueryResult."""

    def __init__(self, rows: list, columns: list[str]):
        self.result_rows = rows
        self.column_names = columns


class FakeClient:
    """Minimal ClickHouse client double for Silver Plan 3 reader/repository unit tests.

    ``responses`` is consumed in call order by ``.query()``; callers control exactly what
    "the database" returns for each successive query without needing to parse real SQL.
    """

    def __init__(self, responses: list | None = None):
        self._responses = list(responses or [])
        self.queries: list[tuple[str, dict]] = []
        self.commands: list[str] = []
        self.inserted: list[tuple[str, list, list[str]]] = []

    def query(self, sql: str, parameters: dict | None = None) -> FakeQueryResult:
        self.queries.append((sql, parameters or {}))
        if not self._responses:
            return FakeQueryResult([], [])
        return self._responses.pop(0)

    def command(self, sql: str, parameters: dict | None = None):
        self.commands.append(sql)
        return 1

    def insert(self, table: str, data: list, column_names: list[str]) -> None:
        self.inserted.append((table, data, column_names))

    def close(self) -> None:
        pass
