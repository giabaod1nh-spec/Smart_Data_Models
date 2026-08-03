"""Fake ClickHouse client helpers for Silver Plan 3 unit tests."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeQueryResult:
    result_rows: list[tuple] = field(default_factory=list)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self._fact_index: dict[str, list[dict]] = {}
        self._dim_index: dict[str, list[dict]] = {}
        self._quarantine_index: dict[str, list[dict]] = {}
        self._ledger_index: dict[str, list[dict]] = {}
        self._tables: dict[str, list[dict]] = {}
        self.inserted: list[tuple[str, list]] = []

    def query(self, sql: str, parameters: dict | None = None) -> FakeQueryResult:
        parameters = parameters or {}
        s = " ".join(sql.split()).lower()
        if s.startswith("select 1"):
            return FakeQueryResult([(1,)])
        if "distinct topic, partition" in s:
            table = "bronze_run_events" if "bronze_run_events" in s else "bronze_entity_events"
            rows = self._tables.get(table, [])
            seen = sorted({(r["topic"], r["partition"]) for r in rows})
            return FakeQueryResult(seen)
        if s.startswith("select min(offset)"):
            table = "bronze_run_events" if "bronze_run_events" in sql else "bronze_entity_events"
            rows = [
                r
                for r in self._tables.get(table, [])
                if r["topic"] == parameters.get("t") and r["partition"] == parameters.get("p")
            ]
            if not rows:
                return FakeQueryResult([(None,)])
            return FakeQueryResult([(min(int(r["offset"]) for r in rows),)])
        if s.startswith("select max(offset)"):
            table = "bronze_run_events" if "bronze_run_events" in sql else "bronze_entity_events"
            rows = [
                r
                for r in self._tables.get(table, [])
                if r["topic"] == parameters.get("t") and r["partition"] == parameters.get("p")
            ]
            if not rows:
                return FakeQueryResult([(None,)])
            return FakeQueryResult([(max(int(r["offset"]) for r in rows),)])
        if "from silver_processing_ledger" in s:
            ns = parameters.get("ns")
            ids = [parameters[k] for k in parameters if k.startswith("id")]
            out = []
            for row in self._ledger_index.get(ns, []):
                if row["source_bronze_event_id"] in ids:
                    out.append(
                        (
                            row["source_bronze_event_id"],
                            row["payload_hash"],
                            row["disposition"],
                            row["target_table"],
                            row.get("raw_ingestion_id", ""),
                        )
                    )
            return FakeQueryResult(out)
        if "order by offset" in s:
            table = "bronze_run_events" if "bronze_run_events" in sql else "bronze_entity_events"
            rows = [
                r
                for r in self._tables.get(table, [])
                if r["topic"] == parameters.get("t")
                and int(r["partition"]) == int(parameters.get("p"))
                and int(r["offset"]) > int(parameters.get("a"))
            ]
            if "e" in parameters:
                rows = [r for r in rows if int(r["offset"]) < int(parameters["e"])]
            rows = sorted(rows, key=lambda r: int(r["offset"]))[: int(parameters.get("plim", 500))]
            cols = (
                list(rows[0].keys())
                if rows
                else []
            )
            # Ensure stable column order matching reader
            from de.silver.readers import BronzeReader

            col_order = (
                BronzeReader.RUN_COLUMNS
                if table == "bronze_run_events"
                else BronzeReader.ENTITY_COLUMNS
            )
            result_rows = [tuple(r.get(c) for c in col_order) for r in rows]
            return FakeQueryResult(result_rows)
        return FakeQueryResult([])

    def insert(self, table: str, data: list, column_names: list | None = None) -> None:
        self.inserted.append((table, data))

    def close(self) -> None:
        return None
