"""Deterministic Gold 3 runtime fixtures and in-memory fakes."""
from __future__ import annotations

import shutil
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import pytest

# Prefer a repo-local temp root: default pytest temp under %TEMP% can be Access Denied
# on locked Windows environments.
_REPO_TMP_ROOT = Path(__file__).resolve().parents[3] / "de" / "artifacts" / "gold_runtime_test_tmp"

from de.gold_runtime.checkpoint_store import GoldRuntimeStore
from de.gold_runtime.config import (
    SOURCE_TABLE_CAMERA,
    SOURCE_TABLE_DIM_APPROACH,
    SOURCE_TABLE_DIM_INTERSECTION,
    SOURCE_TABLE_DIM_RUN,
    SOURCE_TABLE_DIM_SCENARIO,
    SOURCE_TABLE_INTERSECTION,
    SOURCE_TABLE_SIGNAL,
    SOURCE_TABLE_TRAFFIC,
    GoldSettings,
)
from de.gold_runtime.cursor import FactCursor, build_receipt
from de.gold_runtime.dimensions import DIM_BUSINESS_KEY, DimensionCandidate
from de.gold_runtime.repositories import (
    TARGET_IDENTITY_COLUMNS,
    ExistingRow,
    ExistingState,
    SchemaReport,
    WriteReceipt,
    logical_identity,
)
from de.gold_runtime.silver_readers import SOURCE_COLUMNS

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
RUN_ID = "run-1"
SCENARIO_ID = "scenario-1"
INTERSECTION_ID = "J1"
DIRECTIONS = ("N", "S", "E", "W")
TOPIC = "traffic.entity-events.v2"


def _processed_at(index: int) -> datetime:
    return NOW + timedelta(milliseconds=index)


def traffic_row(
    *,
    direction: str = "N",
    simulation_time_sec: float = 10.0,
    offset: int = 1,
    partition: int = 0,
    quality_status: str = "VALID",
    payload_hash: Optional[str] = None,
    simulation_run_id: str = RUN_ID,
) -> dict:
    event = f"traffic-{direction}-{simulation_time_sec}-{offset}"
    return {
        "simulation_run_id": simulation_run_id,
        "cycle_sequence": int(simulation_time_sec),
        "simulation_time_sec": float(simulation_time_sec),
        "intersection_id": INTERSECTION_ID,
        "direction": direction,
        "source_entity_id": f"TrafficFlowObserved:{INTERSECTION_ID}:{direction}",
        "vehicle_count": 10,
        "pcu_equivalent": 10.0,
        "average_speed_kmh": 25.0,
        "queue_length_m": 40.0,
        "waiting_vehicle_count": 4,
        "occupancy_pct": 55.0,
        "arrival_rate_pcu_per_sec": 1.5,
        "traffic_status": "CONGESTED",
        "spillback_risk": 0,
        "dominant_waiting_reason": "",
        "scenario_id": SCENARIO_ID,
        "source_bronze_event_id": event,
        "source_raw_ingestion_id": f"raw-{event}",
        "source_topic": TOPIC,
        "source_partition": partition,
        "source_offset": offset,
        "source_payload_hash": payload_hash or f"hash-{event}",
        "quality_status": quality_status,
        "quality_flags": "",
        "processed_at": _processed_at(offset),
        "migration_version": "k9-silver-v1",
    }


def intersection_row(
    *,
    simulation_time_sec: float = 10.0,
    offset: int = 500,
    total_vehicle_count: Optional[int] = 40,
    current_phase: str = "NS_GREEN",
    simulation_run_id: str = RUN_ID,
) -> dict:
    event = f"intersection-{simulation_time_sec}-{offset}"
    return {
        "simulation_run_id": simulation_run_id,
        "cycle_sequence": int(simulation_time_sec),
        "simulation_time_sec": float(simulation_time_sec),
        "intersection_id": INTERSECTION_ID,
        "source_entity_id": f"TrafficIntersection:{INTERSECTION_ID}",
        "overall_traffic_status": "CONGESTED",
        "derived_traffic_state": "CONGESTED",
        "current_phase": current_phase,
        "has_active_incident": 0,
        "has_spillback": 0,
        "is_box_blocked": 0,
        "total_vehicle_count": total_vehicle_count,
        "scenario_id": SCENARIO_ID,
        "source_bronze_event_id": event,
        "source_raw_ingestion_id": f"raw-{event}",
        "source_topic": TOPIC,
        "source_partition": 1,
        "source_offset": offset,
        "source_payload_hash": f"hash-{event}",
        "quality_flags": "",
        "processed_at": _processed_at(offset),
        "migration_version": "k9-silver-v1",
    }


def signal_row(
    *,
    direction: str = "N",
    simulation_time_sec: float = 10.0,
    offset: int = 900,
    signal_status: str = "GREEN",
    current_phase: str = "NS_GREEN",
    green_duration_sec: Optional[float] = 30.0,
    simulation_run_id: str = RUN_ID,
) -> dict:
    event = f"signal-{direction}-{simulation_time_sec}-{offset}"
    return {
        "simulation_run_id": simulation_run_id,
        "cycle_sequence": int(simulation_time_sec),
        "simulation_time_sec": float(simulation_time_sec),
        "intersection_id": INTERSECTION_ID,
        "direction": direction,
        "source_entity_id": f"TrafficLight:{INTERSECTION_ID}:{direction}",
        "signal_status": signal_status,
        "current_phase": current_phase,
        "green_duration_sec": green_duration_sec,
        "red_duration_sec": 25.0,
        "yellow_duration_sec": 5.0,
        "timing_mode": "FIXED_TIME",
        "scenario_id": SCENARIO_ID,
        "source_bronze_event_id": event,
        "source_raw_ingestion_id": f"raw-{event}",
        "source_topic": TOPIC,
        "source_partition": 2,
        "source_offset": offset,
        "source_payload_hash": f"hash-{event}",
        "quality_flags": "",
        "processed_at": _processed_at(offset),
        "migration_version": "k9-silver-v1",
    }


def camera_row(*, simulation_time_sec: float = 10.0, offset: int = 1300) -> dict:
    event = f"camera-{simulation_time_sec}-{offset}"
    return {
        "simulation_run_id": RUN_ID,
        "cycle_sequence": int(simulation_time_sec),
        "simulation_time_sec": float(simulation_time_sec),
        "intersection_id": INTERSECTION_ID,
        "source_entity_id": f"Camera:{INTERSECTION_ID}",
        "vehicle_count": 8,
        "average_speed_kmh": 24.0,
        "occupancy_pct": 50.0,
        "traffic_status": "CONGESTED",
        "incident_detected": 0,
        "confidence": 0.9,
        "recommended_signal_action": "KEEP",
        "incident_type": "NONE",
        "incident_severity": "NONE",
        "scenario_id": SCENARIO_ID,
        "source_bronze_event_id": event,
        "source_raw_ingestion_id": f"raw-{event}",
        "source_topic": TOPIC,
        "source_partition": 3,
        "source_offset": offset,
        "source_payload_hash": f"hash-{event}",
        "quality_flags": "",
        "processed_at": _processed_at(offset),
        "migration_version": "k9-silver-v1",
    }


DIM_ROWS: dict[str, list[dict]] = {
    SOURCE_TABLE_DIM_RUN: [
        {
            "simulation_run_id": RUN_ID,
            "scenario_id": SCENARIO_ID,
            "seed": "42",
            "producer_id": "producer-1",
            "started_at": NOW,
            "ended_at": None,
            "run_status": "RUNNING",
            "contract_version": "v2",
            "node_count": 4,
            "source_bronze_run_id": "bronze-run-1",
            "created_at": NOW,
            "updated_at": NOW,
        }
    ],
    SOURCE_TABLE_DIM_SCENARIO: [
        {"scenario_id": SCENARIO_ID, "description": "peak hour", "created_at": NOW}
    ],
    SOURCE_TABLE_DIM_INTERSECTION: [
        {
            "intersection_id": INTERSECTION_ID,
            "intersection_name": "Junction 1",
            "latitude": 10.77,
            "longitude": 106.7,
            "network_zone": "CORE",
            "connected_intersections": ["J2", "J0"],
            "source_hash": "a" * 64,
            "source_bronze_event_id": "bronze-int-1",
            "created_at": NOW,
            "updated_at": NOW,
        }
    ],
    SOURCE_TABLE_DIM_APPROACH: [
        {
            "intersection_id": INTERSECTION_ID,
            "direction": direction,
            "source_bronze_event_id": f"bronze-app-{direction}",
            "created_at": NOW,
            "updated_at": NOW,
        }
        for direction in DIRECTIONS
    ],
}


def build_silver_dataset(
    *, times: Sequence[float] = tuple(float(t) for t in range(0, 180, 10))
) -> dict[str, list[dict]]:
    traffic: list[dict] = []
    intersection: list[dict] = []
    signal: list[dict] = []
    camera: list[dict] = []
    offset = 1
    for time_index, moment in enumerate(times):
        for direction in DIRECTIONS:
            traffic.append(
                traffic_row(direction=direction, simulation_time_sec=moment, offset=offset)
            )
            offset += 1
            signal.append(
                signal_row(
                    direction=direction,
                    simulation_time_sec=moment,
                    offset=900 + time_index * 10 + DIRECTIONS.index(direction),
                )
            )
        intersection.append(
            intersection_row(simulation_time_sec=moment, offset=500 + time_index)
        )
        camera.append(camera_row(simulation_time_sec=moment, offset=1300 + time_index))
    return {
        SOURCE_TABLE_TRAFFIC: traffic,
        SOURCE_TABLE_INTERSECTION: intersection,
        SOURCE_TABLE_SIGNAL: signal,
        SOURCE_TABLE_CAMERA: camera,
    }


# ── fakes ───────────────────────────────────────────────────────────────────


def _cursor_key(row: Mapping[str, Any]) -> tuple:
    return FactCursor.from_row(row).key()


class FakeSilverReader:
    def __init__(self, dataset: Mapping[str, Sequence[dict]], dims: Optional[Mapping[str, Sequence[dict]]] = None) -> None:
        self.dataset = {name: sorted(rows, key=_cursor_key) for name, rows in dataset.items()}
        self.dims = {name: list(rows) for name, rows in (dims or DIM_ROWS).items()}
        self.initialized = True
        self.read_calls: list[tuple] = []
        self.fail_next: Optional[Exception] = None

    # lifecycle
    def connect(self) -> None:
        self.initialized = True

    def close(self) -> None:
        self.initialized = False

    def ping(self) -> bool:
        return True

    def verify_source_schema(self, tables: Optional[Sequence[str]] = None) -> Any:
        return SchemaReport(tuple(self.dataset), True, ())

    # reads
    def snapshot_upper_bound(self, source_name: str) -> Optional[FactCursor]:
        rows = self.dataset.get(source_name) or []
        if not rows:
            return None
        return FactCursor.from_row(rows[-1])

    def read_fact_batch(self, source_name, cursor, upper_bound, limit=None):
        self._maybe_fail()
        rows = [
            row for row in self.dataset.get(source_name, [])
            if FactCursor.from_row(row).is_after(cursor)
            and not FactCursor.from_row(row).is_after(upper_bound)
        ]
        rows = rows[: (limit or 500)]
        return tuple(rows), build_receipt(source_name, rows)

    def read_window_rows(
        self,
        source_name,
        *,
        simulation_run_id,
        window_start_sim_sec,
        window_end_sim_sec,
        upper_bound,
        limit=None,
    ):
        self._maybe_fail()
        self.read_calls.append(
            (source_name, simulation_run_id, window_start_sim_sec, window_end_sim_sec)
        )
        rows = [
            row for row in self.dataset.get(source_name, [])
            if row["simulation_run_id"] == simulation_run_id
            and window_start_sim_sec <= float(row["simulation_time_sec"]) < window_end_sim_sec
            and not FactCursor.from_row(row).is_after(upper_bound)
        ]
        return tuple(rows), build_receipt(source_name, rows)

    def max_simulation_time(self, source_name, *, simulation_run_id, upper_bound):
        rows = [
            row for row in self.dataset.get(source_name, [])
            if row["simulation_run_id"] == simulation_run_id
            and not FactCursor.from_row(row).is_after(upper_bound)
        ]
        if not rows:
            return None
        return max(float(row["simulation_time_sec"]) for row in rows)

    def discover_runs(self, upper_bounds, *, limit: int = 100):
        runs = {
            (row["simulation_run_id"], row["scenario_id"])
            for rows in self.dataset.values()
            for row in rows
        }
        return tuple(sorted(runs))

    def read_run_events(self, *, simulation_run_id, upper_bound, limit=500):
        return ()

    def read_dimension_rows(self, source_name, cursor, limit=None):
        from de.gold_runtime.cursor import DimensionCursor
        from de.gold_runtime.silver_readers import DIMENSION_CURSOR_MAP

        rows = list(self.dims.get(source_name, []))
        if not rows:
            return (), cursor
        effective_col, key_cols = DIMENSION_CURSOR_MAP[source_name]
        remaining = [
            row for row in rows
            if DimensionCursor(
                row[effective_col], "", "\x1f".join(str(row[c]) for c in key_cols)
            ).is_after(cursor)
        ]
        if not remaining:
            return (), cursor
        last = remaining[-1]
        next_cursor = DimensionCursor(
            last[effective_col],
            str(last.get("source_hash", "")),
            "\x1f".join(str(last[c]) for c in key_cols),
        )
        return tuple(remaining), next_cursor

    def _maybe_fail(self) -> None:
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error


class FakeGoldRepository:
    def __init__(self, settings: GoldSettings) -> None:
        self.settings = settings
        self.tables: dict[str, list[Any]] = {}
        self.dimensions: dict[tuple, DimensionCandidate] = {}
        self.ledger: list[Any] = []
        self.fail_next: Optional[Exception] = None
        self.suppress_insert: set[str] = set()
        self.insert_calls: list[str] = []

    # lifecycle
    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def ping(self) -> bool:
        return True

    def verify_schema(self) -> SchemaReport:
        return SchemaReport((), True, ())

    # writes
    def _insert(self, table: str, rows: Sequence[Any]) -> WriteReceipt:
        self.insert_calls.append(table)
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error
        if table in self.suppress_insert:
            return WriteReceipt(table, len(rows), 0, len(rows))
        self.tables.setdefault(table, []).extend(rows)
        return WriteReceipt(table, len(rows), len(rows), 0)

    def insert_facts(self, rows: Sequence[Any]) -> tuple[WriteReceipt, ...]:
        grouped: dict[str, list[Any]] = {}
        for row in rows:
            table = (
                "gold_fact_traffic_window"
                if type(row).__name__ == "GoldFactTrafficWindow"
                else "gold_fact_intersection_window"
            )
            grouped.setdefault(table, []).append(row)
        return tuple(self._insert(table, group) for table, group in sorted(grouped.items()))

    def insert_comparisons(self, rows):
        return self._insert("gold_fact_traffic_comparison", rows)

    def insert_signal_windows(self, rows):
        return self._insert("gold_fact_signal_operation_window", rows)

    def insert_kpis(self, rows):
        return self._insert("gold_fact_kpi_result", rows)

    def upsert_dimensions(self, candidates: Sequence[DimensionCandidate]):
        receipts = []
        for candidate in candidates:
            if candidate.identity in self.dimensions:
                continue
            self.dimensions[candidate.identity] = candidate
            receipts.append(WriteReceipt(candidate.target_table, 1, 1, 0))
        return tuple(receipts)

    def find_dimension_versions(self, candidates):
        return {
            candidate.identity: self.dimensions[candidate.identity].source_hash
            for candidate in candidates
            if candidate.identity in self.dimensions
        }

    def verify_dimension_hashes(self, expected):
        stored = {identity: candidate.source_hash for identity, candidate in self.dimensions.items()}
        return tuple(
            sorted(
                identity for identity, value in expected.items() if stored.get(identity) != value
            )
        )

    def find_existing(self, batch_id, identities, *, revision_seq: int = 0) -> ExistingState:
        wanted = set(identities)
        found: list[ExistingRow] = []
        for table, rows in self.tables.items():
            if table not in TARGET_IDENTITY_COLUMNS:
                continue
            for row in rows:
                identity = logical_identity(table, row)
                if identity in wanted and int(row.revision_seq) == int(revision_seq):
                    found.append(
                        ExistingRow(identity, str(row.source_set_hash), int(row.revision_seq))
                    )
        return ExistingState(batch_id, tuple(found))

    def record_ledger(self, row) -> WriteReceipt:
        self.ledger.append(row)
        return WriteReceipt("gold_processing_ledger", 1, 1, 0)

    def find_ledger_dispositions(self, namespace, source_set_hashes):
        return {
            (row.source_set_hash, int(row.revision_seq)): row.disposition
            for row in self.ledger
            if row.source_set_hash in set(source_set_hashes)
        }


class CountingEngine:
    """Wraps the real Gold 2 engine and counts public transform calls."""

    def __init__(self) -> None:
        from de.gold.engine import GoldTransformationEngine

        self._engine = GoldTransformationEngine()
        self.calls = 0

    def transform(self, records, context):
        self.calls += 1
        return self._engine.transform(records, context)


# ── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_path():
    """Workspace-local temporary directory (avoids Windows Access Denied on pytest-of-*)."""
    _REPO_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix=f"g3-{uuid.uuid4().hex[:8]}-", dir=str(_REPO_TMP_ROOT)))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def make_settings(tmp_path, **overrides: Any) -> GoldSettings:
    values: dict[str, Any] = {
        "traffic_expected_cadence_sec": 10.0,
        "intersection_expected_cadence_sec": 10.0,
        "signal_expected_cadence_sec": 10.0,
        "checkpoint_path": str(tmp_path / "gold-runtime.sqlite3"),
        "instance_lock_path": str(tmp_path / "gold-runtime.lock"),
    }
    values.update(overrides)
    return GoldSettings(**values).validate_all()


@pytest.fixture
def settings(tmp_path) -> GoldSettings:
    return make_settings(tmp_path)


@pytest.fixture
def store(tmp_path) -> Iterable[GoldRuntimeStore]:
    instance = GoldRuntimeStore(tmp_path / "runtime.sqlite3")
    instance.open()
    yield instance
    instance.close()


@pytest.fixture
def dataset() -> dict[str, list[dict]]:
    return build_silver_dataset()


@pytest.fixture
def reader(dataset) -> FakeSilverReader:
    return FakeSilverReader(dataset)


@pytest.fixture
def repository(settings) -> FakeGoldRepository:
    return FakeGoldRepository(settings)


@pytest.fixture
def processor(settings, reader, repository, store):
    from de.gold_runtime.processor import GoldProcessor

    engine = CountingEngine()
    instance = GoldProcessor(
        settings,
        reader=reader,
        repository=repository,
        store=store,
        engine=engine,
        clock=lambda: NOW,
        lock_held=True,
    )
    instance.engine = engine
    return instance
