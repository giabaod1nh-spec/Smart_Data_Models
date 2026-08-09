"""Silver Plan 2 — typed Bronze input records (pure, no I/O)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Union


@dataclass(frozen=True)
class BronzeEntityInputRecord:
    topic: str
    partition: int
    offset: int
    raw_ingestion_id: str
    event_id: str
    event_type: str  # TrafficEntityObserved
    contract_version: str
    simulation_run_id: str
    simulation_time: float
    cycle_sequence: int
    captured_at: datetime
    entity_id: str
    entity_type: str
    entity_payload_hash: str
    entity_payload_json: str
    bronze_canonical_hash: str
    processed_at: datetime
    # Required by BRONZE_TO_SILVER_CONTRACT §5 (Plan 2 snippet omitted; contract wins).
    scenario_id: str


@dataclass(frozen=True)
class BronzeRunInputRecord:
    topic: str
    partition: int
    offset: int
    raw_ingestion_id: str
    event_type: str  # TrafficSimulationRunStarted
    contract_version: str
    source: str
    producer_id: str
    producer_session_id: str
    simulation_run_id: str
    started_at: datetime
    scenario_id: str
    event_payload_json: str
    bronze_canonical_hash: str
    processed_at: datetime


BronzeInputRecord = Union[BronzeEntityInputRecord, BronzeRunInputRecord]
