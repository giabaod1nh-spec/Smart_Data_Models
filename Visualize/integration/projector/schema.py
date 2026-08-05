"""SQLite schema for Orion Projector (K-3)."""
from __future__ import annotations

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS projector_event_ledger (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    simulation_run_id TEXT NOT NULL,
    cycle_sequence INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    replacement_event_id TEXT,
    status TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE(topic, partition, offset)
);

CREATE TABLE IF NOT EXISTS projector_entity_state (
    simulation_run_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    last_cycle_sequence INTEGER NOT NULL,
    last_event_id TEXT NOT NULL,
    last_payload_hash TEXT NOT NULL,
    last_applied_at TEXT NOT NULL,
    last_simulation_time REAL,
    PRIMARY KEY (simulation_run_id, entity_id)
);

CREATE TABLE IF NOT EXISTS projector_active_runs (
    source TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    producer_session_id TEXT NOT NULL,
    simulation_run_id TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (source, producer_id)
);

CREATE TABLE IF NOT EXISTS projector_partition_commits (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (topic, partition)
);

CREATE INDEX IF NOT EXISTS idx_ledger_status_completed
ON projector_event_ledger(status, completed_at);

CREATE INDEX IF NOT EXISTS idx_entity_state_run
ON projector_entity_state(simulation_run_id);

CREATE TABLE IF NOT EXISTS projector_runtime_state (
    state_key TEXT PRIMARY KEY,
    simulation_run_id TEXT,
    scenario_id TEXT,
    simulation_time REAL,
    status TEXT NOT NULL,
    last_applied_cycle INTEGER NOT NULL DEFAULT 0,
    freshness_seconds REAL,
    updated_at TEXT NOT NULL
);
"""

STATUS_APPLIED = "APPLIED"
STATUS_COALESCED_SUPERSEDED = "COALESCED_SUPERSEDED"
STATUS_STALE_SKIPPED = "STALE_SKIPPED"
STATUS_FENCE_SKIPPED = "FENCE_SKIPPED"
STATUS_SIM_TIME_REGRESSION = "SIM_TIME_REGRESSION"
STATUS_INVALID_JSON = "INVALID_JSON"
STATUS_INVALID_SCHEMA = "INVALID_SCHEMA"
STATUS_INVALID_PROTOCOL = "INVALID_PROTOCOL"
STATUS_DLQ_FAILED = "DLQ_FAILED"
STATUS_QUARANTINED = "QUARANTINED"
STATUS_FAILED = "FAILED"
STATUS_FAILED_PERMANENT = "FAILED_PERMANENT"
STATUS_NODE_PARTIAL_APPLIED = "NODE_PARTIAL_APPLIED"
STATUS_PENDING = "PENDING"

COMPLETED_STATUSES = (
    STATUS_APPLIED,
    STATUS_COALESCED_SUPERSEDED,
    STATUS_STALE_SKIPPED,
    STATUS_FENCE_SKIPPED,
    STATUS_SIM_TIME_REGRESSION,
    STATUS_INVALID_JSON,
    STATUS_INVALID_SCHEMA,
    STATUS_INVALID_PROTOCOL,
    STATUS_QUARANTINED,
    STATUS_FAILED,
    STATUS_FAILED_PERMANENT,
    STATUS_NODE_PARTIAL_APPLIED,
)

RUNTIME_STATE_KEY = "current"

ACTIVE = "ACTIVE"
INACTIVE = "INACTIVE"
