"""Kafka durable outbox — SQLite schema + PRAGMA (K-2b / K-5 RunStarted)."""
from __future__ import annotations

EVENT_KIND_ENTITY = "entity"
EVENT_KIND_RUN_STARTED = "run_started"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kafka_outbox (
    event_id TEXT PRIMARY KEY,
    simulation_run_id TEXT NOT NULL,
    cycle_sequence INTEGER NOT NULL,
    entity_sequence INTEGER NOT NULL,
    event_key TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_error TEXT,
    kafka_partition INTEGER,
    kafka_offset INTEGER,
    created_at TEXT NOT NULL,
    queued_at TEXT,
    acked_at TEXT,
    updated_at TEXT NOT NULL,
    event_kind TEXT NOT NULL DEFAULT 'entity',
    outbox_sequence INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_delivery
ON kafka_outbox(status, next_retry_at, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_sequence
ON kafka_outbox(outbox_sequence);
"""

MIGRATION_SQL = """
ALTER TABLE kafka_outbox ADD COLUMN event_kind TEXT NOT NULL DEFAULT 'entity';
ALTER TABLE kafka_outbox ADD COLUMN outbox_sequence INTEGER NOT NULL DEFAULT 0;
"""

STATUS_OUTBOXED = "OUTBOXED"
STATUS_QUEUED = "QUEUED"
STATUS_ACKED = "ACKED"
STATUS_FAILED_RETRYABLE = "FAILED_RETRYABLE"
STATUS_FAILED_PERMANENT = "FAILED_PERMANENT"

PENDING_STATUSES = (
    STATUS_OUTBOXED,
    STATUS_QUEUED,
    STATUS_FAILED_RETRYABLE,
)

REDRIVE_STATUSES = (
    STATUS_OUTBOXED,
    STATUS_FAILED_RETRYABLE,
)
