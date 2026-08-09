"""Deterministic bronze_canonical_hash for replay parity."""
from __future__ import annotations

import hashlib
from typing import Any, Dict

from contracts.canonical_json import canonical_hash, canonical_json

from de.bronze import (
    BRONZE_SCHEMA_VERSION,
    PROCESSOR_NAME,
    PROCESSOR_VERSION,
)


def bronze_ingestion_id(raw_ingestion_id: str, processor_version: str, destination: str) -> str:
    material = f"{raw_ingestion_id}|{processor_version}|{destination}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def entity_canonical_hash(row: Dict[str, Any]) -> str:
    payload = {
        "topic": row["topic"],
        "partition": row["partition"],
        "offset": row["offset"],
        "raw_ingestion_id": row["raw_ingestion_id"],
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "contract_version": row["contract_version"],
        "event_version": row["event_version"],
        "source": row["source"],
        "producer_id": row.get("producer_id"),
        "producer_session_id": row.get("producer_session_id"),
        "simulation_run_id": row["simulation_run_id"],
        "simulation_time": row["simulation_time"],
        "scenario_id": row.get("scenario_id"),
        "node_id": row.get("node_id"),
        "cycle_sequence": row["cycle_sequence"],
        "entity_sequence": row["entity_sequence"],
        "cycle_entity_count": row["cycle_entity_count"],
        "node_entity_count": row.get("node_entity_count"),
        "captured_at": _ts_iso(row.get("captured_at")),
        "entity_id": row["entity_id"],
        "entity_type": row["entity_type"],
        "entity_payload_hash": row["entity_payload_hash"],
        "event_payload_json": row["event_payload_json"],
        "upstream_duplicate_event_id": int(row.get("upstream_duplicate_event_id") or 0),
        "processor_name": row.get("processor_name", PROCESSOR_NAME),
        "processor_version": row.get("processor_version", PROCESSOR_VERSION),
        "bronze_schema_version": row.get("bronze_schema_version", BRONZE_SCHEMA_VERSION),
        "destination": "STORED",
    }
    return canonical_hash(payload)


def run_canonical_hash(row: Dict[str, Any]) -> str:
    payload = {
        "topic": row["topic"],
        "partition": row["partition"],
        "offset": row["offset"],
        "raw_ingestion_id": row["raw_ingestion_id"],
        "event_type": row["event_type"],
        "contract_version": row["contract_version"],
        "event_version": row["event_version"],
        "source": row["source"],
        "producer_id": row["producer_id"],
        "producer_session_id": row["producer_session_id"],
        "simulation_run_id": row["simulation_run_id"],
        "started_at": _ts_iso(row.get("started_at")),
        "scenario_id": row.get("scenario_id"),
        "event_payload_json": row["event_payload_json"],
        "processor_name": row.get("processor_name", PROCESSOR_NAME),
        "processor_version": row.get("processor_version", PROCESSOR_VERSION),
        "bronze_schema_version": row.get("bronze_schema_version", BRONZE_SCHEMA_VERSION),
        "destination": "STORED",
    }
    return canonical_hash(payload)


def quarantine_canonical_hash(row: Dict[str, Any]) -> str:
    payload = {
        "topic": row["topic"],
        "partition": row["partition"],
        "offset": row["offset"],
        "raw_ingestion_id": row["raw_ingestion_id"],
        "event_id": row.get("event_id"),
        "event_type": row.get("event_type"),
        "simulation_run_id": row.get("simulation_run_id"),
        "failure_stage": row["failure_stage"],
        "error_code": row["error_code"],
        "payload_bytes_hash": row["payload_bytes_hash"],
        "processor_name": row.get("processor_name", PROCESSOR_NAME),
        "processor_version": row.get("processor_version", PROCESSOR_VERSION),
        "bronze_schema_version": row.get("bronze_schema_version", BRONZE_SCHEMA_VERSION),
        "destination": "QUARANTINED",
    }
    return canonical_hash(payload)


def normalize_event_json(event: dict) -> str:
    return canonical_json(event)


def _ts_iso(v: Any) -> str:
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat().replace("+00:00", "Z")
    return str(v)
