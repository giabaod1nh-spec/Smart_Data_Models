"""Deterministic bronze hash tests."""
from __future__ import annotations

from de.bronze.canonical_hash import bronze_ingestion_id, quarantine_canonical_hash


def test_bronze_ingestion_id_deterministic() -> None:
    a = bronze_ingestion_id("raw" * 16, "1.0.0", "ENTITY")
    b = bronze_ingestion_id("raw" * 16, "1.0.0", "ENTITY")
    assert a == b
    assert len(a) == 64


def test_quarantine_hash_excludes_error_detail() -> None:
    base = {
        "topic": "t",
        "partition": 0,
        "offset": 1,
        "raw_ingestion_id": "r" * 64,
        "failure_stage": "VALIDATE",
        "error_code": "SCHEMA_INVALID",
        "payload_bytes_hash": "h" * 64,
    }
    h1 = quarantine_canonical_hash({**base, "error_detail": "detail-a"})
    h2 = quarantine_canonical_hash({**base, "error_detail": "detail-b"})
    assert h1 == h2
