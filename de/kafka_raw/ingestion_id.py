"""Deterministic raw_ingestion_id from Kafka lineage."""
from __future__ import annotations

import hashlib


def raw_ingestion_id(topic: str, partition: int, offset: int) -> str:
    material = f"{topic}|{int(partition)}|{int(offset)}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def payload_bytes_hash(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
