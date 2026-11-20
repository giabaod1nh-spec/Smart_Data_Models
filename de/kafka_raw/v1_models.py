"""Immutable Kafka transport representation; no semantic identity decisions."""
from __future__ import annotations
import base64
import hashlib
from dataclasses import dataclass
from contracts.traffic_observation import canonical_json

def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()

def encoded(value: bytes | None) -> str:
    return base64.b64encode(value or b"").decode("ascii")

@dataclass(frozen=True)
class V1Record:
    topic: str
    partition: int
    offset: int
    key_bytes: bytes | None
    value_bytes: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()
    broker_timestamp_ms: int | None = None
    broker_timestamp_type: int = 0
    topic_identity: str = ""

    def __post_init__(self):
        if not self.topic or self.partition<0 or self.offset<0:
            raise ValueError("invalid physical Kafka lineage")
        if self.value_bytes is not None and not isinstance(self.value_bytes,bytes):
            raise TypeError("Kafka value must be bytes or null")
        if self.key_bytes is not None and not isinstance(self.key_bytes,bytes):
            raise TypeError("Kafka key must be bytes or null")

    def transport(self) -> dict:
        return dict(topic=self.topic, partition=self.partition, offset=self.offset,
                    topic_identity=self.topic_identity, key_is_null=self.key_bytes is None,
                    key=encoded(self.key_bytes), value_is_null=self.value_bytes is None,
                    value=encoded(self.value_bytes),
                    headers=[[k, v is None, encoded(v)] for k, v in self.headers],
                    timestamp_ms=self.broker_timestamp_ms, timestamp_type=self.broker_timestamp_type)

    @property
    def raw_ingestion_id(self) -> str:
        return digest(f"{self.topic}|{self.partition}|{self.offset}".encode())

    @property
    def physical_record_hash(self) -> str:
        return digest(canonical_json(self.transport()).encode())

@dataclass(frozen=True)
class V1ClassifiedRecord:
    destination: str
    row: dict
    failure_stage: str | None = None
    error_code: str | None = None
    error_detail: str | None = None

