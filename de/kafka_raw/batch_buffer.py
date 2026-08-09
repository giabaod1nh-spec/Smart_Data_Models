"""Per-TopicPartition Raw/Quarantine buffers."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

TopicPartitionKey = Tuple[str, int]


@dataclass
class BufferedRecord:
    topic: str
    partition: int
    offset: int
    raw_ingestion_id: str
    destination: str  # RAW | QUARANTINE
    row: Dict[str, Any]
    size_bytes: int
    received_at: float = field(default_factory=time.monotonic)


@dataclass
class PartitionBuffer:
    records: List[BufferedRecord] = field(default_factory=list)
    first_at: float = field(default_factory=time.monotonic)

    @property
    def size_bytes(self) -> int:
        return sum(r.size_bytes for r in self.records)

    def age_ms(self) -> float:
        return (time.monotonic() - self.first_at) * 1000.0


class BatchBufferManager:
    def __init__(
        self,
        *,
        batch_size: int = 500,
        flush_ms: int = 500,
        max_buffered_records: int = 5000,
        max_buffered_bytes: int = 32 * 1024 * 1024,
    ) -> None:
        self.batch_size = batch_size
        self.flush_ms = flush_ms
        self.max_buffered_records = max_buffered_records
        self.max_buffered_bytes = max_buffered_bytes
        self.raw: Dict[TopicPartitionKey, PartitionBuffer] = {}
        self.quarantine: Dict[TopicPartitionKey, PartitionBuffer] = {}

    def _map(self, destination: str) -> Dict[TopicPartitionKey, PartitionBuffer]:
        return self.raw if destination == "RAW" else self.quarantine

    def add(self, rec: BufferedRecord) -> None:
        m = self._map(rec.destination)
        key = (rec.topic, rec.partition)
        buf = m.get(key)
        if buf is None:
            buf = PartitionBuffer()
            m[key] = buf
        if not buf.records:
            buf.first_at = time.monotonic()
        buf.records.append(rec)

    def total_records(self) -> int:
        return sum(len(b.records) for b in self.raw.values()) + sum(
            len(b.records) for b in self.quarantine.values()
        )

    def total_bytes(self) -> int:
        return sum(b.size_bytes for b in self.raw.values()) + sum(
            b.size_bytes for b in self.quarantine.values()
        )

    def should_pause(self) -> bool:
        return (
            self.total_records() >= self.max_buffered_records
            or self.total_bytes() >= self.max_buffered_bytes
        )

    def ready_keys(self, destination: str) -> List[TopicPartitionKey]:
        m = self._map(destination)
        out: List[TopicPartitionKey] = []
        for key, buf in m.items():
            if not buf.records:
                continue
            if len(buf.records) >= self.batch_size or buf.age_ms() >= self.flush_ms:
                out.append(key)
        return out

    def pop(self, destination: str, key: TopicPartitionKey) -> List[BufferedRecord]:
        m = self._map(destination)
        buf = m.pop(key, None)
        return list(buf.records) if buf else []

    def discard_partition(self, topic: str, partition: int) -> None:
        key = (topic, partition)
        self.raw.pop(key, None)
        self.quarantine.pop(key, None)
