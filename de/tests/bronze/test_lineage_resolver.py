"""Lineage resolver end-of-data vs gap-wait semantics (batch-first)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from de.bronze.lineage_resolver import LineageResolver
from de.bronze.models import RawRow, ResolveKind


class _FakeRepo:
    def __init__(self, raw: Dict[int, RawRow], quar: Dict[int, Dict[str, Any]], max_off: int) -> None:
        self.raw = raw
        self.quar = quar
        self.max_off = max_off

    def fetch_raw_batch(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: int,
        batch_size: int,
    ) -> List[RawRow]:
        out: List[RawRow] = []
        for off in range(start_offset, min(end_offset, start_offset + batch_size)):
            if off in self.raw:
                out.append(self.raw[off])
        return out

    def fetch_raw_quarantine_batch(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: int,
        batch_size: int,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for off in range(start_offset, min(end_offset, start_offset + batch_size)):
            if off in self.quar:
                d = dict(self.quar[off])
                d["offset"] = off
                out.append(d)
        return out

    def source_max_offset(self, topic: str, partition: int) -> Optional[int]:
        return self.max_off


def _raw(off: int) -> RawRow:
    now = datetime.now(timezone.utc)
    return RawRow(
        topic="traffic.entity-events.v2",
        partition=0,
        offset=off,
        raw_ingestion_id="x" * 64,
        broker_timestamp=now,
        consumed_at=now,
        payload_encoding="utf8",
        payload_stored="{}",
        payload_bytes_hash="y" * 64,
    )


def test_end_of_available_data_when_offset_past_max() -> None:
    repo = _FakeRepo({}, {}, max_off=10)
    batch, stop = LineageResolver(repo).resolve_batch(
        "traffic.entity-events.v2", 0, 11, 12, 1, 10
    )
    assert not batch
    assert stop == ResolveKind.END_OF_AVAILABLE_DATA


def test_raw_quarantine_skipped() -> None:
    repo = _FakeRepo({}, {5: {"raw_ingestion_id": "q" * 64}}, max_off=10)
    batch, stop = LineageResolver(repo).resolve_batch(
        "traffic.entity-events.v2", 0, 5, 6, 1, 10
    )
    assert len(batch) == 1
    assert batch[0].kind == ResolveKind.RAW_QUARANTINE_SKIPPED
    assert stop is None


def test_offset_gap_wait_when_missing_within_range() -> None:
    repo = _FakeRepo({4: _raw(4)}, {}, max_off=10)
    batch, stop = LineageResolver(repo).resolve_batch(
        "traffic.entity-events.v2", 0, 5, 6, 1, 10
    )
    assert not batch
    assert stop == ResolveKind.OFFSET_GAP_WAIT
