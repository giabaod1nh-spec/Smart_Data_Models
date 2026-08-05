"""Kafka consumer bootstrap — normal group vs demo fence (RT-A)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Sequence

log = logging.getLogger(__name__)


class ConsumerMode(str, Enum):
    NORMAL = "normal"
    DEMO = "demo"


class OffsetAuthorityConflict(Exception):
    """Broker committed offset is ahead of SQLite authority."""


@dataclass(frozen=True)
class PartitionSeek:
    partition: int
    offset: int


def sqlite_seek_offset(
    store,
    topic: str,
    partition: int,
    *,
    has_sqlite_any: bool,
) -> Optional[int]:
    """Next offset to consume: SQLite committed + 1, or None if bootstrap at latest."""
    committed = store.get_committed_offset(topic, partition)
    if committed is not None:
        return int(committed) + 1
    if has_sqlite_any:
        return None
    return None  # signal latest bootstrap for brand-new DB


def reconcile_broker_commit(
    *,
    sqlite_offset: Optional[int],
    broker_committed: Optional[int],
) -> None:
    """Fail closed if broker next offset is ahead of SQLite last-processed + 1.

    SQLite stores the **last processed** offset.  Kafka consumer group commits
    store the **next** offset to consume.  When in sync, ``broker_committed ==
    sqlite_offset + 1`` — that is not a conflict.
    """
    if sqlite_offset is None or broker_committed is None:
        return
    sqlite_last = int(sqlite_offset)
    broker_next = int(broker_committed)
    if broker_next > sqlite_last + 1:
        raise OffsetAuthorityConflict(
            f"broker committed {broker_next} ahead of sqlite {sqlite_last}"
        )


def build_normal_on_assign_seek(
    store,
    topic: str,
    partitions: Sequence[int],
    *,
    brand_new_sqlite: bool,
) -> List[PartitionSeek]:
    """Seek targets for normal subscribe mode."""
    seeks: List[PartitionSeek] = []
    for part in partitions:
        committed = store.get_committed_offset(topic, part)
        if committed is not None:
            seeks.append(PartitionSeek(part, int(committed) + 1))
        elif brand_new_sqlite:
            seeks.append(PartitionSeek(part, -1))  # sentinel: use log end (latest)
        else:
            seeks.append(PartitionSeek(part, -1))
    return seeks


def build_demo_assignments(
    fence_manifest: dict,
    resume_offsets: Optional[Dict[int, int]] = None,
) -> List[PartitionSeek]:
    parts = fence_manifest.get("partitions") or []
    assignments: List[PartitionSeek] = []
    for p in parts:
        part = int(p["partition"])
        start = int(p["nextOffset"])
        resume = (resume_offsets or {}).get(part)
        if resume is not None and resume > start:
            start = resume
        assignments.append(PartitionSeek(part, start))
    return assignments
