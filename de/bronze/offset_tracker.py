"""Per-partition contiguous offset tracker (adapted from K-4 Raw)."""
from __future__ import annotations

from typing import Dict, Optional, Set, Tuple


class OffsetTracker:
    def __init__(self) -> None:
        self._completed: Dict[Tuple[str, int], Set[int]] = {}
        self._last_committed_record: Dict[Tuple[str, int], int] = {}

    def mark_completed(self, topic: str, partition: int, offset: int) -> None:
        self._completed.setdefault((topic, partition), set()).add(int(offset))

    def contiguous_completed_record_offset(
        self, topic: str, partition: int, source_start: int = 0
    ) -> Optional[int]:
        key = (topic, partition)
        done = self._completed.get(key) or set()
        if not done:
            return None
        last_c = self._last_committed_record.get(key)
        if last_c is None:
            cur = min(done)
            if cur > source_start:
                return None
            last = cur
            nxt = cur + 1
            while nxt in done:
                last = nxt
                nxt += 1
            return last
        last = last_c
        nxt = last_c + 1
        while nxt in done:
            last = nxt
            nxt += 1
        return last if last > last_c else None

    def advance_after_commit(
        self, topic: str, partition: int, record_offset: int
    ) -> None:
        key = (topic, partition)
        prev = self._last_committed_record.get(key, -1)
        if record_offset > prev:
            self._last_committed_record[key] = int(record_offset)
            done = self._completed.get(key)
            if done:
                self._completed[key] = {o for o in done if o > record_offset}

    def load_committed_record(self, topic: str, partition: int, record_offset: int) -> None:
        self._last_committed_record[(topic, partition)] = int(record_offset)

    def reset_partition(self, topic: str, partition: int) -> None:
        key = (topic, partition)
        self._completed.pop(key, None)
        self._last_committed_record.pop(key, None)
