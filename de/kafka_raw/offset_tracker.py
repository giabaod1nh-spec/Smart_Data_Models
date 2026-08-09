"""Per-partition contiguous offset tracker with Kafka commit = N+1."""
from __future__ import annotations

from typing import Dict, Optional, Set, Tuple


class OffsetTracker:
    def __init__(self) -> None:
        self._completed: Dict[Tuple[str, int], Set[int]] = {}
        # last completed record offset that was committed as Kafka position N+1
        # store the last *record* offset whose commit position was sent
        self._last_committed_record: Dict[Tuple[str, int], int] = {}

    def mark_completed(self, topic: str, partition: int, offset: int) -> None:
        self._completed.setdefault((topic, partition), set()).add(int(offset))

    def contiguous_completed_record_offset(
        self, topic: str, partition: int
    ) -> Optional[int]:
        """Highest record offset N such that all offsets from base..N are complete."""
        key = (topic, partition)
        done = self._completed.get(key) or set()
        if not done:
            return None
        last_c = self._last_committed_record.get(key)
        if last_c is None:
            cur = min(done)
            last = cur
            nxt = cur + 1
            while nxt in done:
                last = nxt
                nxt += 1
            # only valid if we started at min and it's contiguous from min
            return last
        last = last_c
        nxt = last_c + 1
        while nxt in done:
            last = nxt
            nxt += 1
        return last if last > last_c else None

    def kafka_commit_offset(self, completed_record_offset: int) -> int:
        """Kafka commit API wants the next offset to read (= N+1)."""
        return int(completed_record_offset) + 1

    def advance_after_commit(self, topic: str, partition: int, record_offset: int) -> None:
        key = (topic, partition)
        prev = self._last_committed_record.get(key, -1)
        if record_offset > prev:
            self._last_committed_record[key] = int(record_offset)
            done = self._completed.get(key)
            if done:
                self._completed[key] = {o for o in done if o > record_offset}

    def load_committed_record(self, topic: str, partition: int, record_offset: int) -> None:
        """Load last completed record offset (Kafka position was record_offset+1)."""
        self._last_committed_record[(topic, partition)] = int(record_offset)

    def last_committed_record_offset(self, topic: str, partition: int) -> Optional[int]:
        """Return the last durably completed record offset known locally."""
        return self._last_committed_record.get((topic, partition))
