"""Per-partition contiguous offset tracker (K-3)."""
from __future__ import annotations

from typing import Dict, Optional, Set, Tuple


class OffsetTracker:
    """Tracks completed offsets; exposes contiguous commit prefix."""

    def __init__(self) -> None:
        # (topic, partition) → set of completed offsets
        self._completed: Dict[Tuple[str, int], Set[int]] = {}
        # (topic, partition) → next expected / last committed
        self._committed: Dict[Tuple[str, int], int] = {}

    def mark_completed(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        self._completed.setdefault(key, set()).add(int(offset))

    def contiguous_commit_offset(self, topic: str, partition: int) -> Optional[int]:
        """Highest offset such that all from (committed+1 or min) .. offset are complete.

        If nothing committed yet, start from min completed offset only if it forms
        a contiguous run — for simplicity start from min(completed) and walk up.
        """
        key = (topic, partition)
        done = self._completed.get(key) or set()
        if not done:
            return self._committed.get(key)
        start = self._committed.get(key)
        if start is None:
            # begin at minimum offset present
            cur = min(done)
            if cur not in done:
                return None
            last = cur
            nxt = cur + 1
            while nxt in done:
                last = nxt
                nxt += 1
            return last
        # already committed `start`; need start+1, start+2, ...
        last = start
        nxt = start + 1
        while nxt in done:
            last = nxt
            nxt += 1
        return last if last > start else start

    def advance_commit(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        prev = self._committed.get(key, -1)
        if offset > prev:
            self._committed[key] = offset
            # prune completed below/equal
            done = self._completed.get(key)
            if done:
                self._completed[key] = {o for o in done if o > offset}

    def load_committed(self, topic: str, partition: int, offset: int) -> None:
        self._committed[(topic, partition)] = int(offset)
