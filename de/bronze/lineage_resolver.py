"""Resolve offsets: Raw valid, Raw quarantine skip, end-of-data, or gap (batch-first)."""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.models import ResolveKind, ResolvedRecord


class LineageResolver:
    def __init__(self, repo: BronzeClickHouseRepository) -> None:
        self.repo = repo
        self._gap_since: Dict[Tuple[str, int, int], float] = {}

    def resolve_batch(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset_exclusive: int,
        batch_size: int,
        max_offset: Optional[int],
    ) -> Tuple[List[ResolvedRecord], Optional[ResolveKind]]:
        """Fetch a contiguous prefix of offsets via batch CH reads.

        Returns (records, stop_kind) where stop_kind is set when the prefix
        ends before filling batch_size (END_OF_AVAILABLE_DATA or OFFSET_GAP_WAIT).
        """
        if batch_size <= 0 or start_offset >= end_offset_exclusive:
            return [], None

        window_end = min(end_offset_exclusive, start_offset + batch_size)
        limit = window_end - start_offset

        raw_rows = self.repo.fetch_raw_batch(
            topic, partition, start_offset, window_end, limit
        )
        quar_rows = self.repo.fetch_raw_quarantine_batch(
            topic, partition, start_offset, window_end, limit
        )
        raw_by = {r.offset: r for r in raw_rows}
        quar_by: Dict[int, Dict] = {}
        for q in quar_rows:
            quar_by[int(q["offset"])] = q

        batch: List[ResolvedRecord] = []
        off = start_offset
        while off < window_end and len(batch) < batch_size:
            if off in raw_by:
                self._gap_since.pop((topic, partition, off), None)
                batch.append(
                    ResolvedRecord(
                        kind=ResolveKind.RAW_VALID,
                        topic=topic,
                        partition=partition,
                        offset=off,
                        raw_row=raw_by[off],
                    )
                )
            elif off in quar_by:
                self._gap_since.pop((topic, partition, off), None)
                batch.append(
                    ResolvedRecord(
                        kind=ResolveKind.RAW_QUARANTINE_SKIPPED,
                        topic=topic,
                        partition=partition,
                        offset=off,
                        quarantine_row=quar_by[off],
                    )
                )
            elif max_offset is not None and off > max_offset:
                return batch, ResolveKind.END_OF_AVAILABLE_DATA
            else:
                if (topic, partition, off) not in self._gap_since:
                    self._gap_since[(topic, partition, off)] = time.time()
                return batch, ResolveKind.OFFSET_GAP_WAIT
            off += 1
        return batch, None

    def gap_wait_elapsed(self, topic: str, partition: int, offset: int) -> float:
        t0 = self._gap_since.get((topic, partition, offset))
        if t0 is None:
            return 0.0
        return time.time() - t0

    def resolve(self, topic: str, partition: int, offset: int) -> ResolvedRecord:
        """Single-offset resolve — not for processor hot path (use resolve_batch)."""
        max_off = self.repo.source_max_offset(topic, partition)
        batch, stop_kind = self.resolve_batch(
            topic, partition, offset, offset + 1, 1, max_off
        )
        if batch:
            return batch[0]
        if stop_kind == ResolveKind.END_OF_AVAILABLE_DATA:
            return ResolvedRecord(
                kind=ResolveKind.END_OF_AVAILABLE_DATA,
                topic=topic,
                partition=partition,
                offset=offset,
            )
        return ResolvedRecord(
            kind=ResolveKind.OFFSET_GAP_WAIT,
            topic=topic,
            partition=partition,
            offset=offset,
        )
