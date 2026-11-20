"""Resolve offsets: Raw valid, Raw quarantine skip, end-of-data, or gap (batch-first)."""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.models import ResolveKind, ResolvedRecord
from de.kafka_raw.v1_ledger_store import V1LedgerReader, IntegrityFault
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository


class LineageResolver:
    def __init__(self, repo: BronzeClickHouseRepository) -> None:
        self.repo = repo
        self._gap_since: Dict[Tuple[str, int, int], float] = {}

    @staticmethod
    def read_v1_batch(raw_repository: V1ClickHouseRepository, ledger_path: str,
                      topic: str, partition: int, next_source_offset: int,
                      max_span: int) -> tuple[list[dict], int, str]:
        """Read one bounded, confirmed Raw prefix under a SQLite WAL snapshot.

        Completeness assumes the certified single-owner Raw ledger has not been
        pruned or replaced. An orphan CH row is detectable; arbitrary deletion
        of both matching evidence stores is outside this local contract.
        """
        if max_span <= 0 or next_source_offset < 0:
            raise ValueError("invalid V1 source range")
        reader = V1LedgerReader(ledger_path)
        try:
            reader.db.execute("BEGIN")
            progress = reader.db.execute(
                "SELECT topic_identity,safe_next_offset,committed_next_offset "
                "FROM partition_progress WHERE topic=? AND partition_id=?",
                (topic, partition)).fetchone()
            if progress is None or not progress["topic_identity"] or progress["safe_next_offset"] is None:
                raise IntegrityFault("unbound Raw partition or missing safe boundary")
            safe = int(progress["safe_next_offset"])
            if next_source_offset > safe:
                raise IntegrityFault("Bronze cursor exceeds Raw safe boundary")
            end = min(safe, next_source_offset + max_span)
            faults = reader.db.execute("SELECT count(*) FROM integrity_faults").fetchone()[0]
            if faults:
                raise IntegrityFault("unresolved Raw integrity fault")
            rows = raw_repository.logical_read(reader, topic, partition, next_source_offset, end)
            for row in rows:
                if row["status"] not in ("CONFIRMED_RAW", "CONFIRMED_QUARANTINE"):
                    raise IntegrityFault("unresolved Raw delivery or physical conflict")
                if row["identity"][2] >= end or row["record"]["topic_identity"] != progress["topic_identity"]:
                    raise IntegrityFault("Raw source identity/boundary mismatch")
            expected = {row["record"]["raw_ingestion_id"] for row in rows}
            for destination in ("RAW", "QUARANTINE"):
                table = raw_repository.table(destination)
                if raw_repository.replay_id:
                    clause = " AND replay_run_id={replay:String}"
                    params = dict(topic=topic, partition=partition, start=next_source_offset,
                                  end=end, replay=raw_repository.replay_id)
                else:
                    clause = ""
                    params = dict(topic=topic, partition=partition, start=next_source_offset,end=end)
                query = raw_repository.client.query(
                    "SELECT DISTINCT raw_ingestion_id FROM " + table +
                    " WHERE topic={topic:String} AND partition={partition:Int32} "
                    "AND offset>={start:Int64} AND offset<{end:Int64}" + clause,
                    parameters=params)
                for (raw_id,) in query.result_rows:
                    raw_id = raw_id.decode("ascii") if isinstance(raw_id, bytes) else raw_id
                    if raw_id not in expected:
                        raise IntegrityFault("ClickHouse Raw evidence lacks confirmed ledger delivery")
            reader.db.execute("COMMIT")
            return rows, end, progress["topic_identity"]
        except BaseException:
            reader.db.execute("ROLLBACK")
            raise
        finally:
            reader.close()

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
