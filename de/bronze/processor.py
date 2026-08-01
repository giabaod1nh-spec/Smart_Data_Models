"""Bronze processor core loop."""
from __future__ import annotations

import logging
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from de.bronze import (
    DEST_BRONZE_QUARANTINE,
    DEST_ENTITY,
    DEST_RUN,
    MIGRATION_VERSION,
    STATUS_IDEMPOTENT_SKIP,
    STATUS_QUARANTINED,
    STATUS_RAW_QUARANTINE_SKIPPED,
    STATUS_STORED,
)
from de.bronze.checkpoint_store import CheckpointStore
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import BronzeSettings
from de.bronze.lineage_resolver import LineageResolver
from de.bronze.metrics import Metrics
from de.bronze.models import PendingLedgerEntry, ResolveKind, ResolvedRecord
from de.bronze.offset_tracker import OffsetTracker
from de.bronze.payload_codec import decode_payload
from de.bronze.transformer import BronzeTransformer
from de.bronze.validator import BronzeValidator

log = logging.getLogger(__name__)


class ProcessorState(str, Enum):
    STARTING = "STARTING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class BronzeProcessor:
    def __init__(
        self,
        settings: BronzeSettings,
        *,
        repo: Optional[BronzeClickHouseRepository] = None,
        checkpoint: Optional[CheckpointStore] = None,
        validator: Optional[BronzeValidator] = None,
        replay_run_id: Optional[str] = None,
        write_main_tables: bool = True,
        max_offset_exclusive: Optional[Dict[tuple[str, int], int]] = None,
    ) -> None:
        self.settings = settings
        self.repo = repo or BronzeClickHouseRepository(settings)
        self.checkpoint = checkpoint or CheckpointStore(Path(settings.checkpoint_path))
        self.validator = validator or BronzeValidator(
            Path(settings.entity_schema_path),
            Path(settings.run_started_schema_path),
        )
        self.transformer = BronzeTransformer(
            processor_name=settings.processor_name,
            processor_version=settings.processor_version,
            bronze_schema_version=settings.bronze_schema_version,
            source_contract_version=settings.source_contract_version,
        )
        self.resolver = LineageResolver(self.repo)
        self.offsets = OffsetTracker()
        self.metrics = Metrics()
        self.state = ProcessorState.STARTING
        self.namespace = settings.checkpoint_namespace
        self.replay_run_id = replay_run_id
        self.write_main_tables = write_main_tables
        self.max_offset_exclusive = max_offset_exclusive or {}
        self._max_offset_cache: Dict[tuple[str, int], Optional[int]] = {}
        self.migrations_ok = False
        self.schemas_ok = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self.state = ProcessorState.STARTING
        self.metrics.state = self.state.value
        self.validator.load()
        self.schemas_ok = self.validator.ready
        self.repo.connect()
        self.migrations_ok = self.repo.verify_tables()
        if not self.migrations_ok:
            self.state = ProcessorState.FAULTED
            self.metrics.state = self.state.value
            self.metrics.fault_message = "Bronze/Raw CH tables missing — run de-migrate"
            raise RuntimeError(self.metrics.fault_message)
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="k7-bronze-processor", daemon=True)
        self._thread.start()
        self.state = ProcessorState.READY
        self.metrics.state = self.state.value

    def stop(self) -> None:
        self.state = ProcessorState.STOPPING
        self.metrics.state = self.state.value
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30.0)
        self.repo.close()
        self.checkpoint.close()
        self.state = ProcessorState.STOPPED
        self.metrics.state = self.state.value

    def _run(self) -> None:
        topic = self.settings.topic
        while not self._stop.is_set():
            try:
                self._max_offset_cache = {}
                for part in self.settings.partition_list():
                    self._max_offset_cache[(topic, part)] = self.repo.source_max_offset(
                        topic, part
                    )
                for part in self.settings.partition_list():
                    self._poll_partition(topic, part)
            except Exception:
                log.exception("bronze poll cycle failed")
                self.metrics.bronze_retry_total += 1
            time.sleep(self.settings.poll_interval_sec)

    def _poll_partition(self, topic: str, partition: int) -> None:
        next_off = self._init_cursor(topic, partition)
        if next_off is None:
            return
        bound = self.max_offset_exclusive.get((topic, partition))
        if bound is not None and next_off >= bound:
            return

        max_off = self._max_offset_cache.get((topic, partition))
        end_exclusive = bound if bound is not None else (max_off + 1 if max_off is not None else next_off + self.settings.batch_size)

        batch, stop_kind = self.resolver.resolve_batch(
            topic,
            partition,
            next_off,
            end_exclusive,
            self.settings.batch_size,
            max_off,
        )

        if stop_kind == ResolveKind.END_OF_AVAILABLE_DATA:
            self.metrics.bronze_end_of_data_total += 1
            self.state = ProcessorState.READY
            self.metrics.state = self.state.value
        elif stop_kind == ResolveKind.OFFSET_GAP_WAIT and batch:
            cursor = batch[-1].offset + 1
            elapsed = self.resolver.gap_wait_elapsed(topic, partition, cursor)
            self.metrics.bronze_gap_wait_total += 1
            if elapsed > self.settings.gap_wait_max_sec:
                self.state = ProcessorState.DEGRADED
                self.metrics.state = self.state.value
        elif stop_kind == ResolveKind.OFFSET_GAP_WAIT:
            cursor = next_off
            elapsed = self.resolver.gap_wait_elapsed(topic, partition, cursor)
            self.metrics.bronze_gap_wait_total += 1
            if elapsed > self.settings.gap_wait_max_sec:
                self.state = ProcessorState.DEGRADED
                self.metrics.state = self.state.value

        if not batch:
            self._update_lag(topic, partition)
            return
        self.metrics.begin_batch()
        self._process_batch(topic, partition, batch)
        self.metrics.end_batch(len(batch))
        self._update_lag(topic, partition)

    def _init_cursor(self, topic: str, partition: int) -> Optional[int]:
        cp = self.checkpoint.get(self.namespace, topic, partition)
        if cp is None:
            source_start = self.repo.min_source_offset(topic, partition)
            if source_start is None:
                return None
            last = source_start - 1
            self.checkpoint.init_checkpoint(
                namespace=self.namespace,
                topic=topic,
                partition=partition,
                source_start_offset=source_start,
                last_completed_offset=last,
                start_mode=self.settings.start_mode,
                processor_name=self.settings.processor_name,
                processor_version=self.settings.processor_version,
                bronze_schema_version=self.settings.bronze_schema_version,
            )
            cp = self.checkpoint.get(self.namespace, topic, partition)
        assert cp is not None
        return max(cp.source_start_offset, cp.last_completed_offset + 1)

    def _process_batch(
        self, topic: str, partition: int, batch: List[ResolvedRecord]
    ) -> None:
        entity_rows: List[Dict[str, Any]] = []
        run_rows: List[Dict[str, Any]] = []
        quarantine_rows: List[Dict[str, Any]] = []
        pending: List[PendingLedgerEntry] = []

        sorted_batch = sorted(batch, key=lambda r: r.offset)
        offsets = [r.offset for r in sorted_batch]
        completed_offsets = self.checkpoint.is_complete_batch(
            self.namespace, topic, partition, offsets
        )

        raw_ids_for_lookup: List[str] = []
        for resolved in sorted_batch:
            if resolved.offset in completed_offsets:
                continue
            if resolved.kind == ResolveKind.RAW_QUARANTINE_SKIPPED:
                continue
            if resolved.raw_row is not None:
                raw_ids_for_lookup.append(resolved.raw_row.raw_ingestion_id)

        existing_ids = (
            self.repo.find_existing_raw_ingestion_ids(raw_ids_for_lookup)
            if raw_ids_for_lookup
            else set()
        )

        entity_checks: List[tuple[str, str, str, int, int]] = []
        parsed: Dict[int, tuple[Any, Dict[str, Any], Any]] = {}
        for resolved in sorted_batch:
            if resolved.offset in completed_offsets:
                continue
            if resolved.kind != ResolveKind.RAW_VALID or resolved.raw_row is None:
                continue
            raw = resolved.raw_row
            if raw.raw_ingestion_id in existing_ids:
                continue
            try:
                event, _ = decode_payload(raw)
            except Exception:
                continue
            outcome = self.validator.validate(event)
            parsed[resolved.offset] = (raw, event, outcome)
            if outcome.ok and outcome.kind == "ENTITY":
                entity_checks.append(
                    (
                        str(event.get("eventId")),
                        str(event.get("entityPayloadHash")),
                        topic,
                        partition,
                        resolved.offset,
                    )
                )

        dup_offsets = (
            self.repo.upstream_duplicate_offsets(entity_checks)
            if entity_checks
            else set()
        )

        for resolved in sorted_batch:
            off = resolved.offset
            if off in completed_offsets:
                pending.append(
                    PendingLedgerEntry(
                        topic, partition, off,
                        resolved.raw_row.raw_ingestion_id if resolved.raw_row else
                        str(resolved.quarantine_row["raw_ingestion_id"]),
                        STATUS_IDEMPOTENT_SKIP,
                        "SKIP",
                    )
                )
                self.metrics.bronze_idempotent_skip_total += 1
                continue

            if resolved.kind == ResolveKind.RAW_QUARANTINE_SKIPPED:
                rid = str(resolved.quarantine_row["raw_ingestion_id"])
                pending.append(
                    PendingLedgerEntry(
                        topic, partition, off, rid,
                        STATUS_RAW_QUARANTINE_SKIPPED, "RAW_QUARANTINE",
                        payload_hash=str(resolved.quarantine_row.get("payload_bytes_hash")),
                    )
                )
                continue

            raw = resolved.raw_row
            assert raw is not None
            self.metrics.raw_rows_read_total += 1
            if raw.raw_ingestion_id in existing_ids:
                pending.append(
                    PendingLedgerEntry(
                        topic, partition, off, raw.raw_ingestion_id,
                        STATUS_IDEMPOTENT_SKIP, "SKIP",
                        payload_hash=raw.payload_bytes_hash,
                    )
                )
                self.metrics.bronze_idempotent_skip_total += 1
                continue

            if off in parsed:
                raw_p, event, outcome = parsed[off]
                upstream_dup = off in dup_offsets
                result = self.transformer.transform(
                    raw_p, event, outcome, upstream_duplicate=upstream_dup
                )
            else:
                try:
                    event, _ = decode_payload(raw)
                except Exception as e:
                    from de.bronze.validator import ValidationOutcome
                    outcome = ValidationOutcome(
                        False, "QUARANTINE",
                        error_code="PAYLOAD_DECODE_FAILED",
                        error_detail=str(e),
                        failure_stage="PARSE",
                    )
                    qrow = self.transformer.transform(raw, {}, outcome).quarantine_row
                    if qrow:
                        quarantine_rows.append(qrow)
                        pending.append(
                            PendingLedgerEntry(
                                topic, partition, off, raw.raw_ingestion_id,
                                STATUS_QUARANTINED, DEST_BRONZE_QUARANTINE,
                                payload_hash=raw.payload_bytes_hash,
                            )
                        )
                    continue
                outcome = self.validator.validate(event)
                upstream_dup = off in dup_offsets
                result = self.transformer.transform(
                    raw, event, outcome, upstream_duplicate=upstream_dup
                )

            if result.kind == "ENTITY" and result.entity_row:
                entity_rows.append(result.entity_row)
                pending.append(
                    PendingLedgerEntry(
                        topic, partition, off, raw.raw_ingestion_id,
                        STATUS_STORED, DEST_ENTITY,
                        payload_hash=raw.payload_bytes_hash,
                    )
                )
            elif result.kind == "RUN" and result.run_row:
                run_rows.append(result.run_row)
                pending.append(
                    PendingLedgerEntry(
                        topic, partition, off, raw.raw_ingestion_id,
                        STATUS_STORED, DEST_RUN,
                        payload_hash=raw.payload_bytes_hash,
                    )
                )
            elif result.quarantine_row:
                quarantine_rows.append(result.quarantine_row)
                pending.append(
                    PendingLedgerEntry(
                        topic, partition, off, raw.raw_ingestion_id,
                        STATUS_QUARANTINED, DEST_BRONZE_QUARANTINE,
                        payload_hash=raw.payload_bytes_hash,
                    )
                )

        confirmed_ids: set[str] = set()
        replay_id = self.replay_run_id if not self.write_main_tables else None

        def _insert_batch(name: str, rows: List[Dict[str, Any]], insert_fn) -> None:
            nonlocal confirmed_ids
            if not rows:
                return
            ids = [str(r["raw_ingestion_id"]) for r in rows]
            try:
                insert_fn(rows, replay_run_id=replay_id)
                confirmed_ids.update(ids)
            except Exception:
                log.exception("CH insert failed %s", name)
                found = self.repo.find_existing_raw_ingestion_ids(ids)
                confirmed_ids.update(found)

        if self.write_main_tables or replay_id:
            _insert_batch("entity", entity_rows, self.repo.insert_entity_batch)
            _insert_batch("run", run_rows, self.repo.insert_run_batch)
            _insert_batch("quarantine", quarantine_rows, self.repo.insert_quarantine_batch)

        self.metrics.bronze_rows_stored_total += len(entity_rows) + len(run_rows)
        self.metrics.bronze_quarantined_total += len(quarantine_rows)

        ch_confirmed = [
            p for p in pending
            if p.status in (STATUS_STORED, STATUS_QUARANTINED)
            and p.raw_ingestion_id in confirmed_ids
        ]
        durable_ledger_only = [
            p for p in pending
            if p.status in (STATUS_RAW_QUARANTINE_SKIPPED, STATUS_IDEMPOTENT_SKIP)
        ]
        all_entries = sorted(ch_confirmed + durable_ledger_only, key=lambda p: p.offset)

        cp = self.checkpoint.get(self.namespace, topic, partition)
        source_start = cp.source_start_offset if cp else 0
        last_committed = cp.last_completed_offset if cp else source_start - 1
        for p in all_entries:
            self.offsets.mark_completed(topic, partition, p.offset)

        contiguous_end = self._contiguous_prefix_end(all_entries, last_committed + 1)
        if contiguous_end is None:
            return

        slice_entries = [e for e in all_entries if e.offset <= contiguous_end]
        ledger_payload = [
            {
                "offset": e.offset,
                "raw_ingestion_id": e.raw_ingestion_id,
                "destination": e.destination,
                "status": e.status,
                "payload_hash": e.payload_hash,
            }
            for e in slice_entries
        ]
        if self.namespace:
            self.checkpoint.commit_batch(
                self.namespace, topic, partition, ledger_payload, contiguous_end
            )
        self.offsets.advance_after_commit(topic, partition, contiguous_end)
        self.metrics.mark_checkpoint(topic, partition, contiguous_end)

    def _contiguous_prefix_end(
        self, entries: List[PendingLedgerEntry], expected_start: int
    ) -> Optional[int]:
        if not entries:
            return None
        offsets = sorted({e.offset for e in entries})
        expected = expected_start
        last: Optional[int] = None
        offset_set = set(offsets)
        while expected in offset_set:
            last = expected
            expected += 1
        return last

    def _update_lag(self, topic: str, partition: int) -> None:
        cp = self.checkpoint.get(self.namespace, topic, partition)
        max_off = self._max_offset_cache.get((topic, partition))
        if max_off is None:
            max_off = self.repo.source_max_offset(topic, partition)
        if cp is None or max_off is None:
            self.metrics.source_lag_offsets[f"{topic}:{partition}"] = 0
            return
        lag = max(0, max_off - cp.last_completed_offset)
        self.metrics.source_lag_offsets[f"{topic}:{partition}"] = lag

    def health(self) -> Dict[str, Any]:
        stale = False
        any_lag = any(v > 0 for v in self.metrics.source_lag_offsets.values())
        last_cp = self.metrics.last_successful_checkpoint_time
        if any_lag and last_cp:
            stale = (time.time() - last_cp) > self.settings.readiness_stale_sec
        thread_alive = self._thread is not None and self._thread.is_alive()
        ready = (
            self.migrations_ok
            and self.schemas_ok
            and self.repo.ping()
            and thread_alive
            and self.state not in (ProcessorState.FAULTED,)
            and not stale
        )
        return {
            **self.metrics.snapshot(),
            "ready": ready,
            "migrations_ok": self.migrations_ok,
            "schemas_ok": self.schemas_ok,
            "processor_thread_alive": thread_alive,
            "migration_version": MIGRATION_VERSION,
            "checkpoint_stale": stale,
            "checkpoint_namespace": self.namespace,
        }
