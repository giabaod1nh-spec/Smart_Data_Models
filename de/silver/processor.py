"""Silver Plan 3 — processor state machine + batch orchestration."""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from de.silver.batch_ledger import (
    LedgerConflictError as BatchLedgerConflictError,
    LedgerEntryState as BatchLedgerEntryState,
    materialize_ledger_entry,
)
from de.silver.checkpoint_store import (
    CheckpointBusyError,
    CheckpointCasConflictError,
    SilverCheckpointStore,
)
from de.silver.config import (
    BACKOFF_SCHEDULE_SEC,
    CasResult,
    CheckpointKey,
    DestinationMode,
    ProcessorState,
    SilverSettings,
    SourceStream,
)
from de.silver.contracts import DISPOSITION_PROCESSED, DISPOSITION_QUARANTINED
from de.silver.dimension_state import (
    decide_persisted_candidates,
    fetch_current_hashes,
    filter_for_replay,
)
from de.silver.engine import TransformationEngine
from de.silver.input_models import BronzeEntityInputRecord, BronzeRunInputRecord
from de.silver.metrics import HealthSnapshot, Metrics
from de.silver.models import SilverLedgerEntry
from de.silver.readers import BronzeReader
from de.silver.repositories import (
    FACT_BUSINESS_KEY_COLUMNS,
    FactBusinessKeyConflictError,
    FactIdentity,
    FactReconcileResult,
    InvalidTargetTableError,
    LedgerConflictError,
    LedgerEntryState as RepoLedgerEntryState,
    RetryableRepositoryError,
    SilverClickHouseRepository,
    SourceOffsetConflictError,
    UncertainWriteError,
)


def _utc_dt() -> datetime:
    return datetime.now(timezone.utc)


def _utc_str() -> str:
    return _utc_dt().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _source_id(record: Any) -> str:
    if isinstance(record, BronzeEntityInputRecord):
        return record.event_id
    return record.bronze_canonical_hash


def _payload_hash(record: Any) -> str:
    if isinstance(record, BronzeEntityInputRecord):
        return record.entity_payload_hash
    return record.bronze_canonical_hash


def _to_batch_ledger(entry: Optional[RepoLedgerEntryState]) -> Optional[BatchLedgerEntryState]:
    if entry is None:
        return None
    return BatchLedgerEntryState(
        checkpoint_namespace=entry.checkpoint_namespace,
        source_bronze_event_id=entry.source_bronze_event_id,
        raw_ingestion_id=entry.raw_ingestion_id,
        payload_hash=entry.payload_hash,
        disposition=entry.disposition,
        target_table=entry.target_table,
    )


def _fact_identity(fact: Any, primary: str) -> FactIdentity:
    key_cols = FACT_BUSINESS_KEY_COLUMNS[primary]
    return FactIdentity(
        source_bronze_event_id=fact.source_bronze_event_id,
        source_payload_hash=fact.source_payload_hash,
        business_key=tuple(getattr(fact, c) for c in key_cols),
        source_topic=fact.source_topic,
        source_partition=int(fact.source_partition),
        source_offset=int(fact.source_offset),
    )


class SilverProcessor:
    def __init__(
        self,
        settings: SilverSettings,
        *,
        reader: Optional[BronzeReader] = None,
        repo: Optional[SilverClickHouseRepository] = None,
        checkpoint: Optional[SilverCheckpointStore] = None,
        engine: Optional[TransformationEngine] = None,
        lock_held: bool = False,
    ) -> None:
        self.settings = settings
        self.reader = reader or BronzeReader(settings)
        self.repo = repo or SilverClickHouseRepository(settings)
        self.checkpoint = checkpoint or SilverCheckpointStore(Path(settings.checkpoint_path))
        self.engine = engine or TransformationEngine()
        self.metrics = Metrics()
        self.state = ProcessorState.STARTING
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock_held = lock_held
        self._streams: tuple[SourceStream, ...] = ()
        self._rr_index = 0
        self._schema_ok = False
        self._clickhouse_ok = False
        self._sqlite_ok = False
        self._health_snapshot = HealthSnapshot(
            state=self.state.value,
            ready=False,
            worker_alive=False,
            reader_initialized=False,
            clickhouse_ok=False,
            sqlite_ok=False,
            schema_ok=False,
            lock_held=lock_held,
            namespace=settings.namespace,
            mode=settings.destination_mode,
            shutdown_requested=False,
            snapshot_at=_utc_str(),
            metrics={},
        )
        self._snapshot_lock = threading.RLock()
        self._retry_idx = 0
        self._last_discovery = 0.0
        self._epoch = time.monotonic()

    def start(self) -> None:
        self.state = ProcessorState.STARTING
        self.checkpoint.open()
        self.reader.connect()
        self.repo.connect()
        try:
            self.repo.verify_schema(self.settings.destination_mode)
            self.reader.verify_source_schema()
            self._schema_ok = True
        except Exception:
            self._schema_ok = False
            raise
        self._streams = self.reader.discover_streams(self.settings.topic_list())
        self._last_discovery = time.monotonic()
        self.state = ProcessorState.RECOVERING
        self._recover_streams()
        self.state = ProcessorState.READY
        self._refresh_deps()
        self._publish_snapshot()
        self._thread = threading.Thread(target=self._run, name="silver-processor", daemon=True)
        self._thread.start()
        self._publish_snapshot()

    def stop(self) -> None:
        self.state = ProcessorState.STOPPING
        self._stop.set()
        self._publish_snapshot()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        for closer in (self.reader.close, self.repo.close, self.checkpoint.close):
            try:
                closer()
            except Exception:
                pass
        self.state = ProcessorState.STOPPED
        self._publish_snapshot()

    def request_shutdown(self) -> None:
        self._stop.set()

    def health_snapshot(self) -> HealthSnapshot:
        with self._snapshot_lock:
            return self._health_snapshot

    def _seconds_since_progress(self) -> float:
        ts = self.metrics.last_progress_at or self.metrics.last_checkpoint_at
        if not ts:
            return time.monotonic() - self._epoch
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return time.monotonic() - self._epoch
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())

    def _stale_with_lag(self) -> bool:
        """Plan 3: lag>0 alone is OK; lag>0 without progress for readiness_stale_sec is stale."""
        if not any(int(v) > 0 for v in self.metrics.source_lag.values()):
            return False
        return self._seconds_since_progress() > float(self.settings.readiness_stale_sec)

    def _publish_snapshot(self) -> None:
        stale_lag = self._stale_with_lag()
        if stale_lag and self.state == ProcessorState.READY:
            self.state = ProcessorState.DEGRADED
        elif (
            not stale_lag
            and self.state == ProcessorState.DEGRADED
            and not self.metrics.fault_code
            and self._retry_idx == 0
        ):
            self.state = ProcessorState.READY
        snap = HealthSnapshot(
            state=self.state.value,
            ready=(
                self.state == ProcessorState.READY
                and self._clickhouse_ok
                and self._sqlite_ok
                and self._schema_ok
                and self.reader.initialized
                and (self._thread.is_alive() if self._thread else False)
                and self._lock_held
                and not self._stop.is_set()
                and not stale_lag
            ),
            worker_alive=bool(self._thread and self._thread.is_alive()),
            reader_initialized=self.reader.initialized,
            clickhouse_ok=self._clickhouse_ok,
            sqlite_ok=self._sqlite_ok,
            schema_ok=self._schema_ok,
            lock_held=self._lock_held,
            namespace=self.settings.namespace,
            mode=self.settings.destination_mode,
            shutdown_requested=self._stop.is_set(),
            snapshot_at=_utc_str(),
            metrics=self.metrics.snapshot(),
            fault_code=self.metrics.fault_code,
            fault_message=self.metrics.fault_message,
            streams=tuple(
                {
                    "source_table": s.source_table,
                    "topic": s.topic,
                    "partition": s.partition,
                    "lag": self.metrics.source_lag.get(
                        f"{s.source_table}|{s.topic}|{s.partition}", 0
                    ),
                }
                for s in self._streams
            ),
        )
        with self._snapshot_lock:
            self._health_snapshot = snap

    def _refresh_deps(self) -> None:
        self._clickhouse_ok = self.reader.ping() and self.repo.ping()
        self._sqlite_ok = self.checkpoint.is_readable()

    def _run(self) -> None:
        while not self._stop.is_set() and self.state not in {
            ProcessorState.FAULTED,
            ProcessorState.STOPPING,
            ProcessorState.STOPPED,
        }:
            try:
                self._refresh_deps()
                now = time.monotonic()
                if now - self._last_discovery >= self.settings.discovery_interval_sec:
                    discovered = self.reader.discover_streams(self.settings.topic_list())
                    existing = {(s.source_table, s.topic, s.partition): s for s in self._streams}
                    for s in discovered:
                        existing.setdefault((s.source_table, s.topic, s.partition), s)
                    self._streams = tuple(
                        sorted(
                            existing.values(),
                            key=lambda s: (
                                s.topic,
                                s.partition,
                                0 if s.source_table.endswith("run_events") else 1,
                            ),
                        )
                    )
                    self._last_discovery = now
                if not self._streams:
                    self._publish_snapshot()
                    self._stop.wait(self.settings.poll_interval_sec)
                    continue
                stream = self._streams[self._rr_index % len(self._streams)]
                self._rr_index += 1
                # Do not fetch a new batch after shutdown was requested.
                if self._stop.is_set():
                    break
                self.process_stream_once(stream)
                self._retry_idx = 0
                if self.state == ProcessorState.RETRYING:
                    self.state = ProcessorState.READY
                self._publish_snapshot()
            except (
                SourceOffsetConflictError,
                LedgerConflictError,
                BatchLedgerConflictError,
                FactBusinessKeyConflictError,
                CheckpointCasConflictError,
                InvalidTargetTableError,
            ) as exc:
                self._fault(type(exc).__name__, str(exc))
                break
            except (RetryableRepositoryError, CheckpointBusyError, UncertainWriteError):
                self.state = ProcessorState.RETRYING
                self.metrics.retries_total += 1
                delay = BACKOFF_SCHEDULE_SEC[min(self._retry_idx, len(BACKOFF_SCHEDULE_SEC) - 1)]
                self._retry_idx += 1
                if self._retry_idx >= 3:
                    self.state = ProcessorState.DEGRADED
                self._publish_snapshot()
                self._stop.wait(delay)
            except Exception as exc:
                self._fault("UNHANDLED_PROCESSOR_EXCEPTION", str(exc))
                break
            self._stop.wait(0)  # cooperative yield

    def _fault(self, code: str, message: str) -> None:
        self.state = ProcessorState.FAULTED
        self.metrics.fault_code = code
        self.metrics.fault_message = message
        self._publish_snapshot()

    def _recover_streams(self) -> None:
        for stream in self._streams:
            self.process_stream_once(stream)

    def process_stream_once(self, stream: SourceStream) -> int:
        """Process at most one batch. Persistence order:

        Facts → Dimensions → Quarantine → Ledger → (after all records) Checkpoint CAS.
        Checkpoint runs only after every fetched logical record has a terminal ledger row.
        """
        if self._stop.is_set() and self.state != ProcessorState.RECOVERING:
            return 0
        key = CheckpointKey(
            self.settings.namespace, stream.source_table, stream.topic, stream.partition
        )
        row = self.checkpoint.get(key)
        if row is None:
            mn = self.reader.min_offset(stream)
            if mn is None:
                return 0
            row = self.checkpoint.initialize(
                key,
                source_start=mn,
                last_completed=mn - 1,
                start_mode=(
                    "earliest"
                    if self.settings.destination_mode == DestinationMode.MAIN.value
                    else "explicit"
                ),
                processor_name=self.settings.processor_name,
                processor_version=self.settings.processor_version,
                silver_schema_version=self.settings.silver_schema_version,
            )
        expected = row.last_completed_offset
        records, receipt = self.reader.fetch_batch(
            stream, after_offset=expected, limit=self.settings.batch_size
        )
        mx = self.reader.max_offset(stream)
        lag = 0 if mx is None else max(0, int(mx) - int(expected))
        self.metrics.source_lag[f"{stream.source_table}|{stream.topic}|{stream.partition}"] = lag
        if not records:
            return 0

        offsets = [r.offset for r in records]
        if offsets != sorted(offsets) or len(set(offsets)) != len(offsets):
            raise SourceOffsetConflictError("batch offsets not strictly ascending unique")

        source_ids = [_source_id(r) for r in records]
        existing = self.repo.find_ledger_entries(self.settings.namespace, source_ids)

        persisted_at = _utc_dt()
        replay = self.settings.destination_mode == DestinationMode.REPLAY.value
        replay_run_id = self.settings.replay_run_id or None

        for record in records:
            sid = _source_id(record)
            phash = _payload_hash(record)
            prior = existing.get(sid)

            # Compatible terminal ledger before transform → IDEMPOTENT_SKIPPED (no builder).
            if prior is not None and prior.payload_hash == phash and prior.disposition in {
                DISPOSITION_PROCESSED,
                DISPOSITION_QUARANTINED,
            }:
                mat = materialize_ledger_entry(
                    namespace=self.settings.namespace,
                    source_bronze_event_id=sid,
                    raw_ingestion_id=record.raw_ingestion_id,
                    payload_hash=phash,
                    proposed_disposition=prior.disposition,
                    is_replay=replay,
                    primary_fact_table=(
                        prior.target_table
                        if prior.disposition == DISPOSITION_PROCESSED
                        else None
                    ),
                    existing_before_batch=_to_batch_ledger(prior),
                    outputs_recovered_from_prior_attempt=False,
                    processed_at=persisted_at,
                )
                if mat.idempotent_observed:
                    self.metrics.idempotent_observed_count += 1
                    continue

            result = self.engine.transform(record)
            if result.proposed_disposition == DISPOSITION_PROCESSED:
                if len(result.facts) != 1 or result.quarantine is not None:
                    self._fault("ENGINE_CONTRACT_VIOLATION", "bad PROCESSED cardinality")
                    raise RuntimeError("ENGINE_CONTRACT_VIOLATION")
            elif result.proposed_disposition == DISPOSITION_QUARANTINED:
                if result.facts or result.quarantine is None:
                    self._fault("ENGINE_CONTRACT_VIOLATION", "bad QUARANTINED cardinality")
                    raise RuntimeError("ENGINE_CONTRACT_VIOLATION")
            else:
                self._fault("ENGINE_CONTRACT_VIOLATION", "unexpected disposition")
                raise RuntimeError("ENGINE_CONTRACT_VIOLATION")

            primary = result.proposal.primary_target_table
            outputs_present = False

            # 1) Facts
            if result.facts:
                fact = result.facts[0]
                fact.processed_at = persisted_at
                if primary == "silver_quarantine":
                    raise RuntimeError("ENGINE_CONTRACT_VIOLATION: PROCESSED without fact table")
                identity = _fact_identity(fact, primary)
                fr_map = self.repo.find_fact_states(
                    primary, [identity], replay_run_id=replay_run_id
                )
                fr = fr_map[fact.source_bronze_event_id]
                if fr.result == FactReconcileResult.MISSING:
                    self.repo.insert_fact_batch(primary, [fact], replay_run_id=replay_run_id)
                elif fr.result in {
                    FactReconcileResult.SOURCE_MATCH_PAYLOAD_CONFLICT,
                    FactReconcileResult.BUSINESS_KEY_OWNED_BY_OTHER_SOURCE,
                }:
                    raise FactBusinessKeyConflictError(fr.result.value)
                else:
                    outputs_present = True

                # 2) Dimensions
                active, suppressed = filter_for_replay(result.dimensions, is_replay=replay)
                self.metrics.suppressed_dimension_candidates += suppressed
                current_hashes = fetch_current_hashes(
                    self.repo, active, replay_run_id=replay_run_id
                )
                to_persist = decide_persisted_candidates(active, current_hashes)
                for cand in to_persist:
                    row_obj = cand.row
                    if hasattr(row_obj, "created_at") and getattr(row_obj, "created_at") is None:
                        row_obj.created_at = persisted_at
                    if hasattr(row_obj, "updated_at"):
                        row_obj.updated_at = persisted_at
                    self.repo.insert_dimension_batch(
                        cand.target_table, [row_obj], replay_run_id=replay_run_id
                    )

            # 3) Quarantine
            if result.quarantine is not None:
                q = result.quarantine
                q.created_at = persisted_at
                existing_q = self.repo.find_quarantine_ids(
                    [q.source_bronze_event_id], replay_run_id=replay_run_id
                )
                if q.silver_quarantine_id not in existing_q and not existing_q:
                    self.repo.insert_quarantine_batch([q], replay_run_id=replay_run_id)
                else:
                    outputs_present = True
                self.metrics.quarantined_total += 1

            # 4) Ledger (only after required outputs for this event)
            mat = materialize_ledger_entry(
                namespace=self.settings.namespace,
                source_bronze_event_id=sid,
                raw_ingestion_id=record.raw_ingestion_id,
                payload_hash=phash,
                proposed_disposition=result.proposed_disposition,
                is_replay=replay,
                primary_fact_table=(
                    primary if result.proposed_disposition == DISPOSITION_PROCESSED else None
                ),
                existing_before_batch=_to_batch_ledger(prior),
                outputs_recovered_from_prior_attempt=outputs_present,
                processed_at=persisted_at,
            )
            if mat.recovered_partial:
                self.metrics.recovered_partial_count += 1
            if mat.idempotent_observed:
                self.metrics.idempotent_observed_count += 1
                continue

            entry = SilverLedgerEntry(
                checkpoint_namespace=mat.entry.checkpoint_namespace,
                source_bronze_event_id=mat.entry.source_bronze_event_id,
                raw_ingestion_id=mat.entry.raw_ingestion_id,
                payload_hash=mat.entry.payload_hash,
                disposition=mat.entry.disposition,
                target_table=mat.entry.target_table,
                processed_at=persisted_at,
                migration_version=self.settings.silver_schema_version,
            )
            self.repo.insert_ledger_batch(self.settings.namespace, [entry])

        # 5) Checkpoint CAS — only after every record has a terminal ledger disposition.
        new_off = int(receipt.last_offset) if receipt.last_offset is not None else expected
        cas = self.checkpoint.compare_and_advance(key, expected, new_off)
        if cas == CasResult.RETRY_SAME:
            cas = self.checkpoint.compare_and_advance(key, expected, new_off)
        if cas == CasResult.CAS_CONFLICT:
            raise CheckpointCasConflictError("CAS_CONFLICT")
        if cas not in {CasResult.ADVANCED, CasResult.ALREADY_ADVANCED}:
            raise CheckpointCasConflictError(str(cas))
        self.metrics.mark_batch(len(records))
        self.metrics.mark_checkpoint()
        return len(records)
