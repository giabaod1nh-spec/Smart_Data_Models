"""Gold 3 orchestration and state machine.

``STARTING → RECOVERING → READY → PROCESSING → READY`` is the normal path.
Retryable failures enter ``RETRYING``; exhausted retries enter ``DEGRADED``;
schema/identity conflicts enter ``FAULTED``; shutdown enters ``STOPPING →
STOPPED``. The processor computes no business value: it calls
``GoldTransformationEngine.transform`` exactly once per non-terminal work unit and
persists the result in the Gold Runtime Contract v1 order.
"""
from __future__ import annotations

import dataclasses
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from de.gold.engine import GoldTransformationEngine, GoldTransformationResult
from de.gold_runtime.checkpoint_store import (
    CheckpointBusyError,
    CheckpointCasConflictError,
    GoldRuntimeStore,
    TerminalStateImmutableError,
)
from de.gold_runtime.config import (
    DIM_SOURCE_TABLES,
    SOURCE_TABLE_CAMERA,
    SOURCE_TABLE_INTERSECTION,
    SOURCE_TABLE_SIGNAL,
    SOURCE_TABLE_TRAFFIC,
    CasResult,
    GoldSettings,
    LateClass,
    ProcessorState,
    WindowState,
    WorkUnitState,
)
from de.gold_runtime.context_builder import build_context, build_inputs, order_records
from de.gold_runtime.cursor import (
    ZERO_FACT_CURSOR,
    FactCursor,
    deduplicate_rows,
    source_set_hash,
)
from de.gold_runtime.dimensions import (
    DimensionCandidate,
    build_dim_window,
    build_dimension_candidates,
    metric_definition_candidates,
)
from de.gold_runtime.instance_lock import GoldLockLost, InstanceLock
from de.gold_runtime.metrics import HealthSnapshot, Metrics, utc_str
from de.gold_runtime.processing_ledger import (
    DISPOSITION_PERSISTED,
    DISPOSITION_RECEIVED,
    DISPOSITION_REPLAYED,
    ExpectedOutputManifest,
    ManifestError,
    ReconcileStatus,
    batch_id_for,
    batch_identity,
    build_ledger_row,
    build_manifest,
    input_digest,
    output_digest,
)
from de.gold_runtime.repositories import (
    PERSISTENCE_ORDER,
    RESULT_FIELD_BY_TABLE,
    GoldClickHouseRepository,
    IdentityConflictError,
    InvalidTargetTableError,
    NamespaceGuardError,
    RetryableRepositoryError,
    SchemaMismatchError,
    UncertainWriteError,
)
from de.gold_runtime.revisions import RevisionAction, decide_revision
from de.gold_runtime.silver_readers import (
    RetryableReadError,
    SilverReader,
    SourceSchemaError,
)
from de.gold_runtime.window_scheduler import (
    StreamMaxima,
    WindowIdentity,
    candidate_windows,
    previous_window,
    runtime_watermark,
    source_lag,
)

WINDOW_STREAMS: dict[str, str] = {
    SOURCE_TABLE_TRAFFIC: "traffic",
    SOURCE_TABLE_INTERSECTION: "intersection",
    SOURCE_TABLE_SIGNAL: "signal",
    SOURCE_TABLE_CAMERA: "camera",
}
REQUIRED_WATERMARK_SOURCES: tuple[str, ...] = (
    SOURCE_TABLE_TRAFFIC,
    SOURCE_TABLE_INTERSECTION,
    SOURCE_TABLE_SIGNAL,
)

RETRYABLE_ERRORS = (
    RetryableRepositoryError,
    RetryableReadError,
    UncertainWriteError,
    CheckpointBusyError,
)
PERMANENT_ERRORS = (
    SchemaMismatchError,
    SourceSchemaError,
    IdentityConflictError,
    InvalidTargetTableError,
    NamespaceGuardError,
    CheckpointCasConflictError,
    TerminalStateImmutableError,
    ManifestError,
    GoldLockLost,
)


class WindowOutcome(str):
    """Simple string outcome so tests can assert on readable values."""


OUTCOME_PROCESSED = WindowOutcome("PROCESSED")
OUTCOME_IDEMPOTENT = WindowOutcome("IDEMPOTENT")
OUTCOME_QUARANTINED = WindowOutcome("QUARANTINED")
OUTCOME_CONFLICTED = WindowOutcome("CONFLICTED")
OUTCOME_PERSISTENCE_UNKNOWN = WindowOutcome("PERSISTENCE_UNKNOWN")
OUTCOME_SKIPPED = WindowOutcome("SKIPPED")


@dataclass(frozen=True)
class WindowResult:
    outcome: str
    batch_id: str
    window_id: str
    revision_seq: int
    manifest: Optional[ExpectedOutputManifest] = None
    rows_written: int = 0
    reason: str = ""


@dataclass
class RunPollState:
    upper_bounds: dict[str, Optional[FactCursor]] = field(default_factory=dict)
    maxima: StreamMaxima = field(default_factory=StreamMaxima)


def filter_result_to_window(
    result: GoldTransformationResult, window: WindowIdentity
) -> GoldTransformationResult:
    """Gold 2 emits 60s and 300s outputs; only the target window is published."""
    size = int(window.window_size_sec)
    window_id = window.window_id

    def keep(row: Any) -> bool:
        current = getattr(row, "window_id", None) or getattr(row, "current_window_id", "")
        current_size = int(
            getattr(row, "window_size_sec", None)
            if getattr(row, "window_size_sec", None) is not None
            else getattr(row, "current_window_size_sec", -1)
        )
        return str(current) == window_id and current_size == size

    return dataclasses.replace(
        result,
        **{
            field_name: tuple(row for row in getattr(result, field_name) if keep(row))
            for field_name in RESULT_FIELD_BY_TABLE.values()
        },
    )


class GoldProcessor:
    def __init__(
        self,
        settings: GoldSettings,
        *,
        reader: Optional[SilverReader] = None,
        repository: Optional[GoldClickHouseRepository] = None,
        store: Optional[GoldRuntimeStore] = None,
        engine: Optional[GoldTransformationEngine] = None,
        lock: Optional[InstanceLock] = None,
        clock: Optional[Callable[[], datetime]] = None,
        lock_held: bool = False,
    ) -> None:
        self.settings = settings
        self.reader = reader or SilverReader(settings)
        self.repository = repository or GoldClickHouseRepository(settings)
        self.store = store or GoldRuntimeStore(Path(settings.checkpoint_path))
        self.engine = engine or GoldTransformationEngine()
        self.lock = lock
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.metrics = Metrics()
        self.state = ProcessorState.STARTING
        self._lock_held = lock_held or (lock.held if lock else False)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._snapshot_lock = threading.RLock()
        self._schema_ok = False
        self._clickhouse_ok = False
        self._sqlite_ok = False
        self._retry_index = 0
        self._epoch = time.monotonic()
        self._poll_state: dict[str, RunPollState] = {}
        self._pending_cursors: dict[str, tuple[int, FactCursor]] = {}
        self._non_terminal = 0
        self._health = self._build_snapshot("STARTUP")

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self, *, background: bool = True) -> None:
        self.state = ProcessorState.STARTING
        self.settings.validate_all()
        self.store.open()
        self.reader.connect()
        self.repository.connect()
        try:
            self.repository.verify_schema()
            self.reader.verify_source_schema()
            self._schema_ok = True
        except Exception:
            self._schema_ok = False
            self._fault("SCHEMA_MISMATCH", "target or source schema verification failed")
            raise
        self.state = ProcessorState.RECOVERING
        self.recover()
        self.state = ProcessorState.READY
        self._refresh_dependencies()
        self._publish()
        if background:
            self._thread = threading.Thread(
                target=self._run, name="gold-runtime-processor", daemon=True
            )
            self._thread.start()
        self._publish()

    def stop(self) -> None:
        self.state = ProcessorState.STOPPING
        self._stop.set()
        self._publish()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.settings.shutdown_timeout_sec)
        for closer in (self.reader.close, self.repository.close, self.store.close):
            try:
                closer()
            except Exception:
                pass
        self.state = ProcessorState.STOPPED
        self._publish()

    def request_shutdown(self) -> None:
        self._stop.set()

    def health_snapshot(self) -> HealthSnapshot:
        with self._snapshot_lock:
            return self._health

    # ── main loop ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self._stop.is_set() and self.state not in {
            ProcessorState.FAULTED,
            ProcessorState.STOPPING,
            ProcessorState.STOPPED,
        }:
            try:
                self._refresh_dependencies()
                self.run_cycle()
                self._retry_index = 0
                if self.state in {ProcessorState.RETRYING, ProcessorState.PROCESSING}:
                    self.state = ProcessorState.READY
                self._publish()
                self._stop.wait(self.settings.poll_interval_sec)
            except PERMANENT_ERRORS as exc:
                self._fault(type(exc).__name__, str(exc))
                break
            except RETRYABLE_ERRORS as exc:
                self._enter_retry(exc)
            except Exception as exc:  # noqa: BLE001 — unexpected failures are permanent
                self._fault("UNHANDLED_PROCESSOR_EXCEPTION", str(exc))
                break

    def _enter_retry(self, exc: Exception) -> None:
        self.metrics.retries_total += 1
        if isinstance(exc, UncertainWriteError):
            self.metrics.uncertain_write_reconciliations += 1
        delay = self.settings.retry_delay(self._retry_index)
        self._retry_index += 1
        self.state = (
            ProcessorState.DEGRADED
            if self._retry_index >= self.settings.retry_max_attempts
            else ProcessorState.RETRYING
        )
        self.metrics.set_fault(type(exc).__name__, str(exc))
        self._publish()
        self._stop.wait(delay)

    def run_cycle(self) -> int:
        """One bounded poll/schedule/process cycle. Returns processed window count."""
        if self._stop.is_set():
            return 0
        if self.lock is not None and not self.lock.verify():
            raise GoldLockLost(f"instance lock lost for {self.settings.namespace}")
        upper_bounds = self._snapshot_upper_bounds()
        if not any(bound is not None for bound in upper_bounds.values()):
            return 0
        self._poll_sources(upper_bounds)
        runs = self._runs_in_scope(upper_bounds)
        processed = 0
        for simulation_run_id, scenario_id in runs:
            maxima = self._stream_maxima(simulation_run_id, upper_bounds)
            mark = runtime_watermark(maxima)
            self.metrics.watermark = mark
            windows = candidate_windows(
                namespace=self.settings.namespace,
                simulation_run_id=simulation_run_id,
                scenario_id=scenario_id,
                maxima=maxima,
                window_sizes_sec=self.settings.window_size_list(),
                floor_by_size=self._closed_floor(simulation_run_id, scenario_id),
                allowed_lateness_sec=self.settings.allowed_lateness_sec,
                delay_sec=self.settings.watermark_delay_sec,
                limit=self.settings.max_windows_per_cycle,
            )
            if not windows:
                continue
            self.metrics.set_lag(source_lag(maxima, windows[0].window_end_sim_sec))
            for window in windows:
                started = time.monotonic()
                self.state = ProcessorState.PROCESSING
                self._publish()
                result = self.process_window(window, upper_bounds=upper_bounds)
                self.metrics.mark_window(time.monotonic() - started)
                if result.outcome == OUTCOME_PROCESSED:
                    processed += 1
                if result.outcome in {OUTCOME_CONFLICTED}:
                    raise IdentityConflictError(result.reason or "source identity conflict")
                if result.outcome == OUTCOME_PERSISTENCE_UNKNOWN:
                    self.state = ProcessorState.DEGRADED
                    self._publish()
                    return processed
        self._advance_cursors()
        self.state = ProcessorState.READY if self.state is ProcessorState.PROCESSING else self.state
        return processed

    # ── polling and watermark ───────────────────────────────────────────────

    def _fact_sources(self) -> tuple[str, ...]:
        return tuple(
            table for table in self.settings.source_table_list() if table in WINDOW_STREAMS
        )

    def _snapshot_upper_bounds(self) -> dict[str, Optional[FactCursor]]:
        return {source: self.reader.snapshot_upper_bound(source) for source in self._fact_sources()}

    def _cursor_for(self, source: str) -> tuple[int, FactCursor]:
        row = self.store.get_cursor(self.settings.namespace, source)
        if row is None:
            row = self.store.initialize_cursor(
                self.settings.namespace, source, ZERO_FACT_CURSOR.to_json()
            )
        return int(row.generation), FactCursor.from_json(row.cursor_json)

    def _poll_sources(self, upper_bounds: Mapping[str, Optional[FactCursor]]) -> int:
        """Bounded ordered poll; the durable cursor moves only after terminal work."""
        total = 0
        for source, bound in upper_bounds.items():
            if bound is None:
                continue
            generation, cursor = self._cursor_for(source)
            rows, receipt = self.reader.read_fact_batch(source, cursor, bound)
            total += receipt.physical_count
            self.metrics.duplicates_total += receipt.duplicate_count
            if receipt.conflicts:
                raise IdentityConflictError(
                    f"{source}: conflicting payload hashes for {list(receipt.conflicts)[:3]}"
                )
            if receipt.last_cursor is not None:
                self._pending_cursors[source] = (generation, receipt.last_cursor)
        if total:
            self.metrics.mark_batch(total)
        return total

    def _advance_cursors(self) -> None:
        for source, (generation, cursor) in list(self._pending_cursors.items()):
            result = self.store.compare_and_advance_cursor(
                self.settings.namespace,
                source,
                expected_generation=generation,
                cursor_json=cursor.to_json(),
            )
            if result is CasResult.RETRY_SAME:
                result = self.store.compare_and_advance_cursor(
                    self.settings.namespace,
                    source,
                    expected_generation=generation,
                    cursor_json=cursor.to_json(),
                )
            if result not in {CasResult.ADVANCED, CasResult.ALREADY_ADVANCED}:
                self.metrics.cas_conflicts_total += 1
                raise CheckpointCasConflictError(f"cursor CAS failed for {source}: {result}")
            self._pending_cursors.pop(source, None)
        self.metrics.mark_checkpoint()

    def _runs_in_scope(
        self, upper_bounds: Mapping[str, Optional[FactCursor]]
    ) -> tuple[tuple[str, str], ...]:
        discovered = self.reader.discover_runs(upper_bounds)
        scope = self.settings.run_scope_list()
        if not scope:
            return discovered
        allowed = set(scope)
        return tuple(item for item in discovered if item[0] in allowed)

    def _stream_maxima(
        self, simulation_run_id: str, upper_bounds: Mapping[str, Optional[FactCursor]]
    ) -> StreamMaxima:
        values: dict[str, Optional[float]] = {}
        for source, stream in WINDOW_STREAMS.items():
            bound = upper_bounds.get(source)
            if bound is None:
                values[stream] = None
                continue
            values[stream] = self.reader.max_simulation_time(
                source, simulation_run_id=simulation_run_id, upper_bound=bound
            )
        return StreamMaxima(**values)

    def _closed_floor(self, simulation_run_id: str, scenario_id: str) -> dict[int, float]:
        """Highest already-closed window end per size, so no window is re-queued."""
        floors: dict[int, float] = {}
        for size in self.settings.window_size_list():
            floors[int(size)] = 0.0
        closed = self.store.closed_window_ends(self.settings.namespace)
        if not closed:
            return floors
        lookup = set(closed)
        for size in self.settings.window_size_list():
            start = 0.0
            while True:
                window = self._identity(simulation_run_id, scenario_id, int(size), start)
                if window.window_id not in lookup:
                    break
                floors[int(size)] = window.window_end_sim_sec
                start += float(size)
        return floors

    def _identity(
        self, simulation_run_id: str, scenario_id: str, size: int, start: float
    ) -> WindowIdentity:
        from de.gold_runtime.window_scheduler import make_window_identity

        return make_window_identity(
            self.settings.namespace, simulation_run_id, scenario_id, size, start
        )

    # ── work unit ───────────────────────────────────────────────────────────

    def process_window(
        self,
        window: WindowIdentity,
        *,
        upper_bounds: Optional[Mapping[str, Optional[FactCursor]]] = None,
        revision_seq: Optional[int] = None,
    ) -> WindowResult:
        bounds = dict(upper_bounds or self._snapshot_upper_bounds())
        previous = previous_window(window)
        rows_by_source, conflicts = self._read_work_unit_rows(window, previous, bounds)
        if conflicts:
            return WindowResult(
                OUTCOME_CONFLICTED, "", window.window_id, 0,
                reason=f"conflicting source identity: {list(conflicts)[:3]}",
            )

        records = order_records(
            record
            for source, rows in rows_by_source.items()
            for record in build_inputs(source, rows)
        )
        observed_hash = source_set_hash(
            (
                record.__class__.__name__,
                record.simulation_run_id,
                record.intersection_id,
                getattr(record, "canonical_direction", ""),
                float(record.simulation_time_sec),
                record.source_payload_hash,
            )
            for record in records
        )

        state_row = self.store.latest_window_state(self.settings.namespace, window.window_id)
        decision = decide_revision(
            window_state=state_row, observed_source_set_hash=observed_hash
        )
        if decision.action is RevisionAction.IDEMPOTENT:
            self.metrics.idempotent_windows_total += 1
            return WindowResult(
                OUTCOME_IDEMPOTENT, state_row.batch_id or "", window.window_id,
                decision.revision_seq, reason=decision.reason,
            )
        if decision.action is RevisionAction.QUARANTINE:
            self.metrics.quarantines_total += 1
            return WindowResult(
                OUTCOME_QUARANTINED, state_row.batch_id or "", window.window_id,
                decision.revision_seq, reason=decision.reason,
            )
        if decision.action is RevisionAction.CONFLICTED:
            return WindowResult(
                OUTCOME_CONFLICTED, "", window.window_id, decision.revision_seq,
                reason=decision.reason,
            )
        target_revision = (
            int(revision_seq) if revision_seq is not None
            else (decision.revision_seq if decision.action is RevisionAction.REVISE else 0)
        )
        if decision.action is RevisionAction.REVISE:
            self.metrics.revisions_total += 1
            self.metrics.mark_late(LateClass.LATE_AFTER_CLOSE)

        identity = batch_identity(
            namespace=self.settings.namespace,
            simulation_run_id=window.simulation_run_id,
            scenario_id=window.scenario_id,
            window_id=window.window_id,
            source_set_hash=observed_hash,
            definition_version=self.settings.definition_version,
            gold_schema_version=self.settings.gold_schema_version,
            revision_seq=target_revision,
        )
        batch_id = batch_id_for(identity)
        existing_unit = self.store.get_work_unit(batch_id)
        if existing_unit is not None and WorkUnitState(existing_unit.state) in {
            WorkUnitState.CHECKPOINTED, WorkUnitState.REPLAYED, WorkUnitState.QUARANTINED,
        }:
            self.metrics.idempotent_windows_total += 1
            return WindowResult(
                OUTCOME_IDEMPOTENT, batch_id, window.window_id, target_revision,
                reason="TERMINAL_WORK_UNIT",
            )

        self.store.upsert_window_state(
            self.settings.namespace, window.window_id, target_revision,
            state=WindowState.OPEN,
        )
        self._transition_window(window, target_revision, WindowState.OPEN, WindowState.ELIGIBLE)
        self._transition_window(
            window, target_revision, WindowState.ELIGIBLE, WindowState.PROCESSING,
            increment_attempt=True,
        )
        self.store.upsert_work_unit(
            batch_id=batch_id,
            namespace=self.settings.namespace,
            window_id=window.window_id,
            revision_seq=target_revision,
            state=WorkUnitState.RECEIVED,
            input_digest=input_digest(records),
            expected_manifest_json="",
        )
        computed_at = self.clock()
        self._record_ledger(observed_hash, target_revision, DISPOSITION_RECEIVED, computed_at)

        intersections = tuple(sorted({record.intersection_id for record in records}))
        context = build_context(
            self.settings,
            computed_at=computed_at,
            windows=(window, previous),
            intersections=intersections,
            revision_seq=target_revision,
        )
        result = self.engine.transform(records, context)
        targeted = filter_result_to_window(result, window)
        manifest = build_manifest(
            targeted,
            batch_id=batch_id,
            namespace=self.settings.namespace,
            window_id=window.window_id,
            revision_seq=target_revision,
        )
        self.store.set_work_unit_state(
            batch_id, WorkUnitState.TRANSFORMED, expected_manifest_json=manifest.to_json()
        )
        self.store.cas_window_state(
            self.settings.namespace, window.window_id, target_revision,
            expected_state=WindowState.PROCESSING, new_state=WindowState.PROCESSING,
            source_set_hash=observed_hash, batch_id=batch_id,
            output_digest=output_digest(manifest),
        )

        written = self._persist(window, previous, targeted, manifest, computed_at, context)
        if written is None:
            self.store.set_work_unit_state(batch_id, WorkUnitState.PERSISTENCE_UNKNOWN)
            return WindowResult(
                OUTCOME_PERSISTENCE_UNKNOWN, batch_id, window.window_id, target_revision,
                manifest=manifest, reason="reconciliation incomplete",
            )
        if written == -1:
            self.store.set_work_unit_state(batch_id, WorkUnitState.CONFLICTED)
            return WindowResult(
                OUTCOME_CONFLICTED, batch_id, window.window_id, target_revision,
                manifest=manifest, reason="manifest identity hash conflict",
            )

        self.store.set_work_unit_state(batch_id, WorkUnitState.PERSISTED)
        terminal = DISPOSITION_REPLAYED if self.settings.is_replay() else DISPOSITION_PERSISTED
        self._record_ledger(observed_hash, target_revision, terminal, computed_at)
        self._close_window(window, target_revision, decision.action is RevisionAction.REVISE)
        self.store.set_work_unit_state(
            batch_id,
            WorkUnitState.REPLAYED if self.settings.is_replay() else WorkUnitState.CHECKPOINTED,
        )
        if self.settings.is_replay():
            self.metrics.replay_batches_total += 1
        self.metrics.facts_written_total += written
        self.metrics.mark_batch(len(records), batch_id=batch_id, window_id=window.window_id)
        self.metrics.mark_checkpoint()
        return WindowResult(
            OUTCOME_PROCESSED, batch_id, window.window_id, target_revision,
            manifest=manifest, rows_written=written,
        )

    def _transition_window(
        self,
        window: WindowIdentity,
        revision_seq: int,
        expected: WindowState,
        new: WindowState,
        *,
        increment_attempt: bool = False,
    ) -> None:
        current = self.store.get_window_state(
            self.settings.namespace, window.window_id, revision_seq
        )
        if current is not None and current.state == new.value:
            return
        result = self.store.cas_window_state(
            self.settings.namespace, window.window_id, revision_seq,
            expected_state=expected, new_state=new, increment_attempt=increment_attempt,
        )
        if result not in {CasResult.ADVANCED, CasResult.ALREADY_ADVANCED}:
            self.metrics.cas_conflicts_total += 1
            raise CheckpointCasConflictError(
                f"window state CAS failed: {window.window_id} {expected.value}->{new.value}"
            )

    def _close_window(self, window: WindowIdentity, revision_seq: int, revised: bool) -> None:
        self.store.cas_window_state(
            self.settings.namespace, window.window_id, revision_seq,
            expected_state=WindowState.PROCESSING,
            new_state=WindowState.REVISED if revised else WindowState.CLOSED,
        )

    def _read_work_unit_rows(
        self,
        window: WindowIdentity,
        previous: WindowIdentity,
        bounds: Mapping[str, Optional[FactCursor]],
    ) -> tuple[dict[str, tuple[dict, ...]], tuple[tuple, ...]]:
        """Target plus immediately preceding window, from one Silver snapshot."""
        rows_by_source: dict[str, tuple[dict, ...]] = {}
        conflicts: list[tuple] = []
        for source in self._fact_sources():
            bound = bounds.get(source)
            if bound is None:
                continue
            collected: list[dict] = []
            for bounds_window in (previous, window):
                rows, receipt = self.reader.read_window_rows(
                    source,
                    simulation_run_id=window.simulation_run_id,
                    window_start_sim_sec=bounds_window.window_start_sim_sec,
                    window_end_sim_sec=bounds_window.window_end_sim_sec,
                    upper_bound=bound,
                )
                conflicts.extend(receipt.conflicts)
                collected.extend(rows)
            deduplicated = deduplicate_rows(source, collected)
            conflicts.extend(deduplicated.conflicts)
            self.metrics.duplicates_total += deduplicated.duplicates
            if deduplicated.rows:
                rows_by_source[source] = deduplicated.rows
        return rows_by_source, tuple(sorted(set(conflicts)))

    # ── persistence ─────────────────────────────────────────────────────────

    def _persist(
        self,
        window: WindowIdentity,
        previous: WindowIdentity,
        result: GoldTransformationResult,
        manifest: ExpectedOutputManifest,
        computed_at: datetime,
        context: Any,
    ) -> Optional[int]:
        """Dimensions → Facts → Comparisons → Signal → KPI. ``None`` = unknown."""
        self._sync_dimensions(window, previous, result, computed_at)

        existing = self.repository.find_existing(
            manifest.batch_id,
            sorted(manifest.identity_set()),
            revision_seq=manifest.revision_seq,
        )
        report = manifest.reconcile(existing)
        if report.status is ReconcileStatus.CONFLICTED:
            return -1
        missing = {entry.logical_identity for entry in report.missing}

        written = 0
        for table in PERSISTENCE_ORDER:
            rows = [
                row for row in getattr(result, RESULT_FIELD_BY_TABLE[table])
                if self._identity_of(table, row) in missing
            ]
            if not rows:
                continue
            written += self._write_target(table, rows)

        final_state = self.repository.find_existing(
            manifest.batch_id,
            sorted(manifest.identity_set()),
            revision_seq=manifest.revision_seq,
        )
        final_report = manifest.reconcile(final_state)
        if final_report.status is ReconcileStatus.CONFLICTED:
            return -1
        if not final_report.complete:
            return None
        return written

    @staticmethod
    def _identity_of(table: str, row: Any) -> tuple:
        from de.gold_runtime.repositories import logical_identity

        return logical_identity(table, row)

    def _write_target(self, table: str, rows: Sequence[Any]) -> int:
        if table == "gold_fact_traffic_comparison":
            receipt = self.repository.insert_comparisons(rows)
        elif table == "gold_fact_signal_operation_window":
            receipt = self.repository.insert_signal_windows(rows)
        elif table == "gold_fact_kpi_result":
            receipt = self.repository.insert_kpis(rows)
        else:
            receipts = self.repository.insert_facts(rows)
            return sum(item.confirmed for item in receipts)
        return receipt.confirmed

    def _sync_dimensions(
        self,
        window: WindowIdentity,
        previous: WindowIdentity,
        result: GoldTransformationResult,
        computed_at: datetime,
    ) -> None:
        """Deterministic dimension sync precedes fact visibility; replay never writes."""
        if self.settings.is_replay():
            return
        candidates: list[DimensionCandidate] = []
        advanced: list[tuple[str, int, str]] = []
        for source in self.settings.source_table_list():
            if source not in DIM_SOURCE_TABLES:
                continue
            row = self.store.get_cursor(self.settings.namespace, source)
            if row is None:
                from de.gold_runtime.cursor import ZERO_DIMENSION_CURSOR

                row = self.store.initialize_cursor(
                    self.settings.namespace, source, ZERO_DIMENSION_CURSOR.to_json()
                )
            from de.gold_runtime.cursor import DimensionCursor

            cursor = DimensionCursor.from_json(row.cursor_json)
            rows, next_cursor = self.reader.read_dimension_rows(source, cursor)
            if not rows:
                continue
            candidates.extend(
                build_dimension_candidates(source, rows, self.settings, computed_at)
            )
            advanced.append((source, int(row.generation), next_cursor.to_json()))
        candidates.append(build_dim_window(window, self.settings, computed_at))
        candidates.append(build_dim_window(previous, self.settings, computed_at))
        candidates.extend(metric_definition_candidates(result.metric_definitions))
        receipts = self.repository.upsert_dimensions(candidates)
        self.metrics.dimensions_written_total += sum(item.confirmed for item in receipts)
        for source, generation, cursor_json in advanced:
            self.store.compare_and_advance_cursor(
                self.settings.namespace, source,
                expected_generation=generation, cursor_json=cursor_json,
            )

    def _record_ledger(
        self, hash_value: str, revision_seq: int, disposition: str, computed_at: datetime
    ) -> None:
        self.repository.record_ledger(
            build_ledger_row(
                namespace=self.settings.namespace,
                source_set_hash=hash_value,
                definition_version=self.settings.definition_version,
                revision_seq=revision_seq,
                disposition=disposition,
                computed_at=computed_at,
                gold_schema_version=self.settings.gold_schema_version,
            )
        )

    # ── recovery ────────────────────────────────────────────────────────────

    def recover(self) -> int:
        """Reconcile non-terminal work units by identity before scheduling new work."""
        units = self.store.non_terminal_work_units(self.settings.namespace)
        recovered = 0
        for unit in units:
            if not unit.expected_manifest_json:
                self.store.set_work_unit_state(unit.batch_id, WorkUnitState.FAILED_RETRYABLE)
                continue
            manifest = ExpectedOutputManifest.from_json(unit.expected_manifest_json)
            existing = self.repository.find_existing(
                manifest.batch_id,
                sorted(manifest.identity_set()),
                revision_seq=manifest.revision_seq,
            )
            report = manifest.reconcile(existing)
            if report.status is ReconcileStatus.CONFLICTED:
                self.store.set_work_unit_state(unit.batch_id, WorkUnitState.CONFLICTED)
                self._fault("CONFLICTED", f"identity hash conflict in {unit.batch_id}")
                raise IdentityConflictError(f"identity hash conflict in {unit.batch_id}")
            if report.complete:
                self.store.set_work_unit_state(
                    unit.batch_id,
                    WorkUnitState.REPLAYED
                    if self.settings.is_replay()
                    else WorkUnitState.CHECKPOINTED,
                )
                recovered += 1
            else:
                self.store.set_work_unit_state(unit.batch_id, WorkUnitState.FAILED_RETRYABLE)
        self.metrics.recovered_work_units_total += recovered
        self.metrics.ledger_recovery_counts = {
            "non_terminal_at_start": len(units),
            "recovered": recovered,
        }
        self._non_terminal = len(self.store.non_terminal_work_units(self.settings.namespace))
        return recovered

    # ── health ──────────────────────────────────────────────────────────────

    def _refresh_dependencies(self) -> None:
        self._clickhouse_ok = self.reader.ping() and self.repository.ping()
        self._sqlite_ok = self.store.is_readable()
        if self.lock is not None:
            self._lock_held = self.lock.verify()

    def _seconds_since_progress(self) -> float:
        stamp = self.metrics.last_progress_at or self.metrics.last_checkpoint_at
        if not stamp:
            return time.monotonic() - self._epoch
        try:
            parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            return time.monotonic() - self._epoch
        return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())

    def _readiness_reason(self) -> str:
        if not self._lock_held:
            return "LOCK_NOT_HELD"
        if not self._schema_ok:
            return "SCHEMA_NOT_VERIFIED"
        if not self._clickhouse_ok:
            return "CLICKHOUSE_UNAVAILABLE"
        if not self._sqlite_ok:
            return "SQLITE_UNAVAILABLE"
        if self._stop.is_set():
            return "SHUTDOWN_REQUESTED"
        if self.state in {ProcessorState.FAULTED, ProcessorState.DEGRADED}:
            return self.state.value
        if self.state not in {ProcessorState.READY, ProcessorState.PROCESSING}:
            return self.state.value
        if self._seconds_since_progress() > self.settings.readiness_lag_threshold_sec:
            return "PROGRESS_STALE"
        return ""

    def _build_snapshot(self, reason: str = "") -> HealthSnapshot:
        metrics = self.metrics.snapshot()
        return HealthSnapshot(
            state=self.state.value,
            ready=not reason,
            worker_alive=bool(self._thread and self._thread.is_alive()),
            reader_initialized=self.reader.initialized,
            clickhouse_ok=self._clickhouse_ok,
            sqlite_ok=self._sqlite_ok,
            schema_ok=self._schema_ok,
            lock_held=self._lock_held,
            namespace=self.settings.namespace,
            mode=self.settings.destination_mode,
            shutdown_requested=self._stop.is_set(),
            snapshot_at=utc_str(),
            metrics=metrics,
            fault_code=self.metrics.fault_code,
            fault_message=self.metrics.fault_message,
            last_batch_id=self.metrics.last_batch_id,
            last_window_id=self.metrics.last_window_id,
            last_checkpoint_at=self.metrics.last_checkpoint_at,
            watermark=self.metrics.watermark,
            non_terminal_work_units=self._non_terminal,
            reason=reason,
        )

    def _publish(self) -> None:
        snapshot = self._build_snapshot(self._readiness_reason())
        with self._snapshot_lock:
            if snapshot.ready != self._health.ready:
                self.metrics.readiness_transitions += 1
            self._health = snapshot

    def _fault(self, code: str, message: str) -> None:
        self.state = ProcessorState.FAULTED
        self.metrics.set_fault(code, message)
        self._publish()
