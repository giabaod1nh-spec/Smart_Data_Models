"""Silver V1 journal worker, lifecycle and durable recovery."""
from __future__ import annotations

import threading
import json
import sqlite3
import time
from dataclasses import replace
from datetime import datetime, timezone
from itertools import groupby
from pathlib import Path
from typing import Any, Optional

from de.silver.checkpoint_store import (
    CheckpointError,
    CheckpointBusyError,
    CheckpointCasConflictError,
    SilverCheckpointStore,
)
from de.silver.config import BACKOFF_SCHEDULE_SEC, DestinationMode, ProcessorState, SilverSettings, SourceStream
from de.silver.fact_builders import build_v1_fact_rows
from de.silver.dimension_builders import build_v1_dimensions
from de.silver.source_binding import SourceBoundaryChangedError, SourceVerificationError, verify_bronze_source
from de.silver.archive_resolver import ArchiveResolver, ArchiveIntegrityError
from de.silver.lifecycle_refresher import LifecycleRefresher
from de.bronze.checkpoint_store import V1SemanticReader
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.silver.metrics import HealthSnapshot, Metrics
from de.silver.repositories import (
    FactBusinessKeyConflictError, InvalidTargetTableError, SchemaMismatchError, SilverClickHouseRepository,
    V1_DIRECTION_BATCH_LIMIT, V1_DIRECTION_TABLES,
)


def _utc_dt() -> datetime:
    return datetime.now(timezone.utc)


def _utc_str() -> str:
    return _utc_dt().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SilverProcessor:
    def __init__(
        self,
        settings: SilverSettings,
        *,
        repo: Optional[SilverClickHouseRepository] = None,
        checkpoint: Optional[SilverCheckpointStore] = None,
        lock_held: bool = False,
    ) -> None:
        self.settings = settings
        self.repo = repo or SilverClickHouseRepository(settings)
        self.checkpoint = checkpoint or SilverCheckpointStore(Path(settings.checkpoint_path))
        self.metrics = Metrics()
        self.state = ProcessorState.STARTING
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lifecycle_thread: Optional[threading.Thread] = None
        self._lifecycle_ok = False
        self._lifecycle_error = ''
        self._lifecycle_last_poll = 0.0
        self._lifecycle_fault: Exception | None = None
        self._lock_held = lock_held
        self._streams: tuple[SourceStream, ...] = ()
        self._rr_index = 0
        self._schema_ok = False
        self._v1_reader_initialized = False
        self.bronze_repo: Optional[BronzeClickHouseRepository] = None
        self.archive_resolver: Optional[ArchiveResolver] = None
        self.lifecycle: Optional[LifecycleRefresher] = None
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
        self._publish_snapshot()
        self._thread = threading.Thread(target=self._run_v1, name="silver-processor", daemon=True)
        self._thread.start()
        self._publish_snapshot()

    def _initialize_v1(self) -> None:
        """Verify full source history before resume; retry temporary startup outages."""
        self.repo.connect()
        try:
            self.repo.verify_v1_schema(replay=self.settings.destination_mode == DestinationMode.REPLAY.value)
            binding = verify_bronze_source(Path(self.settings.v1_bronze_path), self.settings.v1_source_namespace)
            self.checkpoint.bind_v1_source(self.settings.namespace, binding)
            self._v1_reader_initialized = True
            bronze_settings = ObservationSettings(
                checkpoint_path=self.settings.v1_bronze_path,
                clickhouse_host=self.settings.clickhouse_host,
                clickhouse_port=self.settings.clickhouse_port,
                clickhouse_user=self.settings.clickhouse_user,
                clickhouse_password=self.settings.clickhouse_password,
                clickhouse_database=self.settings.clickhouse_database,
                clickhouse_secure=self.settings.clickhouse_secure)
            if self.bronze_repo is None:
                self.bronze_repo = BronzeClickHouseRepository(bronze_settings)
            self.bronze_repo.connect()
            self.archive_resolver = ArchiveResolver(Path(self.settings.v1_archive_root))
            self.archive_resolver.verify_registry()
            self.lifecycle = LifecycleRefresher(Path(self.settings.checkpoint_path),
                                                self.archive_resolver,
                                                interval_seconds=self.settings.v1_lifecycle_interval_sec)
        except Exception:
            self._schema_ok = False
            raise
        while not self._stop.is_set() and self.checkpoint.pending_v1_obligations(self.settings.namespace, 100):
            self.recover_v1_obligations(100)
        self._schema_ok = True
        self.state = ProcessorState.RECOVERING
        if self._lifecycle_thread is None:
            self._lifecycle_thread = threading.Thread(target=self._run_lifecycle,
                name='silver-lifecycle', daemon=True)
            self._lifecycle_thread.start()
        self._clickhouse_ok = self.repo.ping() and self.bronze_repo.ping()
        self._sqlite_ok = self.checkpoint.is_readable()
        self._publish_snapshot()

    def stop(self) -> None:
        self.state = ProcessorState.STOPPING
        self._stop.set()
        self._publish_snapshot()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        if self._lifecycle_thread and self._lifecycle_thread.is_alive():
            self._lifecycle_thread.join(timeout=30)
        closers = [self.repo.close, self.checkpoint.close]
        if self.bronze_repo is not None:
            closers.append(self.bronze_repo.close)
        for closer in closers:
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
            snapshot = self._health_snapshot
        if snapshot.ready and time.monotonic() - self._lifecycle_last_poll > (
                self.settings.v1_lifecycle_interval_sec * self.settings.v1_lifecycle_stale_intervals):
            return replace(snapshot, ready=False, lifecycle_ok=False, state=ProcessorState.DEGRADED.value)
        return snapshot

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
        lifecycle_current = (self._lifecycle_ok and self._lifecycle_thread is not None and
            self._lifecycle_thread.is_alive() and time.monotonic() - self._lifecycle_last_poll <=
            self.settings.v1_lifecycle_interval_sec * self.settings.v1_lifecycle_stale_intervals)
        if stale_lag and self.state == ProcessorState.READY:
            self.state = ProcessorState.DEGRADED
        elif (
            not stale_lag
            and self.state == ProcessorState.DEGRADED
            and not self.metrics.fault_code
            and self._retry_idx == 0
        ):
            self.state = ProcessorState.READY
        if self._lifecycle_fault is not None and self.state not in {ProcessorState.STOPPING, ProcessorState.STOPPED}:
            self.state = ProcessorState.FAULTED
        elif not lifecycle_current and self.state == ProcessorState.READY:
            self.state = ProcessorState.DEGRADED
        snap = HealthSnapshot(
            state=self.state.value,
            ready=(
                self.state == ProcessorState.READY
                and self._clickhouse_ok
                and self._sqlite_ok
                and self._schema_ok
                and self._v1_reader_initialized
                and (self._thread.is_alive() if self._thread else False)
                and self._lock_held
                and not self._stop.is_set()
                and not stale_lag
                and lifecycle_current
            ),
            worker_alive=bool(self._thread and self._thread.is_alive()),
            reader_initialized=self._v1_reader_initialized,
            clickhouse_ok=self._clickhouse_ok,
            sqlite_ok=self._sqlite_ok,
            schema_ok=self._schema_ok,
            lock_held=self._lock_held,
            namespace=self.settings.namespace,
            mode=self.settings.destination_mode,
            shutdown_requested=self._stop.is_set(),
            snapshot_at=_utc_str(),
            metrics=self.metrics.snapshot(),
            lifecycle_ok=bool(lifecycle_current),
            lifecycle_error=self._lifecycle_error,
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


    def _run_lifecycle(self) -> None:
        """Poll independently of event batches with an independent CH connection."""
        repo = SilverClickHouseRepository(self.settings)
        prior = self.lifecycle.poll_state()
        failures = prior['retry_count'] if prior else 0
        try:
            while not self._stop.is_set():
                try:
                    self.lifecycle.mark_stale(self.settings.v1_lifecycle_interval_sec *
                                              self.settings.v1_lifecycle_stale_intervals)
                    self.lifecycle.poll()
                    self._lifecycle_last_poll = time.monotonic()
                    if self.lifecycle.has_pending_projection():
                        repo.connect()
                        while not self._stop.is_set() and self.lifecycle.project_pending(repo,
                                replay_run_id=self.settings.replay_run_id or None):
                            pass
                    self._lifecycle_ok = self.lifecycle.is_current(
                        self.settings.v1_lifecycle_interval_sec * self.settings.v1_lifecycle_stale_intervals)
                    failures = 0 if self._lifecycle_ok else min(failures + 1, 6)
                    self._lifecycle_error = '' if self._lifecycle_ok else 'lifecycle authority stale'
                except (ArchiveIntegrityError, FactBusinessKeyConflictError) as exc:
                    self._lifecycle_fault = exc
                    self._lifecycle_ok = False
                    self._lifecycle_error = str(exc)
                    self.metrics.fault_code = 'LIFECYCLE_INTEGRITY'
                    self.metrics.fault_message = str(exc)
                    self._publish_snapshot()
                    return
                except Exception as exc:
                    self._lifecycle_ok = False
                    self._lifecycle_error = str(exc)
                    failures = min(failures + 1, 6)
                delay = min(30.0, 2 ** (failures - 1)) if failures else self.lifecycle.interval_seconds
                try:
                    self.lifecycle.record_poll_result(failures, delay, self._lifecycle_error)
                except sqlite3.Error:
                    self._lifecycle_ok = False
                self._publish_snapshot()
                self._stop.wait(delay)
        finally:
            repo.close()

    def _run_v1(self) -> None:
        source_reader = None
        pass_limit = 10
        try:
            while not self._stop.is_set():
                continue_backlog = False
                try:
                    if not self._schema_ok:
                        self._initialize_v1()
                    if source_reader is None:
                        source_reader = V1SemanticReader(self.settings.v1_bronze_path)
                    self._clickhouse_ok = self.metrics.timed("silver_ping", self.repo.ping) and (
                        self.metrics.timed("bronze_ping", self.bronze_repo.ping))
                    self._sqlite_ok = self.metrics.timed("sqlite_probe", self.checkpoint.is_readable)
                    if not self._clickhouse_ok or not self._sqlite_ok:
                        raise ConnectionError("V1 Silver dependency unavailable")
                    if self._lifecycle_fault is not None:
                        raise self._lifecycle_fault
                    processed = self.metrics.timed("journal_pass", self.run_v1_journal_pass,
                        source_reader, self.bronze_repo,
                        self.archive_resolver, self.lifecycle,
                        batch_size=min(self.settings.batch_size, pass_limit), max_entries=pass_limit)
                    if self._lifecycle_fault is not None:
                        raise self._lifecycle_fault
                    self.state = ProcessorState.READY
                    self._retry_idx = 0
                    self.metrics.fault_code = ""
                    self.metrics.fault_message = ""
                    if processed:
                        self.metrics.mark_batch(processed)
                        self.metrics.mark_checkpoint()
                    # Only remove idle delay after a full successful pass with
                    # remaining work inside the boundary verified by that pass.
                    continue_backlog = (
                        processed == pass_limit and sum(self.metrics.source_lag.values()) > processed)
                except ArchiveIntegrityError as exc:
                    self.metrics.fault_code = "ARCHIVE_INTEGRITY"
                    self.metrics.fault_message = str(exc)
                    self.state = ProcessorState.FAULTED
                    self._publish_snapshot()
                    return
                except CheckpointBusyError as exc:
                    self.metrics.fault_code = "SQLITE_BUSY"
                    self.metrics.fault_message = str(exc)
                    self.state = ProcessorState.RETRYING
                    self._retry_idx = min(self._retry_idx + 1, len(BACKOFF_SCHEDULE_SEC) - 1)
                except (SourceVerificationError, FactBusinessKeyConflictError, CheckpointError,
                        SchemaMismatchError) as exc:
                    fallback = ("SOURCE_BINDING_CHANGED" if str(exc) == "SOURCE_BINDING_CHANGED" else
                                "SILVER_STATE_INTEGRITY" if isinstance(exc, CheckpointError) else
                                "SOURCE_SCHEMA_UNSUPPORTED" if isinstance(exc, SchemaMismatchError) else
                                "CANONICAL_CONTENT_CONFLICT")
                    code = getattr(exc, "code", fallback)
                    self.metrics.fault_code = code
                    self.metrics.fault_message = str(exc)
                    if code == "AUTHORITY_UNAVAILABLE":
                        self.state = ProcessorState.RETRYING
                        self._retry_idx = min(self._retry_idx + 1, len(BACKOFF_SCHEDULE_SEC) - 1)
                    else:
                        self.state = ProcessorState.FAULTED
                        self._publish_snapshot()
                        return
                except Exception as exc:
                    self.metrics.fault_code = "V1_DEPENDENCY_OR_SOURCE"
                    self.metrics.fault_message = str(exc)
                    self.state = ProcessorState.RETRYING
                    self._retry_idx = min(self._retry_idx + 1, len(BACKOFF_SCHEDULE_SEC) - 1)
                self._publish_snapshot()
                if continue_backlog:
                    continue
                delay = (BACKOFF_SCHEDULE_SEC[self._retry_idx] if self._retry_idx
                         else self.settings.poll_interval_sec)
                self.metrics.timed("retry_wait" if self._retry_idx else "poll_wait", self._stop.wait, delay)
        finally:
            if source_reader is not None:
                source_reader.close()


    def _fault(self, code: str, message: str) -> None:
        self.state = ProcessorState.FAULTED
        self.metrics.fault_code = code
        self.metrics.fault_message = message
        self._publish_snapshot()


    def recover_v1_obligations(self, limit: int = 100) -> int:
        """Reconcile frozen V1 rows before attempting new journal progress."""
        timed = self.metrics.timed
        pending = timed("pending_lookup", self.checkpoint.pending_v1_obligations,
                        self.settings.namespace, limit)
        replay_run_id = (self.settings.replay_run_id
                         if self.settings.destination_mode == DestinationMode.REPLAY.value else None)
        verified = []
        try:
            for key, group in groupby(pending, key=lambda row: (
                    row["source_kind"], row["source_sequence"], row["event_id"], row["target_table"])):
                obligations = list(group)
                if (key[-1] in V1_DIRECTION_TABLES
                        and 1 < len(obligations) <= V1_DIRECTION_BATCH_LIMIT
                        and all(item["status"] != "FAULT" for item in obligations)
                        and hasattr(self.repo, "reconcile_v1_directions")
                        and self._recover_v1_directions(key[-1], obligations, replay_run_id, verified)):
                    continue
                for obligation in obligations:
                    if obligation["status"] == "FAULT":
                        raise CheckpointError("frozen V1 obligation has a permanent fault")
                    row = json.loads(obligation["expected_json"])
                    target = obligation["target_table"]
                    try:
                        if not timed("reconcile_existing", self.repo.reconcile_v1_row,
                                     target, row, replay_run_id=replay_run_id):
                            timed("insert_obligation", self.repo.insert_v1_rows,
                                  target, [row], replay_run_id=replay_run_id)
                            if not timed("reconcile_visibility", self.repo.reconcile_v1_row,
                                         target, row, replay_run_id=replay_run_id):
                                raise RuntimeError("V1 inserted obligation is not visible")
                    except Exception as exc:
                        self.checkpoint.record_v1_obligation_failure(obligation, exc,
                            permanent=isinstance(exc, (FactBusinessKeyConflictError, InvalidTargetTableError)))
                        raise
                    verified.append(obligation)
        finally:
            timed("ack_obligations", self.checkpoint.reconcile_v1_obligations, verified)
        return len(pending)

    def _recover_v1_directions(self, target, obligations, replay_run_id, verified):
        rows = [json.loads(item["expected_json"]) for item in obligations]
        timed = self.metrics.timed
        reconcile = self.repo.reconcile_v1_directions
        found = [False] * len(rows)
        try:
            existing = timed("reconcile_existing_group", reconcile, target, rows, replay_run_id=replay_run_id)
            if len(existing) != len(rows):
                raise ValueError("direction reconciliation returned incomplete key accounting")
            found = existing
            missing = [index for index, present in enumerate(found) if not present]
            if missing:
                inserts = [rows[index] for index in missing]
                timed("insert_obligation_group", self.repo.insert_v1_rows, target, inserts,
                      replay_run_id=replay_run_id)
                visible = timed("reconcile_visibility_group", reconcile, target, inserts, replay_run_id=replay_run_id)
                for index, present in zip(missing, visible, strict=True):
                    found[index] = present
        except (FactBusinessKeyConflictError, InvalidTargetTableError, ValueError):
            # Reconcile each key independently to attribute a conflict or excessive duplicates.
            return False
        except Exception as exc:
            # An ambiguous insert cannot confirm any previously absent member.
            # Preserve positive evidence from the successful initial lookup.
            for obligation, present in zip(obligations, found, strict=True):
                if present:
                    verified.append(obligation)
                else:
                    self.checkpoint.record_v1_obligation_failure(obligation, exc, permanent=False)
            raise
        verified.extend(obligation for obligation, present in zip(obligations, found, strict=True) if present)
        if not all(found):
            exc = RuntimeError("V1 inserted direction obligation is not visible")
            for obligation, present in zip(obligations, found, strict=True):
                if not present:
                    self.checkpoint.record_v1_obligation_failure(obligation, exc, permanent=False)
            raise exc
        return True

    def process_v1_accepted_change(self, change: dict, bronze_repo: Any,
                                   archive_resolver: Any, lifecycle: Any,
                                   source_reader: Any = None) -> None:
        """Materialize one Bronze ACCEPTED journal entry with durable obligations."""
        if change["change_type"] != "ACCEPTED":
            raise ValueError("expected an ACCEPTED journal entry")
        timed = self.metrics.timed
        namespace = self.settings.namespace
        source_namespace = self.settings.v1_source_namespace
        sequence = int(change["change_sequence"])
        cursors = self.checkpoint.v1_cursors(namespace)
        if sequence != cursors["change_cursor"] + 1:
            raise CheckpointCasConflictError("V1 change sequence gap")
        current = timed("bronze_read", bronze_repo.read_valid_observation,
            source_namespace, change["event_id"], int(change["revision"]),
            state_path=self.settings.v1_bronze_path)
        if current is None:
            invalidated = self.checkpoint.conn.execute(
                "SELECT state FROM silver_v1_event_validity WHERE namespace=? AND event_id=?",
                (namespace, change["event_id"])).fetchone()
            if invalidated is None or invalidated[0] != "INVALIDATED":
                # The invalidation journal must establish local suppression first.
                raise RuntimeError("V1 accepted source is no longer current")
            identity = source_reader.current(source_namespace, change["event_id"]) if source_reader else None
            if identity is None or identity["state"] != "CONFLICTED":
                raise RuntimeError("V1 conflicted source identity unavailable")
            existing = self.checkpoint.conn.execute(
                "SELECT 1 FROM silver_v1_obligation WHERE namespace=? AND source_kind='changes' "
                "AND source_sequence=?", (namespace, sequence)).fetchone()
            if not existing:
                row = dict(namespace=source_namespace, event_id=change["event_id"],
                    simulation_run_id=identity["simulation_run_id"],
                    topology_digest=identity["topology_digest"],
                    semantic_revision=change["revision"],
                    validity_state="INVALIDATED", bronze_change_sequence=sequence,
                    bronze_invalidation_sequence=None, invalidation_id=None,
                    reason="SUPERSEDED_BY_CONFLICT", created_at=_utc_str())
                self.checkpoint.freeze_v1_obligation(namespace, "changes", sequence,
                    change["event_id"], "silver_event_validity_v1", row)
            while self.checkpoint.pending_v1_obligations(namespace, 100):
                self.recover_v1_obligations(100)
            self.checkpoint.advance_v1_cursor(namespace, "changes", sequence,
                                              cursors["generation"])
            return
        event = current["event"]
        context = timed("archive_resolve", archive_resolver.resolve,
                        event["simulationRunId"], event["topologyDigest"])
        timed("archive_validate_event", context.validate_event, event)
        timed("lifecycle_register", lifecycle.register_verified, context)
        existing = self.checkpoint.conn.execute(
            "SELECT 1 FROM silver_v1_obligation WHERE namespace=? AND source_kind='changes' "
            "AND source_sequence=? LIMIT 1", (namespace, sequence)).fetchone()
        if not existing:
            deliveries = json.loads(current["primary_delivery"]["expected_json"])
            primary = next((item["row"] for item in deliveries
                            if item["table"] == "bronze_delivery_dispositions_v1"), None)
            if primary is None:
                raise RuntimeError("Bronze primary delivery obligation missing")
            provenance = dict(
                bronze_semantic_revision=current["semantic_revision"],
                bronze_canonical_hash=current["bronze_canonical_hash"],
                source_raw_ingestion_id=primary["raw_ingestion_id"],
                source_topic=primary["topic"], source_partition=primary["partition"],
                source_offset=primary["offset"],
                source_physical_record_hash=primary["physical_record_hash"],
                source_byte_hash=primary["source_byte_hash"])
            facts = timed("build_facts", build_v1_fact_rows, event, context, provenance)
            created_at = _utc_str()
            rows: list[tuple[str, dict]] = []
            dimensions = timed("build_dimensions", build_v1_dimensions, context, self.settings.v1_archive_root)
            for table, row in dimensions.items():
                rows.append((table, dict(row, created_at=created_at)))
            for family, table in (("traffic", "silver_traffic_observation_fact_v1"),
                                  ("signal", "silver_signal_state_fact_v1"),
                                  ("intersection", "silver_intersection_state_fact_v1")):
                rows.extend((table, dict(row, created_at=created_at)) for row in facts[family])
            rows.append(("silver_event_validity_v1", dict(
                namespace=source_namespace, event_id=change["event_id"],
                simulation_run_id=event["simulationRunId"],
                topology_digest=event["topologyDigest"],
                semantic_revision=change["revision"], validity_state="VALID",
                bronze_change_sequence=sequence, bronze_invalidation_sequence=None,
                invalidation_id=None, reason="ACCEPTED", created_at=created_at)))
            timed("freeze_bundle", self.checkpoint.freeze_v1_bundle, namespace, "changes", sequence,
                  change["event_id"], change["revision"], rows)
        while self.checkpoint.pending_v1_obligations(namespace, 100):
            self.recover_v1_obligations(100)
        if timed("final_bronze_read", bronze_repo.read_valid_observation, source_namespace, change["event_id"],
                int(change["revision"]), state_path=self.settings.v1_bronze_path) is None:
            raise RuntimeError("V1 source transitioned before validity publication")
        timed("finalize_validity", self.checkpoint.finalize_v1_event, namespace, change['event_id'],
              change['revision'], sequence, cursors['generation'])

    def process_v1_invalidation(self, entry: dict, source_reader: Any,
                                archive_resolver: Any) -> None:
        """Suppress locally, mirror the frozen tombstone, then advance the cursor."""
        namespace = self.settings.namespace
        source_namespace = self.settings.v1_source_namespace
        sequence = int(entry["invalidation_sequence"])
        cursors = self.checkpoint.v1_cursors(namespace)
        if sequence != cursors["invalidation_cursor"] + 1:
            raise CheckpointCasConflictError("V1 invalidation sequence gap")
        provenance = source_reader.read_invalidation_provenance(
            source_namespace, entry["invalidation_id"])
        if provenance is None or provenance["event_id"] != entry["event_id"]:
            raise RuntimeError("V1 invalidation provenance missing")
        context = archive_resolver.resolve(provenance["simulation_run_id"],
                                           provenance["topology_digest"])
        existing = self.checkpoint.conn.execute(
            "SELECT 1 FROM silver_v1_obligation WHERE namespace=? "
            "AND source_kind='invalidations' AND source_sequence=?",
            (namespace, sequence)).fetchone()
        if not existing:
            row = dict(namespace=source_namespace, invalidation_id=entry["invalidation_id"],
                invalidation_sequence=sequence, event_id=entry["event_id"],
                semantic_revision=entry["revision"],
                simulation_run_id=provenance["simulation_run_id"],
                topology_digest=provenance["topology_digest"],
                run_generation=context.generation,
                intersection_id=provenance["intersection_id"],
                cycle_sequence=provenance["cycle_sequence"],
                simulation_time_seconds=provenance["simulation_time_seconds"],
                previous_immutable_envelope_hash=entry["previous_hash"],
                conflicting_immutable_envelope_hash=entry["conflicting_hash"],
                original_raw_ingestion_id=entry["first_raw_id"],
                conflicting_raw_ingestion_id=entry["conflicting_raw_id"],
                lineage_json=json.dumps({"original": provenance["original_delivery"],
                    "conflicting": provenance["conflicting_delivery"]},
                    sort_keys=True, separators=(",", ":")), created_at=_utc_str())
            self.checkpoint.suppress_v1_event(namespace, entry["event_id"],
                entry["revision"], provenance["change_sequence"], sequence,
                "silver_invalidation_v1", row)
        while self.checkpoint.pending_v1_obligations(namespace, 100):
            self.recover_v1_obligations(100)
        self.checkpoint.advance_v1_cursor(namespace, "invalidations", sequence,
                                          cursors["generation"])

    def process_v1_conflicted_change(self, change: dict, source_reader: Any) -> None:
        namespace = self.settings.namespace
        source_namespace = self.settings.v1_source_namespace
        sequence = int(change["change_sequence"])
        cursors = self.checkpoint.v1_cursors(namespace)
        if change["change_type"] != "CONFLICTED" or sequence != cursors["change_cursor"] + 1:
            raise CheckpointCasConflictError("V1 conflict change sequence mismatch")
        identity = source_reader.current(source_namespace, change["event_id"])
        local = self.checkpoint.conn.execute("SELECT state FROM silver_v1_event_validity "
            "WHERE namespace=? AND event_id=?", (namespace, change["event_id"])).fetchone()
        if identity is None or identity["state"] != "CONFLICTED" or local is None or local[0] != "INVALIDATED":
            raise RuntimeError("V1 conflict has no durable local invalidation")
        existing = self.checkpoint.conn.execute("SELECT 1 FROM silver_v1_obligation "
            "WHERE namespace=? AND source_kind='changes' AND source_sequence=?",
            (namespace, sequence)).fetchone()
        if not existing:
            row = dict(namespace=source_namespace, event_id=change["event_id"],
                simulation_run_id=identity["simulation_run_id"],
                topology_digest=identity["topology_digest"],
                semantic_revision=change["revision"], validity_state="INVALIDATED",
                bronze_change_sequence=sequence, bronze_invalidation_sequence=None,
                invalidation_id=change["invalidation_id"], reason="BRONZE_CONFLICT",
                created_at=_utc_str())
            self.checkpoint.freeze_v1_obligation(namespace, "changes", sequence,
                change["event_id"], "silver_event_validity_v1", row)
        while self.checkpoint.pending_v1_obligations(namespace, 100):
            self.recover_v1_obligations(100)
        self.checkpoint.advance_v1_cursor(namespace, "changes", sequence,
                                          cursors["generation"])

    def run_v1_journal_pass(self, source_reader: Any, bronze_repo: Any,
                            archive_resolver: Any, lifecycle: Any,
                            batch_size: int = 100,
                            frozen_boundary: dict | None = None,
                            max_entries: int | None = None) -> int:
        """Process a fixed verified boundary, including invalidations first."""
        namespace = self.settings.namespace
        source_namespace = self.settings.v1_source_namespace
        timed = self.metrics.timed
        binding = timed("source_binding", verify_bronze_source,
                        Path(self.settings.v1_bronze_path), source_namespace, full_history=False)
        def check_frozen() -> None:
            if frozen_boundary is None:
                return
            current = verify_bronze_source(Path(self.settings.v1_bronze_path), source_namespace)
            keys = ("namespace", "source_path", "schema_fingerprint", "bronze_version", "file_identity")
            if (any(current.get(key) != frozen_boundary["source_binding"].get(key) for key in keys)
                    or current["change_high_water"] != frozen_boundary["change_high_water"]
                    or current["invalidation_high_water"] != frozen_boundary["invalidation_high_water"]):
                raise SourceBoundaryChangedError()
            context = archive_resolver.resolve(frozen_boundary["simulation_run_id"],
                frozen_boundary["topology_digest"], frozen_boundary["run_generation"])
            if (context.manifest_identity != frozen_boundary["manifest_identity"] or
                    context.manifest_file_hash != frozen_boundary["manifest_file_hash"] or
                    context.catalog_file_hash != frozen_boundary["catalog_file_hash"]):
                raise SourceBoundaryChangedError()
        check_frozen()
        self.checkpoint.bind_v1_source(namespace, binding)
        cursors = self.checkpoint.v1_cursors(namespace)
        self.metrics.source_lag = {
            "changes": max(0, binding["change_high_water"] - cursors["change_cursor"]),
            "invalidations": max(0, binding["invalidation_high_water"] - cursors["invalidation_cursor"]),
        }
        if (binding["change_high_water"] < cursors["change_cursor"] or
                binding["invalidation_high_water"] < cursors["invalidation_cursor"]):
            raise SourceBoundaryChangedError()
        processed = 0
        while self.checkpoint.pending_v1_obligations(namespace, batch_size):
            timed("startup_recovery", self.recover_v1_obligations, batch_size)
        for kind, high, method in (
            ("invalidations", binding["invalidation_high_water"], self.process_v1_invalidation),
            ("changes", binding["change_high_water"], None),
        ):
            while True:
                after = self.checkpoint.v1_cursors(namespace)[
                    "invalidation_cursor" if kind == "invalidations" else "change_cursor"]
                if after >= high:
                    break
                check_frozen()
                page = timed("journal_page", source_reader.read_page, source_namespace, kind,
                             after=after, limit=batch_size, high_water=high)
                sequence_key = 'invalidation_sequence' if kind == 'invalidations' else 'change_sequence'
                expected_sequences = list(range(after + 1, min(after + batch_size, high) + 1))
                if [row[sequence_key] for row in page['rows']] != expected_sequences:
                    raise SourceVerificationError("CURSOR_GAP", "journal page does not continue cursor")
                for entry in page["rows"]:
                    if max_entries is not None and processed >= max_entries:
                        check_frozen()
                        return processed
                    filter_run = self.settings.v1_replay_run_filter
                    if filter_run:
                        identity = source_reader.current(source_namespace, entry["event_id"])
                        if identity is None:
                            raise RuntimeError("V1 replay source identity missing")
                        if identity["simulation_run_id"] != filter_run:
                            generation = self.checkpoint.v1_cursors(namespace)["generation"]
                            self.checkpoint.record_v1_replay_skip(namespace, kind,
                                entry["invalidation_sequence" if kind == "invalidations" else "change_sequence"],
                                entry["event_id"], generation)
                            processed += 1
                            continue
                    if kind == "invalidations":
                        timed("apply_invalidation", method, entry, source_reader, archive_resolver)
                    elif entry["change_type"] == "ACCEPTED":
                        timed("apply_accepted", self.process_v1_accepted_change,
                              entry, bronze_repo, archive_resolver, lifecycle, source_reader)
                    elif entry["change_type"] == "CONFLICTED":
                        timed("apply_conflicted", self.process_v1_conflicted_change, entry, source_reader)
                    else:
                        raise RuntimeError("unsupported V1 Bronze change type")
                    processed += 1
                check_frozen()
        check_frozen()
        return processed
