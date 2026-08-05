"""Orion Projector core — process Kafka records → Orion batch (K-3)."""
from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from integration.projector.node_buffer import BufferedEvent, NodeBuffer, NodeBufferManager
from integration.projector.offset_tracker import OffsetTracker
from integration.projector.runtime_cache import RuntimeCache, RuntimeStatus
from integration.projector.retry import OrionRetryManager, classify_batch_result
from integration.projector.schema import (
    COMPLETED_STATUSES,
    STATUS_APPLIED,
    STATUS_COALESCED_SUPERSEDED,
    STATUS_FAILED_PERMANENT,
    STATUS_FENCE_SKIPPED,
    STATUS_NODE_PARTIAL_APPLIED,
    STATUS_QUARANTINED,
    STATUS_SIM_TIME_REGRESSION,
    STATUS_STALE_SKIPPED,
)
from integration.projector.shadow_mapper import to_namespaced_entity, to_shadow_entity
from integration.projector.store import ProjectorStore

log = logging.getLogger(__name__)

# Realtime Runtime Contract v1 (§57)
READY_LAG_EVENTS = 40
READY_MAX_PARTITION_LAG = 40
READY_FRESHNESS_SEC = 2.0
CATCH_UP_ENTER_LAG = 200
CATCH_UP_EXIT_LAG = 80
CATCH_UP_ENTER_FRESHNESS_SEC = 5.0


class ProjectorMode(str, Enum):
    NORMAL = "NORMAL"
    CATCH_UP = "CATCH_UP"


class RuntimePhase(str, Enum):
    READY_IDLE = "READY_IDLE"
    ACTIVE = "ACTIVE"
    CATCH_UP = "CATCH_UP"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"
    RECOVERING = "RECOVERING"


class WriteMode(str, Enum):
    DISABLED = "disabled"
    ARMED = "armed"
    ACTIVE = "active"


class ProjectorFault(Exception):
    pass


class OrionProjector:
    """
    Unit-testable projector core.

    batch_upsert: Callable[[list[dict]], Any] — inject shared client.batch_upsert_entities
    shadow=True → rewrite IDs before Orion apply (namespace from target_namespace)
    target_namespace: shadow|test|production — production skips rewrite even if shadow=True
    """

    def __init__(
        self,
        store: ProjectorStore,
        *,
        batch_upsert: Callable[[Sequence[dict]], Any],
        source: str = "kafka-projector",
        shadow: bool = True,
        target_namespace: str = "shadow",
        node_timeout_ms: float = 100.0,
        max_buffered_events: int = 2000,
        max_buffered_cycles: int = 32,
        catch_up_lag_high: float = 5.0,
        catch_up_lag_low: float = 2.0,
        state_retention_hours: float = 24.0,
        ledger_retention_hours: float = 24.0,
        write_mode: WriteMode = WriteMode.ACTIVE,
        target_simulation_run_id: Optional[str] = None,
        fence_offsets: Optional[Dict[int, int]] = None,
        armed_buffer_max: int = 10_000,
        defer_ready_until_idle: bool = False,
    ) -> None:
        self.store = store
        self.batch_upsert = batch_upsert
        self.source = source
        self.shadow = shadow
        self.target_namespace = (target_namespace or "shadow").strip().lower()
        self.write_mode = write_mode
        self.target_simulation_run_id = target_simulation_run_id
        self.fence_offsets: Dict[int, int] = dict(fence_offsets or {})
        self.armed_buffer_max = int(armed_buffer_max)
        self._armed_buffer: List[BufferedEvent] = []
        self._partitions_paused = False
        self.defer_ready_until_idle = bool(defer_ready_until_idle)
        self.runtime_cache = RuntimeCache()
        self.retry_manager = OrionRetryManager()
        self.runtime_phase = RuntimePhase.RECOVERING
        self._last_lag_events = 0
        self._last_max_partition_lag = 0
        self._lag_samples: deque = deque(maxlen=3)
        self.buffers = NodeBufferManager(
            timeout_ms=node_timeout_ms,
            max_buffered_events=max_buffered_events,
            max_buffered_cycles=max_buffered_cycles,
        )
        self.offsets = OffsetTracker()
        self.mode = ProjectorMode.NORMAL
        self.catch_up_lag_high = catch_up_lag_high
        self.catch_up_lag_low = catch_up_lag_low
        self.state_retention_hours = state_retention_hours
        self.ledger_retention_hours = ledger_retention_hours
        # Hold entity events that arrive before RunStarted (cross-partition reorder).
        self.awaiting_run_timeout_ms = max(float(node_timeout_ms) * 50.0, 2000.0)
        self._awaiting_run: List[BufferedEvent] = []
        self.faulted = False
        self.fault_message: Optional[str] = None
        self._pipeline_latency_samples = deque(maxlen=10_000)
        self._stage_samples = {
            name: deque(maxlen=10_000)
            for name in (
                "capture_to_broker_ms",
                "broker_to_consumer_ms",
                "buffer_wait_ms",
                "grouping_cpu_ms",
                "orion_http_ms",
                "sqlite_tx_ms",
                "offset_local_ms",
                "apply_total_ms",
                "ledger_lookup_ms",
                "active_run_lookup_ms",
                "record_prebuffer_ms",
                "ledger_select_count_per_cycle",
                "active_run_select_count_per_cycle",
                "ledger_lookup_cumulative_ms_per_cycle",
                "active_run_lookup_cumulative_ms_per_cycle",
                "record_prebuffer_cumulative_ms_per_cycle",
            )
        }
        # Per-cycle accumulators for lookup instrumentation (reset on cycle apply).
        self._cycle_lookup: Dict[str, float] = {
            "ledger_select_count": 0.0,
            "active_run_select_count": 0.0,
            "ledger_lookup_ms": 0.0,
            "active_run_lookup_ms": 0.0,
            "record_prebuffer_ms": 0.0,
        }
        # Active-run cache keyed by (source, producer_id). Never caches None.
        self._active_run_cache: Dict[tuple[str, str], dict] = {}
        self._last_active_lookup_ns: int = 0
        # Discard early post-activation cycles from SLA/stage histograms (catch-up poison).
        self._sla_warmup_cycles_remaining: int = 0
        self.metrics: Dict[str, Any] = {
            "node_apply_count": 0,
            "node_partial_count": 0,
            "stale_event_count": 0,
            "quarantine_count": 0,
            "orion_apply_count": 0,
            "orion_partial_count": 0,
            "coalesced_event_count": 0,
            "run_started_count": 0,
            "awaiting_run_count": 0,
            "armed_buffer_count": 0,
            "armed_buffer_full": False,
            "fence_skipped_count": 0,
            "projector_stale_total": 0,
            "projector_fence_total": 0,
            "projector_dlq_total": 0,
            "projector_retry_total": 0,
            "projector_apply_latency_ms": None,
            "projector_batch_latency_ms": None,
            "projector_lag_events": 0,
            "projector_buffer_size": 0,
            "write_mode": write_mode.value,
            "target_simulation_run_id": target_simulation_run_id,
            "last_orion_batch_duration_ms": None,
            "last_pipeline_e2e_latency_ms": None,
            "pipeline_e2e_latency_ms_p95": None,
            "pipeline_e2e_latency_ms_max": None,
            "pipeline_e2e_latency_sample_count": 0,
            "pipeline_e2e_latency_spikes_gt_500ms": 0,
            "pipeline_freshness_sec": None,
            "last_applied_captured_at_epoch": None,
            "writer_role": "projector",
            "last_cycle_lookup": None,
        }

    def set_write_mode(self, mode: WriteMode) -> None:
        prev = self.write_mode
        self.write_mode = mode
        self.metrics["write_mode"] = mode.value
        # Fresh histograms when entering ACTIVE so catch-up / prior-run samples
        # cannot poison diagnostic p95.
        if mode == WriteMode.ACTIVE and prev != WriteMode.ACTIVE:
            self.reset_latency_histograms(warmup_cycles=25)

    def reset_latency_histograms(self, *, warmup_cycles: int = 25) -> None:
        self._pipeline_latency_samples.clear()
        for dq in self._stage_samples.values():
            dq.clear()
        self._sla_warmup_cycles_remaining = max(0, int(warmup_cycles))
        self.metrics["last_pipeline_e2e_latency_ms"] = None
        self.metrics["pipeline_e2e_latency_ms_p95"] = None
        self.metrics["pipeline_e2e_latency_ms_max"] = None
        self.metrics["pipeline_e2e_latency_sample_count"] = 0
        self.metrics["pipeline_e2e_latency_spikes_gt_500ms"] = 0
        self.metrics["last_applied_captured_at_epoch"] = None
        self.metrics["last_cycle_lookup"] = None

    def load_fence_manifest(
        self,
        *,
        target_simulation_run_id: str,
        fence_offsets: Dict[int, int],
    ) -> None:
        prev_target = self.target_simulation_run_id
        self.target_simulation_run_id = target_simulation_run_id
        self.fence_offsets = dict(fence_offsets)
        self.metrics["target_simulation_run_id"] = target_simulation_run_id
        self._active_run_cache.clear()
        # Fresh target run must not inherit catch-up / prior-run latency samples
        # (including when write_mode is already ACTIVE).
        if target_simulation_run_id and target_simulation_run_id != prev_target:
            self.reset_latency_histograms(warmup_cycles=25)

    def _cache_key(self, producer_id: str) -> tuple[str, str]:
        return (self.source, producer_id)

    def _set_active_run_cache(self, producer_id: str, active: dict) -> None:
        self._active_run_cache[self._cache_key(producer_id)] = dict(active)

    def _invalidate_active_run_cache(self, producer_id: Optional[str] = None) -> None:
        if producer_id is None:
            self._active_run_cache.clear()
            return
        self._active_run_cache.pop(self._cache_key(producer_id), None)

    def _resolve_active_run(self, producer_id: str) -> tuple[Optional[dict], int]:
        """Return (active_row_or_None, select_count). Never caches a None miss."""
        key = self._cache_key(producer_id)
        cached = self._active_run_cache.get(key)
        if cached is not None:
            if (
                self.target_simulation_run_id
                and str(cached.get("simulation_run_id") or "")
                != self.target_simulation_run_id
            ):
                # Stale cache from a prior cutover target — drop and re-resolve.
                self._active_run_cache.pop(key, None)
            else:
                self._last_active_lookup_ns = 0
                return cached, 0
        t0 = time.perf_counter_ns()
        active = self.store.get_active_run(source=self.source, producer_id=producer_id)
        self._last_active_lookup_ns = time.perf_counter_ns() - t0
        if active is not None:
            if (
                self.target_simulation_run_id
                and str(active.get("simulation_run_id") or "")
                != self.target_simulation_run_id
            ):
                # Persisted active run from a previous cutover — treat as absent so
                # target entities await RunStarted instead of being stale_skipped.
                return None, 1
            self._set_active_run_cache(producer_id, active)
        return active, 1

    def _fence_allows(self, *, partition: int, offset: int, run_id: str) -> bool:
        if self.target_simulation_run_id and run_id != self.target_simulation_run_id:
            return False
        min_off = self.fence_offsets.get(partition)
        if min_off is not None and offset < min_off:
            return False
        return True

    def _can_commit_offsets(self) -> bool:
        return self.write_mode == WriteMode.ACTIVE

    @staticmethod
    def _captured_at_epoch(raw: Any) -> Optional[float]:
        if not raw or not isinstance(raw, str):
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return None

    def _record_pipeline_sla(self, meta: Sequence[BufferedEvent]) -> None:
        if not meta:
            return
        if self._sla_warmup_cycles_remaining > 0:
            return
        now = time.time()
        captured = [
            ts
            for ts in (
                self._captured_at_epoch(be.event.get("capturedAt")) for be in meta
            )
            if ts is not None
        ]
        if not captured:
            return
        latencies = [max(0.0, (now - ts) * 1000.0) for ts in captured]
        self._pipeline_latency_samples.extend(latencies)
        self.metrics["last_pipeline_e2e_latency_ms"] = latencies[-1]
        self.metrics["last_applied_captured_at_epoch"] = max(captured)

    def _record_stage(self, name: str, value_ms: float) -> None:
        if self._sla_warmup_cycles_remaining > 0:
            return
        self._stage_samples[name].append(max(0.0, float(value_ms)))

    @staticmethod
    def _ns_to_ms(ns: int) -> float:
        return ns / 1_000_000.0

    def _record_ledger_lookup(self, elapsed_ns: int) -> None:
        ms = self._ns_to_ms(elapsed_ns)
        self._record_stage("ledger_lookup_ms", ms)
        self._cycle_lookup["ledger_select_count"] += 1.0
        self._cycle_lookup["ledger_lookup_ms"] += ms

    def _record_active_run_lookup(self, elapsed_ns: int, *, selects: int = 1) -> None:
        ms = self._ns_to_ms(elapsed_ns)
        self._record_stage("active_run_lookup_ms", ms)
        self._cycle_lookup["active_run_select_count"] += float(selects)
        self._cycle_lookup["active_run_lookup_ms"] += ms

    def _record_prebuffer(self, elapsed_ns: int) -> None:
        ms = self._ns_to_ms(elapsed_ns)
        self._record_stage("record_prebuffer_ms", ms)
        self._cycle_lookup["record_prebuffer_ms"] += ms

    def _flush_cycle_lookup_metrics(self, *, cycle_ms: float, cycle_key: str) -> None:
        snap = {
            "ledger_select_count": int(self._cycle_lookup["ledger_select_count"]),
            "active_run_select_count": int(self._cycle_lookup["active_run_select_count"]),
            "ledger_lookup_ms": self._cycle_lookup["ledger_lookup_ms"],
            "active_run_lookup_ms": self._cycle_lookup["active_run_lookup_ms"],
            "record_prebuffer_ms": self._cycle_lookup["record_prebuffer_ms"],
            "cycle_apply_ms": cycle_ms,
            "cycle_key": cycle_key,
        }
        self._record_stage(
            "ledger_select_count_per_cycle", float(snap["ledger_select_count"])
        )
        self._record_stage(
            "active_run_select_count_per_cycle", float(snap["active_run_select_count"])
        )
        self._record_stage(
            "ledger_lookup_cumulative_ms_per_cycle", snap["ledger_lookup_ms"]
        )
        self._record_stage(
            "active_run_lookup_cumulative_ms_per_cycle", snap["active_run_lookup_ms"]
        )
        self._record_stage(
            "record_prebuffer_cumulative_ms_per_cycle", snap["record_prebuffer_ms"]
        )
        self.metrics["last_cycle_lookup"] = snap
        if cycle_ms > 500.0:
            log.warning("cycle_lookup_spike %s", snap)
        for key in self._cycle_lookup:
            self._cycle_lookup[key] = 0.0

    @staticmethod
    def _distribution(values) -> dict:
        ordered = sorted(values)
        if not ordered:
            return {"p50": None, "p95": None, "p99": None, "max": None,
                    "sample_count": 0, "spikes_gt_100ms": 0, "spikes_gt_500ms": 0}
        def pct(q: float) -> float:
            return ordered[max(0, int(len(ordered) * q + 0.999999) - 1)]
        return {
            "p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99),
            "max": ordered[-1], "sample_count": len(ordered),
            "spikes_gt_100ms": sum(v > 100.0 for v in ordered),
            "spikes_gt_500ms": sum(v > 500.0 for v in ordered),
        }

    def _buffer_armed(self, be: BufferedEvent) -> str:
        if len(self._armed_buffer) >= self.armed_buffer_max:
            self.metrics["armed_buffer_full"] = True
            self._partitions_paused = True
            return "armed_buffer_full"
        self._armed_buffer.append(be)
        self.metrics["armed_buffer_count"] = len(self._armed_buffer)
        if len(self._armed_buffer) >= int(self.armed_buffer_max * 0.9):
            self._partitions_paused = True
        return "armed_buffered"

    def drain_armed_buffer(self) -> None:
        pending = list(self._armed_buffer)
        self._armed_buffer.clear()
        self.metrics["armed_buffer_count"] = 0
        self.metrics["armed_buffer_full"] = False
        self._partitions_paused = False
        for be in pending:
            self.process_record(
                topic=be.topic,
                partition=be.partition,
                offset=be.offset,
                value=be.event,
            )

    def recover(self, topic: str, partitions: Sequence[int], *, producer_id: str = "visualize-traci") -> None:
        for p in partitions:
            last = self.store.rebuild_completed_offsets(topic, p)
            if last is not None:
                self.offsets.load_committed(topic, p, last)
                self.store.set_committed_offset(topic, p, last)
        self.runtime_cache.rebuild_from_store(
            self.store, source=self.source, producer_id=producer_id
        )
        active = self.store.get_active_run(source=self.source, producer_id=producer_id)
        self.runtime_phase = RuntimePhase.READY_IDLE if active is None else RuntimePhase.ACTIVE

    def process_record(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        value: dict,
        broker_timestamp_epoch: Optional[float] = None,
        consumer_received_epoch: Optional[float] = None,
    ) -> str:
        """Process one Kafka record. Returns action label."""
        prebuffer_t0 = time.perf_counter_ns()
        if self.faulted:
            raise ProjectorFault(self.fault_message or "projector faulted")

        if self.write_mode == WriteMode.DISABLED:
            return "disabled_skip"

        event_type = value.get("eventType")
        if event_type == "TrafficSimulationRunStarted":
            if self.write_mode == WriteMode.ARMED:
                return self._handle_run_started(
                    value, topic=topic, partition=partition, offset=offset
                )
            if self.write_mode == WriteMode.ACTIVE:
                return self._handle_run_started(
                    value, topic=topic, partition=partition, offset=offset
                )
            return "disabled_skip"

        if event_type != "TrafficEntityObserved":
            return "ignored"

        run_id = str(value.get("simulationRunId") or "")
        if not self._fence_allows(partition=partition, offset=offset, run_id=run_id):
            self._mark_fence_skipped(topic, partition, offset, value, run_id=run_id)
            return "fence_skipped"

        if self.write_mode == WriteMode.ARMED:
            be = BufferedEvent(
                event=value, topic=topic, partition=partition, offset=offset,
                broker_timestamp_epoch=broker_timestamp_epoch,
                consumer_received_epoch=consumer_received_epoch or time.time(),
            )
            return self._buffer_armed(be)

        # Ledger idempotency is checked once per cycle in _apply_node_buffer via
        # get_ledgers() — not per-event here (avoids 40 SELECTs/cycle).

        producer_id = str(value.get("producerId") or "visualize-traci")
        run_id = str(value["simulationRunId"])
        active, selects = self._resolve_active_run(producer_id)
        self._record_active_run_lookup(self._last_active_lookup_ns if selects else 0, selects=selects)
        if active is None:
            be = BufferedEvent(
                event=value, topic=topic, partition=partition, offset=offset,
                broker_timestamp_epoch=broker_timestamp_epoch,
                consumer_received_epoch=consumer_received_epoch or time.time(),
            )
            self._awaiting_run.append(be)
            self.metrics["awaiting_run_count"] = len(self._awaiting_run)
            self._record_prebuffer(time.perf_counter_ns() - prebuffer_t0)
            return "awaiting_run"
        if str(active.get("simulation_run_id") or "") != run_id:
            self._mark_stale(topic, partition, offset, value)
            self._record_prebuffer(time.perf_counter_ns() - prebuffer_t0)
            return "stale_skipped"

        self._record_prebuffer(time.perf_counter_ns() - prebuffer_t0)
        return self._ingest_active_entity(
            topic, partition, offset, value,
            broker_timestamp_epoch=broker_timestamp_epoch,
            consumer_received_epoch=consumer_received_epoch,
        )

    def _ingest_active_entity(
        self, topic: str, partition: int, offset: int, value: dict,
        *, broker_timestamp_epoch: Optional[float] = None,
        consumer_received_epoch: Optional[float] = None,
    ) -> str:
        run_id = str(value["simulationRunId"])
        entity = value.get("entity") or {}
        entity_id = str(entity.get("id") or "")
        sim_t = float(value.get("simulationTime") or 0)
        st = self.store.get_entity_state(run_id, entity_id)
        if st and st.get("last_simulation_time") is not None:
            if sim_t < float(st["last_simulation_time"]) - 1e-9:
                self._mark_sim_time_regression(topic, partition, offset, value)
                return "time_regression_skipped"

        be = BufferedEvent(
            event=value, topic=topic, partition=partition, offset=offset,
            broker_timestamp_epoch=broker_timestamp_epoch,
            consumer_received_epoch=consumer_received_epoch or time.time(),
        )
        action, buf = self.buffers.ingest(be)
        if action == "quarantine":
            self._mark_quarantine(topic, partition, offset, value)
            return "quarantine"
        if action == "ready" and buf is not None:
            if self.defer_ready_until_idle:
                return "node_ready_deferred"
            self._apply_node_buffer(buf, partial=False)
            return "node_applied"
        return "buffered"

    def tick(
        self, *, allow_partial: bool = True, complete_cycles_only: bool = False
    ) -> List[str]:
        """Flush complete node buffers; optionally expire incomplete ones."""
        if self.retry_manager.has_pending and self.retry_manager.circuit_allows_attempt():
            self.buffers.paused = False
            self._partitions_paused = False
        actions = []
        actions.extend(self._flush_awaiting_run_timeouts())
        complete = (
            self.buffers.take_complete_cycles()
            if complete_cycles_only
            else self.buffers.take_complete()
        )
        grouped: Dict[tuple[str, int], List[NodeBuffer]] = {}
        for buf in complete:
            grouped.setdefault((buf.simulation_run_id, buf.cycle_sequence), []).append(buf)

        if self.mode == ProjectorMode.CATCH_UP and len(grouped) > 1:
            by_run: Dict[str, int] = {}
            for run_id, cycle_seq in grouped:
                by_run[run_id] = max(by_run.get(run_id, 0), cycle_seq)
            supersede_keys = {
                k for k in grouped if k[1] < by_run.get(k[0], k[1])
            }
            for key in supersede_keys:
                self._supersede_cycle(grouped.pop(key))
                actions.append("cycle_superseded")

        for cycle_buffers in grouped.values():
            if len(cycle_buffers) == 1:
                self._apply_node_buffer(cycle_buffers[0], partial=False)
            else:
                events = {}
                for node_buf in cycle_buffers:
                    events.update(node_buf.events)
                merged = NodeBuffer(
                    simulation_run_id=cycle_buffers[0].simulation_run_id,
                    cycle_sequence=cycle_buffers[0].cycle_sequence,
                    node_id="__cycle__",
                    node_entity_count=len(events),
                    events=events,
                    first_seen_at=min(b.first_seen_at for b in cycle_buffers),
                )
                self._apply_node_buffer(merged, partial=False)
            actions.append("cycle_applied")
        if allow_partial:
            for buf in self.buffers.flush_timed_out():
                self._apply_node_buffer(buf, partial=True)
                actions.append("node_partial")
                self.metrics["node_partial_count"] += 1
        return actions

    def update_lag_probe(
        self,
        *,
        lag_events: int,
        max_partition_lag: int,
        freshness_sec: Optional[float],
    ) -> None:
        """Runtime Contract v1 lag/freshness hysteresis."""
        self._last_lag_events = int(lag_events)
        self._last_max_partition_lag = int(max_partition_lag)
        self.metrics["projector_lag_events"] = int(lag_events)
        self._lag_samples.append(int(lag_events))

        if self.mode == ProjectorMode.NORMAL:
            enter = (
                lag_events > CATCH_UP_ENTER_LAG
                or (
                    freshness_sec is not None
                    and freshness_sec > CATCH_UP_ENTER_FRESHNESS_SEC
                )
            )
            if enter:
                self.mode = ProjectorMode.CATCH_UP
                self.runtime_phase = RuntimePhase.CATCH_UP
        elif self.mode == ProjectorMode.CATCH_UP:
            exit_ok = lag_events <= CATCH_UP_EXIT_LAG and (
                freshness_sec is None or freshness_sec <= READY_FRESHNESS_SEC
            )
            if exit_ok:
                self.mode = ProjectorMode.NORMAL
                if self.runtime_phase == RuntimePhase.CATCH_UP:
                    self.runtime_phase = RuntimePhase.ACTIVE

        if len(self._lag_samples) >= 3 and self.mode == ProjectorMode.CATCH_UP:
            samples = list(self._lag_samples)
            if samples[0] < samples[1] < samples[2]:
                self.runtime_phase = RuntimePhase.DEGRADED

    def update_lag(self, lag_sec: float) -> None:
        """Backward-compatible seconds-based hook."""
        self.update_lag_probe(
            lag_events=self._last_lag_events,
            max_partition_lag=self._last_max_partition_lag,
            freshness_sec=lag_sec,
        )

    def cleanup(self) -> dict:
        return self.store.cleanup(
            state_retention_hours=self.state_retention_hours,
            ledger_retention_hours=self.ledger_retention_hours,
        )

    def health(self) -> dict:
        metrics = dict(self.metrics)
        pipeline = self._distribution(self._pipeline_latency_samples)
        metrics["pipeline_e2e_latency_ms_p95"] = pipeline["p95"]
        metrics["pipeline_e2e_latency_ms_max"] = pipeline["max"]
        metrics["pipeline_e2e_latency_sample_count"] = pipeline["sample_count"]
        metrics["pipeline_e2e_latency_spikes_gt_500ms"] = pipeline["spikes_gt_500ms"]
        metrics["stage_latency"] = {
            name: self._distribution(values)
            for name, values in self._stage_samples.items()
        }
        last_cap = metrics.get("last_applied_captured_at_epoch")
        metrics["pipeline_freshness_sec"] = (
            max(0.0, time.time() - float(last_cap)) if last_cap is not None else None
        )
        metrics["projector_buffer_size"] = self.buffers.buffered_event_count
        metrics["projector_retry_total"] = sum(
            self.retry_manager.metrics_retry_total.values()
        )
        return {
            **metrics,
            **self.store.metrics(),
            "mode": self.mode.value,
            "runtime_phase": self.runtime_phase.value,
            "write_mode": self.write_mode.value,
            "paused": self.buffers.paused or self._partitions_paused,
            "buffered_events": self.buffers.buffered_event_count,
            "buffered_cycles": self.buffers.buffered_cycle_count,
            "faulted": self.faulted,
            "fault_message": self.fault_message,
        }

    # ── internals ───────────────────────────────────────────────────

    def _handle_run_started(
        self, value: dict, *, topic: str, partition: int, offset: int
    ) -> str:
        run_id = str(value.get("simulationRunId") or "")
        if self.target_simulation_run_id and run_id != self.target_simulation_run_id:
            self._mark_fence_skipped(topic, partition, offset, value, run_id=run_id)
            return "fence_skipped"
        producer_id = str(value["producerId"])
        run_started_event_id = str(
            value.get("eventId")
            or f"run-started:{value['producerSessionId']}:{value['simulationRunId']}"
        )
        existing = self.store.get_ledger(run_started_event_id)
        if existing and existing.get("status") in COMPLETED_STATUSES:
            if self._can_commit_offsets():
                self.offsets.mark_completed(topic, partition, offset)
                self._maybe_commit(topic, partition)
            return "run_started_duplicate"
        self.store.activate_run(
            source=self.source,
            producer_id=producer_id,
            producer_session_id=str(value["producerSessionId"]),
            simulation_run_id=str(value["simulationRunId"]),
        )
        # Populate cache immediately after durable activate (source stays self.source).
        self._set_active_run_cache(
            producer_id,
            {
                "source": self.source,
                "producer_id": producer_id,
                "producer_session_id": str(value["producerSessionId"]),
                "simulation_run_id": str(value["simulationRunId"]),
                "status": "ACTIVE",
            },
        )
        self.store.apply_batch_tx(
            ledger_rows=[
                {
                    "event_id": run_started_event_id,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "simulation_run_id": run_id,
                    "cycle_sequence": 0,
                    "entity_id": "",
                    "status": STATUS_APPLIED,
                    "payload_hash": run_started_event_id,
                }
            ],
            entity_updates=[],
        )
        self.metrics["run_started_count"] += 1
        self.runtime_phase = RuntimePhase.ACTIVE
        self._drain_awaiting_run()
        if self.write_mode == WriteMode.ARMED:
            self.drain_armed_buffer()
        # RunStarted is a durable control record. Mark it completed only after
        # activation and any cross-partition drain succeed; otherwise it leaves
        # a permanent gap in the contiguous offset prefix and forces replay.
        if self._can_commit_offsets():
            self.offsets.mark_completed(topic, partition, offset)
            self._maybe_commit(topic, partition)
        return "run_started"

    def _drain_awaiting_run(self) -> None:
        """Re-process holds after RunStarted (cross-partition reorder)."""
        pending = list(self._awaiting_run)
        self._awaiting_run.clear()
        self.metrics["awaiting_run_count"] = 0
        for be in pending:
            self.process_record(
                topic=be.topic,
                partition=be.partition,
                offset=be.offset,
                value=be.event,
            )

    def _flush_awaiting_run_timeouts(self) -> List[str]:
        actions: List[str] = []
        keep: List[BufferedEvent] = []
        now = time.monotonic()
        for be in self._awaiting_run:
            age_ms = (now - be.received_at) * 1000.0
            if age_ms >= self.awaiting_run_timeout_ms:
                self._mark_stale(be.topic, be.partition, be.offset, be.event)
                actions.append("awaiting_run_stale")
            else:
                keep.append(be)
        self._awaiting_run = keep
        self.metrics["awaiting_run_count"] = len(keep)
        return actions

    def _mark_stale(self, topic, partition, offset, value) -> None:
        self.metrics["stale_event_count"] += 1
        self.metrics["projector_stale_total"] += 1
        entity = value.get("entity") or {}
        self.store.apply_batch_tx(
            ledger_rows=[
                {
                    "event_id": value["eventId"],
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "simulation_run_id": value["simulationRunId"],
                    "cycle_sequence": value["cycleSequence"],
                    "entity_id": entity.get("id") or "",
                    "status": STATUS_STALE_SKIPPED,
                    "payload_hash": value.get("entityPayloadHash") or "",
                }
            ],
            entity_updates=[],
        )
        if self._can_commit_offsets():
            self.offsets.mark_completed(topic, partition, offset)
            self._maybe_commit(topic, partition)

    def _mark_fence_skipped(
        self, topic, partition, offset, value, *, run_id: str = ""
    ) -> None:
        self.metrics["fence_skipped_count"] += 1
        self.metrics["projector_fence_total"] += 1
        event_id = str(
            value.get("eventId") or f"fence:{partition}:{offset}"
        )
        self.store.apply_batch_tx(
            ledger_rows=[
                {
                    "event_id": event_id,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "simulation_run_id": run_id or str(value.get("simulationRunId") or ""),
                    "cycle_sequence": int(value.get("cycleSequence") or 0),
                    "entity_id": (value.get("entity") or {}).get("id", "")
                    if isinstance(value.get("entity"), dict)
                    else "",
                    "status": STATUS_FENCE_SKIPPED,
                    "payload_hash": str(value.get("entityPayloadHash") or event_id),
                }
            ],
            entity_updates=[],
        )
        if self._can_commit_offsets():
            self.offsets.mark_completed(topic, partition, offset)
            self._maybe_commit(topic, partition)

    def _mark_sim_time_regression(self, topic, partition, offset, value) -> None:
        self.metrics["stale_event_count"] += 1
        self.metrics["projector_stale_total"] += 1
        entity = value.get("entity") or {}
        self.store.apply_batch_tx(
            ledger_rows=[
                {
                    "event_id": value["eventId"],
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "simulation_run_id": value["simulationRunId"],
                    "cycle_sequence": value["cycleSequence"],
                    "entity_id": entity.get("id") or "",
                    "status": STATUS_SIM_TIME_REGRESSION,
                    "payload_hash": value.get("entityPayloadHash") or "",
                }
            ],
            entity_updates=[],
        )
        if self._can_commit_offsets():
            self.offsets.mark_completed(topic, partition, offset)
            self._maybe_commit(topic, partition)

    def _supersede_cycle(self, cycle_buffers: List[NodeBuffer]) -> None:
        ledger_rows = []
        for buf in cycle_buffers:
            for be in buf.events.values():
                ledger_rows.append(
                    {
                        "event_id": be.event["eventId"],
                        "topic": be.topic,
                        "partition": be.partition,
                        "offset": be.offset,
                        "simulation_run_id": buf.simulation_run_id,
                        "cycle_sequence": buf.cycle_sequence,
                        "entity_id": be.event["entity"]["id"],
                        "status": STATUS_COALESCED_SUPERSEDED,
                        "payload_hash": be.event["entityPayloadHash"],
                    }
                )
                if self._can_commit_offsets():
                    self.offsets.mark_completed(be.topic, be.partition, be.offset)
        if ledger_rows:
            self.store.apply_batch_tx(ledger_rows=ledger_rows, entity_updates=[])
            self.metrics["coalesced_event_count"] += len(ledger_rows)
            parts = {(r["topic"], r["partition"]) for r in ledger_rows}
            for topic, part in parts:
                self._maybe_commit(topic, part)
        for buf in cycle_buffers:
            self.buffers.pop_ready(buf.key)

    def _mark_quarantine(self, topic, partition, offset, value) -> None:
        self.metrics["quarantine_count"] += 1
        entity = value.get("entity") or {}
        self.store.apply_batch_tx(
            ledger_rows=[
                {
                    "event_id": value.get("eventId") or f"q-{partition}-{offset}",
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "simulation_run_id": value.get("simulationRunId") or "",
                    "cycle_sequence": int(value.get("cycleSequence") or 0),
                    "entity_id": (entity.get("id") if isinstance(entity, dict) else "")
                    or "",
                    "status": STATUS_QUARANTINED,
                    "payload_hash": value.get("entityPayloadHash") or "",
                }
            ],
            entity_updates=[],
        )
        if self._can_commit_offsets():
            self.offsets.mark_completed(topic, partition, offset)
            self._maybe_commit(topic, partition)

    def _apply_node_buffer(self, buf: NodeBuffer, *, partial: bool) -> None:
        if self.write_mode != WriteMode.ACTIVE:
            return
        apply_started = time.perf_counter()
        entities_out: List[dict] = []
        meta: List[BufferedEvent] = []
        for be in buf.events.values():
            ent = be.event["entity"]
            if not self.shadow or self.target_namespace == "production":
                entities_out.append(ent)
            elif self.target_namespace not in ("", "production"):
                entities_out.append(to_namespaced_entity(ent, self.target_namespace))
            else:
                entities_out.append(to_shadow_entity(ent))
            meta.append(be)

        # One batch ledger SELECT for the whole cycle (idempotency + hash FAULT).
        event_ids = [str(be.event.get("eventId") or "") for be in meta if be.event.get("eventId")]
        existing_by_id: Dict[str, dict] = {}
        if event_ids:
            t_ledger = time.perf_counter_ns()
            existing_by_id = self.store.get_ledgers(event_ids)
            self._record_ledger_lookup(time.perf_counter_ns() - t_ledger)

        filtered_entities: List[dict] = []
        filtered_meta: List[BufferedEvent] = []
        for ent, be in zip(entities_out, meta):
            eid = str(be.event.get("eventId") or "")
            payload_hash = str(be.event.get("entityPayloadHash") or "")
            existing = existing_by_id.get(eid) if eid else None
            if existing and existing["payload_hash"] != payload_hash:
                self.faulted = True
                self.fault_message = f"eventId {eid} payload_hash mismatch"
                raise ProjectorFault(self.fault_message)
            if existing and existing["status"] == STATUS_APPLIED:
                if self._can_commit_offsets():
                    self.offsets.mark_completed(be.topic, be.partition, be.offset)
                continue
            filtered_entities.append(ent)
            filtered_meta.append(be)
        entities_out = filtered_entities
        meta = filtered_meta

        if not meta:
            # Entire cycle already applied — commit offsets only.
            if self._can_commit_offsets():
                parts = {(be.topic, be.partition) for be in buf.events.values()}
                for topic, part in parts:
                    self._maybe_commit(topic, part)
            self.buffers.pop_ready(buf.key)
            cycle_ms = (time.perf_counter() - apply_started) * 1000.0
            self._record_stage("apply_total_ms", cycle_ms)
            self._flush_cycle_lookup_metrics(
                cycle_ms=cycle_ms,
                cycle_key=f"{buf.simulation_run_id}:{buf.cycle_sequence}:{buf.node_id}",
            )
            self._consume_sla_warmup()
            return

        # grouping_cpu starts AFTER ledger lookup (ledger has its own stage).
        grouping_started = time.perf_counter()
        # CATCH_UP: keep latest per entity only
        if self.mode == ProjectorMode.CATCH_UP and len(entities_out) > 1:
            latest: Dict[str, tuple] = {}
            for ent, be in zip(entities_out, meta):
                eid = ent["id"]
                prev = latest.get(eid)
                if prev is None or be.event["cycleSequence"] >= prev[1].event["cycleSequence"]:
                    if prev is not None:
                        self.metrics["coalesced_event_count"] += 1
                    latest[eid] = (ent, be)
            entities_out = [t[0] for t in latest.values()]
            meta = [t[1] for t in latest.values()]

        self._record_stage("grouping_cpu_ms", (time.perf_counter() - grouping_started) * 1000.0)
        self._record_stage("buffer_wait_ms", buf.age_ms())
        for be in meta:
            captured = self._captured_at_epoch(be.event.get("capturedAt"))
            if captured is not None and be.broker_timestamp_epoch is not None:
                self._record_stage(
                    "capture_to_broker_ms",
                    (be.broker_timestamp_epoch - captured) * 1000.0,
                )
                self._record_stage(
                    "broker_to_consumer_ms",
                    (be.consumer_received_epoch - be.broker_timestamp_epoch) * 1000.0,
                )

        t0 = time.perf_counter()
        if not self.retry_manager.circuit_allows_attempt():
            self.runtime_phase = RuntimePhase.DEGRADED
            self.buffers.pause()
            return
        result = self.batch_upsert(entities_out)
        batch_ms = (time.perf_counter() - t0) * 1000.0
        self._record_stage("orion_http_ms", batch_ms)
        self.metrics["orion_apply_count"] += 1
        self.metrics["last_orion_batch_duration_ms"] = batch_ms
        self.metrics["projector_batch_latency_ms"] = batch_ms
        success_ids, retryable, permanent, transport_transient = classify_batch_result(
            result
        )
        entity_ids = [str(e["id"]) for e in entities_out]
        if retryable or transport_transient:
            self.metrics["orion_partial_count"] += 1
            self.retry_manager.start_or_continue(entity_ids)
            should_retry, _sleep = self.retry_manager.record_failure(
                retryable=True,
                error="orion transient",
            )
            if should_retry:
                self.runtime_phase = RuntimePhase.DEGRADED
                self.buffers.pause()
                return
            self.runtime_phase = RuntimePhase.DEGRADED
            self.buffers.pause()
            return
        self.retry_manager.record_success()

        status = STATUS_NODE_PARTIAL_APPLIED if partial else STATUS_APPLIED
        ledger_rows = []
        entity_updates = []
        permanent_map = permanent  # set of entity ids
        for ent, be in zip(entities_out, meta):
            eid_prod = be.event["entity"]["id"]
            ok = ent["id"] in success_ids or eid_prod in success_ids
            if not ok and eid_prod in permanent_map:
                st = STATUS_FAILED_PERMANENT
            elif not ok:
                st = STATUS_FAILED_PERMANENT
            else:
                st = status

            ledger_rows.append(
                {
                    "event_id": be.event["eventId"],
                    "topic": be.topic,
                    "partition": be.partition,
                    "offset": be.offset,
                    "simulation_run_id": buf.simulation_run_id,
                    "cycle_sequence": buf.cycle_sequence,
                    "entity_id": eid_prod,
                    "status": st,
                    "payload_hash": be.event["entityPayloadHash"],
                }
            )
            if st in (STATUS_APPLIED, STATUS_NODE_PARTIAL_APPLIED):
                captured = be.event.get("capturedAt")
                entity_updates.append(
                    {
                        "simulation_run_id": buf.simulation_run_id,
                        "entity_id": eid_prod,
                        "last_cycle_sequence": buf.cycle_sequence,
                        "last_event_id": be.event["eventId"],
                        "last_payload_hash": be.event["entityPayloadHash"],
                        "last_simulation_time": float(be.event["simulationTime"]),
                    }
                )
                if self._can_commit_offsets():
                    self.offsets.mark_completed(be.topic, be.partition, be.offset)
            elif st == STATUS_FAILED_PERMANENT and self._can_commit_offsets():
                self.offsets.mark_completed(be.topic, be.partition, be.offset)

        sqlite_started = time.perf_counter()
        self.store.apply_batch_tx(
            ledger_rows=ledger_rows, entity_updates=entity_updates
        )
        self._record_stage("sqlite_tx_ms", (time.perf_counter() - sqlite_started) * 1000.0)
        self.metrics["node_apply_count"] += 1
        self._record_pipeline_sla(meta)
        self.buffers.pop_ready(buf.key)
        if self._can_commit_offsets():
            offset_started = time.perf_counter()
            parts = {(be.topic, be.partition) for be in meta}
            for topic, part in parts:
                self._maybe_commit(topic, part)
            self._record_stage("offset_local_ms", (time.perf_counter() - offset_started) * 1000.0)
        self._record_stage("apply_total_ms", (time.perf_counter() - apply_started) * 1000.0)
        cycle_ms = (time.perf_counter() - apply_started) * 1000.0
        self.metrics["projector_apply_latency_ms"] = cycle_ms
        self._flush_cycle_lookup_metrics(
            cycle_ms=cycle_ms,
            cycle_key=f"{buf.simulation_run_id}:{buf.cycle_sequence}:{buf.node_id}",
        )
        self._consume_sla_warmup()
        if status == STATUS_APPLIED and not partial:
            scenario_id = None
            sim_time = 0.0
            for be in meta:
                ent = be.event.get("entity") or {}
                props = ent if isinstance(ent, dict) else {}
                sid = be.event.get("scenarioId")
                if sid:
                    scenario_id = str(sid)
                sim_time = max(sim_time, float(be.event.get("simulationTime") or 0))
            fresh = self.metrics.get("pipeline_freshness_sec")
            rt_status = (
                RuntimeStatus.CATCH_UP
                if self.mode == ProjectorMode.CATCH_UP
                else RuntimeStatus.ACTIVE
            )
            if self.runtime_phase == RuntimePhase.DEGRADED:
                rt_status = RuntimeStatus.DEGRADED
            self.store.set_runtime_state(
                simulation_run_id=buf.simulation_run_id,
                scenario_id=scenario_id,
                simulation_time=sim_time,
                status=rt_status.value,
                last_applied_cycle=buf.cycle_sequence,
                freshness_seconds=fresh,
            )
            self.runtime_cache.update_after_apply(
                simulation_run_id=buf.simulation_run_id,
                scenario_id=scenario_id,
                simulation_time=sim_time,
                last_applied_cycle=buf.cycle_sequence,
                freshness_seconds=fresh,
                status=rt_status,
            )
            self.runtime_phase = (
                RuntimePhase.CATCH_UP
                if self.mode == ProjectorMode.CATCH_UP
                else RuntimePhase.ACTIVE
            )
            self.buffers.paused = False
            self._partitions_paused = False

    def readiness_ok(
        self,
        *,
        lag_events: int,
        max_partition_lag: int,
        freshness_sec: Optional[float],
        assigned: bool,
        unresolved_dlq: bool = False,
    ) -> tuple[bool, str]:
        if self.faulted:
            return False, "faulted"
        if self.runtime_phase == RuntimePhase.FAULTED:
            return False, "runtime_faulted"
        if unresolved_dlq:
            return False, "unresolved_dlq"
        if self.retry_manager.has_pending or self.retry_manager.degraded:
            return False, "retry_pending"
        if self.buffers.paused or self._partitions_paused:
            return False, "buffers_paused"
        if not assigned:
            return False, "no_assignment"
        if self.write_mode != WriteMode.ACTIVE:
            return False, "write_mode_not_active"
        active = self.store.get_active_run(
            source=self.source, producer_id="visualize-traci"
        )
        if active is None:
            return True, "idle"
        if lag_events > READY_LAG_EVENTS or max_partition_lag > READY_MAX_PARTITION_LAG:
            return False, "lag_high"
        if freshness_sec is not None and freshness_sec > READY_FRESHNESS_SEC:
            return False, "freshness_high"
        return True, "active"

    def _consume_sla_warmup(self) -> None:
        if self._sla_warmup_cycles_remaining > 0:
            self._sla_warmup_cycles_remaining -= 1

    def _maybe_commit(self, topic: str, partition: int) -> None:
        if not self._can_commit_offsets():
            return
        off = self.offsets.contiguous_commit_offset(topic, partition)
        if off is None:
            return
        prev = self.store.get_committed_offset(topic, partition)
        if prev is not None and off <= prev:
            return
        self.store.set_committed_offset(topic, partition, off)
        self.offsets.advance_commit(topic, partition, off)
