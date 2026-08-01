"""
Async Orion publisher: bounded FIFO full-cycle queue + single worker.

Hard rules:
- One queue item = one full PublishCycle (JSON bytes).
- Worker never imports TraCI / SumoBackend.
- Sequential: entity cursor — never re-send successfully published entities.
- Batch: remaining-subset cursor — never re-send confirmed success_ids.
- Transient after RETRY_MAX → DEGRADED slow retry (never skip cycle).
- Permanent (400/401/403) / protocol → FAULTED.
- No auto-fallback between sequential and batch.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from enum import Enum
from typing import Optional

from integration.orion.client import (
    BatchUpsertResult,
    OrionBatchProtocolError,
    OrionPermanentError,
    OrionTransientError,
    batch_upsert_entities,
    close_session,
    get_session,
    upsert_entity,
)
from integration.orion.publish_cycle import PublishCycle
from integration.orion.publisher_metrics import PublisherMetrics

log = logging.getLogger(__name__)

_SENTINEL = object()
_VALID_MODES = frozenset({"sequential", "batch"})


class PublisherState(str, Enum):
    READY = "READY"
    BACKPRESSURE = "BACKPRESSURE"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"


class AsyncOrionPublisher:
    def __init__(
        self,
        *,
        queue_size: int = 5,
        retry_max: int = 10,
        retry_base_sec: float = 0.5,
        retry_slow_sec: float = 5.0,
        shutdown_timeout_sec: float = 15.0,
        worker_count: int = 1,
        publish_mode: str = "sequential",
    ) -> None:
        if worker_count != 1:
            raise ValueError("Only ORION_PUBLISH_WORKER_COUNT=1 is supported")
        mode = (publish_mode or "sequential").strip().lower()
        if mode not in _VALID_MODES:
            raise ValueError(
                f"publish_mode must be one of {sorted(_VALID_MODES)}, got {publish_mode!r}"
            )
        self.publish_mode = mode
        self.queue_size = int(queue_size)
        self.retry_max = int(retry_max)
        self.retry_base_sec = float(retry_base_sec)
        self.retry_slow_sec = float(retry_slow_sec)
        self.shutdown_timeout_sec = float(shutdown_timeout_sec)

        self._q: queue.Queue = queue.Queue(maxsize=self.queue_size)
        self._metrics = PublisherMetrics(
            queue_capacity=self.queue_size, publish_mode=self.publish_mode
        )
        self._state = PublisherState.READY
        self._state_lock = threading.Lock()

        self._worker: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._accepting = True

        self._track_lock = threading.Lock()
        self._in_flight: Optional[PublishCycle] = None
        self._in_flight_entity_index = 0
        self._entity_retry_count = 0
        self._oldest_queued_sim_t: Optional[float] = None
        self._oldest_queued_captured_at: Optional[float] = None
        self._queued_order: list[PublishCycle] = []  # FIFO view for age tracking

        self._last_fully_published_sim_t: Optional[float] = None
        self._flushed_cycles = 0
        self._fault_message: Optional[str] = None
        self._traci_pending: Optional[PublishCycle] = None

    # ── public API ──────────────────────────────────────────────────

    @property
    def metrics(self) -> PublisherMetrics:
        return self._metrics

    def start(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        get_session()  # warm pool on worker side
        self._stop_event.clear()
        self._accepting = True
        self._set_state(PublisherState.READY)
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="orion-async-publisher",
            daemon=False,
        )
        self._worker.start()
        self._metrics.update(
            worker_alive=True,
            publisher_state=PublisherState.READY.value,
            publish_mode=self.publish_mode,
        )
        log.info(
            "AsyncOrionPublisher started mode=%s queue_size=%d retry_max=%d",
            self.publish_mode,
            self.queue_size,
            self.retry_max,
        )

    def stop(self, timeout: Optional[float] = None) -> dict:
        """Best-effort drain. Returns flush stats (remaining may be lost on timeout)."""
        timeout = self.shutdown_timeout_sec if timeout is None else float(timeout)
        self._accepting = False
        try:
            self._q.put(_SENTINEL, timeout=min(2.0, timeout))
        except queue.Full:
            pass
        # Allow interruptible backoff to wake; worker still drains until SENTINEL/timeout.
        self._stop_event.set()

        if self._worker is not None:
            self._worker.join(timeout=timeout)
            alive = self._worker.is_alive()
        else:
            alive = False

        with self._track_lock:
            in_flight_aborted = self._in_flight is not None
            remaining_after = len(self._queued_order) + (1 if self._in_flight else 0)

        if alive:
            log.warning(
                "AsyncOrionPublisher shutdown timeout: remaining_cycles=%d "
                "in_flight_aborted=%s durability_limit_reached=true",
                remaining_after,
                in_flight_aborted,
            )
        else:
            log.info(
                "AsyncOrionPublisher stopped flushed_cycles=%d remaining_cycles=%d",
                self._flushed_cycles,
                remaining_after,
            )

        self._metrics.update(
            worker_alive=False,
            flushed_cycles=self._flushed_cycles,
            remaining_cycles=remaining_after,
            shutdown_pending_count=remaining_after,
        )
        close_session()
        return {
            "flushed_cycles": self._flushed_cycles,
            "remaining_cycles": remaining_after,
            "in_flight_aborted": in_flight_aborted,
            "durability_limit_reached": alive,
        }

    def try_enqueue(self, cycle: PublishCycle) -> bool:
        """Non-blocking enqueue. False → caller must hold pending / backpressure."""
        if not self._accepting or self.is_faulted:
            return False
        # Append to tracking list BEFORE put so worker cannot race past us.
        with self._track_lock:
            if len(self._queued_order) >= self.queue_size:
                return False
            self._queued_order.append(cycle)
            self._refresh_oldest_locked()
        try:
            self._q.put_nowait(cycle)
        except queue.Full:
            with self._track_lock:
                if cycle in self._queued_order:
                    self._queued_order.remove(cycle)
                self._refresh_oldest_locked()
            return False
        self._metrics.update(
            last_enqueued_simulation_time=cycle.simulation_time,
            queue_depth=self._q.qsize(),
            cycle_sequence=cycle.sequence_number,
        )
        self._metrics.incr("legacy_orion_cycles_enqueued_total")
        return True

    def queue_full(self) -> bool:
        with self._track_lock:
            return len(self._queued_order) >= self.queue_size

    def queue_depth(self) -> int:
        return self._q.qsize()

    @property
    def is_faulted(self) -> bool:
        return self.state == PublisherState.FAULTED

    @property
    def is_degraded(self) -> bool:
        return self.state == PublisherState.DEGRADED

    @property
    def state(self) -> PublisherState:
        with self._state_lock:
            return self._state

    def mark_faulted(self, reason: str) -> None:
        self._fault_message = reason
        self._set_state(PublisherState.FAULTED)
        log.error("AsyncOrionPublisher FAULTED: %s", reason)

    def last_fully_published_simulation_time(self) -> Optional[float]:
        with self._track_lock:
            return self._last_fully_published_sim_t

    def set_traci_pending(self, pending: Optional[PublishCycle]) -> None:
        """TraCI thread reports held pending cycle for depth/stats (P1 monitoring)."""
        with self._track_lock:
            self._traci_pending = pending

    def traci_pending(self) -> Optional[PublishCycle]:
        with self._track_lock:
            return getattr(self, "_traci_pending", None)

    def in_flight_cycle(self) -> Optional[PublishCycle]:
        with self._track_lock:
            return self._in_flight

    def oldest_queued_sim_t(self) -> Optional[float]:
        with self._track_lock:
            return self._oldest_queued_sim_t

    def oldest_queued_captured_at(self) -> Optional[float]:
        with self._track_lock:
            return self._oldest_queued_captured_at

    def effective_depth(self, pending_on_traci: bool = False) -> int:
        with self._track_lock:
            inflight = 1 if self._in_flight is not None else 0
            queued = len(self._queued_order)
            traci_pending = getattr(self, "_traci_pending", None) is not None
        pending = pending_on_traci or traci_pending
        return (1 if pending else 0) + queued + inflight

    def oldest_pending_age_sec(
        self,
        pending_cycle: Optional[PublishCycle] = None,
    ) -> float:
        now = time.monotonic()
        ages: list[float] = []
        if pending_cycle is not None:
            ages.append(now - pending_cycle.captured_at_monotonic)
        with self._track_lock:
            if self._in_flight is not None:
                ages.append(now - self._in_flight.captured_at_monotonic)
            if self._oldest_queued_captured_at is not None:
                ages.append(now - self._oldest_queued_captured_at)
        return min(ages) if ages else 0.0

    def oldest_pending_simulation_time(
        self,
        pending_cycle: Optional[PublishCycle] = None,
    ) -> Optional[float]:
        times: list[float] = []
        if pending_cycle is not None:
            times.append(pending_cycle.simulation_time)
        with self._track_lock:
            if self._in_flight is not None:
                times.append(self._in_flight.simulation_time)
            if self._oldest_queued_sim_t is not None:
                times.append(self._oldest_queued_sim_t)
        return min(times) if times else None

    def compute_realtime_lag(
        self,
        current_simulation_time: float,
        pending_cycle: Optional[PublishCycle] = None,
    ) -> float:
        oldest = self.oldest_pending_simulation_time(pending_cycle)
        oldest_cap = None
        if pending_cycle is not None:
            oldest_cap = pending_cycle.captured_at_monotonic
        with self._track_lock:
            caps = []
            if oldest_cap is not None:
                caps.append(oldest_cap)
            if self._in_flight is not None:
                caps.append(self._in_flight.captured_at_monotonic)
            if self._oldest_queued_captured_at is not None:
                caps.append(self._oldest_queued_captured_at)
            oldest_captured = min(caps) if caps else None
        return self._metrics.set_lag(
            current_simulation_time=current_simulation_time,
            last_fully_published=self.last_fully_published_simulation_time(),
            oldest_pending_sim_t=oldest,
            oldest_captured_at=oldest_captured,
        )

    # ── worker ──────────────────────────────────────────────────────

    def _set_state(self, state: PublisherState) -> None:
        with self._state_lock:
            self._state = state
        self._metrics.update(publisher_state=state.value)

    def _refresh_oldest_locked(self) -> None:
        if self._queued_order:
            head = self._queued_order[0]
            self._oldest_queued_sim_t = head.simulation_time
            self._oldest_queued_captured_at = head.captured_at_monotonic
        else:
            self._oldest_queued_sim_t = None
            self._oldest_queued_captured_at = None

    def _interruptible_sleep(self, delay: float) -> bool:
        """Wait up to delay seconds. True if stop requested (caller should abort)."""
        return self._stop_event.wait(delay)

    def _worker_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                if self.is_faulted:
                    if self._interruptible_sleep(0.2):
                        break
                    continue
                try:
                    item = self._q.get(timeout=0.2)
                except queue.Empty:
                    continue
                if item is _SENTINEL:
                    self._q.task_done()
                    break
                cycle: PublishCycle = item
                with self._track_lock:
                    if self._queued_order and self._queued_order[0] is cycle:
                        self._queued_order.pop(0)
                    elif cycle in self._queued_order:
                        self._queued_order.remove(cycle)
                    self._refresh_oldest_locked()
                    self._in_flight = cycle
                    self._in_flight_entity_index = (
                        -1 if self.publish_mode == "batch" else 0
                    )
                    self._entity_retry_count = 0
                self._metrics.update(
                    queue_depth=self._q.qsize(),
                    in_flight_cycle_id=cycle.sequence_number,
                    in_flight_entity_index=self._in_flight_entity_index,
                    entity_retry_count=0,
                    publish_mode=self.publish_mode,
                )
                ok = self._publish_cycle_with_cursor(cycle)
                with self._track_lock:
                    self._in_flight = None
                clear_index = -1 if self.publish_mode == "batch" else 0
                self._metrics.update(
                    in_flight_cycle_id=None, in_flight_entity_index=clear_index
                )
                self._q.task_done()
                if not ok:
                    # FAULTED or stop — stop consuming further cycles
                    break
        except Exception as e:
            log.error("AsyncOrionPublisher worker crashed: %s", e, exc_info=True)
            self.mark_faulted(f"worker unhandled exception: {e}")
        finally:
            self._metrics.update(worker_alive=False)

    def _publish_cycle_with_cursor(self, cycle: PublishCycle) -> bool:
        if self.publish_mode == "batch":
            return self._publish_cycle_batch(cycle)
        return self._publish_cycle_sequential(cycle)

    def _publish_cycle_sequential(self, cycle: PublishCycle) -> bool:
        """Publish all entities via entity-index cursor. Returns False if FAULTED/stop."""
        t0 = time.perf_counter()
        n = len(cycle.entities_json)
        index = 0
        with self._track_lock:
            index = self._in_flight_entity_index
            if index < 0:
                index = 0
                self._in_flight_entity_index = 0

        while index < n:
            if self._stop_event.is_set():
                return False
            eid = cycle.entity_ids[index] if index < len(cycle.entity_ids) else "?"
            try:
                entity = cycle.entity_at(index)
                upsert_entity(entity)
            except OrionPermanentError as e:
                self.mark_faulted(
                    f"permanent error entity={eid} seq={cycle.sequence_number} "
                    f"index={index} status={e.status}: {e}"
                )
                self._metrics.incr("entity_failure_count")
                return False
            except OrionTransientError as e:
                self._metrics.incr("entity_failure_count")
                self._metrics.incr("retry_count")
                with self._track_lock:
                    self._entity_retry_count += 1
                    retries = self._entity_retry_count
                self._metrics.update(entity_retry_count=retries)
                if retries <= self.retry_max:
                    delay = self.retry_base_sec * (2 ** min(retries - 1, 6))
                    log.warning(
                        "Transient Orion error entity=%s seq=%d attempt=%d/%d "
                        "backoff=%.2fs: %s",
                        eid,
                        cycle.sequence_number,
                        retries,
                        self.retry_max,
                        delay,
                        e,
                    )
                else:
                    self._set_state(PublisherState.DEGRADED)
                    delay = self.retry_slow_sec
                    log.warning(
                        "DEGRADED slow retry entity=%s seq=%d attempt=%d delay=%.2fs: %s",
                        eid,
                        cycle.sequence_number,
                        retries,
                        delay,
                        e,
                    )
                if self._interruptible_sleep(delay):
                    return False
                continue
            except Exception as e:
                self.mark_faulted(
                    f"unexpected error entity={eid} seq={cycle.sequence_number}: {e}"
                )
                return False

            # success
            self._metrics.incr("entity_success_count")
            self._metrics.incr("legacy_orion_entity_success_total")
            index += 1
            with self._track_lock:
                self._in_flight_entity_index = index
                self._entity_retry_count = 0
            if self.state == PublisherState.DEGRADED:
                self._set_state(PublisherState.READY)
            self._metrics.update(
                in_flight_entity_index=index,
                entity_retry_count=0,
            )

        return self._mark_cycle_complete(cycle, t0)

    def _publish_cycle_batch(self, cycle: PublishCycle) -> bool:
        """Publish via remaining-subset batch upsert. Returns False if FAULTED/stop."""
        t0 = time.perf_counter()
        entities = [cycle.entity_at(i) for i in range(len(cycle.entities_json))]
        by_id = {e["id"]: e for e in entities}
        remaining = list(entities)
        confirmed: set[str] = set()
        burst_retry = 0

        self._metrics.update(
            in_flight_entity_index=-1,
            batch_remaining_entity_count=len(remaining),
            batch_attempt_number=0,
            publish_mode="batch",
        )

        while remaining:
            if self._stop_event.is_set():
                return False

            req_ids = [e["id"] for e in remaining]
            self._metrics.incr("batch_request_count")
            self._metrics.incr("batch_entity_submitted_count", len(remaining))
            self._metrics.update(
                batch_remaining_entity_count=len(remaining),
                batch_attempt_number=burst_retry + 1,
                in_flight_entity_index=-1,
            )

            req_t0 = time.perf_counter()
            try:
                result: BatchUpsertResult = batch_upsert_entities(remaining)
            except OrionBatchProtocolError as e:
                self.mark_faulted(
                    f"batch protocol error seq={cycle.sequence_number}: {e}"
                )
                self._metrics.incr("entity_failure_count")
                return False
            except OrionPermanentError as e:
                self.mark_faulted(
                    f"batch permanent error seq={cycle.sequence_number}: {e}"
                )
                self._metrics.incr("entity_failure_count")
                return False
            except Exception as e:
                self.mark_faulted(
                    f"unexpected batch error seq={cycle.sequence_number}: {e}"
                )
                return False

            duration_ms = (time.perf_counter() - req_t0) * 1000.0
            self._metrics.update(batch_last_request_duration_ms=duration_ms)

            if result.permanent_errors:
                pe = result.permanent_errors[0]
                self.mark_faulted(
                    f"batch permanent entity={pe.entity_id} status={pe.status} "
                    f"seq={cycle.sequence_number}: {pe.title} {pe.detail}"
                )
                self._metrics.incr("entity_failure_count", len(result.permanent_errors))
                return False

            new_success = [eid for eid in result.success_ids if eid in by_id]
            for eid in new_success:
                if eid not in confirmed:
                    confirmed.add(eid)
                    self._metrics.incr("entity_success_count")
                    self._metrics.incr("legacy_orion_entity_success_total")
                    self._metrics.incr("batch_entity_confirmed_success_count")

            unresolved_ids = set(result.retryable_error_ids) | set(result.ambiguous_ids)
            # Never re-include confirmed successes
            next_remaining = [
                by_id[eid]
                for eid in req_ids
                if eid in unresolved_ids and eid not in confirmed and eid in by_id
            ]

            if result.ambiguous_ids:
                self._metrics.incr("batch_ambiguous_request_count")

            if not next_remaining:
                if result.http_status in (201, 204) or (
                    result.http_status == 207 and not unresolved_ids
                ):
                    self._metrics.incr("batch_request_full_success_count")
                self._metrics.incr("batch_cycle_success_count")
                self._metrics.update(batch_remaining_entity_count=0)
                return self._mark_cycle_complete(cycle, t0)

            # Partial / retry needed
            if result.success_ids and next_remaining:
                self._metrics.incr("batch_request_partial_count")

            prev_len = len(remaining)
            if len(next_remaining) < prev_len:
                burst_retry = 0  # progress → reset burst counter
            else:
                burst_retry += 1
                self._metrics.incr("batch_retry_count_total")
                self._metrics.incr("retry_count")
                self._metrics.incr("entity_failure_count")

            remaining = next_remaining
            self._metrics.update(
                batch_remaining_entity_count=len(remaining),
                batch_attempt_number=burst_retry,
                in_flight_entity_index=-1,
            )

            if burst_retry <= self.retry_max:
                delay = self.retry_base_sec * (2 ** min(max(burst_retry - 1, 0), 6))
                log.warning(
                    "Batch retry seq=%d remaining=%d attempt=%d/%d backoff=%.2fs "
                    "ambiguous=%d http=%s",
                    cycle.sequence_number,
                    len(remaining),
                    burst_retry,
                    self.retry_max,
                    delay,
                    len(result.ambiguous_ids),
                    result.http_status,
                )
            else:
                self._set_state(PublisherState.DEGRADED)
                delay = self.retry_slow_sec
                log.warning(
                    "DEGRADED batch slow retry seq=%d remaining=%d attempt=%d delay=%.2fs",
                    cycle.sequence_number,
                    len(remaining),
                    burst_retry,
                    delay,
                )

            if self._interruptible_sleep(delay):
                return False

        self._metrics.incr("batch_cycle_success_count")
        return self._mark_cycle_complete(cycle, t0)

    def _mark_cycle_complete(self, cycle: PublishCycle, t0: float) -> bool:
        with self._track_lock:
            self._last_fully_published_sim_t = cycle.simulation_time
            self._flushed_cycles += 1
            flushed = self._flushed_cycles
        duration_ms = (time.perf_counter() - t0) * 1000.0
        self._metrics.update(
            last_fully_published_simulation_time=cycle.simulation_time,
            publish_cycle_duration_ms=duration_ms,
            flushed_cycles=flushed,
        )
        self._metrics.incr("legacy_orion_cycles_published_total")
        if self.state == PublisherState.DEGRADED:
            self._set_state(PublisherState.READY)
        log.info(
            "Published cycle mode=%s seq=%d sim_t=%.3f entities=%d duration_ms=%.1f",
            self.publish_mode,
            cycle.sequence_number,
            cycle.simulation_time,
            len(cycle.entities_json),
            duration_ms,
        )
        return True
