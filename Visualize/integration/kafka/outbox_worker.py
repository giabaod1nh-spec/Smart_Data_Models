"""Single outbox delivery worker (K-2b) — bounded in-flight produce + poll."""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Callable, List, Optional, Tuple

from integration.kafka.outbox_store import (
    KafkaOutboxStore,
    compute_next_retry_at,
)
from integration.kafka.outbox_schema import EVENT_KIND_RUN_STARTED

log = logging.getLogger(__name__)


class OutboxDeliveryWorker:
    """One worker: fetch eligible → mark QUEUED (batch) → produce → batched ACK/FAIL writes.

    Delivery callbacks only touch in-memory buffers; all SQLite writes happen on
    the worker thread in short batched transactions, so the TraCI append never
    queues behind per-event status commits.
    """

    def __init__(
        self,
        store: KafkaOutboxStore,
        *,
        producer: Any,
        max_in_flight: int = 32,
        poll_interval_sec: float = 0.1,
        loop_sleep_sec: float = 0.05,
        cleanup_every_sec: float = 60.0,
        acked_retention_days: int = 7,
        checkpoint_every_sec: float = 5.0,
    ) -> None:
        self.store = store
        self._producer = producer
        self.max_in_flight = int(max_in_flight)
        self.poll_interval_sec = float(poll_interval_sec)
        self.loop_sleep_sec = float(loop_sleep_sec)
        self.cleanup_every_sec = float(cleanup_every_sec)
        self.acked_retention_days = int(acked_retention_days)
        self.checkpoint_every_sec = float(checkpoint_every_sec)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._in_flight = 0
        self._in_flight_lock = threading.Lock()
        self._buffer_lock = threading.Lock()
        self._pending_acks: List[Tuple[str, int, int]] = []
        self._pending_fails: List[Tuple[str, str, str]] = []
        self.produced_total = 0
        self.acked_total = 0
        self.failed_total = 0

    def start(self) -> int:
        recovered = self.store.recover_orphaned_queued()
        if recovered:
            log.warning("outbox recovered orphaned QUEUED → FAILED_RETRYABLE count=%d", recovered)
        self._stop.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="outbox-kafka-poll", daemon=True
        )
        self._poll_thread.start()
        self._thread = threading.Thread(
            target=self._run, name="outbox-delivery-worker", daemon=True
        )
        self._thread.start()
        return recovered

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        try:
            if self._producer is not None:
                self._producer.flush(timeout)
        except Exception:
            log.exception("outbox worker flush failed")
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2.0)
        # Late callbacks landed in the buffers during flush — persist them so a
        # clean shutdown never leaves delivered events stuck in QUEUED.
        try:
            self._flush_status_buffers()
        except Exception:
            log.exception("outbox status flush on stop failed")
        try:
            self.store.checkpoint_wal("TRUNCATE")
        except Exception:
            log.exception("outbox checkpoint on stop failed")

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if self._producer is not None:
                    self._producer.poll(self.poll_interval_sec)
                else:
                    time.sleep(self.poll_interval_sec)
            except Exception:
                log.exception("outbox poll error")
                time.sleep(self.poll_interval_sec)

    def _run(self) -> None:
        last_cleanup = time.monotonic()
        last_checkpoint = time.monotonic()
        while not self._stop.is_set():
            try:
                self._flush_status_buffers()
                with self._in_flight_lock:
                    room = self.max_in_flight - self._in_flight
                if room > 0:
                    batch = self.store.fetch_eligible(limit=room)
                    if batch:
                        self._dispatch_batch(batch)
                if self._producer is not None:
                    self._producer.poll(0)
                now = time.monotonic()
                if now - last_cleanup >= self.cleanup_every_sec:
                    n = self.store.cleanup_acked(
                        older_than_days=self.acked_retention_days
                    )
                    if n:
                        log.info("outbox cleaned ACKED rows=%d", n)
                    last_cleanup = now
                if now - last_checkpoint >= self.checkpoint_every_sec:
                    self.store.checkpoint_wal()
                    last_checkpoint = now
            except Exception:
                log.exception("outbox worker loop error")
            self._stop.wait(self.loop_sleep_sec)

    def _flush_status_buffers(self) -> None:
        with self._buffer_lock:
            acks, self._pending_acks = self._pending_acks, []
            fails, self._pending_fails = self._pending_fails, []
        if acks:
            try:
                self.store.mark_acked_batch(acks)
            except Exception:
                with self._buffer_lock:
                    self._pending_acks = acks + self._pending_acks
                raise
        if fails:
            try:
                self.store.mark_failed_retryable_batch(fails)
            except Exception:
                with self._buffer_lock:
                    self._pending_fails = fails + self._pending_fails
                raise

    @staticmethod
    def _payload_defect(rec) -> Optional[str]:
        """Return a permanent-failure reason, or None if the row is deliverable."""
        kind = getattr(rec, "event_kind", None)
        if kind is None and hasattr(rec, "payload_json"):
            try:
                body = json.loads(rec.payload_json)
                if body.get("eventType") == "TrafficSimulationRunStarted":
                    kind = EVENT_KIND_RUN_STARTED
            except Exception:
                pass
        if kind == EVENT_KIND_RUN_STARTED:
            try:
                body = json.loads(rec.payload_json)
            except Exception as e:
                return f"payload parse: {e}"
            if body.get("eventType") != "TrafficSimulationRunStarted":
                return "run_started eventType mismatch"
            return None
        try:
            body = json.loads(rec.payload_json)
        except Exception as e:
            return f"payload parse: {e}"
        if body.get("eventId") != rec.event_id:
            return "payload eventId mismatch"
        if body.get("entityPayloadHash") != rec.payload_hash:
            return "payload_hash mismatch vs frozen row"
        return None

    def _dispatch_batch(self, records) -> None:
        permanent: List[Tuple[str, str]] = []
        ready = []
        for rec in records:
            reason = self._payload_defect(rec)
            if reason is None:
                ready.append(rec)
            else:
                permanent.append((rec.event_id, reason))
        if permanent:
            self.store.mark_failed_permanent_batch(permanent)
            self.failed_total += len(permanent)
        if not ready:
            return
        self.store.mark_queued_batch([rec.event_id for rec in ready])
        for rec in ready:
            self._dispatch(rec)

    def _dispatch(self, rec) -> None:
        with self._in_flight_lock:
            self._in_flight += 1

        value = rec.payload_json.encode("utf-8")
        key = rec.event_key.encode("utf-8")

        def _cb(err, msg, event_id=rec.event_id, attempt=rec.attempt_count + 1):
            try:
                if err is not None:
                    nxt = compute_next_retry_at(attempt)
                    with self._buffer_lock:
                        self._pending_fails.append((event_id, str(err), nxt))
                    self.failed_total += 1
                else:
                    part = msg.partition() if msg is not None else -1
                    off = msg.offset() if msg is not None else -1
                    with self._buffer_lock:
                        self._pending_acks.append(
                            (
                                event_id,
                                int(part) if part is not None else -1,
                                int(off) if off is not None else -1,
                            )
                        )
                    self.acked_total += 1
            finally:
                with self._in_flight_lock:
                    self._in_flight = max(0, self._in_flight - 1)

        try:
            self._producer.produce(
                rec.topic,
                key=key,
                value=value,
                on_delivery=_cb,
            )
            self.produced_total += 1
            try:
                self._producer.poll(0)
            except Exception:
                pass
        except BufferError as e:
            nxt = compute_next_retry_at(rec.attempt_count + 1)
            self.store.mark_failed_retryable(
                rec.event_id, error=f"BufferError:{e}", next_retry_at=nxt
            )
            with self._in_flight_lock:
                self._in_flight = max(0, self._in_flight - 1)
            self.failed_total += 1
        except Exception as e:
            nxt = compute_next_retry_at(rec.attempt_count + 1)
            self.store.mark_failed_retryable(
                rec.event_id, error=f"produce:{e}", next_retry_at=nxt
            )
            with self._in_flight_lock:
                self._in_flight = max(0, self._in_flight - 1)
            self.failed_total += 1
            log.exception("outbox produce failed event_id=%s", rec.event_id)

    def health(self) -> dict:
        return {
            "in_flight": self._in_flight,
            "produced_total": self.produced_total,
            "acked_total": self.acked_total,
            "failed_total": self.failed_total,
            "worker_alive": self._thread is not None and self._thread.is_alive(),
            "poll_alive": self._poll_thread is not None and self._poll_thread.is_alive(),
            **self.store.capacity_metrics(),
        }
