"""Async Kafka producer (K-2a) — librdkafka buffer only; no Python Kafka backlog."""
from __future__ import annotations

import copy
import json
import logging
import threading
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from integration.kafka.event_mapper import (
    DEFAULT_PRODUCER_ID,
    build_cycle_events,
    partition_key,
)
from integration.kafka.event_validator import validate_entity_event
from integration.kafka.evidence_writer import AckEvidence, EvidenceWriter, FailedEvidence
from integration.kafka.producer_metrics import ProducerMetrics

log = logging.getLogger(__name__)

TOPIC_MAIN = "traffic.entity-events.v2"


class ProducerState(str, Enum):
    STARTING = "STARTING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class AsyncKafkaProducer:
    """Non-blocking dual-publish helper for TraCI.

    TraCI calls publish_cycle() which validates + produce() into librdkafka only.
    Delivery reports and evidence I/O run on dedicated threads.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str = "localhost:29092",
        topic: str = TOPIC_MAIN,
        client_id: str = "visualize-kafka-producer",
        producer_id: str = DEFAULT_PRODUCER_ID,
        producer_session_id: Optional[str] = None,
        evidence_root: Optional[Path] = None,
        simulation_run_id: str = "unknown-run",
        linger_ms: int = 10,
        delivery_timeout_ms: int = 30_000,
        request_timeout_ms: int = 10_000,
        statistics_interval_ms: int = 10_000,
        poll_interval_sec: float = 0.1,
        producer_factory: Optional[Callable[[dict], Any]] = None,
        enabled: bool = True,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client_id = client_id
        self.producer_id = producer_id
        self.producer_session_id = producer_session_id or str(uuid.uuid4())
        self.simulation_run_id = simulation_run_id
        self.linger_ms = int(linger_ms)
        self.delivery_timeout_ms = int(delivery_timeout_ms)
        self.request_timeout_ms = int(request_timeout_ms)
        self.statistics_interval_ms = int(statistics_interval_ms)
        self.poll_interval_sec = float(poll_interval_sec)
        self._producer_factory = producer_factory
        self.enabled = bool(enabled)

        self._metrics = ProducerMetrics()
        self._state = ProducerState.STARTING
        self._state_lock = threading.Lock()
        self._fault_message: Optional[str] = None

        self._producer: Any = None
        self._poll_thread: Optional[threading.Thread] = None
        self._stop_poll = threading.Event()
        self._accepting = False
        self._pending_meta: Dict[str, dict] = {}
        self._pending_lock = threading.Lock()

        root = evidence_root or (
            Path(__file__).resolve().parents[2] / "artifacts" / "kafka_producer_ledger"
        )
        self._evidence = EvidenceWriter(
            root=root, simulation_run_id=simulation_run_id
        )

    # ── lifecycle ───────────────────────────────────────────────────

    @property
    def metrics(self) -> ProducerMetrics:
        return self._metrics

    @property
    def state(self) -> ProducerState:
        with self._state_lock:
            return self._state

    @property
    def is_ready(self) -> bool:
        return self.state == ProducerState.READY

    @property
    def is_faulted(self) -> bool:
        return self.state == ProducerState.FAULTED

    def _set_state(self, state: ProducerState) -> None:
        with self._state_lock:
            prev = self._state
            self._state = state
        if prev != state:
            log.info("kafka_producer state %s → %s", prev.value, state.value)

    def start(self) -> None:
        if not self.enabled:
            self._set_state(ProducerState.STOPPED)
            return
        self._set_state(ProducerState.STARTING)
        self._evidence.start()
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": self.linger_ms,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "statistics.interval.ms": self.statistics_interval_ms,
            # idempotence-compatible defaults
            "max.in.flight.requests.per.connection": 5,
            "retries": 2147483647,
            "error_cb": self._on_error,
        }
        factory = self._producer_factory
        if factory is None:
            from confluent_kafka import Producer  # type: ignore

            factory = Producer
        try:
            self._producer = factory(conf)
        except Exception as e:
            self._fault_message = str(e)
            self._set_state(ProducerState.FAULTED)
            log.exception("kafka producer construct failed")
            return

        self._accepting = True
        self._stop_poll.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="kafka-producer-poll", daemon=True
        )
        self._poll_thread.start()
        self._emit_run_started()
        self._set_state(ProducerState.READY)

    def _emit_run_started(self) -> None:
        """First control record of the session — activates Projector active-run."""
        if self._producer is None:
            return
        from integration.kafka.event_mapper import (
            build_run_started_event,
            run_started_partition_key,
        )

        event = build_run_started_event(
            simulation_run_id=self.simulation_run_id,
            producer_session_id=self.producer_session_id,
            producer_id=self.producer_id,
        )
        key = run_started_partition_key(self.simulation_run_id).encode("utf-8")
        payload = json.dumps(event, ensure_ascii=True, separators=(",", ":")).encode(
            "utf-8"
        )
        try:
            self._producer.produce(self.topic, key=key, value=payload)
            self._producer.poll(0)
            log.info(
                "emitted TrafficSimulationRunStarted run=%s session=%s",
                self.simulation_run_id,
                self.producer_session_id,
            )
        except Exception:
            log.exception("failed to emit TrafficSimulationRunStarted")

    def stop(self, flush_timeout_sec: float = 10.0) -> list[str]:
        """Stop accept → poll → flush → return remaining pending eventIds."""
        self._accepting = False
        self._set_state(ProducerState.STOPPING)
        if self._producer is not None:
            try:
                remaining = int(self._producer.flush(flush_timeout_sec))
                if remaining:
                    log.warning("kafka flush left %d in librdkafka queue", remaining)
            except Exception:
                log.exception("kafka flush failed")
        self._stop_poll.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=flush_timeout_sec + 2.0)
            if self._poll_thread.is_alive():
                log.error("kafka poll thread still alive after stop")
                self._set_state(ProducerState.FAULTED)
        pending = self._metrics.pending_ids()
        if pending:
            log.warning("kafka producer stop with pending=%d ids=%s", len(pending), pending[:5])
        self._evidence.stop(timeout=5.0)
        if self.state != ProducerState.FAULTED:
            self._set_state(ProducerState.STOPPED)
        return pending

    # ── TraCI API ───────────────────────────────────────────────────

    def publish_cycle(
        self,
        entities: Sequence[dict[str, Any]],
        *,
        cycle_sequence: int,
        captured_at: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> int:
        """Validate + produce each event. Returns count successfully handed to librdkafka.

        Mutates neither the caller's list nor nested dicts (deep-copies first).
        """
        t0 = time.perf_counter()
        if not self.enabled or not self._accepting or self._producer is None:
            return 0
        if self.state in (ProducerState.FAULTED, ProducerState.STOPPING, ProducerState.STOPPED):
            return 0

        frozen = [copy.deepcopy(e) for e in entities]
        events = build_cycle_events(
            frozen,
            cycle_sequence=cycle_sequence,
            captured_at=captured_at,
            producer_id=self.producer_id,
            producer_session_id=self.producer_session_id,
            trace_id=trace_id,
        )
        enqueued = 0
        for event in events:
            if self._publish_one(event):
                enqueued += 1
        self._metrics.set_fanout_ms((time.perf_counter() - t0) * 1000.0)
        return enqueued

    def _publish_one(self, event: dict[str, Any]) -> bool:
        event_id = str(event.get("eventId") or "")
        entity = event.get("entity") if isinstance(event.get("entity"), dict) else {}
        entity_id = str(entity.get("id") or "")
        cycle_sequence = int(event.get("cycleSequence") or 0)
        run_id = str(event.get("simulationRunId") or self.simulation_run_id)

        self._metrics.mark_created(event_id)

        result = validate_entity_event(event)
        if not result.ok:
            self._metrics.mark_failed(event_id)
            self._metrics.mark_rejected()
            self._evidence.try_enqueue(
                FailedEvidence(
                    eventId=event_id,
                    simulationRunId=run_id,
                    cycleSequence=cycle_sequence,
                    entityId=entity_id,
                    reason=f"FAILED_PERMANENT:{result.reason}",
                    permanent=True,
                )
            )
            self._set_state(ProducerState.DEGRADED)
            log.error("kafka validate fail eventId=%s: %s", event_id, result.reason)
            return False

        key = partition_key(run_id, str(event["nodeId"]))
        payload = json.dumps(event, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        meta = {
            "eventId": event_id,
            "simulationRunId": run_id,
            "cycleSequence": cycle_sequence,
            "entityId": entity_id,
            "enqueued_at": time.perf_counter(),
        }
        t_enq = time.perf_counter()
        try:
            with self._pending_lock:
                self._pending_meta[event_id] = meta
            self._producer.produce(
                self.topic,
                key=key.encode("utf-8"),
                value=payload,
                on_delivery=self._on_delivery,
            )
            # trigger immediate poll so callbacks drain without waiting poll thread
            try:
                self._producer.poll(0)
            except Exception:
                pass
        except BufferError as e:
            with self._pending_lock:
                self._pending_meta.pop(event_id, None)
            self._metrics.mark_failed(event_id)
            self._metrics.mark_rejected()
            self._evidence.try_enqueue(
                FailedEvidence(
                    eventId=event_id,
                    simulationRunId=run_id,
                    cycleSequence=cycle_sequence,
                    entityId=entity_id,
                    reason=f"BufferError:{e}",
                    permanent=False,
                )
            )
            self._set_state(ProducerState.DEGRADED)
            log.warning("kafka BufferError eventId=%s", event_id)
            return False
        except Exception as e:
            with self._pending_lock:
                self._pending_meta.pop(event_id, None)
            self._metrics.mark_failed(event_id)
            self._evidence.try_enqueue(
                FailedEvidence(
                    eventId=event_id,
                    simulationRunId=run_id,
                    cycleSequence=cycle_sequence,
                    entityId=entity_id,
                    reason=f"produce:{e}",
                    permanent=False,
                )
            )
            self._set_state(ProducerState.DEGRADED)
            log.exception("kafka produce failed eventId=%s", event_id)
            return False

        self._metrics.mark_enqueued((time.perf_counter() - t_enq) * 1000.0)
        return True

    # ── callbacks / poll ────────────────────────────────────────────

    def _on_delivery(self, err: Any, msg: Any) -> None:
        # Extract eventId from value if needed
        event_id = ""
        try:
            if msg is not None and msg.value() is not None:
                body = json.loads(msg.value().decode("utf-8"))
                event_id = str(body.get("eventId") or "")
        except Exception:
            pass

        with self._pending_lock:
            meta = self._pending_meta.pop(event_id, None) if event_id else None
            if meta is None and self._pending_meta:
                # fallback: cannot match — leave metrics conservative
                meta = {"eventId": event_id or "unknown", "simulationRunId": self.simulation_run_id, "cycleSequence": -1, "entityId": "", "enqueued_at": time.perf_counter()}

        if meta is None:
            meta = {
                "eventId": event_id or "unknown",
                "simulationRunId": self.simulation_run_id,
                "cycleSequence": -1,
                "entityId": "",
                "enqueued_at": time.perf_counter(),
            }

        eid = meta["eventId"]
        if err is not None:
            self._metrics.mark_failed(eid)
            self._evidence.try_enqueue(
                FailedEvidence(
                    eventId=eid,
                    simulationRunId=str(meta["simulationRunId"]),
                    cycleSequence=int(meta["cycleSequence"]),
                    entityId=str(meta.get("entityId") or ""),
                    reason=str(err),
                    permanent=False,
                )
            )
            self._set_state(ProducerState.DEGRADED)
            return

        latency_ms = (time.perf_counter() - float(meta["enqueued_at"])) * 1000.0
        self._metrics.mark_acked(
            eid,
            cycle_sequence=int(meta["cycleSequence"]),
            latency_ms=latency_ms,
        )
        partition = msg.partition() if msg is not None else -1
        offset = msg.offset() if msg is not None else -1
        self._evidence.try_enqueue(
            AckEvidence(
                eventId=eid,
                simulationRunId=str(meta["simulationRunId"]),
                cycleSequence=int(meta["cycleSequence"]),
                entityId=str(meta.get("entityId") or ""),
                topic=self.topic,
                partition=int(partition) if partition is not None else -1,
                offset=int(offset) if offset is not None else -1,
                ackLatencyMs=latency_ms,
            )
        )
        if self.state == ProducerState.DEGRADED and self._metrics.pending_count() == 0:
            self._set_state(ProducerState.READY)

    def _on_error(self, err: Any) -> None:
        log.error("kafka error_cb: %s", err)
        # Prolonged unavailability → FAULTED (K-0.5); mark degraded first
        self._set_state(ProducerState.DEGRADED)
        self._fault_message = str(err)

    def _poll_loop(self) -> None:
        while not self._stop_poll.is_set():
            try:
                if self._producer is not None:
                    self._producer.poll(self.poll_interval_sec)
                else:
                    time.sleep(self.poll_interval_sec)
            except Exception:
                log.exception("kafka poll loop error")
                self._set_state(ProducerState.FAULTED)
                break
        # drain
        try:
            if self._producer is not None:
                self._producer.poll(0)
        except Exception:
            pass

    def health(self) -> dict[str, Any]:
        poll_alive = self._poll_thread is not None and self._poll_thread.is_alive()
        if (
            self._accepting
            and self._poll_thread is not None
            and not poll_alive
            and self.state not in (ProducerState.STOPPING, ProducerState.STOPPED)
        ):
            self._fault_message = "poll thread dead"
            self._set_state(ProducerState.FAULTED)
        snap = self._metrics.snapshot(producer_state=self.state.value)
        return {
            **snap.to_dict(),
            "poll_thread_alive": poll_alive,
            "invariant_ok": self._metrics.invariant_ok(),
            "fault_message": self._fault_message,
            "enabled": self.enabled,
        }
