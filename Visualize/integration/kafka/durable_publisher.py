"""Durable Kafka publisher (K-2b / K-5): TraCI appends to SQLite outbox; worker delivers."""
from __future__ import annotations

import copy
import json
import logging
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from integration.kafka.event_mapper import DEFAULT_PRODUCER_ID, build_cycle_events
from integration.kafka.event_validator import validate_cycle_events
from integration.kafka.outbox_store import (
    KafkaOutboxStore,
    OutboxAppendError,
    events_to_outbox_rows,
    payload_hash_bytes,
    run_started_event_id,
)
from integration.kafka.outbox_worker import OutboxDeliveryWorker
from integration.kafka.producer import TOPIC_MAIN

log = logging.getLogger(__name__)

RUN_STARTED_ACK_TIMEOUT_SEC = 30.0
RUN_STARTED_ACK_POLL_SEC = 0.05


class DurablePublisherState(str, Enum):
    STARTING = "STARTING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class DurableKafkaPublisher:
    """TraCI-facing API: validate → cycle TX OUTBOXED; delivery is async via worker."""

    def __init__(
        self,
        *,
        db_path: Path,
        bootstrap_servers: str = "localhost:29092",
        topic: str = TOPIC_MAIN,
        client_id: str = "visualize-kafka-outbox",
        producer_id: str = DEFAULT_PRODUCER_ID,
        producer_session_id: Optional[str] = None,
        linger_ms: int = 10,
        delivery_timeout_ms: int = 30_000,
        request_timeout_ms: int = 10_000,
        max_in_flight: int = 32,
        acked_retention_days: int = 7,
        producer_factory: Optional[Callable[[dict], Any]] = None,
        disk_warn_free_bytes: int = 512 * 1024 * 1024,
        disk_fault_free_bytes: int = 64 * 1024 * 1024,
        enabled: bool = True,
    ) -> None:
        self.enabled = bool(enabled)
        self.topic = topic
        self.producer_id = producer_id
        self.producer_session_id = producer_session_id or str(uuid.uuid4())
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.linger_ms = linger_ms
        self.delivery_timeout_ms = delivery_timeout_ms
        self.request_timeout_ms = request_timeout_ms
        self.max_in_flight = max_in_flight
        self.acked_retention_days = acked_retention_days
        self._producer_factory = producer_factory
        self._state = DurablePublisherState.STARTING
        self._fault_message: Optional[str] = None
        self.store = KafkaOutboxStore(
            db_path,
            disk_warn_free_bytes=disk_warn_free_bytes,
            disk_fault_free_bytes=disk_fault_free_bytes,
        )
        self._producer: Any = None
        self._worker: Optional[OutboxDeliveryWorker] = None
        self.last_append_ms: float = 0.0
        self.cycles_outboxed: int = 0
        self.cycles_failed: int = 0
        self._run_started_acked_runs: set[str] = set()

    @property
    def state(self) -> DurablePublisherState:
        if self.store.is_faulted:
            return DurablePublisherState.FAULTED
        if self.store.is_degraded and self._state == DurablePublisherState.READY:
            return DurablePublisherState.DEGRADED
        return self._state

    @property
    def is_faulted(self) -> bool:
        return self.state == DurablePublisherState.FAULTED

    def start(self) -> None:
        if not self.enabled:
            self._state = DurablePublisherState.STOPPED
            return
        self._state = DurablePublisherState.STARTING
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": self.linger_ms,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "max.in.flight.requests.per.connection": 5,
            "retries": 2147483647,
        }
        factory = self._producer_factory
        if factory is None:
            from confluent_kafka import Producer  # type: ignore

            factory = Producer
        try:
            self._producer = factory(conf)
        except Exception as e:
            self._fault_message = str(e)
            self.store._faulted = True
            self.store._fault_message = str(e)
            self._state = DurablePublisherState.FAULTED
            log.exception("durable kafka producer construct failed")
            return

        self._worker = OutboxDeliveryWorker(
            self.store,
            producer=self._producer,
            max_in_flight=self.max_in_flight,
            acked_retention_days=self.acked_retention_days,
        )
        self._worker.start()
        self._state = DurablePublisherState.READY
        log.info("DurableKafkaPublisher READY db=%s topic=%s", self.store.db_path, self.topic)

    def _wait_run_started_acked(self, simulation_run_id: str) -> None:
        if simulation_run_id in self._run_started_acked_runs:
            return
        deadline = time.monotonic() + RUN_STARTED_ACK_TIMEOUT_SEC
        while time.monotonic() < deadline:
            if self.store.is_run_started_acked(simulation_run_id):
                self._run_started_acked_runs.add(simulation_run_id)
                return
            time.sleep(RUN_STARTED_ACK_POLL_SEC)
        raise OutboxAppendError(
            f"RunStarted not broker-ACKed within {RUN_STARTED_ACK_TIMEOUT_SEC}s run={simulation_run_id}"
        )

    def _append_run_started_durable(self, simulation_run_id: str) -> None:
        from integration.kafka.event_mapper import (
            build_run_started_event,
            run_started_partition_key,
        )

        if simulation_run_id in self._run_started_acked_runs:
            return
        event = build_run_started_event(
            simulation_run_id=simulation_run_id,
            producer_session_id=self.producer_session_id,
            producer_id=self.producer_id,
        )
        payload = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
        eid = run_started_event_id(
            producer_session_id=self.producer_session_id,
            simulation_run_id=simulation_run_id,
        )
        key = run_started_partition_key(simulation_run_id)
        self.store.append_run_started(
            simulation_run_id=simulation_run_id,
            producer_session_id=self.producer_session_id,
            topic=self.topic,
            payload_json=payload,
            event_key=key,
            event_id=eid,
            payload_hash=payload_hash_bytes(payload),
        )
        log.info(
            "outboxed TrafficSimulationRunStarted run=%s session=%s",
            simulation_run_id,
            self.producer_session_id,
        )
        self._wait_run_started_acked(simulation_run_id)

    def stop(self, flush_timeout_sec: float = 10.0) -> dict:
        self._state = DurablePublisherState.STOPPING
        if self._worker is not None:
            self._worker.stop(timeout=flush_timeout_sec)
        health = self.health()
        self.store.close()
        self._state = DurablePublisherState.STOPPED
        return health

    def append_cycle(
        self,
        entities: Sequence[dict[str, Any]],
        *,
        cycle_sequence: int,
        captured_at: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> int:
        """Build+validate events, COMMIT whole cycle as OUTBOXED. Returns N or raises."""
        if not self.enabled:
            return 0
        if self.is_faulted:
            raise OutboxAppendError(self._fault_message or "publisher faulted")

        frozen = [copy.deepcopy(e) for e in entities]
        events = build_cycle_events(
            frozen,
            cycle_sequence=cycle_sequence,
            captured_at=captured_at,
            producer_id=self.producer_id,
            producer_session_id=self.producer_session_id,
            trace_id=trace_id,
        )
        vr = validate_cycle_events(events)
        if not vr.ok:
            self.cycles_failed += 1
            raise OutboxAppendError(f"FAILED_PERMANENT validate: {vr.reason}")

        real_run = str(events[0]["simulationRunId"])
        if real_run not in self._run_started_acked_runs:
            self._append_run_started_durable(real_run)

        rows = events_to_outbox_rows(events, topic=self.topic)
        try:
            ms = self.store.append_cycle(rows)
            self.last_append_ms = ms
            self.cycles_outboxed += 1
            if ms > 100.0:
                log.warning("outbox append spike ms=%.2f cycle=%d", ms, cycle_sequence)
            return len(rows)
        except OutboxAppendError:
            self.cycles_failed += 1
            self._state = DurablePublisherState.FAULTED
            raise

    def health(self) -> dict:
        h = self.store.capacity_metrics()
        if self._worker is not None:
            h.update({k: v for k, v in self._worker.health().items() if k not in h})
        h["state"] = self.state.value
        h["cycles_outboxed"] = self.cycles_outboxed
        h["cycles_failed"] = self.cycles_failed
        h["last_append_ms"] = self.last_append_ms
        h["fault_message"] = self._fault_message or self.store.fault_message
        return h
