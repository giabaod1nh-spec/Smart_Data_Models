"""K-4 Raw Kafka consumer core loop."""
from __future__ import annotations

import logging
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from de.kafka_raw import MIGRATION_VERSION, SCHEMA_VERSION
from de.kafka_raw.batch_buffer import BatchBufferManager, BufferedRecord
from de.kafka_raw.clickhouse_repository import ClickHouseRawRepository
from de.kafka_raw.config import KafkaRawSettings
from de.kafka_raw.ledger_store import (
    STATUS_QUARANTINED,
    STATUS_STORED,
    LedgerStore,
)
from de.kafka_raw.metrics import Metrics
from de.kafka_raw.offset_tracker import OffsetTracker
from de.kafka_raw.validator import EventValidator

log = logging.getLogger(__name__)


class ConsumerState(str, Enum):
    STARTING = "STARTING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    PAUSED = "PAUSED"
    FAULTED = "FAULTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class RawConsumerFault(Exception):
    pass


class RawKafkaConsumer:
    def __init__(
        self,
        settings: KafkaRawSettings,
        *,
        repo: Optional[ClickHouseRawRepository] = None,
        ledger: Optional[LedgerStore] = None,
        validator: Optional[EventValidator] = None,
        consumer_factory: Optional[Callable[[dict], Any]] = None,
    ) -> None:
        self.settings = settings
        self.repo = repo or ClickHouseRawRepository(settings)
        self.ledger = ledger or LedgerStore(Path(settings.ledger_path))
        self.validator = validator or EventValidator(
            Path(settings.entity_schema_path),
            Path(settings.run_started_schema_path),
        )
        self.buffers = BatchBufferManager(
            batch_size=settings.batch_size,
            flush_ms=settings.flush_ms,
            max_buffered_records=settings.max_buffered_records,
            max_buffered_bytes=settings.max_buffered_bytes,
        )
        self.offsets = OffsetTracker()
        self.metrics = Metrics()
        self.state = ConsumerState.STARTING
        self._consumer_factory = consumer_factory
        self._consumer: Any = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._assigned: Set[Tuple[str, int]] = set()
        self._paused = False
        self.migrations_ok = False
        self.schemas_ok = False
        self._last_watermark_sample = 0.0

    def start(self) -> None:
        self.state = ConsumerState.STARTING
        self.metrics.state = self.state.value
        self.validator.load()
        self.schemas_ok = self.validator.ready
        self.repo.connect()
        self.migrations_ok = self.repo.verify_tables()
        if not self.migrations_ok:
            self.state = ConsumerState.FAULTED
            self.metrics.state = self.state.value
            self.metrics.fault_message = "CH tables missing — run de-migrate first"
            raise RawConsumerFault(self.metrics.fault_message)

        conf = {
            "bootstrap.servers": self.settings.bootstrap_servers,
            "group.id": self.settings.group_id,
            "client.id": self.settings.client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": self.settings.max_poll_interval_ms,
            "session.timeout.ms": self.settings.session_timeout_ms,
            "heartbeat.interval.ms": self.settings.heartbeat_interval_ms,
            "max.poll.records": self.settings.max_poll_records,
        }
        factory = self._consumer_factory
        if factory is None:
            from confluent_kafka import Consumer  # type: ignore

            factory = Consumer
        self._consumer = factory(conf)
        self._consumer.subscribe(
            [self.settings.topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="k4-raw-consumer", daemon=True
        )
        self._thread.start()
        self.state = ConsumerState.READY
        self.metrics.state = self.state.value
        log.info(
            "RawKafkaConsumer READY group=%s client=%s",
            self.settings.group_id,
            self.settings.client_id,
        )

    def stop(self, timeout: float = 10.0) -> None:
        self.state = ConsumerState.STOPPING
        self.metrics.state = self.state.value
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        try:
            self._flush_all(force=True)
        except Exception:
            log.exception("final flush failed")
        if self._consumer:
            self._consumer.close()
        self.repo.close()
        self.ledger.close()
        self.state = ConsumerState.STOPPED
        self.metrics.state = self.state.value

    def _on_assign(self, consumer: Any, partitions: list) -> None:
        for tp in partitions:
            self._assigned.add((tp.topic, tp.partition))
            if isinstance(getattr(tp, "offset", None), int) and tp.offset >= 0:
                self.offsets.load_committed_record(tp.topic, tp.partition, tp.offset - 1)
        consumer.assign(partitions)
        log.info("assigned %s", [(p.topic, p.partition) for p in partitions])

    def _on_revoke(self, consumer: Any, partitions: list) -> None:
        deadline = time.monotonic() + (
            self.settings.rebalance_flush_timeout_ms / 1000.0
        )
        for tp in partitions:
            key = (tp.topic, tp.partition)
            try:
                if time.monotonic() < deadline:
                    self._flush_tp(tp.topic, tp.partition, "RAW")
                    self._flush_tp(tp.topic, tp.partition, "QUARANTINE")
                    self._commit_tp(tp.topic, tp.partition)
                else:
                    self.buffers.discard_partition(tp.topic, tp.partition)
            except Exception:
                log.exception("revoke flush failed %s", key)
                self.buffers.discard_partition(tp.topic, tp.partition)
            self._assigned.discard(key)
        log.info("revoked %s", [(p.topic, p.partition) for p in partitions])

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.state == ConsumerState.FAULTED:
                    time.sleep(0.5)
                    # still poll for heartbeat if consumer exists
                    if self._consumer:
                        self._consumer.poll(0.1)
                    continue
                self._maybe_resume_or_pause()
                self._maybe_refresh_partition_offsets()
                for dest in ("RAW", "QUARANTINE"):
                    for key in list(self.buffers.ready_keys(dest)):
                        self._flush_tp(key[0], key[1], dest)
                        self._commit_tp(key[0], key[1])
                msg = self._consumer.poll(0.2)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("kafka error: %s", msg.error())
                    continue
                self._handle_msg(msg)
            except RawConsumerFault as e:
                self.state = ConsumerState.FAULTED
                self.metrics.state = self.state.value
                self.metrics.fault_message = str(e)
                log.error("FAULTED: %s", e)
            except Exception:
                log.exception("consumer loop error")
                self.state = ConsumerState.DEGRADED
                self.metrics.state = self.state.value
                time.sleep(0.5)

    def _handle_msg(self, msg: Any) -> None:
        value = msg.value() or b""
        headers = msg.headers()
        ts_type, ts_ms = msg.timestamp()
        ts_name = {0: "NotAvailable", 1: "CreateTime", 2: "LogAppendTime"}.get(
            ts_type, "NotAvailable"
        )
        classified = self.validator.classify(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            value=value,
            kafka_key=msg.key(),
            headers=headers,
            broker_timestamp_ms=ts_ms if ts_ms and ts_ms > 0 else None,
            broker_timestamp_type=ts_name,
        )
        rid = classified.row["raw_ingestion_id"]
        self.metrics.note_record()

        # Idempotent if ledger already complete
        led = self.ledger.get(msg.topic(), msg.partition(), msg.offset())
        if led and led["status"] in (STATUS_STORED, STATUS_QUARANTINED):
            if led.get("payload_hash") and led["payload_hash"] != classified.row[
                "payload_bytes_hash"
            ]:
                raise RawConsumerFault(
                    f"payload_bytes_hash mismatch ledger offset={msg.offset()}"
                )
            self.offsets.mark_completed(msg.topic(), msg.partition(), msg.offset())
            self._commit_tp(msg.topic(), msg.partition())
            return

        rec = BufferedRecord(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            raw_ingestion_id=rid,
            destination=classified.destination,
            row=classified.row,
            size_bytes=int(classified.row["payload_size_bytes"]),
        )
        self.buffers.add(rec)
        if self.buffers.should_pause():
            self._pause_partitions()

    def _flush_tp(self, topic: str, partition: int, destination: str) -> None:
        key = (topic, partition)
        records = self.buffers.pop(destination, key)
        if not records:
            return
        rows = [r.row for r in records]
        t0 = time.perf_counter()
        try:
            if destination == "RAW":
                self.repo.insert_raw(rows)
            else:
                self.repo.insert_quarantine(rows)
        except Exception:
            # ambiguous / fail — reconcile existence
            log.exception("CH insert failed dest=%s tp=%s", destination, key)
            ids = [r.raw_ingestion_id for r in records]
            found = self.repo.find_existing_ingestion_ids(ids)
            missing = [r for r in records if r.raw_ingestion_id not in found]
            for r in records:
                if r.raw_ingestion_id in found:
                    info = found[r.raw_ingestion_id]
                    if info["payload_bytes_hash"] != r.row["payload_bytes_hash"]:
                        raise RawConsumerFault("CH hash mismatch after failed insert")
                    status = (
                        STATUS_STORED
                        if info["destination"] == "RAW"
                        else STATUS_QUARANTINED
                    )
                    self.ledger.mark_complete(
                        topic=r.topic,
                        partition=r.partition,
                        offset=r.offset,
                        raw_ingestion_id=r.raw_ingestion_id,
                        destination=info["destination"],
                        status=status,
                        event_id=r.row.get("event_id"),
                        payload_hash=r.row["payload_bytes_hash"],
                    )
                    self.offsets.mark_completed(r.topic, r.partition, r.offset)
            if missing:
                # re-queue missing and pause
                for r in missing:
                    self.buffers.add(r)
                self.state = ConsumerState.PAUSED
                self.metrics.state = self.state.value
                self._pause_partitions()
                raise
            self.metrics.batch_insert_latency_ms = (time.perf_counter() - t0) * 1000
            return

        self.metrics.batch_insert_latency_ms = (time.perf_counter() - t0) * 1000
        status = STATUS_STORED if destination == "RAW" else STATUS_QUARANTINED
        for r in records:
            self.ledger.mark_complete(
                topic=r.topic,
                partition=r.partition,
                offset=r.offset,
                raw_ingestion_id=r.raw_ingestion_id,
                destination=destination,
                status=status,
                event_id=r.row.get("event_id"),
                payload_hash=r.row["payload_bytes_hash"],
            )
            self.offsets.mark_completed(r.topic, r.partition, r.offset)
            if destination == "RAW":
                self.metrics.records_stored += 1
            else:
                self.metrics.records_quarantined += 1
        if self.state == ConsumerState.PAUSED and not self.buffers.should_pause():
            self._resume_partitions()
            self.state = ConsumerState.READY
            self.metrics.state = self.state.value

    def _flush_all(self, force: bool = False) -> None:
        for dest in ("RAW", "QUARANTINE"):
            m = self.buffers.raw if dest == "RAW" else self.buffers.quarantine
            for key in list(m.keys()):
                if force or key in self.buffers.ready_keys(dest) or force:
                    self._flush_tp(key[0], key[1], dest)
                    self._commit_tp(key[0], key[1])

    def _commit_tp(self, topic: str, partition: int) -> None:
        n = self.offsets.contiguous_completed_record_offset(topic, partition)
        if n is None:
            return
        commit_pos = self.offsets.kafka_commit_offset(n)
        if self._consumer is None:
            self.offsets.advance_after_commit(topic, partition, n)
            self.metrics.note_commit()
            return
        from confluent_kafka import TopicPartition

        t0 = time.perf_counter()
        self._consumer.commit(
            offsets=[TopicPartition(topic, partition, commit_pos)],
            asynchronous=False,
        )
        self.metrics.commit_latency_ms = (time.perf_counter() - t0) * 1000
        self.offsets.advance_after_commit(topic, partition, n)
        self.metrics.note_commit()

    def _pause_partitions(self) -> None:
        if self._consumer is None or self._paused:
            return
        from confluent_kafka import TopicPartition

        tps = [TopicPartition(t, p) for t, p in self._assigned]
        if tps:
            self._consumer.pause(tps)
            self._paused = True
            self.state = ConsumerState.PAUSED
            self.metrics.state = self.state.value

    def _resume_partitions(self) -> None:
        if self._consumer is None or not self._paused:
            return
        from confluent_kafka import TopicPartition

        tps = [TopicPartition(t, p) for t, p in self._assigned]
        if tps:
            self._consumer.resume(tps)
        self._paused = False

    def _maybe_resume_or_pause(self) -> None:
        if self.buffers.should_pause():
            self._pause_partitions()
        elif self._paused and self.repo.ping():
            try:
                self._flush_all(force=True)
            except Exception:
                return
            self._resume_partitions()
            if self.state != ConsumerState.FAULTED:
                self.state = ConsumerState.READY
                self.metrics.state = self.state.value

    def health(self) -> Dict[str, Any]:
        snap = self.metrics.snapshot()
        assigned = bool(self._assigned)
        stale = False
        if assigned and snap["last_successful_commit_time"]:
            stale = (
                time.time() - float(snap["last_successful_commit_time"])
            ) > self.settings.commit_stale_sec
        elif assigned and snap["last_successful_commit_time"] is None:
            # allow grace after start — not stale until first window exceeded from start
            stale = False
        thread_alive = self._thread is not None and self._thread.is_alive()
        ready = (
            self.migrations_ok
            and self.schemas_ok
            and self.repo.ping()
            and thread_alive
            and assigned
            and self.state != ConsumerState.FAULTED
            and not stale
        )
        partition_offsets = snap.get("partition_offsets", [])
        cutover_ready = bool(partition_offsets) and all(
            p.get("lag") is not None
            and int(p["lag"]) <= self.settings.cutover_max_lag
            and not p.get("commit_ahead_of_durable", False)
            for p in partition_offsets
        )
        return {
            **snap,
            "ready": ready,
            "migrations_ok": self.migrations_ok,
            "schemas_ok": self.schemas_ok,
            "partitions_assigned": assigned,
            "assigned": sorted(self._assigned),
            "consumer_thread_alive": thread_alive,
            "schema_version": SCHEMA_VERSION,
            "migration_version": MIGRATION_VERSION,
            "commit_stale": stale,
            "client_id": self.settings.client_id,
            "group_id": self.settings.group_id,
            "partition_offsets": partition_offsets,
            "cutover_max_lag": self.settings.cutover_max_lag,
            "cutover_ready": ready and cutover_ready,
        }

    def _maybe_refresh_partition_offsets(self) -> None:
        """Sample broker positions on the consumer thread; health reads the cache."""
        if self._consumer is None or not self._assigned:
            return
        now = time.monotonic()
        if now - self._last_watermark_sample < self.settings.watermark_sample_interval_sec:
            return
        self._last_watermark_sample = now
        result: List[Dict[str, Any]] = []
        from confluent_kafka import TopicPartition
        for topic, partition in sorted(self._assigned):
            runtime_record = self.offsets.last_committed_record_offset(topic, partition)
            ledger_record = self.ledger.max_completed_offset(topic, partition)
            durable_record = max(
                (v for v in (runtime_record, ledger_record) if v is not None),
                default=None,
            )
            durable_next = None if durable_record is None else durable_record + 1
            row: Dict[str, Any] = {
                "topic": topic,
                "partition": partition,
                "durable_contiguous_record_offset": durable_record,
                "durable_next_offset": durable_next,
            }
            try:
                low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=1.0, cached=False
                )
                committed = self._consumer.committed(
                    [TopicPartition(topic, partition)], timeout=1.0
                )[0].offset
                committed_next = int(committed) if int(committed) >= 0 else None
                row.update(
                    {
                        "broker_low_watermark": int(low),
                        "broker_high_watermark": int(high),
                        "lag": None
                        if durable_next is None
                        else max(0, int(high) - durable_next),
                        "broker_committed_next_offset": committed_next,
                        "commit_ahead_of_durable": committed_next is not None
                        and (durable_next is None or committed_next > durable_next),
                    }
                )
            except Exception as exc:
                row.update({"lag": None, "watermark_error": str(exc)})
            result.append(row)
        self.metrics.note_partition_offsets(result)
