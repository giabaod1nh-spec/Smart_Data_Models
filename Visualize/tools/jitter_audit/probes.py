"""Runtime monkeypatches for jitter audit (tools-only, no production edits)."""
from __future__ import annotations

import copy
import logging
import sqlite3
import time
from typing import Any, Callable, Optional

from tools.jitter_audit.recorder import JitterRecorder, get_recorder, _ns_ms


_PATCH_STATE: dict[str, Any] = {"installed": False, "orig": {}}


def _rec() -> Optional[JitterRecorder]:
    return get_recorder()


def install_probes(*, noop_outbox: bool = False, disable_worker: bool = False) -> None:
    if _PATCH_STATE["installed"]:
        return
    _PATCH_STATE["installed"] = True
    _PATCH_STATE["noop_outbox"] = noop_outbox
    _PATCH_STATE["disable_worker"] = disable_worker

    import app.traci_runner as tr
    from integration.kafka import outbox_store as obs_mod
    from integration.kafka import durable_publisher as dp_mod
    from integration.kafka import event_mapper as em_mod
    from integration.kafka.outbox_store import KafkaOutboxStore
    from integration.orion import publish_cycle as pc_mod
    from simulation.backend import SumoBackend

    _PATCH_STATE["orig"]["step"] = SumoBackend.step
    _PATCH_STATE["orig"]["capture_entity_list"] = tr.capture_entity_list
    _PATCH_STATE["orig"]["fanout"] = tr.fanout_publish_cycle
    _PATCH_STATE["orig"]["sleep_pace"] = tr._sleep_to_realtime_pace
    _PATCH_STATE["orig"]["append_cycle"] = KafkaOutboxStore.append_cycle
    _PATCH_STATE["orig"]["write_batch"] = KafkaOutboxStore._write_batch
    _PATCH_STATE["orig"]["checkpoint"] = KafkaOutboxStore.checkpoint_wal
    _PATCH_STATE["orig"]["build_cycle_events"] = em_mod.build_cycle_events
    _PATCH_STATE["orig"]["entity_payload_hash"] = em_mod.entity_payload_hash
    _PATCH_STATE["orig"]["events_to_outbox_rows"] = obs_mod.events_to_outbox_rows
    _PATCH_STATE["orig"]["priority"] = None
    _PATCH_STATE["orig"]["background"] = None
    import contracts.canonical_json as cj_mod

    _PATCH_STATE["orig"]["entity_payload_hash"] = cj_mod.entity_payload_hash

    _PATCH_STATE["orig"]["worker_start"] = None
    _PATCH_STATE["orig"]["logging_emit"] = logging.Handler.emit

    if disable_worker:
        from integration.kafka.outbox_worker import OutboxDeliveryWorker

        _PATCH_STATE["orig"]["worker_start"] = OutboxDeliveryWorker.start

        def _no_worker_start(self, *a, **k):
            return 0

        OutboxDeliveryWorker.start = _no_worker_start  # type: ignore[method-assign]

    # --- logging emit timing (aggregate per iteration) ---
    def patched_emit(self, record):
        rec = _rec()
        t0 = time.perf_counter_ns()
        try:
            return _PATCH_STATE["orig"]["logging_emit"](self, record)
        finally:
            if rec is not None:
                rec._pending_logging_ms = getattr(rec, "_pending_logging_ms", 0.0) + _ns_ms(t0)

    logging.Handler.emit = patched_emit  # type: ignore[method-assign]

    # --- hash / envelope build ---
    def patched_entity_payload_hash(entity):
        rec = _rec()
        t0 = time.perf_counter_ns()
        out = _PATCH_STATE["orig"]["entity_payload_hash"](entity)
        if rec is not None:
            rec._pending_publish = getattr(rec, "_pending_publish", {})
            rec._pending_publish["canonical_hash_ms"] = (
                rec._pending_publish.get("canonical_hash_ms", 0.0) + _ns_ms(t0)
            )
        return out

    cj_mod.entity_payload_hash = patched_entity_payload_hash  # type: ignore

    def patched_build_cycle_events(*a, **k):
        rec = _rec()
        t0 = time.perf_counter_ns()
        out = _PATCH_STATE["orig"]["build_cycle_events"](*a, **k)
        if rec is not None:
            rec._pending_publish = getattr(rec, "_pending_publish", {})
            rec._pending_publish["event_envelope_build_ms"] = (
                rec._pending_publish.get("event_envelope_build_ms", 0.0) + _ns_ms(t0)
            )
        return out

    em_mod.build_cycle_events = patched_build_cycle_events  # type: ignore

    def patched_events_to_outbox_rows(*a, **k):
        rec = _rec()
        t0 = time.perf_counter_ns()
        out = _PATCH_STATE["orig"]["events_to_outbox_rows"](*a, **k)
        if rec is not None:
            rec._pending_publish = getattr(rec, "_pending_publish", {})
            rec._pending_publish["json_serialize_ms"] = (
                rec._pending_publish.get("json_serialize_ms", 0.0) + _ns_ms(t0)
            )
        return out

    obs_mod.events_to_outbox_rows = patched_events_to_outbox_rows  # type: ignore

    # --- outbox append ---
    def patched_append_cycle(self, rows, *a, **k):
        rec = _rec()
        if _PATCH_STATE.get("noop_outbox"):
            if rec is not None:
                rec._pending_outbox = {
                    "outbox_append_total_ms": 0.0,
                    "outbox_lock_wait_ms": 0.0,
                    "outbox_begin_tx_ms": 0.0,
                    "outbox_insert_rows_ms": 0.0,
                    "outbox_commit_ms": 0.0,
                    "sqlite_op": "noop",
                }
            return 0.0

        total_t0 = time.perf_counter_ns()
        lock_wait = 0.0
        begin_ms = 0.0
        insert_ms = 0.0
        commit_ms = 0.0
        conn = self._conn
        now_iso = obs_mod._utc_now_iso()
        params = [
            (
                r.event_id,
                r.simulation_run_id,
                int(r.cycle_sequence),
                int(r.entity_sequence),
                r.event_key,
                r.topic,
                r.payload_json,
                r.payload_hash,
                obs_mod.STATUS_OUTBOXED,
                now_iso,
                now_iso,
            )
            for r in rows
        ]
        try:
            gate_wait_t0 = time.perf_counter_ns()
            with self._write_gate.priority():
                lock_wait += _ns_ms(gate_wait_t0)
                t_begin = time.perf_counter_ns()
                conn.execute("BEGIN IMMEDIATE")
                begin_ms = _ns_ms(t_begin)
                t_ins = time.perf_counter_ns()
                conn.executemany(
                    """
                    INSERT INTO kafka_outbox (
                        event_id, simulation_run_id, cycle_sequence, entity_sequence,
                        event_key, topic, payload_json, payload_hash,
                        status, attempt_count, next_retry_at, last_error,
                        kafka_partition, kafka_offset,
                        created_at, queued_at, acked_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, NULL, NULL, ?)
                    """,
                    params,
                )
                insert_ms = _ns_ms(t_ins)
                t_commit = time.perf_counter_ns()
                conn.execute("COMMIT")
                commit_ms = _ns_ms(t_commit)
        except sqlite3.IntegrityError as e:
            self._rollback_quiet(conn)
            raise obs_mod.OutboxDuplicateError(str(e)) from e
        except Exception as e:
            self._rollback_quiet(conn)
            raise obs_mod.OutboxAppendError(str(e)) from e

        total_ms = _ns_ms(total_t0)
        if rec is not None:
            rec.outbox_stats["worker_tx_count"] = rec.outbox_stats.get("worker_tx_count", 0) + 1
            rec._pending_outbox = {
                "outbox_lock_wait_ms": lock_wait,
                "outbox_begin_tx_ms": begin_ms,
                "outbox_insert_rows_ms": insert_ms,
                "outbox_commit_ms": commit_ms,
                "outbox_append_total_ms": total_ms,
                "sqlite_op": "append_cycle",
                "outbox_lock_owner": "traci",
            }
        return total_ms

    KafkaOutboxStore.append_cycle = patched_append_cycle  # type: ignore

    def patched_write_batch(self, sql, params):
        rec = _rec()
        t0 = time.perf_counter_ns()
        n = _PATCH_STATE["orig"]["write_batch"](self, sql, params)
        ms = _ns_ms(t0)
        if rec is not None:
            rec.outbox_stats["worker_tx_count"] = rec.outbox_stats.get("worker_tx_count", 0) + 1
            if "mark_queued" in sql or "QUEUED" in sql:
                rec.outbox_stats["mark_queued_batch_rows"] = (
                    rec.outbox_stats.get("mark_queued_batch_rows", 0) + len(params)
                )
            if "ACKED" in sql or "acked_at" in sql:
                rec.outbox_stats["mark_acked_batch_rows"] = (
                    rec.outbox_stats.get("mark_acked_batch_rows", 0) + len(params)
                )
            rec._pending_outbox = getattr(rec, "_pending_outbox", {})
            rec._pending_outbox.setdefault("worker_batch_ms", 0.0)
            rec._pending_outbox["worker_batch_ms"] += ms
            rec._pending_outbox["sqlite_op"] = "worker_batch"
            rec._pending_outbox["outbox_lock_owner"] = "worker"
        return n

    KafkaOutboxStore._write_batch = patched_write_batch  # type: ignore

    def patched_checkpoint(self, mode="PASSIVE"):
        rec = _rec()
        t0 = time.perf_counter_ns()
        _PATCH_STATE["orig"]["checkpoint"](self, mode)
        ms = _ns_ms(t0)
        if rec is not None:
            rec.outbox_stats["checkpoint_count"] = rec.outbox_stats.get("checkpoint_count", 0) + 1
            rec.outbox_stats["checkpoint_ms"] = rec.outbox_stats.get("checkpoint_ms", 0.0) + ms

    KafkaOutboxStore.checkpoint_wal = patched_checkpoint  # type: ignore

    # --- capture / fanout ---
    def patched_capture_entity_list(backend, build_all_entities):
        rec = _rec()
        cap_ms = 0.0
        map_ms = 0.0
        nodes = list(backend.publish_nodes)
        all_entities = []
        for node in nodes:
            t0 = time.perf_counter_ns()
            snapshot = backend.get_snapshot(node, fresh=False)
            cap_ms += _ns_ms(t0)
            t1 = time.perf_counter_ns()
            all_entities.extend(build_all_entities(node, snapshot))
            map_ms += _ns_ms(t1)
        if rec is not None:
            rec._pending_publish = getattr(rec, "_pending_publish", {})
            rec._pending_publish["snapshot_capture_ms"] = cap_ms
            rec._pending_publish["entity_mapping_ms"] = map_ms
        return nodes, all_entities

    tr.capture_entity_list = patched_capture_entity_list  # type: ignore

    def patched_fanout(backend, build_all_entities, sequence_number, **kw):
        rec = _rec()
        dc_t0 = time.perf_counter_ns()
        nodes, entities = patched_capture_entity_list(backend, build_all_entities)
        orion_entities = copy.deepcopy(entities)
        kafka_entities = copy.deepcopy(entities)
        dc_ms = _ns_ms(dc_t0) - (
            (rec._pending_publish.get("snapshot_capture_ms", 0.0) if rec else 0.0)
            + (rec._pending_publish.get("entity_mapping_ms", 0.0) if rec else 0.0)
        )
        if rec is not None:
            rec._pending_publish["deepcopy_ms"] = max(0.0, dc_ms)
        t_pc = time.perf_counter_ns()
        cycle = pc_mod.PublishCycle.from_entities(
            sequence_number=sequence_number,
            nodes=nodes,
            entities=orion_entities,
        )
        if rec is not None:
            rec._pending_publish["publish_cycle_build_ms"] = _ns_ms(t_pc)
            rec._pending_iteration = getattr(rec, "_pending_iteration", {})
            rec._pending_iteration["cycle_sequence"] = sequence_number
            rec._pending_iteration["phase"] = "publish"
        kafka_outbox = kw.get("kafka_outbox")
        kafka_producer = kw.get("kafka_producer")
        if kafka_outbox is not None:
            kafka_outbox.append_cycle(kafka_entities, cycle_sequence=sequence_number)
        elif kafka_producer is not None:
            kafka_producer.publish_cycle(kafka_entities, cycle_sequence=sequence_number)
        return cycle

    tr.fanout_publish_cycle = patched_fanout  # type: ignore

    # --- realtime sleep ---
    def patched_sleep_pace(**kwargs):
        rec = _rec()
        t0 = time.perf_counter_ns()
        slept = _PATCH_STATE["orig"]["sleep_pace"](**kwargs)
        ms = _ns_ms(t0)
        if rec is not None:
            rec._pending_sleep_ms = getattr(rec, "_pending_sleep_ms", 0.0) + ms
        return slept

    tr._sleep_to_realtime_pace = patched_sleep_pace  # type: ignore

    # --- backend.step boundary ---
    def patched_step(self):
        rec = _rec()
        if rec is not None:
            _flush_pending_iteration(rec)
            rec.mark_loop_start()
        t0 = time.perf_counter_ns()
        out = _PATCH_STATE["orig"]["step"](self)
        if rec is not None:
            rec._pending_iteration = {
                "backend_step_ms": _ns_ms(t0),
                "sim_t": float(self.simulation_time_sec),
                "phase": "step",
            }
        return out

    SumoBackend.step = patched_step  # type: ignore


def _flush_pending_iteration(rec: JitterRecorder) -> None:
    pending = getattr(rec, "_pending_iteration", None)
    if not pending:
        return
    pub = getattr(rec, "_pending_publish", {}) or {}
    ob = getattr(rec, "_pending_outbox", {}) or {}
    rec.record_step(
        sim_t=float(pending.get("sim_t", 0.0)),
        backend_step_ms=float(pending.get("backend_step_ms", 0.0)),
        phase=str(pending.get("phase", "step")),
        cycle_sequence=pending.get("cycle_sequence"),
        publish=pub,
        outbox=ob,
        outbox_lock_owner=ob.get("outbox_lock_owner"),
        sqlite_op=ob.get("sqlite_op"),
        notes={
            "logging_ms": getattr(rec, "_pending_logging_ms", 0.0),
            "worker_batch_ms": ob.get("worker_batch_ms", 0.0),
        },
    )
    rec._pending_iteration = {}
    rec._pending_publish = {}
    rec._pending_outbox = {}
    rec._pending_logging_ms = 0.0


def finalize_recorder() -> None:
    rec = _rec()
    if rec is not None:
        _flush_pending_iteration(rec)
        rec.close()
