"""
traci_runner.py — SUMO TraCI runtime + optional Orion publish + Control API.

Async mode (default): capture full cycle → bounded FIFO → single worker.
Sync mode (--sync-publish): publish_once() blocks TraCI (debug / parity).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
# Repo root — shared `contracts` package (canonical JSON, architecture profiles)
_REPO_ROOT = _HERE.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import configuration.config as cfg
from simulation.backend import SumoBackend

log = logging.getLogger("traci_runner")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _perf_enabled() -> bool:
    return os.getenv("ORION_PERF_AUDIT", "").lower() in ("1", "true", "yes")


def _sleep_to_realtime_pace(
    *,
    anchor_wall: float,
    anchor_sim: float,
    sim_t: float,
    step_length: float,
    now: Optional[float] = None,
) -> float:
    """Hold wall clock ≈ sim time (1 s sim ↔ 1 s wall).

    Returns seconds slept. Skips sleep when work (TraCI step, capture, etc.)
    already consumed the budget for this sim instant — avoids the old fixed
    ``sleep(step_length)`` that added 10 ms on top of every step's work.
    """
    clock = now if now is not None else time.perf_counter()
    target_wall = anchor_wall + max(0.0, sim_t - anchor_sim)
    delay = target_wall - clock
    if delay > 0:
        # Cap a single sleep so a long hitch cannot stall the GUI for seconds;
        # subsequent fast steps catch up via the anchor.
        delay = min(delay, max(step_length * 4.0, 0.05))
        time.sleep(delay)
        return delay
    return 0.0


def publish_once(backend: SumoBackend, upsert_entity, build_all_entities) -> int:
    """Publish all publish_nodes synchronously. Never raises to caller."""
    total_ok = 0
    total_n = 0
    perf = _perf_enabled()
    if perf:
        from integration.orion.perf_probe import end_cycle, start_cycle

        start_cycle(backend.simulation_time_sec, len(backend.publish_nodes))
    try:
        for node in backend.publish_nodes:
            node_t0 = time.perf_counter()
            try:
                snap_t0 = time.perf_counter()
                snapshot = backend.get_snapshot(node, fresh=False)
                if perf:
                    log.info(
                        "timing label=snapshot node=%s duration_ms=%.2f",
                        node,
                        (time.perf_counter() - snap_t0) * 1000.0,
                    )
                map_t0 = time.perf_counter()
                entities = build_all_entities(node, snapshot)
                if perf:
                    log.info(
                        "timing label=mapper node=%s entity_count=%d duration_ms=%.2f",
                        node,
                        len(entities),
                        (time.perf_counter() - map_t0) * 1000.0,
                    )
                total_n += len(entities)
                for ent in entities:
                    try:
                        upsert_entity(ent)
                        total_ok += 1
                    except Exception as e:
                        log.error("Orion upsert failed for %s: %s", ent.get("id"), e)
                if perf:
                    from integration.orion.perf_probe import record_node_duration

                    record_node_duration(node, (time.perf_counter() - node_t0) * 1000.0)
                    log.info(
                        "node_publish_end node=%s duration_ms=%.2f entity_count=%d",
                        node,
                        (time.perf_counter() - node_t0) * 1000.0,
                        len(entities),
                    )
            except Exception as e:
                log.error("Publish failed for node=%s: %s", node, e)
        log.info(
            "Published %d/%d entities nodes=%s sim_t=%.1f",
            total_ok, total_n, backend.publish_nodes, backend.simulation_time_sec,
        )
        return total_ok
    except Exception as e:
        log.error("Publish cycle failed (simulation continues): %s", e, exc_info=True)
        return 0
    finally:
        if perf:
            from integration.orion.perf_probe import end_cycle

            end_cycle()


def capture_entity_list(
    backend: SumoBackend,
    build_all_entities,
):
    """Capture snapshots once (TraCI thread only). Returns (nodes, entities)."""
    all_entities = []
    nodes = list(backend.publish_nodes)
    for node in nodes:
        snapshot = backend.get_snapshot(node, fresh=False)
        all_entities.extend(build_all_entities(node, snapshot))
    return nodes, all_entities


def capture_publish_cycle(
    backend: SumoBackend,
    build_all_entities,
    sequence_number: int,
):
    """Capture snapshots + serialize immutable PublishCycle (TraCI thread only)."""
    from integration.orion.publish_cycle import CaptureValidationError, PublishCycle

    nodes, all_entities = capture_entity_list(backend, build_all_entities)
    try:
        return PublishCycle.from_entities(
            sequence_number=sequence_number,
            nodes=nodes,
            entities=all_entities,
        )
    except CaptureValidationError:
        raise


def fanout_publish_cycle(
    backend: SumoBackend,
    build_all_entities,
    sequence_number: int,
    *,
    kafka_producer=None,
    kafka_outbox=None,
):
    """Capture once → deepcopy/freeze → Orion PublishCycle + optional Kafka path.

    Kafka: prefer durable outbox (K-2b) over direct produce (K-2a).
    OutboxAppendError propagates so TraCI can FAULT/pause.
    """
    import copy

    from integration.orion.publish_cycle import CaptureValidationError, PublishCycle

    nodes, entities = capture_entity_list(backend, build_all_entities)
    orion_entities = copy.deepcopy(entities)
    kafka_entities = copy.deepcopy(entities)
    try:
        cycle = PublishCycle.from_entities(
            sequence_number=sequence_number,
            nodes=nodes,
            entities=orion_entities,
        )
    except CaptureValidationError:
        raise

    if kafka_outbox is not None:
        from integration.kafka.outbox_store import OutboxAppendError

        try:
            kafka_outbox.append_cycle(
                kafka_entities,
                cycle_sequence=sequence_number,
            )
        except OutboxAppendError:
            raise
        except Exception:
            log.exception(
                "Kafka outbox append failed seq=%d",
                sequence_number,
            )
            raise
    elif kafka_producer is not None:
        try:
            kafka_producer.publish_cycle(
                kafka_entities,
                cycle_sequence=sequence_number,
            )
        except Exception:
            log.exception(
                "Kafka publish_cycle failed seq=%d (Orion path continues)",
                sequence_number,
            )
    return cycle

def start_control_api(
    backend: SumoBackend,
    publisher=None,
    kafka_outbox=None,
    kafka_producer=None,
) -> None:
    import api.control_api as control_api
    import uvicorn
    from integration.orion.publish_gate import (
        init_orion_publish_enabled_from_env,
        is_orion_publish_enabled,
    )

    init_orion_publish_enabled_from_env()
    control_api.engine = backend
    if publisher is not None:
        control_api.publish_stats_provider = lambda: {
            "async_publish": True,
            "available": True,
            **publisher.metrics.snapshot().to_dict(),
            "state": publisher.state.value,
            # include TraCI pending via set_traci_pending
            "effective_depth": publisher.effective_depth(),
            "traci_pending": publisher.traci_pending() is not None,
            "orion_publish_enabled": is_orion_publish_enabled(),
        }
    else:
        control_api.publish_stats_provider = lambda: {
            "async_publish": False,
            "available": True,
            "orion_publish_enabled": is_orion_publish_enabled(),
        }

    def _kafka_stats():
        if kafka_outbox is not None:
            h = kafka_outbox.health() if hasattr(kafka_outbox, "health") else {}
            return {
                "available": True,
                "mode": "outbox",
                **(h if isinstance(h, dict) else {}),
            }
        if kafka_producer is not None:
            return {"available": True, "mode": "direct", **kafka_producer.health()}
        return {"available": False}

    control_api.kafka_stats_provider = _kafka_stats

    def _run():
        uvicorn.run(
            control_api.app,
            host="0.0.0.0",
            port=cfg.CONTROL_API_PORT,
            log_level="warning",
        )

    t = threading.Thread(target=_run, name="control-api", daemon=True)
    t.start()
    log.info(
        "Control API listening on :%d orion_publish_enabled=%s",
        cfg.CONTROL_API_PORT,
        is_orion_publish_enabled(),
    )


def _resolve_async_mode(args: argparse.Namespace) -> bool:
    if getattr(args, "sync_publish", False):
        return False
    if getattr(args, "async_publish", False):
        return True
    return bool(cfg.ORION_ASYNC_PUBLISH)


def run(args: argparse.Namespace) -> int:
    setup_logging(args.log_level or cfg.LOG_LEVEL)

    # Architecture profile validation (no-op when ARCHITECTURE_PROFILE=none)
    try:
        from contracts.architecture_profiles import (
            COMPONENT_PRODUCER,
            ProfileValidationError,
            validate_env,
        )
    except ImportError as e:
        log.warning("Architecture profile validation unavailable: %s", e)
        ProfileValidationError = None
        validate_env = None

    if validate_env is not None:
        env_map = dict(os.environ)
        env_map.setdefault(
            "ORION_PUBLISH_ENABLED",
            "true" if getattr(cfg, "ORION_PUBLISH_ENABLED", True) else "false",
        )
        env_map.setdefault(
            "KAFKA_OUTBOX_ENABLED",
            "true" if getattr(cfg, "KAFKA_OUTBOX_ENABLED", False) else "false",
        )
        env_map.setdefault(
            "PROJECTOR_SHADOW_MODE",
            "true" if getattr(cfg, "PROJECTOR_SHADOW_MODE", True) else "false",
        )
        env_map.setdefault(
            "PROJECTOR_TARGET_NAMESPACE",
            getattr(cfg, "PROJECTOR_TARGET_NAMESPACE", "shadow"),
        )
        env_map["ORION_SYNC_PUBLISH"] = (
            "true"
            if getattr(args, "sync_publish", False)
            or getattr(cfg, "ORION_SYNC_PUBLISH", False)
            else "false"
        )
        try:
            validate_env(
                getattr(cfg, "ARCHITECTURE_PROFILE", "none"),
                env_map,
                COMPONENT_PRODUCER,
            )
        except ProfileValidationError as e:
            log.error("Architecture profile validation failed: %s", e)
            return 2

    use_gui = args.gui if args.gui is not None else cfg.SUMO_GUI
    if args.no_gui:
        use_gui = False
    if args.gui_flag:
        use_gui = True

    publish_orion = not args.no_orion
    profile_name = str(getattr(cfg, "ARCHITECTURE_PROFILE", "none") or "none").strip().lower()
    if profile_name == "k5-cutover":
        publish_orion = False
    elif not getattr(cfg, "ORION_PUBLISH_ENABLED", True):
        publish_orion = False
    kafka_wanted = bool(
        getattr(cfg, "KAFKA_OUTBOX_ENABLED", False)
        or getattr(cfg, "KAFKA_PUBLISH_ENABLED", False)
    )
    use_async = _resolve_async_mode(args) if publish_orion else False
    # Sync path does not fanout Kafka — forbidden under locked profiles (validated above).
    if getattr(args, "sync_publish", False) and kafka_wanted:
        log.warning(
            "sync-publish selected while Kafka enabled: Kafka fanout is skipped "
            "(use async for dual-path; migration/final profiles forbid sync)"
        )

    build_all_entities = None
    upsert_entity = None
    publisher = None
    kafka_producer = None
    kafka_outbox = None
    pending_cycle = None
    cycle_sequence = 0
    backpressure_active = False
    pause_wall_start: Optional[float] = None

    # Entity mapper is shared capture for Orion and/or Kafka paths.
    if publish_orion or kafka_wanted:
        try:
            from integration.orion.entity_mapper import build_all_entities as _build

            build_all_entities = _build
        except ImportError as e:
            log.error("Cannot import entity_mapper: %s", e)
            return 2

    if publish_orion:
        try:
            from integration.orion.client import (
                reset_created_cache,
                upsert_entity as _upsert,
                wait_orion_ready as _wait,
            )

            upsert_entity = _upsert
            reset_created_cache()
        except ImportError as e:
            log.error("Cannot import Orion integration modules: %s", e)
            return 2
        try:
            _wait(retries=5, delay=2.0)
        except Exception as e:
            log.warning("Context Broker not ready (%s). Continuing.", e)

        if use_async:
            from integration.orion.async_publisher import AsyncOrionPublisher

            publisher = AsyncOrionPublisher(
                queue_size=cfg.ORION_PUBLISH_QUEUE_SIZE,
                retry_max=cfg.ORION_PUBLISH_RETRY_MAX,
                retry_base_sec=cfg.ORION_PUBLISH_RETRY_BASE_SEC,
                retry_slow_sec=cfg.ORION_PUBLISH_RETRY_SLOW_SEC,
                shutdown_timeout_sec=cfg.ORION_PUBLISH_SHUTDOWN_TIMEOUT_SEC,
                worker_count=cfg.ORION_PUBLISH_WORKER_COUNT,
                publish_mode=getattr(cfg, "ORION_PUBLISH_MODE", "sequential"),
            )
            publisher.start()

    # Kafka path is independent of direct Orion publisher (Architecture Lock F1).
    if kafka_wanted:
        if getattr(cfg, "KAFKA_OUTBOX_ENABLED", False):
            try:
                from integration.kafka.durable_publisher import DurableKafkaPublisher

                kafka_outbox = DurableKafkaPublisher(
                    db_path=cfg.KAFKA_OUTBOX_DB,
                    bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS,
                    topic=cfg.KAFKA_TOPIC,
                    client_id=f"{cfg.KAFKA_CLIENT_ID}-outbox",
                    producer_id=cfg.KAFKA_PRODUCER_ID,
                    linger_ms=cfg.KAFKA_LINGER_MS,
                    delivery_timeout_ms=cfg.KAFKA_DELIVERY_TIMEOUT_MS,
                    request_timeout_ms=cfg.KAFKA_REQUEST_TIMEOUT_MS,
                    max_in_flight=cfg.KAFKA_OUTBOX_MAX_IN_FLIGHT,
                    acked_retention_days=cfg.KAFKA_OUTBOX_ACKED_RETENTION_DAYS,
                    disk_warn_free_bytes=cfg.KAFKA_OUTBOX_DISK_WARN_BYTES,
                    disk_fault_free_bytes=cfg.KAFKA_OUTBOX_DISK_FAULT_BYTES,
                )
                kafka_outbox.start()
                log.info(
                    "Kafka durable outbox enabled db=%s bootstrap=%s state=%s",
                    cfg.KAFKA_OUTBOX_DB,
                    cfg.KAFKA_BOOTSTRAP_SERVERS,
                    kafka_outbox.state.value,
                )
            except Exception as e:
                log.error("Kafka outbox failed to start: %s", e, exc_info=True)
                if profile_name in ("k5-cutover", "final", "migration"):
                    log.error("Outbox mandatory under profile=%s — aborting", profile_name)
                    return 2
                kafka_outbox = None
        elif getattr(cfg, "KAFKA_PUBLISH_ENABLED", False):
            try:
                from integration.kafka.producer import AsyncKafkaProducer

                kafka_producer = AsyncKafkaProducer(
                    bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS,
                    topic=cfg.KAFKA_TOPIC,
                    client_id=cfg.KAFKA_CLIENT_ID,
                    producer_id=cfg.KAFKA_PRODUCER_ID,
                    evidence_root=cfg.KAFKA_EVIDENCE_ROOT,
                    simulation_run_id=f"session-{int(time.time())}",
                    linger_ms=cfg.KAFKA_LINGER_MS,
                    delivery_timeout_ms=cfg.KAFKA_DELIVERY_TIMEOUT_MS,
                    request_timeout_ms=cfg.KAFKA_REQUEST_TIMEOUT_MS,
                )
                kafka_producer.start()
                log.info(
                    "Kafka dual-publish enabled bootstrap=%s topic=%s state=%s",
                    cfg.KAFKA_BOOTSTRAP_SERVERS,
                    cfg.KAFKA_TOPIC,
                    kafka_producer.state.value,
                )
            except Exception as e:
                log.error("Kafka producer failed to start: %s", e, exc_info=True)
                kafka_producer = None

    nodes = cfg.PUBLISH_NODES
    if getattr(args, "nodes", None):
        nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]

    backend = SumoBackend(
        sumo_config=cfg.SUMO_CONFIG,
        use_gui=use_gui,
        publish_nodes=nodes,
        simulation_run_id=getattr(args, "simulation_run_id", None),
    )
    if getattr(args, "control_mode", None):
        backend.set_control_mode(args.control_mode)

    publish_interval = float(args.publish_interval or cfg.PUBLISH_INTERVAL)
    last_publish_sim_t = -1e9
    step_count = 0
    pace_realtime = use_gui and not getattr(args, "fast", False)
    if getattr(args, "realtime", False):
        pace_realtime = True

    demo = getattr(args, "demo", False)
    demo_applied = False
    last_step_wall = time.perf_counter()
    pace_anchor_wall: Optional[float] = None
    pace_anchor_sim: float = 0.0

    try:
        backend.start()
        if pace_realtime:
            pace_anchor_wall = time.perf_counter()
            pace_anchor_sim = float(backend.simulation_time_sec)
        if not args.no_api:
            try:
                start_control_api(
                    backend,
                    publisher=publisher,
                    kafka_outbox=kafka_outbox,
                    kafka_producer=kafka_producer,
                )
            except Exception as e:
                log.warning("Control API failed to start: %s", e)

        log.info(
            "Runtime: gui=%s realtime=%s orion=%s async=%s kafka=%s outbox=%s api=%s "
            "interval=%.2fs nodes=%s version=%s",
            use_gui,
            pace_realtime,
            publish_orion,
            use_async,
            kafka_producer is not None,
            kafka_outbox is not None,
            not args.no_api,
            publish_interval,
            backend.publish_nodes,
            cfg.VERSION,
        )

        while True:
            step_wall_start = time.perf_counter()
            sim_t = backend.simulation_time_sec

            # ── backpressure / lag gate (async, only when Orion gate ON) ─
            should_pause = False
            from integration.orion.publish_gate import is_orion_publish_enabled

            orion_gate_on = is_orion_publish_enabled()
            if (
                publish_orion
                and use_async
                and publisher is not None
                and orion_gate_on
            ):
                publisher.set_traci_pending(pending_cycle)
                lag = publisher.compute_realtime_lag(sim_t, pending_cycle)
                depth = publisher.effective_depth(pending_on_traci=pending_cycle is not None)
                age = publisher.oldest_pending_age_sec(pending_cycle)
                publisher.metrics.update(
                    effective_depth=depth,
                    queue_depth=publisher.queue_depth(),
                )

                if publisher.is_faulted or publisher.is_degraded:
                    should_pause = True
                elif pending_cycle is not None and publisher.queue_full():
                    should_pause = True
                elif pending_cycle is None and depth == 0:
                    # No Orion backlog — do not pause on lag alone (gate-off gaps
                    # inflate lag vs last_fully_published without work to drain).
                    should_pause = False
                elif lag > cfg.ORION_PUBLISH_MAX_LAG_SEC:
                    should_pause = True
                elif age > cfg.ORION_PUBLISH_MAX_LAG_SEC:
                    should_pause = True

                if (
                    depth >= cfg.ORION_PUBLISH_BACKPRESSURE_WARN_DEPTH
                    or lag > cfg.ORION_PUBLISH_RESUME_LAG_SEC
                ):
                    log.warning(
                        "publish_warn depth=%d lag_sim=%.2f age=%.2f state=%s",
                        depth,
                        lag,
                        age,
                        publisher.state.value,
                    )

                if backpressure_active:
                    # hysteresis resume
                    if (
                        not publisher.is_faulted
                        and not publisher.is_degraded
                        and pending_cycle is None
                        and (
                            depth == 0
                            or (
                                lag < cfg.ORION_PUBLISH_RESUME_LAG_SEC
                                and depth <= cfg.ORION_PUBLISH_RESUME_MAX_DEPTH
                            )
                        )
                    ):
                        should_pause = False
                    else:
                        should_pause = True

            if should_pause:
                if not backpressure_active:
                    backpressure_active = True
                    pause_wall_start = time.perf_counter()
                    publisher.metrics.incr("backpressure_count")
                    log.warning(
                        "BACKPRESSURE pause sim_t=%.3f lag=%.2f depth=%d state=%s",
                        sim_t,
                        publisher.compute_realtime_lag(sim_t, pending_cycle),
                        publisher.effective_depth(pending_cycle is not None),
                        publisher.state.value,
                    )
                # Try to flush pending cycle while paused
                if pending_cycle is not None and publisher is not None:
                    if publisher.try_enqueue(pending_cycle):
                        last_publish_sim_t = pending_cycle.simulation_time
                        pending_cycle = None
                time.sleep(0.05)
                continue

            if backpressure_active:
                if pause_wall_start is not None and publisher is not None:
                    publisher.metrics.add_pause_ms(
                        (time.perf_counter() - pause_wall_start) * 1000.0
                    )
                backpressure_active = False
                pause_wall_start = None
                log.info("BACKPRESSURE resume sim_t=%.3f", sim_t)

            if _perf_enabled() and step_count > 0:
                gap_ms = (step_wall_start - last_step_wall) * 1000.0
                log.info(
                    "step_gap sim_time=%.3f gap_ms=%.2f",
                    backend.simulation_time_sec,
                    gap_ms,
                )
            try:
                step_t0 = time.perf_counter()
                cont = backend.step()
                if _perf_enabled():
                    log.info(
                        "backend_step sim_time=%.3f duration_ms=%.2f",
                        backend.simulation_time_sec,
                        (time.perf_counter() - step_t0) * 1000.0,
                    )
            except Exception as e:
                msg = str(e).lower()
                if use_gui and ("connection" in msg or "closed" in msg or "traci" in msg):
                    log.info("SUMO GUI closed by user — stopping.")
                    break
                raise

            step_count += 1
            sim_t = backend.simulation_time_sec

            if demo and not demo_applied and sim_t >= 5.0:
                try:
                    from configuration.model_params import get_registry

                    demo_cfg = get_registry().export_effective_config().get("demo_profile") or {}
                    seq = demo_cfg.get("auto_sequence") or []
                    for step in seq:
                        action = step.get("action")
                        if action == "demand_profile":
                            backend.set_demand_profile(step.get("profile") or "morning_peak")
                            break
                    for step in seq:
                        if step.get("action") == "overlay":
                            backend.add_overlay(
                                overlay_type=step.get("type") or "accident",
                                intersection_id=step.get("intersection_id") or "B",
                                direction=step.get("direction"),
                                segment_role=step.get("segment_role"),
                            )
                            break
                    demo_applied = True
                    log.info("Demo sequence kickoff applied from demo_profile")
                except Exception as e:
                    log.warning("Demo apply failed: %s", e)
                    demo_applied = True

            if step_count % 500 == 0:
                log.info(
                    "sim_t=%.2f steps=%d active=%d exited=%d",
                    sim_t, step_count, backend.count_total_vehicles(),
                    backend.count_exited_network(),
                )

            publish_active = (
                publish_orion
                or kafka_outbox is not None
                or kafka_producer is not None
            )
            if publish_active and (sim_t - last_publish_sim_t) >= publish_interval:
                if use_async and publisher is not None:
                    from integration.orion.publish_gate import is_orion_publish_enabled

                    orion_on = is_orion_publish_enabled()
                    # Retry pending Orion enqueue only when gate ON
                    if orion_on and pending_cycle is not None:
                        enq_t0 = time.perf_counter()
                        if publisher.try_enqueue(pending_cycle):
                            last_publish_sim_t = pending_cycle.simulation_time
                            pending_cycle = None
                            publisher.metrics.update(
                                enqueue_duration_ms=(time.perf_counter() - enq_t0) * 1000.0
                            )
                        # else keep pending; backpressure next iteration
                    elif not orion_on or not publisher.is_faulted:
                        try:
                            enq_t0 = time.perf_counter()
                            cycle_sequence += 1
                            cycle = fanout_publish_cycle(
                                backend,
                                build_all_entities,
                                cycle_sequence,
                                kafka_producer=kafka_producer,
                                kafka_outbox=kafka_outbox,
                            )
                            if _perf_enabled():
                                log.info(
                                    "capture_block sim_time=%.3f entities=%d duration_ms=%.2f",
                                    sim_t,
                                    len(cycle.entities_json),
                                    (time.perf_counter() - enq_t0) * 1000.0,
                                )
                            if orion_on:
                                if publisher.try_enqueue(cycle):
                                    last_publish_sim_t = cycle.simulation_time
                                    publisher.metrics.update(
                                        enqueue_duration_ms=(
                                            time.perf_counter() - enq_t0
                                        )
                                        * 1000.0
                                    )
                                    if _perf_enabled():
                                        log.info(
                                            "publish_block sim_time=%.3f block_ms=%.2f mode=async",
                                            sim_t,
                                            (time.perf_counter() - enq_t0) * 1000.0,
                                        )
                                else:
                                    pending_cycle = cycle
                                    log.warning(
                                        "Queue full — holding pending cycle seq=%d sim_t=%.3f",
                                        cycle.sequence_number,
                                        cycle.simulation_time,
                                    )
                            else:
                                # Kafka/outbox already fanout; Orion skipped
                                last_publish_sim_t = cycle.simulation_time
                        except Exception as e:
                            from integration.orion.publish_cycle import CaptureValidationError
                            from integration.kafka.outbox_store import OutboxAppendError

                            if isinstance(e, OutboxAppendError):
                                log.error("Kafka outbox FAULTED: %s", e)
                                if publisher is not None:
                                    publisher.mark_faulted(f"kafka outbox: {e}")
                            elif isinstance(e, CaptureValidationError):
                                if orion_on:
                                    publisher.mark_faulted(f"capture validation: {e}")
                                log.error("Capture FAULTED: %s", e)
                            else:
                                log.error("Async capture failed: %s", e, exc_info=True)
                elif (
                    not publish_orion
                    and (kafka_outbox is not None or kafka_producer is not None)
                    and build_all_entities is not None
                ):
                    # Kafka-only path (final profile: direct Orion publisher OFF)
                    try:
                        cycle_sequence += 1
                        cycle = fanout_publish_cycle(
                            backend,
                            build_all_entities,
                            cycle_sequence,
                            kafka_producer=kafka_producer,
                            kafka_outbox=kafka_outbox,
                        )
                        last_publish_sim_t = cycle.simulation_time
                    except Exception as e:
                        from integration.kafka.outbox_store import OutboxAppendError

                        if isinstance(e, OutboxAppendError):
                            log.error("Kafka outbox FAULTED: %s", e)
                        else:
                            log.error("Kafka-only capture failed: %s", e, exc_info=True)
                elif publish_orion:
                    from integration.orion.publish_gate import is_orion_publish_enabled

                    pub_t0 = time.perf_counter()
                    if is_orion_publish_enabled():
                        publish_once(backend, upsert_entity, build_all_entities)
                    if _perf_enabled():
                        log.info(
                            "publish_block sim_time=%.3f block_ms=%.2f mode=sync",
                            sim_t,
                            (time.perf_counter() - pub_t0) * 1000.0,
                        )
                    last_publish_sim_t = sim_t

            if args.max_sim_time and sim_t >= args.max_sim_time:
                log.info("Reached max_sim_time=%.1f — stopping.", args.max_sim_time)
                break

            if not use_gui and not cont and sim_t >= cfg.SIM_END_SEC:
                log.info("Simulation ended at t=%.2f", sim_t)
                break

            if pace_realtime and pace_anchor_wall is not None:
                _sleep_to_realtime_pace(
                    anchor_wall=pace_anchor_wall,
                    anchor_sim=pace_anchor_sim,
                    sim_t=sim_t,
                    step_length=cfg.SUMO_STEP_LENGTH,
                )

            last_step_wall = time.perf_counter()

    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C).")
    except Exception as e:
        log.error("Fatal runtime error: %s", e, exc_info=True)
        return 1
    finally:
        if publisher is not None:
            if pending_cycle is not None:
                try:
                    # best-effort blocking put
                    deadline = time.monotonic() + 2.0
                    while time.monotonic() < deadline:
                        if publisher.try_enqueue(pending_cycle):
                            pending_cycle = None
                            break
                        time.sleep(0.05)
                    if pending_cycle is not None:
                        log.warning(
                            "Could not enqueue pending cycle before shutdown "
                            "(durability limit)"
                        )
                except Exception as e:
                    log.warning("Pending cycle flush failed: %s", e)
            stats = publisher.stop(cfg.ORION_PUBLISH_SHUTDOWN_TIMEOUT_SEC)
            log.info("Publisher shutdown stats=%s", stats)
        if kafka_outbox is not None:
            try:
                h = kafka_outbox.stop(
                    flush_timeout_sec=cfg.ORION_PUBLISH_SHUTDOWN_TIMEOUT_SEC
                )
                log.info("Kafka outbox shutdown health=%s", h)
            except Exception as e:
                log.warning("Kafka outbox stop failed: %s", e)
        if kafka_producer is not None:
            try:
                pending_k = kafka_producer.stop(
                    flush_timeout_sec=cfg.ORION_PUBLISH_SHUTDOWN_TIMEOUT_SEC
                )
                log.info(
                    "Kafka producer shutdown pending=%d health=%s",
                    len(pending_k),
                    kafka_producer.health(),
                )
            except Exception as e:
                log.warning("Kafka producer stop failed: %s", e)
        backend.stop()
        log.info("Shutdown complete. steps=%d", step_count)

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="SUMO TraCI runner → NGSI-LD + Control API")
    p.set_defaults(gui=None)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--gui", dest="gui_flag", action="store_true")
    g.add_argument("--no-gui", action="store_true")
    p.add_argument("--no-orion", action="store_true")
    p.add_argument("--no-api", action="store_true", help="Do not start Control API")
    p.add_argument("--nodes", default=None, help="Comma list e.g. A,B,C,D")
    p.add_argument("--publish-interval", type=float, default=None)
    p.add_argument("--max-sim-time", type=float, default=None)
    p.add_argument(
        "--simulation-run-id",
        default=None,
        help="Predeclared run UUID for fenced cutover evidence (default: generated)",
    )
    p.add_argument("--realtime", action="store_true")
    p.add_argument("--fast", action="store_true")
    p.add_argument("--log-level", default=None)
    p.add_argument("--demo", action="store_true", help="Apply demo_profile after t>=5s")
    p.add_argument(
        "--control-mode",
        choices=["FIXED", "PREEMPTION_ENABLED"],
        default=None,
        help="TLS control mode (default FIXED)",
    )
    pub = p.add_mutually_exclusive_group()
    pub.add_argument(
        "--async-publish",
        action="store_true",
        help="Force async Orion publisher (default via ORION_ASYNC_PUBLISH)",
    )
    pub.add_argument(
        "--sync-publish",
        action="store_true",
        help="Force sync publish_once on TraCI thread (debug / parity)",
    )
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    sys.exit(run(args))
