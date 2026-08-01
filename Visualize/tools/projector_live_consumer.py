"""Live Kafka → Orion projector consumer (K-3 / K-5) with health HTTP."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

VIS = Path(__file__).resolve().parents[1]
REPO = VIS.parent
if str(VIS) not in sys.path:
    sys.path.insert(0, str(VIS))
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

log = logging.getLogger("projector.live")

_HEALTH: dict[str, Any] = {
    "ready": False,
    "prepared": False,
    "faulted": False,
    "processed": 0,
    "manifest_loaded": False,
    "process_start_id": str(uuid.uuid4()),
}

# Harness-driven WRITE_MODE transitions (plan §2.2 Step E).
_CONTROL: dict[str, Any] = {"requested_write_mode": None}
_ALLOWED_TRANSITIONS = {
    ("disabled", "armed"),
    ("armed", "active"),
    ("disabled", "active"),
    ("active", "disabled"),
    ("armed", "disabled"),
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _parse_write_mode(raw: str):
    from integration.projector.core import WriteMode

    try:
        return WriteMode(raw.strip().lower())
    except ValueError:
        return WriteMode.ACTIVE


def _load_fence_manifest(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    required = (
        "targetSimulationRunId",
        "topic",
        "partitions",
    )
    for key in required:
        if key not in data:
            raise ValueError(f"fence manifest missing {key}")
    return data


def _request_write_mode(current: str, target: str) -> tuple[int, dict]:
    """Validate a harness-requested transition; the poll loop applies it."""
    if target not in ("disabled", "armed", "active"):
        return 400, {"error": "mode must be disabled|armed|active", "write_mode": current}
    if target == current:
        return 200, {"write_mode": current, "applied": False, "reason": "already in mode"}
    if (current, target) not in _ALLOWED_TRANSITIONS:
        return 409, {"error": f"transition {current}->{target} not allowed", "write_mode": current}
    if target == "armed" and not _HEALTH.get("manifest_loaded"):
        return 409, {"error": "manifest not loaded; Step D incomplete", "write_mode": current}
    if target == "active" and _HEALTH.get("faulted"):
        return 409, {"error": "projector faulted", "write_mode": current}
    _CONTROL["requested_write_mode"] = target
    return 202, {"write_mode": current, "requested": target, "applied": False}


def _start_health_server(host: str, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path not in ("/health", "/ready", "/prepared", "/metrics"):
                self.send_response(404)
                self.end_headers()
                return
            body = dict(_HEALTH)
            if path == "/ready":
                ok = bool(body.get("ready"))
                self.send_response(200 if ok else 503)
            elif path == "/prepared":
                ok = bool(body.get("prepared"))
                self.send_response(200 if ok else 503)
            else:
                self.send_response(200)
            payload = json.dumps(body, default=str).encode("utf-8")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path != "/write-mode":
                self.send_response(404)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length") or 0)
            try:
                req = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
            except Exception:
                req = {}
            target = str(req.get("mode") or "").strip().lower()
            current = str(_HEALTH.get("write_mode") or "disabled")
            status, body = _request_write_mode(current, target)
            payload = json.dumps(body, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("projector health listening on %s:%d", host, port)
    return server


def _discover_partitions(consumer, topic: str, timeout: float = 10.0) -> List[int]:
    md = consumer.list_topics(topic=topic, timeout=timeout)
    t = md.topics.get(topic)
    if t is None:
        raise RuntimeError(f"topic not found: {topic}")
    return sorted(t.partitions.keys())


def _seek_fence_manifest(
    consumer,
    topic: str,
    manifest: dict,
    resume_offsets: Optional[Dict[int, int]] = None,
) -> None:
    """Manual assign to the fence offsets, resuming past already-applied events."""
    from confluent_kafka import TopicPartition

    parts = manifest.get("partitions") or []
    live = _discover_partitions(consumer, topic)
    manifest_parts = {int(p["partition"]) for p in parts}
    if manifest_parts != set(live):
        raise RuntimeError(
            f"partition count mismatch manifest={sorted(manifest_parts)} live={live}"
        )
    assignments = []
    for p in parts:
        part = int(p["partition"])
        start = int(p["nextOffset"])
        resume = (resume_offsets or {}).get(part)
        if resume is not None and resume > start:
            start = resume
        assignments.append(TopicPartition(topic, part, start))
    consumer.assign(assignments)
    log.info(
        "assigned fence manifest offsets=%s",
        [(a.partition, a.offset) for a in assignments],
    )


def _update_health(
    proj,
    *,
    manifest_loaded: bool,
    orion_ok: bool,
    consumer_lag: Optional[Dict[int, int]] = None,
) -> None:
    h = proj.health()
    wm = h.get("write_mode", "active")
    producer_id = str(os.getenv("KAFKA_PRODUCER_ID", "visualize-traci"))
    active_run = proj.store.get_active_run(source=proj.source, producer_id=producer_id)
    _HEALTH.update(
        {
            "health": h,
            "shadow": h.get("shadow"),
            "namespace": proj.target_namespace,
            "write_mode": wm,
            "manifest_loaded": manifest_loaded,
            "faulted": proj.faulted,
            "armed_buffer_full": bool(h.get("armed_buffer_full")),
            "fence_skipped_count": h.get("fence_skipped_count"),
            "orion_apply_count": h.get("orion_apply_count"),
            "pipeline_e2e_latency_ms": h.get("last_pipeline_e2e_latency_ms"),
            "pipeline_e2e_latency_ms_p95": h.get("pipeline_e2e_latency_ms_p95"),
            "pipeline_e2e_latency_ms_max": h.get("pipeline_e2e_latency_ms_max"),
            "pipeline_e2e_latency_sample_count": h.get("pipeline_e2e_latency_sample_count"),
            "pipeline_e2e_latency_spikes_gt_500ms": h.get("pipeline_e2e_latency_spikes_gt_500ms"),
            "pipeline_freshness_sec": h.get("pipeline_freshness_sec"),
            "orion_batch_duration_ms": h.get("last_orion_batch_duration_ms"),
            "stage_latency": h.get("stage_latency"),
            "last_cycle_lookup": h.get("last_cycle_lookup"),
            "writer_role": h.get("writer_role", "projector"),
            "consumer_lag_offsets": consumer_lag or {},
            "stale_event_count": h.get("stale_event_count"),
            "quarantine_count": h.get("quarantine_count"),
            "orion_partial_count": h.get("orion_partial_count"),
            "node_partial_count": h.get("node_partial_count"),
            "prepared": orion_ok
            and not proj.faulted
            and wm in ("disabled", "armed", "active"),
            "ready": wm == "active"
            and not proj.faulted
            and manifest_loaded
            and active_run is not None,
        }
    )


def _sync_consumer_pause(consumer, assigned: List[Any], paused: bool) -> None:
    if not assigned:
        return
    if paused:
        consumer.pause(assigned)
    else:
        consumer.resume(assigned)


def main() -> int:
    p = argparse.ArgumentParser(description="K-3/K-5 live projector consumer → Orion")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic.entity-events.v2"))
    p.add_argument("--group", default=os.getenv("PROJECTOR_GROUP_ID", "projector-shadow-live"))
    p.add_argument(
        "--db",
        default=os.getenv(
            "PROJECTOR_DB",
            str(VIS / "artifacts" / "projector" / "live.sqlite3"),
        ),
    )
    p.add_argument("--max-records", type=int, default=0, help="0 = until idle timeout")
    p.add_argument("--idle-sec", type=float, default=8.0)
    p.add_argument("--from-latest", action="store_true", help="Skip backlog; start at log end")
    p.add_argument("--start-offsets-file", default=os.getenv("PROJECTOR_FENCE_MANIFEST"))
    p.add_argument("--max-wall-sec", type=float, default=0.0, help="0 = run until stopped")
    p.add_argument("--shadow", action="store_true", default=None)
    p.add_argument("--no-shadow", action="store_true")
    p.add_argument(
        "--namespace",
        default=os.getenv("PROJECTOR_TARGET_NAMESPACE", "shadow"),
        help="shadow|test|production",
    )
    p.add_argument("--health-host", default=os.getenv("PROJECTOR_HEALTH_HOST", "0.0.0.0"))
    p.add_argument("--health-port", type=int, default=int(os.getenv("PROJECTOR_HEALTH_PORT", "8092")))
    p.add_argument("--no-health", action="store_true")
    p.add_argument(
        "--node-timeout-ms",
        type=float,
        default=float(os.getenv("PROJECTOR_NODE_TIMEOUT_MS", "2000")),
        help="Incomplete-node partial-flush timeout; must exceed the production publish interval",
    )
    p.add_argument(
        "--write-mode",
        default=os.getenv("PROJECTOR_WRITE_MODE", "active"),
        choices=["disabled", "armed", "active"],
    )
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from contracts.architecture_profiles import (
        COMPONENT_PROJECTOR,
        ProfileValidationError,
        validate_env,
        validate_namespace,
    )

    shadow_mode = _env_bool("PROJECTOR_SHADOW_MODE", True)
    if args.no_shadow:
        shadow_mode = False
    elif args.shadow:
        shadow_mode = True

    try:
        env = dict(os.environ)
        env["PROJECTOR_SHADOW_MODE"] = "true" if shadow_mode else "false"
        env["PROJECTOR_TARGET_NAMESPACE"] = args.namespace
        validate_env(
            os.getenv("ARCHITECTURE_PROFILE", "none"), env, COMPONENT_PROJECTOR
        )
        namespace = validate_namespace(
            args.namespace,
            architecture_lock_smoke=_env_bool("ARCHITECTURE_LOCK_SMOKE", False),
        )
    except ProfileValidationError as e:
        log.error("Projector profile validation failed: %s", e)
        return 2

    if _env_bool("PROJECTOR_SINGLE_INSTANCE", False):
        from integration.projector.instance_lock import (
            InstanceLock,
            ProjectorInstanceAlreadyRunning,
        )

        try:
            InstanceLock(Path(args.db)).acquire()
        except ProjectorInstanceAlreadyRunning as e:
            log.error("%s", e)
            return 2

    health_server: Optional[ThreadingHTTPServer] = None
    if not args.no_health:
        health_server = _start_health_server(args.health_host, args.health_port)

    from confluent_kafka import Consumer, TopicPartition
    from integration.orion.client import batch_upsert_entities, wait_orion_ready
    from integration.projector.core import OrionProjector, WriteMode
    from integration.projector.store import ProjectorStore

    write_mode = _parse_write_mode(args.write_mode)
    manifest_loaded = False
    fence_manifest: Optional[dict] = None
    fence_offsets: Dict[int, int] = {}
    target_run: Optional[str] = None
    if args.start_offsets_file:
        fence_manifest = _load_fence_manifest(Path(args.start_offsets_file))
        target_run = str(fence_manifest["targetSimulationRunId"])
        fence_offsets = {
            int(p["partition"]): int(p["nextOffset"])
            for p in fence_manifest["partitions"]
        }
        manifest_loaded = True

    orion_ok = wait_orion_ready(retries=10, delay=1.0)
    store = ProjectorStore(Path(args.db))
    proj = OrionProjector(
        store,
        batch_upsert=batch_upsert_entities,
        shadow=shadow_mode,
        target_namespace=namespace,
        node_timeout_ms=args.node_timeout_ms,
        write_mode=write_mode,
        target_simulation_run_id=target_run,
        fence_offsets=fence_offsets,
        defer_ready_until_idle=True,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "latest" if args.from_latest else "earliest",
            "enable.auto.commit": False,
        }
    )

    assigned: List[Any] = []

    def _on_assign(cons, partitions):
        if args.from_latest:
            for tp in partitions:
                lo, hi = cons.get_watermark_offsets(tp, timeout=10.0)
                tp.offset = hi
        cons.assign(partitions)
        assigned[:] = partitions

    # A fence manifest pins exact per-partition offsets, so use manual assignment
    # only: subscribe() would hand control to the group rebalance and discard the seek.
    group_managed = fence_manifest is None
    if fence_manifest is not None:
        try:
            resume = {}
            for part in fence_offsets:
                committed = store.get_committed_offset(args.topic, part)
                if committed is not None:
                    resume[part] = committed + 1
            _seek_fence_manifest(consumer, args.topic, fence_manifest, resume)
            assigned[:] = consumer.assignment()
        except Exception as e:
            log.error("fence manifest seek failed: %s", e)
            return 2
    else:
        consumer.subscribe([args.topic], on_assign=_on_assign)
        for _ in range(40):
            consumer.poll(0.25)
            if assigned:
                break

    partitions = sorted({tp.partition for tp in assigned}) if assigned else _discover_partitions(
        consumer, args.topic
    )
    proj.recover(args.topic, partitions)
    _update_health(
        proj,
        manifest_loaded=manifest_loaded,
        orion_ok=orion_ok,
        consumer_lag={},
    )

    processed = 0
    last_msg = time.monotonic()
    started = time.monotonic()
    max_wall = args.max_wall_sec
    partitions_paused = False

    lag_cache: Dict[str, Any] = {"at": 0.0, "value": {}}
    health_cache: Dict[str, float] = {"at": 0.0}

    def _consumer_lag() -> Dict[int, int]:
        """Cached: each watermark probe is a broker round-trip, too slow per message."""
        now = time.monotonic()
        if now - float(lag_cache["at"]) < 2.0:
            return lag_cache["value"]
        lag: Dict[int, int] = {}
        fence = (fence_manifest or {}).get("partitions") or []
        fence_by_p = {int(p["partition"]): int(p["nextOffset"]) for p in fence}
        try:
            positions = {
                int(tp.partition): int(tp.offset)
                for tp in consumer.position(list(assigned))
                if tp.offset is not None and tp.offset >= 0
            }
        except Exception:
            positions = {}
        for tp in assigned:
            try:
                _lo, hi = consumer.get_watermark_offsets(tp, timeout=2.0, cached=True)
                committed = store.get_committed_offset(args.topic, tp.partition)
                # Position is the next offset we will read: max(assignment, committed+1, fence).
                pos = positions.get(int(tp.partition), 0)
                if committed is not None:
                    pos = max(pos, int(committed) + 1)
                if tp.partition in fence_by_p:
                    pos = max(pos, fence_by_p[tp.partition])
                lag[tp.partition] = max(0, hi - pos)
            except Exception:
                continue
        lag_cache["at"] = now
        lag_cache["value"] = lag
        return lag

    try:
        while True:
            requested = _CONTROL.get("requested_write_mode")
            if requested is not None:
                _CONTROL["requested_write_mode"] = None
                new_mode = WriteMode(requested)
                if new_mode != write_mode:
                    log.info("WRITE_MODE %s -> %s", write_mode.value, new_mode.value)
                    write_mode = new_mode
                    proj.set_write_mode(new_mode)
                    if new_mode == WriteMode.ACTIVE:
                        proj.drain_armed_buffer()
            if max_wall and (time.monotonic() - started) > max_wall:
                proj.tick()
                break
            if args.max_records and processed >= args.max_records:
                break
            if (
                max_wall
                and processed > 0
                and (time.monotonic() - last_msg) > args.idle_sec
            ):
                proj.tick()
                break
            if write_mode == WriteMode.DISABLED:
                time.sleep(0.5)
                _update_health(
                    proj,
                    manifest_loaded=manifest_loaded,
                    orion_ok=orion_ok,
                    consumer_lag=_consumer_lag(),
                )
                continue
            if proj._partitions_paused != partitions_paused:
                partitions_paused = proj._partitions_paused
                _sync_consumer_pause(consumer, list(assigned), partitions_paused)
            if proj._partitions_paused:
                time.sleep(0.2)
                proj.tick()
                _update_health(
                    proj,
                    manifest_loaded=manifest_loaded,
                    orion_ok=orion_ok,
                    consumer_lag=_consumer_lag(),
                )
                continue
            # Keep the idle poll short so completed cycles are noticed promptly;
            # the partial timeout itself is intentionally longer than the 1 s
            # production publish interval to avoid splitting an in-flight node.
            msg = consumer.poll(0.05)
            if msg is None:
                # Only expire incomplete buffers when there is no broker lag.
                # poll() can return None for a few dozen ms even while lag>0;
                # partial-flushing then races the in-flight fetch and pauses
                # the writer (node_partial → buffers.pause).
                lag_now = _consumer_lag()
                idle_for = time.monotonic() - last_msg
                allow_partial = (
                    sum(int(v) for v in lag_now.values()) == 0
                    and idle_for >= (args.node_timeout_ms / 1000.0)
                )
                proj.tick(allow_partial=allow_partial)
                _update_health(
                    proj,
                    manifest_loaded=manifest_loaded,
                    orion_ok=orion_ok,
                    consumer_lag=lag_now,
                )
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            raw = msg.value()
            if not raw:
                continue
            try:
                body = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            if not isinstance(body, dict):
                continue
            try:
                received_epoch = time.time()
                _timestamp_type, timestamp_ms = msg.timestamp()
                broker_epoch = (
                    float(timestamp_ms) / 1000.0
                    if timestamp_ms is not None and timestamp_ms >= 0
                    else None
                )
                action = proj.process_record(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    value=body,
                    broker_timestamp_epoch=broker_epoch,
                    consumer_received_epoch=received_epoch,
                )
                # A busy partition can keep poll() non-empty for seconds. Do
                # not make complete node/cycle buffers wait for global consumer
                # idle; flush completed buffers only (never partial) as soon as
                # the core reports one ready.
                if action == "node_ready_deferred":
                    proj.tick(allow_partial=False, complete_cycles_only=True)
            except Exception:
                log.exception("process_record failed")
                _HEALTH["faulted"] = True
                _HEALTH["ready"] = False
                raise
            processed += 1
            last_msg = time.monotonic()
            _HEALTH["processed"] = processed
            if last_msg - float(health_cache["at"]) >= 0.5:
                health_cache["at"] = last_msg
                _update_health(
                    proj,
                    manifest_loaded=manifest_loaded,
                    orion_ok=orion_ok,
                    consumer_lag=_consumer_lag(),
                )
            # In fence-manifest mode the consumer is manually assigned (no group
            # membership), so SQLite is the sole offset store and broker commits
            # would fail with UNKNOWN_MEMBER_ID.
            committed = store.get_committed_offset(msg.topic(), msg.partition())
            if committed is not None and group_managed:
                consumer.commit(
                    offsets=[
                        TopicPartition(msg.topic(), msg.partition(), committed + 1)
                    ],
                    asynchronous=False,
                )
            if processed % 20 == 0:
                log.info(
                    "processed=%d last_action=%s write_mode=%s",
                    processed,
                    action,
                    proj.write_mode.value,
                )
        proj.tick()
        log.info("DONE processed=%d health=%s", processed, proj.health())
        print(json.dumps({"processed": processed, "health": proj.health()}, indent=2))
        return 0 if not proj.faulted else 1
    finally:
        _HEALTH["ready"] = False
        consumer.close()
        store.close()
        if health_server is not None:
            health_server.shutdown()


if __name__ == "__main__":
    sys.exit(main())
