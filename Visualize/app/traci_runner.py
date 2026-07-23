"""
traci_runner.py — SUMO TraCI runtime + optional Orion publish + Control API.
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import configuration.config as cfg
from simulation.backend import SumoBackend

log = logging.getLogger("traci_runner")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def publish_once(backend: SumoBackend, upsert_entity, build_all_entities) -> int:
    """Publish all publish_nodes. Never raises."""
    total_ok = 0
    total_n = 0
    try:
        for node in backend.publish_nodes:
            try:
                snapshot = backend.get_snapshot(node, fresh=False)
                entities = build_all_entities(node, snapshot)
                total_n += len(entities)
                for ent in entities:
                    try:
                        upsert_entity(ent)
                        total_ok += 1
                    except Exception as e:
                        log.error("Orion upsert failed for %s: %s", ent.get("id"), e)
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


def start_control_api(backend: SumoBackend) -> None:
    import api.control_api as control_api
    import uvicorn

    control_api.engine = backend

    def _run():
        uvicorn.run(
            control_api.app,
            host="0.0.0.0",
            port=cfg.CONTROL_API_PORT,
            log_level="warning",
        )

    t = threading.Thread(target=_run, name="control-api", daemon=True)
    t.start()
    log.info("Control API listening on :%d", cfg.CONTROL_API_PORT)


def run(args: argparse.Namespace) -> int:
    setup_logging(args.log_level or cfg.LOG_LEVEL)

    use_gui = args.gui if args.gui is not None else cfg.SUMO_GUI
    if args.no_gui:
        use_gui = False
    if args.gui_flag:
        use_gui = True

    publish_orion = not args.no_orion
    build_all_entities = None
    upsert_entity = None

    if publish_orion:
        try:
            from integration.orion.entity_mapper import build_all_entities as _build
            from integration.orion.client import upsert_entity as _upsert, wait_orion_ready as _wait
            build_all_entities = _build
            upsert_entity = _upsert
        except ImportError as e:
            log.error("Cannot import Orion integration modules: %s", e)
            return 2
        try:
            _wait(retries=5, delay=2.0)
        except Exception as e:
            log.warning("Context Broker not ready (%s). Continuing.", e)

    nodes = cfg.PUBLISH_NODES
    if getattr(args, "nodes", None):
        nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]

    backend = SumoBackend(
        sumo_config=cfg.SUMO_CONFIG,
        use_gui=use_gui,
        publish_nodes=nodes,
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

    try:
        backend.start()
        if not args.no_api:
            try:
                start_control_api(backend)
            except Exception as e:
                log.warning("Control API failed to start: %s", e)

        log.info(
            "Runtime: gui=%s realtime=%s orion=%s api=%s interval=%.2fs nodes=%s version=%s",
            use_gui, pace_realtime, publish_orion, not args.no_api, publish_interval,
            backend.publish_nodes, cfg.VERSION,
        )

        while True:
            try:
                cont = backend.step()
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
                    # Apply first demand + first overlay for interactive demo
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

            if publish_orion and (sim_t - last_publish_sim_t) >= publish_interval:
                publish_once(backend, upsert_entity, build_all_entities)
                last_publish_sim_t = sim_t

            if args.max_sim_time and sim_t >= args.max_sim_time:
                log.info("Reached max_sim_time=%.1f — stopping.", args.max_sim_time)
                break

            if not use_gui and not cont and sim_t >= cfg.SIM_END_SEC:
                log.info("Simulation ended at t=%.2f", sim_t)
                break

            if pace_realtime:
                time.sleep(cfg.SUMO_STEP_LENGTH)

    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C).")
    except Exception as e:
        log.error("Fatal runtime error: %s", e, exc_info=True)
        return 1
    finally:
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
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    sys.exit(run(args))
