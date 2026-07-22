"""
traci_runner.py — SUMO TraCI runtime + optional Orion publish (v1).

Usage (Windows PowerShell):
  $env:SUMO_HOME = "C:\\Program Files (x86)\\Eclipse\\Sumo"
  $env:PATH = "$env:SUMO_HOME\\bin;$env:PATH"
  cd Visualize
  pip install -r requirements.txt
  python traci_runner.py --gui
  python traci_runner.py --no-gui --no-orion   # headless, no Orion
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure local imports work when launched from any CWD
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import config as cfg
from sumo_backend import SumoBackend

log = logging.getLogger("traci_runner")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def publish_once(backend: SumoBackend, upsert_entity, build_all_entities) -> int:
    """Build + upsert entities. Returns number of entities attempted. Never raises."""
    try:
        snapshot = backend.get_snapshot(backend.publish_node)
        entities = build_all_entities(backend.publish_node, snapshot)
        ok = 0
        for ent in entities:
            try:
                upsert_entity(ent)
                ok += 1
            except Exception as e:
                log.error("Orion upsert failed for %s: %s", ent.get("id"), e)
        log.info(
            "Published %d/%d entities for node=%s sim_t=%.1f phase=%s vehicles=%s",
            ok, len(entities), backend.publish_node,
            snapshot.get("simulation_time_sec", 0),
            snapshot.get("phase"),
            sum(d["vehicle_count"] for d in snapshot["directions"].values()),
        )
        return ok
    except Exception as e:
        log.error("Publish cycle failed (simulation continues): %s", e, exc_info=True)
        return 0


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
    wait_orion_ready = None

    if publish_orion:
        cfg.ensure_simulator_on_path()
        try:
            from entity_generator import build_all_entities as _build
            from client import upsert_entity as _upsert, wait_orion_ready as _wait
            build_all_entities = _build
            upsert_entity = _upsert
            wait_orion_ready = _wait
        except ImportError as e:
            log.error("Cannot import simulator entity_generator/client: %s", e)
            return 2

        # Soft wait — do not abort SUMO if Orion is down
        try:
            wait_orion_ready(retries=5, delay=2.0)
        except Exception as e:
            log.warning(
                "Orion not ready (%s). Will keep simulating and retry publish each cycle.",
                e,
            )

    backend = SumoBackend(
        sumo_config=cfg.SUMO_CONFIG,
        use_gui=use_gui,
        publish_node=cfg.PUBLISH_NODE,
    )

    publish_interval = float(args.publish_interval or cfg.PUBLISH_INTERVAL)
    last_publish_sim_t = -1e9
    step_count = 0

    # GUI mặc định chạy realtime để xem được; headless chạy hết tốc độ.
    # --fast: bỏ pacing ngay cả khi GUI (chạy nhanh, khó xem).
    pace_realtime = use_gui and not getattr(args, "fast", False)
    if getattr(args, "realtime", False):
        pace_realtime = True

    try:
        backend.start()
        log.info(
            "Runtime config: gui=%s realtime=%s publish_orion=%s interval=%.2fs node=%s tls=%s",
            use_gui, pace_realtime, publish_orion, publish_interval,
            backend.publish_node, backend.tls_id,
        )
        if use_gui:
            log.info(
                "GUI mode: sim chay realtime. Dong cua so sumo-gui hoac Ctrl+C de dung."
            )

        while True:
            try:
                cont = backend.step()
            except Exception as e:
                # User closed sumo-gui window → TraCI disconnects
                msg = str(e).lower()
                if use_gui and ("connection" in msg or "closed" in msg or "traci" in msg):
                    log.info("SUMO GUI closed by user — stopping.")
                    break
                raise

            step_count += 1
            sim_t = backend.simulation_time_sec

            if step_count % 500 == 0:
                log.info("sim_t=%.2f steps=%d active=%d", sim_t, step_count, backend.count_total_vehicles())

            if publish_orion and (sim_t - last_publish_sim_t) >= publish_interval:
                publish_once(backend, upsert_entity, build_all_entities)
                last_publish_sim_t = sim_t

            if args.max_sim_time and sim_t >= args.max_sim_time:
                log.info("Reached max_sim_time=%.1f — stopping.", args.max_sim_time)
                break

            # Headless only: stop at natural end. GUI keeps running until user closes.
            if not use_gui and not cont and sim_t >= 3600.0:
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
    p = argparse.ArgumentParser(description="SUMO TraCI runner → NGSI-LD Orion (v1)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--gui", dest="gui_flag", action="store_true", help="Force sumo-gui (realtime, den khi ban dong)")
    g.add_argument("--no-gui", action="store_true", help="Force headless sumo")
    p.add_argument("--no-orion", action="store_true", help="Skip Orion publish")
    p.add_argument("--publish-interval", type=float, default=None, help="Sim-time seconds between publishes")
    p.add_argument("--max-sim-time", type=float, default=None, help="Stop after this simulation time (s)")
    p.add_argument("--realtime", action="store_true", help="Force wall-clock pacing (default on with --gui)")
    p.add_argument("--fast", action="store_true", help="GUI chay nhanh het toc do (kho xem)")
    p.add_argument("--log-level", default=None, help="DEBUG|INFO|WARNING|ERROR")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    args.gui = None
    sys.exit(run(args))
