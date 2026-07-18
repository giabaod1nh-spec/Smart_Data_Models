"""
main.py — Entry point cua Simulator (multi-intersection)
"""
import os
import time
import logging
import threading

import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("simulator")

SIMULATION_FPS   = int(os.getenv("SIMULATION_FPS", "15"))
PUBLISH_INTERVAL = float(os.getenv("UPDATE_INTERVAL_SECONDS", "1"))
CONTROL_API_PORT = int(os.getenv("CONTROL_API_PORT", "9090"))


def main():
    from traffic_engine import CityNetworkEngine
    from entity_generator import build_all_entities
    from client import wait_orion_ready, upsert_entity
    import control_api
    import network as net

    log.info("=" * 60)
    log.info("Smart Traffic Simulator (Multi-Intersection) starting")
    log.info(f"  Orion URL       : {os.getenv('ORION_URL', 'http://localhost:1026')}")
    log.info(f"  Intersections   : {net.INTERSECTION_NODES}")
    log.info(f"  Boundary nodes  : {net.BOUNDARY_NODES}")
    log.info(f"  Publish interval: {PUBLISH_INTERVAL}s")
    log.info(f"  Simulation FPS  : {SIMULATION_FPS}")
    log.info("=" * 60)

    wait_orion_ready()

    engine = CityNetworkEngine()
    control_api.engine = engine

    def simulation_loop():
        dt = 1.0 / SIMULATION_FPS
        last_spawn = time.time()
        log.info(f"Simulation loop started @ {SIMULATION_FPS} FPS")
        while True:
            t0 = time.time()
            engine.tick_simulation(dt)
            if time.time() - last_spawn >= 1.0:
                engine.tick_spawn()
                last_spawn = time.time()
            elapsed = time.time() - t0
            time.sleep(max(0.0, dt - elapsed))

    threading.Thread(target=simulation_loop, daemon=True, name="SimLoop").start()

    def run_api():
        uvicorn.run(control_api.app, host="0.0.0.0", port=CONTROL_API_PORT, log_level="warning")

    threading.Thread(target=run_api, daemon=True, name="ControlAPI").start()
    log.info(f"Control API listening on :{CONTROL_API_PORT}")

    log.info(f"Publishing loop started @ {PUBLISH_INTERVAL}s interval")
    tick = 0
    while True:
        t0 = time.time()
        tick += 1
        total_v = 0
        # Moi node publish doc lap trong try/except rieng — neu 1 giao lo
        # loi (vd Orion tam thoi tra ve 500 cho entity do), cac giao lo
        # con lai van duoc publish binh thuong trong cung tick, thay vi
        # 1 loi lam gian doan publish ca 4 giao lo.
        for node_id in net.INTERSECTION_NODES:
            try:
                snapshot = engine.get_snapshot(node_id)
                entities = build_all_entities(node_id, snapshot)
                for entity in entities:
                    upsert_entity(entity)
                dirs = snapshot["directions"]
                total_v += sum(d["vehicle_count"] for d in dirs.values())
            except Exception as e:
                log.error(f"Publishing error at intersection {node_id}: {e}", exc_info=True)

        try:
            edge_v = sum(len(v) for v in engine.edges_vehicles.values())
            log.info(
                f"[tick={tick:05d}] scenario={engine.current_scenario:12s} | "
                f"in_intersections={total_v:4d} | in_transit={edge_v:4d} | "
                f"exited_total={engine.count_exited_network():5d}"
            )
        except Exception as e:
            log.error(f"Logging error: {e}", exc_info=True)

        elapsed = time.time() - t0
        time.sleep(max(0.0, PUBLISH_INTERVAL - elapsed))


if __name__ == "__main__":
    main()
