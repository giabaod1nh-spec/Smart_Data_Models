"""
Scenario manager — maps simulator scenario IDs to TraCI actions.
"""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import config as cfg

log = logging.getLogger(__name__)


class SumoScenarioManager:
    def __init__(self, tls_id: str = "J1"):
        self.tls_id = tls_id
        self.current_scenario: str = "normal"
        self.blocked_direction: Optional[str] = None
        self.incidents: List[dict] = []
        self._base_max_speeds: Dict[str, float] = {}

    def set_scenario(
        self,
        traci_module,
        scenario: str,
        target_direction: Optional[str] = None,
    ) -> None:
        if scenario not in cfg.SCENARIO_IDS:
            raise ValueError(f"Unknown scenario '{scenario}'. Expected one of {cfg.SCENARIO_IDS}")

        # Clear previous accident lane blocks
        self._clear_accident(traci_module)

        self.current_scenario = scenario
        self.blocked_direction = None

        scale = cfg.SCENARIO_TRAFFIC_SCALE.get(scenario, 1.0)
        speed_factor = cfg.SCENARIO_SPEED_FACTOR.get(scenario, 1.0)

        try:
            traci_module.simulation.setScale(scale)
        except Exception as e:
            log.warning("simulation.setScale(%.2f) failed: %s", scale, e)

        self._apply_speed_factor(traci_module, speed_factor)

        if scenario == "accident":
            direction = target_direction or "North"
            if direction not in cfg.DIRECTIONS:
                direction = "North"
            self.blocked_direction = direction
            self._apply_accident(traci_module, direction)
            self.incidents.append({
                "type": "MINOR_ACCIDENT",
                "direction": direction,
                "time": time.time(),
            })
            # Keep last 200 / 1h like simulator
            now = time.time()
            self.incidents = [i for i in self.incidents if now - i["time"] < 3600][-200:]
            log.info("Accident scenario: blocked %s approach on %s", direction, self.tls_id)
        else:
            log.info("Scenario set to %s (scale=%.2f speed_factor=%.2f)", scenario, scale, speed_factor)

    def recent_incidents(self, max_age_sec: float = 300.0) -> List[dict]:
        now = time.time()
        return [i for i in self.incidents if now - i["time"] < max_age_sec]

    def _apply_speed_factor(self, traci_module, factor: float) -> None:
        try:
            vtypes = traci_module.vehicletype.getIDList()
        except Exception:
            return
        for vt in vtypes:
            try:
                if vt not in self._base_max_speeds:
                    self._base_max_speeds[vt] = float(traci_module.vehicletype.getMaxSpeed(vt))
                base = self._base_max_speeds[vt]
                traci_module.vehicletype.setMaxSpeed(vt, max(1.0, base * factor))
            except Exception as e:
                log.debug("setMaxSpeed %s failed: %s", vt, e)

    def _apply_accident(self, traci_module, direction: str) -> None:
        """Close lane 0 of the approach (partial blockage)."""
        edge = cfg.APPROACH_EDGES[self.tls_id][direction]
        lane = f"{edge}_0"
        try:
            traci_module.lane.setDisallowed(lane, ["all"])
            # Slow remaining lane
            lane1 = f"{edge}_1"
            traci_module.lane.setMaxSpeed(lane1, 2.0)  # ~7 km/h
            log.info("Accident: disallowed %s, slowed %s", lane, lane1)
        except Exception as e:
            log.warning("Failed to apply accident on %s: %s", lane, e)

    def _clear_accident(self, traci_module) -> None:
        if not self.blocked_direction:
            return
        edge = cfg.APPROACH_EDGES[self.tls_id][self.blocked_direction]
        for lane in (f"{edge}_0", f"{edge}_1"):
            try:
                traci_module.lane.setAllowed(lane, ["all"])
                # Restore a reasonable urban speed (13.9 m/s = 50 km/h)
                traci_module.lane.setMaxSpeed(lane, 13.9)
            except Exception as e:
                log.debug("clear accident %s: %s", lane, e)
        self.blocked_direction = None
