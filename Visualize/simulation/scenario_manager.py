"""
Scenario manager — thin per-node metadata + compat facade.

Demand/overlays are owned by NetworkRuntimeController.
This class must NOT call simulation.setScale for demand control.
"""
from __future__ import annotations

import logging
import time
from typing import List, Optional

import configuration.config as cfg

log = logging.getLogger(__name__)


class SumoScenarioManager:
    def __init__(self, tls_id: str = "J1"):
        self.tls_id = tls_id
        self.current_scenario: str = "normal"
        self.blocked_direction: Optional[str] = None
        self.incidents: List[dict] = []

    def set_scenario(
        self,
        traci_module,
        scenario: str,
        target_direction: Optional[str] = None,
    ) -> None:
        """Compat: record metadata only. Physical effects via NetworkRuntimeController."""
        if scenario not in cfg.SCENARIO_IDS and scenario != "oversaturated":
            # allow oversaturated even if not in old SCENARIO_IDS until config updated
            if scenario not in ("normal", "morning_peak", "evening_peak", "oversaturated",
                                "accident", "blocked_intersection", "spillback", "heavy_rain",
                                "emergency", "rain"):
                raise ValueError(f"Unknown scenario '{scenario}'")
        self.current_scenario = scenario
        self.blocked_direction = None
        # NEVER setScale for demand — hybrid actuator owns rates
        try:
            traci_module.simulation.setScale(1.0)
        except Exception:
            pass
        if scenario in ("accident", "blocked_intersection"):
            direction = target_direction or "North"
            if direction not in cfg.DIRECTIONS:
                direction = "North"
            self.blocked_direction = direction
            self.incidents.append({
                "type": "MINOR_ACCIDENT",
                "direction": direction,
                "time": time.time(),
            })
            now = time.time()
            self.incidents = [i for i in self.incidents if now - i["time"] < 3600][-200:]
        log.info(
            "Compat scenario metadata=%s on %s (physical effects via NetworkRuntimeController)",
            scenario, self.tls_id,
        )

    def recent_incidents(self, max_age_sec: float = 300.0) -> List[dict]:
        now = time.time()
        return [i for i in self.incidents if now - i["time"] < max_age_sec]

    def note_incident(self, direction: str) -> None:
        self.blocked_direction = direction
        self.incidents.append({
            "type": "MINOR_ACCIDENT",
            "direction": direction,
            "time": time.time(),
        })
