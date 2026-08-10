"""
Scenario manager — thin per-node metadata + compat facade.

Demand/overlays are owned by NetworkRuntimeController / scenario_runtime.
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
        """Compat: record metadata only. Physical effects via scenario_runtime."""
        scenario = cfg.normalize_scenario_id(scenario)
        if scenario not in cfg.CANONICAL_SCENARIO_IDS:
            if scenario not in cfg.SCENARIO_IDS:
                raise ValueError(f"Unknown scenario '{scenario}'")
        self.current_scenario = scenario
        self.blocked_direction = None
        if scenario == "incident":
            direction = target_direction or cfg.incident_approach_direction(
                cfg.TLS_TO_NODE.get(self.tls_id, "A")
            )
            if direction not in cfg.DIRECTIONS:
                direction = cfg.incident_approach_direction(
                    cfg.TLS_TO_NODE.get(self.tls_id, "A")
                )
            self.blocked_direction = direction
            self.incidents.append({
                "type": "MINOR_ACCIDENT",
                "direction": direction,
                "time": time.time(),
            })
            now = time.time()
            self.incidents = [i for i in self.incidents if now - i["time"] < 3600][-200:]
        elif scenario == "normal":
            self.incidents.clear()
        log.info(
            "Scenario metadata=%s on %s (physical via scenario_runtime)",
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
