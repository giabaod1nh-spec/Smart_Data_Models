"""EmergencyActuator — insert EV on boundary route through target intersection."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import configuration.config as cfg
from configuration.model_params import get_registry

log = logging.getLogger(__name__)


@dataclass
class EmergencyVehicle:
    vehicle_id: str
    target_intersection: str
    route_id: str
    inserted_at_s: float
    seen_on_approach: bool = False
    cleared: bool = False


class EmergencyActuator:
    def __init__(self) -> None:
        self.active: List[EmergencyVehicle] = []
        self._seq = 0
        self._last_insert_s: float = -1e9

    def maybe_insert(
        self,
        traci_module,
        *,
        target_intersection: str = "C",
        sim_t: float = 0.0,
        force: bool = False,
    ) -> Optional[EmergencyVehicle]:
        meta = get_registry().local_overlay_types().get("emergency") or {}
        interval = float(meta.get("interval_s", 90))
        vtype = str(meta.get("insert_vtype", "ambulance"))
        if not force and sim_t - self._last_insert_s < interval:
            return None
        # OSM corridors through the four AI-controlled signals.
        routes = {
            "A": [("408246393#6", "408246393#8")],
            "B": [("408246393#6", "408246393#8")],
            "C": [("1184372243", "326771005#1")],
            "D": [("1184372243", "326771005#1")],
        }
        pairs = routes.get(target_intersection) or routes["C"]
        fr, to = pairs[0]
        self._seq += 1
        rid = f"ev_route_{target_intersection}_{self._seq}"
        vid = f"ev_{vtype}_{target_intersection}_{self._seq}"
        try:
            if rid not in traci_module.route.getIDList():
                edges = [fr, to]
                try:
                    found = traci_module.simulation.findRoute(fr, to)
                    if found and getattr(found, "edges", None):
                        edges = list(found.edges)
                except Exception:
                    pass
                traci_module.route.add(rid, edges)
            traci_module.vehicle.add(vid, rid, typeID=vtype, depart="now")
            ev = EmergencyVehicle(vid, target_intersection, rid, sim_t)
            self.active.append(ev)
            self._last_insert_s = sim_t
            log.info("Emergency inserted %s via %s→%s target=%s", vid, fr, to, target_intersection)
            return ev
        except Exception as e:
            log.warning("Emergency insert failed: %s", e)
            return None

    def tick(self, traci_module, sim_t: float) -> None:
        alive = []
        for ev in self.active:
            try:
                if ev.vehicle_id not in traci_module.vehicle.getIDList():
                    ev.cleared = True
                    continue
                lane = traci_module.vehicle.getLaneID(ev.vehicle_id)
                tls = cfg.NODE_TO_TLS.get(ev.target_intersection)
                if tls:
                    for edge in cfg.APPROACH_EDGES[tls].values():
                        if lane.startswith(edge):
                            ev.seen_on_approach = True
                            break
                alive.append(ev)
            except Exception:
                ev.cleared = True
        self.active = alive

    def status(self) -> List[Dict[str, Any]]:
        return [
            {
                "vehicle_id": e.vehicle_id,
                "target_intersection": e.target_intersection,
                "seen_on_approach": e.seen_on_approach,
                "cleared": e.cleared,
                "inserted_at_s": e.inserted_at_s,
            }
            for e in self.active
        ]
