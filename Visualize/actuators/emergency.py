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
        if target_intersection not in cfg.NODE_TO_TLS:
            raise ValueError(f"Unknown emergency target {target_intersection!r}")
        tls = cfg.NODE_TO_TLS[target_intersection]
        direction = cfg.incident_approach_direction(target_intersection)
        approach = cfg.APPROACH_EDGES[tls][direction]
        exits = [edge for edge in cfg.OUTGOING_EDGES[tls]
                 if cfg.TURN_BY_OD.get((approach, edge)) == "straight"]
        if not exits:
            raise RuntimeError(f"No straight emergency exit at {target_intersection}/{direction}")
        exit_edge = exits[0]
        sources = get_registry().boundary_sources().values()
        source_edges = sorted({str(src["source_edge"]) for src in sources})
        destination_edges = sorted({str(dest["to_edge"]) for src in sources
                                    for dest in src["destinations"]})

        def best_path(candidates: List[str], destination: str, reverse: bool = False) -> List[str]:
            paths: List[List[str]] = []
            for candidate in candidates:
                try:
                    start, end = (destination, candidate) if reverse else (candidate, destination)
                    route = traci_module.simulation.findRoute(start, end)
                    edges = list(route.edges)
                    if edges and edges[0] == start and edges[-1] == end:
                        paths.append(edges)
                except Exception:
                    continue
            if not paths:
                raise RuntimeError(f"No boundary route through {target_intersection}/{direction}")
            return min(paths, key=lambda edges: (len(edges), edges))

        prefix = best_path(source_edges, approach)
        suffix = best_path(destination_edges, exit_edge, reverse=True)
        route_edges = prefix + [exit_edge] + suffix[1:]
        fr, to = route_edges[0], route_edges[-1]
        self._seq += 1
        rid = f"ev_route_{target_intersection}_{self._seq}"
        vid = f"ev_{vtype}_{target_intersection}_{self._seq}"
        try:
            if rid not in traci_module.route.getIDList():
                traci_module.route.add(rid, route_edges)
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
                    sensed_edges = {edge for corridor in cfg.SENSING_EDGES[tls].values()
                                    for edge in corridor}
                    ev.seen_on_approach |= lane.rsplit("_", 1)[0] in sensed_edges
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
