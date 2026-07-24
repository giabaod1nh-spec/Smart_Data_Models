"""ScenarioCapacityActuator — scoped lane/edge overlays with ResourceStateRegistry."""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import configuration.config as cfg
from configuration.model_params import get_registry
from actuators.resource_state import ResourcePatch, ResourceStateRegistry

log = logging.getLogger(__name__)

OPPOSITE = {"North": "South", "South": "North", "East": "West", "West": "East"}

# Outbound edge by TLS + cardinal direction (through-exit for opposite approach).
EXIT_EDGES: Dict[str, Dict[str, str]] = {
    "J1": {"North": "J1J3", "East": "J1J2", "South": "J1S1", "West": "J1W1"},
    "J2": {"North": "J2J4", "East": "J2E1", "South": "J2S2", "West": "J2J1"},
    "J3": {"North": "J3N1", "East": "J3J4", "South": "J3J1", "West": "J3W2"},
    "J4": {"North": "J4N2", "East": "J4E2", "South": "J4J2", "West": "J4J3"},
}


class OverlayLifecycle(str, Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    REMOVED = "REMOVED"


@dataclass
class OverlayInstance:
    overlay_id: str
    overlay_type: str
    intersection_id: str
    direction: Optional[str] = None
    segment_role: str = "incoming_approach"
    target_edge: Optional[str] = None
    target_lanes: List[str] = field(default_factory=list)
    corridor_edges: List[str] = field(default_factory=list)
    state: OverlayLifecycle = OverlayLifecycle.CREATED
    created_at_s: float = 0.0
    expires_at_s: Optional[float] = None
    priority: int = 50


def _load_catalog() -> Dict[str, Any]:
    path = cfg.GENERATED_ROOT / "network_topology_catalog.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


class ScenarioCapacityActuator:
    def __init__(self) -> None:
        self.registry = ResourceStateRegistry()
        self.overlays: Dict[str, OverlayInstance] = {}
        self._catalog = _load_catalog()

    def _edge_from_catalog(
        self, intersection_id: str, direction: str, segment_role: str
    ) -> Optional[str]:
        links = self._catalog.get("inter_node_links") or {}
        if isinstance(links, list):
            links = {l.get("id", i): l for i, l in enumerate(links)}
        if segment_role == "incoming_approach":
            for link in links.values():
                if link.get("to_node") == intersection_id and link.get("to_approach") == direction:
                    return link.get("edge")
            tls = cfg.NODE_TO_TLS[intersection_id]
            return cfg.APPROACH_EDGES[tls].get(direction)
        # downstream_exit: through exit opposite the named approach direction
        out_dir = OPPOSITE.get(direction, direction)
        for link in links.values():
            if link.get("from_node") == intersection_id and link.get("from_approach") == out_dir:
                return link.get("edge")
        tls = cfg.NODE_TO_TLS[intersection_id]
        return EXIT_EDGES.get(tls, {}).get(out_dir)

    def _resolve_lanes(
        self,
        *,
        overlay_type: str,
        intersection_id: str,
        direction: Optional[str],
        segment_role: str,
        target_edge: Optional[str],
        target_lanes: Optional[List[str]],
    ) -> Tuple[Optional[str], List[str], List[str]]:
        """Return (edge, lanes, corridor_edges)."""
        if target_lanes:
            edge = target_edge or target_lanes[0].rsplit("_", 1)[0]
            return edge, list(target_lanes), []
        if target_edge:
            lanes = [f"{target_edge}_{i}" for i in range(cfg.LANES_PER_APPROACH)]
            return target_edge, lanes, []

        if overlay_type == "heavy_rain":
            edges = ["J3J4", "J4J3"]
            if direction in ("North", "South") or intersection_id in ("A", "B"):
                # default C–D corridor unless NS requested
                if intersection_id in ("A", "B") and direction in ("North", "South"):
                    edges = ["J1J3", "J3J1"] if intersection_id in ("A", "C") else ["J2J4", "J4J2"]
            lanes: List[str] = []
            for e in edges:
                lanes.extend([f"{e}_{i}" for i in range(cfg.LANES_PER_APPROACH)])
            return None, lanes, edges

        if not direction:
            direction = "North"
        edge = self._edge_from_catalog(intersection_id, direction, segment_role)
        if not edge:
            tls = cfg.NODE_TO_TLS[intersection_id]
            edge = cfg.APPROACH_EDGES[tls][direction]
        lanes = [f"{edge}_{i}" for i in range(cfg.LANES_PER_APPROACH)]
        return edge, lanes, []

    def add_overlay(
        self,
        traci_module,
        *,
        overlay_type: str,
        intersection_id: str,
        direction: Optional[str] = None,
        segment_role: Optional[str] = None,
        target_edge: Optional[str] = None,
        target_lanes: Optional[List[str]] = None,
        duration_s: Optional[float] = None,
        sim_t: float = 0.0,
        overlay_id: Optional[str] = None,
    ) -> OverlayInstance:
        types = get_registry().local_overlay_types()
        if overlay_type not in types:
            raise ValueError(f"Unknown overlay type {overlay_type}")
        meta = types[overlay_type]
        role = segment_role or meta.get("default_segment_role") or "incoming_approach"
        if overlay_type not in ("heavy_rain", "emergency") and role not in (
            "incoming_approach",
            "downstream_exit",
        ):
            raise ValueError("segment_role required: incoming_approach|downstream_exit")

        edge, lanes, corridor = self._resolve_lanes(
            overlay_type=overlay_type,
            intersection_id=intersection_id,
            direction=direction,
            segment_role=role,
            target_edge=target_edge,
            target_lanes=target_lanes,
        )
        oid = overlay_id or f"{overlay_type}:{intersection_id}:{direction or 'X'}:{uuid.uuid4().hex[:8]}"
        inst = OverlayInstance(
            overlay_id=oid,
            overlay_type=overlay_type,
            intersection_id=intersection_id,
            direction=direction,
            segment_role=role,
            target_edge=edge,
            target_lanes=lanes,
            corridor_edges=corridor,
            state=OverlayLifecycle.CREATED,
            created_at_s=sim_t,
            expires_at_s=(sim_t + duration_s) if duration_s else None,
            priority=int(meta.get("priority", 50)),
        )
        for lid in lanes:
            self.registry.capture_lane(traci_module, lid)

        if overlay_type in ("accident", "blocked_intersection"):
            for i, lid in enumerate(lanes):
                if i == 0 and meta.get("lane0_disallow", True):
                    self.registry.add_patch(
                        lid,
                        ResourcePatch(oid, overlay_type, inst.priority, allowed=[], max_speed=None),
                    )
                else:
                    spd = float(meta.get("other_lanes_speed_mps", 2.0))
                    self.registry.add_patch(
                        lid,
                        ResourcePatch(oid, overlay_type, inst.priority, allowed=None, max_speed=spd),
                    )
        elif overlay_type == "downstream_restriction":
            factor = float(meta.get("speed_factor", 0.15))
            for i, lid in enumerate(lanes):
                base = self.registry._resources[lid].original_max_speed
                if i == 0 and meta.get("lane0_disallow", True):
                    self.registry.add_patch(
                        lid,
                        ResourcePatch(oid, overlay_type, inst.priority, allowed=[], max_speed=None),
                    )
                else:
                    self.registry.add_patch(
                        lid,
                        ResourcePatch(
                            oid,
                            overlay_type,
                            inst.priority,
                            allowed=None,
                            max_speed=max(0.5, base * factor),
                        ),
                    )
        elif overlay_type == "heavy_rain":
            factor = float(meta.get("edge_speed_factor", 0.65))
            for lid in lanes:
                base = self.registry._resources[lid].original_max_speed
                self.registry.add_patch(
                    lid,
                    ResourcePatch(
                        oid,
                        overlay_type,
                        inst.priority,
                        allowed=None,
                        max_speed=max(1.0, base * factor),
                    ),
                )
        # emergency: no lane patch — insert handled by EmergencyActuator

        if lanes:
            self.registry.apply_effective(traci_module, lanes)
        inst.state = OverlayLifecycle.ACTIVE
        self.overlays[oid] = inst
        log.info(
            "Overlay ACTIVE %s type=%s node=%s role=%s edge=%s lanes=%s",
            oid,
            overlay_type,
            intersection_id,
            role,
            edge,
            lanes,
        )
        return inst

    def remove_overlay(self, traci_module, overlay_id: str) -> bool:
        inst = self.overlays.get(overlay_id)
        if not inst or inst.state == OverlayLifecycle.REMOVED:
            return False
        touched = self.registry.remove_overlay(overlay_id)
        self.registry.apply_effective(traci_module, touched or inst.target_lanes)
        inst.state = OverlayLifecycle.REMOVED
        log.info("Overlay REMOVED %s", overlay_id)
        return True

    def tick_expiry(self, traci_module, sim_t: float) -> List[str]:
        expired = []
        for oid, inst in list(self.overlays.items()):
            if inst.state != OverlayLifecycle.ACTIVE:
                continue
            if inst.expires_at_s is not None and sim_t >= inst.expires_at_s:
                inst.state = OverlayLifecycle.EXPIRED
                self.remove_overlay(traci_module, oid)
                expired.append(oid)
        return expired

    def active_list(self) -> List[Dict[str, Any]]:
        return [
            {
                "overlay_id": o.overlay_id,
                "type": o.overlay_type,
                "intersection_id": o.intersection_id,
                "direction": o.direction,
                "segment_role": o.segment_role,
                "target_edge": o.target_edge,
                "state": o.state.value,
                "expires_at_s": o.expires_at_s,
            }
            for o in self.overlays.values()
            if o.state == OverlayLifecycle.ACTIVE
        ]
