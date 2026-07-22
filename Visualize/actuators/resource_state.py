"""ResourceStateRegistry — original + patches; compose effective state."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# Higher priority applied later in compose (overrides lower for same key).
OVERLAY_PRIORITY_ORDER = ("heavy_rain", "downstream_restriction", "accident", "blocked_intersection", "emergency")


@dataclass
class ResourcePatch:
    overlay_id: str
    overlay_type: str
    priority: int
    allowed: Optional[List[str]] = None  # None = no change
    max_speed: Optional[float] = None


@dataclass
class ResourceRecord:
    resource_id: str  # lane id
    original_allowed: List[str] = field(default_factory=list)
    original_max_speed: float = 13.9
    patches: Dict[str, ResourcePatch] = field(default_factory=dict)


class ResourceStateRegistry:
    def __init__(self) -> None:
        self._resources: Dict[str, ResourceRecord] = {}

    def capture_lane(self, traci_module, lane_id: str) -> None:
        if lane_id in self._resources:
            return
        try:
            allowed = list(traci_module.lane.getAllowed(lane_id) or [])
            if not allowed:
                allowed = ["all"]
            speed = float(traci_module.lane.getMaxSpeed(lane_id))
        except Exception:
            allowed = ["all"]
            speed = 13.9
        self._resources[lane_id] = ResourceRecord(
            resource_id=lane_id,
            original_allowed=allowed,
            original_max_speed=speed,
        )

    def add_patch(self, lane_id: str, patch: ResourcePatch) -> None:
        rec = self._resources.get(lane_id)
        if rec is None:
            raise KeyError(f"Lane {lane_id} not captured")
        rec.patches[patch.overlay_id] = patch

    def remove_patch(self, lane_id: str, overlay_id: str) -> None:
        rec = self._resources.get(lane_id)
        if rec and overlay_id in rec.patches:
            del rec.patches[overlay_id]

    def remove_overlay(self, overlay_id: str) -> List[str]:
        touched = []
        for lid, rec in self._resources.items():
            if overlay_id in rec.patches:
                del rec.patches[overlay_id]
                touched.append(lid)
        return touched

    def compose(self, lane_id: str) -> Tuple[List[str], float]:
        rec = self._resources[lane_id]
        allowed = list(rec.original_allowed)
        speed = float(rec.original_max_speed)
        patches = sorted(rec.patches.values(), key=lambda p: p.priority)
        for p in patches:
            if p.allowed is not None:
                allowed = list(p.allowed)
            if p.max_speed is not None:
                speed = float(p.max_speed)
        return allowed, speed

    def apply_effective(self, traci_module, lane_ids: Optional[List[str]] = None) -> None:
        ids = lane_ids or list(self._resources.keys())
        for lid in ids:
            if lid not in self._resources:
                continue
            allowed, speed = self.compose(lid)
            try:
                if allowed == ["all"] or (len(allowed) == 1 and allowed[0] == "all"):
                    traci_module.lane.setAllowed(lid, ["all"])
                elif not allowed:
                    traci_module.lane.setDisallowed(lid, ["all"])
                else:
                    traci_module.lane.setAllowed(lid, allowed)
                traci_module.lane.setMaxSpeed(lid, max(0.5, speed))
            except Exception as e:
                log.warning("apply_effective %s failed: %s", lid, e)
