"""Soft lane preferences for Vietnam-style mixed traffic.

SUMO lane index 0 is the right-most lane on a normal right-hand-traffic edge.
This module deliberately returns a *departure preference*, not an allow/disallow
rule: vehicles remain free to change lane for their route near an intersection.
"""
from __future__ import annotations


def movement_from_route_key(route_key: str) -> str:
    key = str(route_key).lower()
    if key.endswith("_left") or "left" in key:
        return "left"
    if key.endswith("_right") or "right" in key:
        return "right"
    return "straight"


def preferred_depart_lane(vtype: str, movement: str, lane_count: int) -> str:
    """Return a valid preferred lane index as a SUMO string.

    Policy:
    - one lane: everybody uses lane 0;
    - two lanes: lane 0 favours motorcycles/right turns, lane 1 straight/left;
    - three or more: lane 0 favours motorcycles/right turns, middle lane is
      general straight traffic, left-most lane is for left turns.

    Motorcycles going left start in the left lane so their route stays valid;
    this is the intentional exception to the right-lane motorcycle preference.
    """
    count = max(1, int(lane_count))
    if count == 1:
        return "0"

    move = str(movement).lower()
    if move == "right":
        return "0"
    if move == "left":
        return str(count - 1)
    if str(vtype).lower() == "motorcycle":
        return "0"
    return "1"
