"""Unit checks for incident local-bypass helpers (no TraCI required)."""
from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from actuators.incident import (  # noqa: E402
    adjacent_lane_indices,
    blocker_rear_from_front_pos,
)


def test_blocker_rear_uses_front_minus_length() -> None:
    # TraCI lanePosition = front bumper; upstream edge = front - length.
    assert blocker_rear_from_front_pos(100.0, 4.5) == 95.5
    assert blocker_rear_from_front_pos(10.0, 5.0) == 5.0


def test_adjacent_lanes_from_middle_of_three() -> None:
    assert adjacent_lane_indices(1, 3) == [2, 0]


def test_adjacent_lanes_edge_cases() -> None:
    assert adjacent_lane_indices(0, 3) == [1]
    assert adjacent_lane_indices(2, 3) == [1]
    assert adjacent_lane_indices(1, 2) == [0]
    assert adjacent_lane_indices(0, 1) == []


def test_bypass_zone_ordering() -> None:
    blocker = 95.5
    lookahead = 25.0
    safety = 5.0
    start = blocker - lookahead
    deadline = blocker - safety
    assert start < deadline < blocker


if __name__ == "__main__":
    test_blocker_rear_uses_front_minus_length()
    test_adjacent_lanes_from_middle_of_three()
    test_adjacent_lanes_edge_cases()
    test_bypass_zone_ordering()
    print("ok")
