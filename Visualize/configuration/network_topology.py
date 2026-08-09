"""Static network topology constants (no ParameterRegistry dependency)."""
from __future__ import annotations

from typing import Dict, List, Tuple

OUTGOING_EDGES: Dict[str, List[str]] = {
    "J1": ["J1J2", "J1J3", "J1S1", "J1W1"],
    "J2": ["J2J1", "J2J4", "J2E1", "J2S2"],
    "J3": ["J3J1", "J3J4", "J3N1", "J3W2"],
    "J4": ["J4J2", "J4J3", "J4N2", "J4E2"],
}

TURN_BY_OD: Dict[Tuple[str, str], str] = {
    ("J3J1", "J1W1"): "right", ("J3J1", "J1S1"): "straight", ("J3J1", "J1J2"): "left", ("J3J1", "J1J3"): "straight",
    ("J2J1", "J1J3"): "right", ("J2J1", "J1W1"): "straight", ("J2J1", "J1S1"): "left", ("J2J1", "J1J2"): "straight",
    ("S1J1", "J1J2"): "right", ("S1J1", "J1J3"): "straight", ("S1J1", "J1W1"): "left", ("S1J1", "J1S1"): "straight",
    ("W1J1", "J1S1"): "right", ("W1J1", "J1J2"): "straight", ("W1J1", "J1J3"): "left", ("W1J1", "J1W1"): "straight",
    ("J4J2", "J2J1"): "right", ("J4J2", "J2S2"): "straight", ("J4J2", "J2E1"): "left", ("J4J2", "J2J4"): "straight",
    ("E1J2", "J2J4"): "right", ("E1J2", "J2J1"): "straight", ("E1J2", "J2S2"): "left", ("E1J2", "J2E1"): "straight",
    ("S2J2", "J2E1"): "right", ("S2J2", "J2J4"): "straight", ("S2J2", "J2J1"): "left", ("S2J2", "J2S2"): "straight",
    ("J1J2", "J2S2"): "right", ("J1J2", "J2E1"): "straight", ("J1J2", "J2J4"): "left", ("J1J2", "J2J1"): "straight",
    ("N1J3", "J3W2"): "right", ("N1J3", "J3J1"): "straight", ("N1J3", "J3J4"): "left", ("N1J3", "J3N1"): "straight",
    ("J4J3", "J3N1"): "right", ("J4J3", "J3W2"): "straight", ("J4J3", "J3J1"): "left", ("J4J3", "J3J4"): "straight",
    ("J1J3", "J3J4"): "right", ("J1J3", "J3N1"): "straight", ("J1J3", "J3W2"): "left", ("J1J3", "J3J1"): "straight",
    ("W2J3", "J3J1"): "right", ("W2J3", "J3J4"): "straight", ("W2J3", "J3N1"): "left", ("W2J3", "J3W2"): "straight",
    ("N2J4", "J4J3"): "right", ("N2J4", "J4J2"): "straight", ("N2J4", "J4E2"): "left", ("N2J4", "J4N2"): "straight",
    ("E2J4", "J4N2"): "right", ("E2J4", "J4J3"): "straight", ("E2J4", "J4J2"): "left", ("E2J4", "J4E2"): "straight",
    ("J2J4", "J4E2"): "right", ("J2J4", "J4N2"): "straight", ("J2J4", "J4J3"): "left", ("J2J4", "J4J2"): "straight",
    ("J3J4", "J4J2"): "right", ("J3J4", "J4E2"): "straight", ("J3J4", "J4N2"): "left", ("J3J4", "J4J3"): "straight",
}
