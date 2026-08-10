"""Neighbor graph and downstream corridor mapping for 2x2 grid."""
from __future__ import annotations

from typing import Dict, List, Tuple

# Undirected neighbor adjacency (from INTER_NODE_LINKS topology)
NEIGHBOR_GRAPH: Dict[str, List[str]] = {
    "A": ["B", "C"],
    "B": ["A", "D"],
    "C": ["A", "D"],
    "D": ["B", "C"],
}

# Downstream corridors: node -> list of (direction_on_self, neighbor_id, neighbor_approach)
# When A exits East, traffic arrives at B's West approach.
DOWNSTREAM_CORRIDORS: Dict[str, List[Tuple[str, str, str]]] = {
    "A": [("East", "B", "West"), ("North", "C", "South")],
    "B": [("West", "A", "East"), ("North", "D", "South")],
    "C": [("South", "A", "North"), ("East", "D", "West")],
    "D": [("South", "B", "North"), ("West", "C", "East")],
}

PHASE_NAMES = ["NS_GREEN", "NS_YELLOW", "EW_GREEN", "EW_YELLOW"]

ACTION_NAMES = [
    "KEEP_CURRENT_PHASE",
    "SWITCH_TO_NEXT_PHASE",
    "EXTEND_GREEN_5S",
    "EXTEND_GREEN_10S",
]


def neighbors_of(node_id: str) -> List[str]:
    return list(NEIGHBOR_GRAPH.get(node_id, []))
