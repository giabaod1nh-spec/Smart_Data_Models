"""Neighbor graph and downstream corridors for the selected OSM signals."""
from __future__ import annotations

from typing import Dict, List, Tuple

# Undirected neighbor adjacency (from INTER_NODE_LINKS topology)
NEIGHBOR_GRAPH: Dict[str, List[str]] = {
    "A": ["B", "M"],
    "B": ["A"],
    "C": ["M", "D"],
    "D": ["C"],
    "M": ["A", "C"],
}

# Downstream corridors: node -> list of (direction_on_self, neighbor_id, neighbor_approach)
# When A exits East, traffic arrives at B's West approach.
DOWNSTREAM_CORRIDORS: Dict[str, List[Tuple[str, str, str]]] = {
    "A": [("East", "M", "West")],
    "B": [],
    "C": [],
    "D": [],
    "M": [("North", "C", "South")],
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
