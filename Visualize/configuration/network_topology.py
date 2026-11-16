"""Read-only network topology facade backed by the generated SUMO catalog."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

_CATALOG_PATH = Path(__file__).resolve().parents[1] / "generated" / "network_topology_catalog.json"
with _CATALOG_PATH.open("r", encoding="utf-8") as _stream:
    _CATALOG = json.load(_stream)

OUTGOING_EDGES: Dict[str, List[str]] = {
    str(tls_id): [str(edge_id) for edge_id in edges]
    for tls_id, edges in _CATALOG["outgoing_edges"].items()
}
KNOWN_TLS = frozenset(_CATALOG["tls_to_node"])
LOGICAL_NODES = frozenset(_CATALOG["node_to_tls"])
JUNCTION_TYPES = dict(_CATALOG["junction_types"])
EDGE_BY_ID = {str(edge["id"]): edge for edge in _CATALOG["edges"]}
CONNECTIONS = tuple(_CATALOG["connections"])
_DIR_MOVEMENT = {"l": "left", "s": "straight", "r": "right"}
TURN_BY_OD: Dict[Tuple[str, str], str] = {
    (str(row["from"]), str(row["to"])): _DIR_MOVEMENT[str(row["dir"])]
    for row in CONNECTIONS
    if str(row["dir"]) in _DIR_MOVEMENT
}
