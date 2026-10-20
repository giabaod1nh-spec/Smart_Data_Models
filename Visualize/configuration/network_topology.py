"""SUMO topology facade backed by the generated network catalog."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

_CATALOG_PATH = Path(__file__).resolve().parents[1] / "generated" / "network_topology_catalog.json"
with _CATALOG_PATH.open("r", encoding="utf-8") as _stream:
    _CATALOG = json.load(_stream)

OUTGOING_EDGES: Dict[str, List[str]] = {
    str(tls_id): [str(edge_id) for edge_id in edges]
    for tls_id, edges in dict(_CATALOG["outgoing_edges"]).items()
}

TURN_BY_OD: Dict[Tuple[str, str], str] = {
    ("408246393#6", "694973742#0"): "right", ("408246393#6", "408246393#7"): "straight", ("408246393#6", "-35114553#3"): "left",
    ("654858471#7", "654858471#9"): "straight", ("654858471#7", "1179966753#1"): "left", ("654858471#7", "-35113721#14"): "right",
    ("35113721#14", "35113721#15"): "straight", ("-1179966753#1", "-1179966753#0"): "straight", ("654858471#7", "654858471#8"): "straight",
    ("35113721#3", "35113721#4"): "straight", ("658328100#1", "658328100#2"): "straight",
    ("-35113721#6", "-35113721#5"): "straight", ("-658328100#4", "-658328100#3"): "straight",
    ("35113721#9", "35113721#10"): "straight", ("-694973742#4", "-694973742#3"): "straight",
    ("-35113721#12", "-35113721#11"): "straight", ("694973742#1", "694973742#2"): "straight",
    ("1267138238#1", "-694973742#5"): "right", ("1267138238#1", "326771005#0"): "straight", ("1267138238#1", "694942823"): "left",
    ("408246393#7", "-658328100#5"): "right", ("408246393#7", "408246393#8"): "straight", ("408246393#7", "658328100#6"): "left",
    ("1184372243", "658328100#0"): "right", ("1184372243", "1267138238#0"): "straight", ("1184372243", "-1184372244#1"): "left",
}
