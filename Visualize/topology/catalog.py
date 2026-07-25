"""
Network topology catalog from SUMO intersection.net.xml + config semantics.

Machine-readable SSOT for edges/connections/movements lives in
Visualize/generated/network_topology_catalog.json (not docs/, not YAML).
topology_hash covers edges + connections + TLS↔node mapping only — XML
comments do not affect the hash.
"""
from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import configuration.config as cfg

VISUALIZE_DIR = cfg.PROJECT_ROOT
DEFAULT_NET_PATH = cfg.SUMO_ASSETS_DIR / "intersection.net.xml"
DEFAULT_CATALOG_PATH = cfg.GENERATED_ROOT / "network_topology_catalog.json"
CATALOG_SCHEMA_VERSION = "1.0.0"

DIR_TO_MOVEMENT = {
    "r": "right",
    "s": "straight",
    "l": "left",
    "t": "turnaround",
}

# Semantic inter-node corridor links (outgoing of upstream = incoming of downstream).
INTER_NODE_LINKS: Dict[str, Dict[str, str]] = {
    "A_E_to_B_W": {
        "edge": "J1J2",
        "from_node": "A",
        "to_node": "B",
        "from_approach": "East",
        "to_approach": "West",
        "segment_role_note": "A East exit and B West approach",
    },
    "B_W_to_A_E": {
        "edge": "J2J1",
        "from_node": "B",
        "to_node": "A",
        "from_approach": "West",
        "to_approach": "East",
        "segment_role_note": "B West exit and A East approach",
    },
    "A_N_to_C_S": {
        "edge": "J1J3",
        "from_node": "A",
        "to_node": "C",
        "from_approach": "North",
        "to_approach": "South",
        "segment_role_note": "A North exit and C South approach",
    },
    "C_S_to_A_N": {
        "edge": "J3J1",
        "from_node": "C",
        "to_node": "A",
        "from_approach": "South",
        "to_approach": "North",
        "segment_role_note": "C South exit and A North approach",
    },
    "B_N_to_D_S": {
        "edge": "J2J4",
        "from_node": "B",
        "to_node": "D",
        "from_approach": "North",
        "to_approach": "South",
        "segment_role_note": "B North exit and D South approach",
    },
    "D_S_to_B_N": {
        "edge": "J4J2",
        "from_node": "D",
        "to_node": "B",
        "from_approach": "South",
        "to_approach": "North",
        "segment_role_note": "D South exit and B North approach",
    },
    "C_E_to_D_W": {
        "edge": "J3J4",
        "from_node": "C",
        "to_node": "D",
        "from_approach": "East",
        "to_approach": "West",
        "segment_role_note": "C East exit and D West approach",
    },
    "D_W_to_C_E": {
        "edge": "J4J3",
        "from_node": "D",
        "to_node": "C",
        "from_approach": "West",
        "to_approach": "East",
        "segment_role_note": "D West exit and C East approach",
    },
}


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_net(path: Path | str | None = None) -> Dict[str, Any]:
    """Parse normal (non-internal) edges and all connections from .net.xml."""
    net_path = Path(path) if path else DEFAULT_NET_PATH
    if not net_path.is_file():
        raise FileNotFoundError(f"SUMO net file not found: {net_path}")

    root = ET.parse(net_path).getroot()
    edges: List[Dict[str, Any]] = []
    for edge_el in root.findall("edge"):
        edge_id = edge_el.get("id") or ""
        if edge_el.get("function") == "internal" or edge_id.startswith(":"):
            continue
        lanes = edge_el.findall("lane")
        edges.append(
            {
                "id": edge_id,
                "from": edge_el.get("from") or "",
                "to": edge_el.get("to") or "",
                "numLanes": len(lanes),
            }
        )
    edges.sort(key=lambda e: e["id"])

    connections: List[Dict[str, Any]] = []
    for conn_el in root.findall("connection"):
        link_raw = conn_el.get("linkIndex")
        connections.append(
            {
                "from": conn_el.get("from") or "",
                "to": conn_el.get("to") or "",
                "fromLane": int(conn_el.get("fromLane") or 0),
                "toLane": int(conn_el.get("toLane") or 0),
                "via": conn_el.get("via"),
                "tl": conn_el.get("tl"),
                "linkIndex": int(link_raw) if link_raw is not None else None,
                "dir": conn_el.get("dir") or "",
            }
        )
    connections.sort(
        key=lambda c: (
            c["from"],
            c["to"],
            c["fromLane"],
            c["toLane"],
            c["linkIndex"] if c["linkIndex"] is not None else -1,
            c["via"] or "",
            c["dir"],
        )
    )
    return {"edges": edges, "connections": connections, "source_net": str(net_path)}


def _hash_payload(
    edges: Sequence[Mapping[str, Any]],
    connections: Sequence[Mapping[str, Any]],
    node_to_tls: Mapping[str, str],
) -> Dict[str, Any]:
    """Canonical subset hashed for topology_hash (no comments / metadata)."""
    return {
        "connections": list(connections),
        "edges": list(edges),
        "node_to_tls": dict(sorted(node_to_tls.items())),
    }


def compute_topology_hash(
    edges: Sequence[Mapping[str, Any]],
    connections: Sequence[Mapping[str, Any]],
    node_to_tls: Mapping[str, str] | None = None,
) -> str:
    mapping = node_to_tls if node_to_tls is not None else cfg.NODE_TO_TLS
    return _sha256_text(_canonical_json(_hash_payload(edges, connections, mapping)))


def _edge_index(edges: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    return {str(e["id"]): e for e in edges}


def _build_movements_by_approach(
    connections: Sequence[Mapping[str, Any]],
    approach_edges: Mapping[str, Mapping[str, str]],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Group signalized movements by TLS approach from net <connection> rows."""
    by_from: Dict[str, List[Mapping[str, Any]]] = {}
    for conn in connections:
        by_from.setdefault(str(conn["from"]), []).append(conn)

    movements: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for tls_id, dirs in approach_edges.items():
        movements[tls_id] = {}
        for direction, edge_id in dirs.items():
            rows: List[Dict[str, Any]] = []
            for conn in by_from.get(edge_id, []):
                # Skip dead-end turnarounds without TLS when building approach movements
                # but keep TLS-controlled and any connection leaving the approach.
                dir_code = str(conn.get("dir") or "")
                rows.append(
                    {
                        "from": conn["from"],
                        "to": conn["to"],
                        "fromLane": conn["fromLane"],
                        "toLane": conn["toLane"],
                        "via": conn.get("via"),
                        "tl": conn.get("tl"),
                        "linkIndex": conn.get("linkIndex"),
                        "dir": dir_code,
                        "movement": DIR_TO_MOVEMENT.get(dir_code, dir_code or "unknown"),
                    }
                )
            rows.sort(
                key=lambda r: (
                    r["fromLane"],
                    r["linkIndex"] if r["linkIndex"] is not None else -1,
                    r["to"],
                    r["toLane"],
                )
            )
            movements[tls_id][direction] = rows
    return movements


def build_catalog(net_path: Path | str | None = None) -> Dict[str, Any]:
    """Build full topology catalog from net XML + config semantic maps."""
    parsed = parse_net(net_path)
    edges = parsed["edges"]
    connections = parsed["connections"]
    node_to_tls = dict(cfg.NODE_TO_TLS)
    tls_to_node = dict(cfg.TLS_TO_NODE)
    approach_edges = {tls: dict(dirs) for tls, dirs in cfg.APPROACH_EDGES.items()}
    outgoing_edges = {tls: list(edges_) for tls, edges_ in cfg.OUTGOING_EDGES.items()}

    topology_hash = compute_topology_hash(edges, connections, node_to_tls)
    movements = _build_movements_by_approach(connections, approach_edges)

    inter_node_links: Dict[str, Dict[str, Any]] = {}
    edge_by_id = _edge_index(edges)
    for link_id, meta in INTER_NODE_LINKS.items():
        edge_id = meta["edge"]
        edge = edge_by_id.get(edge_id)
        inter_node_links[link_id] = {
            **meta,
            "numLanes": int(edge["numLanes"]) if edge else None,
            "from_junction": edge["from"] if edge else None,
            "to_junction": edge["to"] if edge else None,
        }

    catalog: Dict[str, Any] = {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "source_net": "Visualize/intersection.net.xml",
        "topology_hash": topology_hash,
        "node_to_tls": node_to_tls,
        "tls_to_node": tls_to_node,
        "approach_edges": approach_edges,
        "outgoing_edges": outgoing_edges,
        "inter_node_links": inter_node_links,
        "edges": edges,
        "connections": connections,
        "movements_by_approach": movements,
        "lane_movement_convention_note": (
            "ADR-002 lane index convention is project guidance only; "
            "movements_by_approach is SSOT from net <connection> (a lane may have multiple dirs)."
        ),
    }
    return catalog


def validate_catalog(catalog: Mapping[str, Any]) -> List[str]:
    """Return list of validation errors (empty = OK)."""
    errors: List[str] = []
    required = (
        "topology_hash",
        "node_to_tls",
        "edges",
        "connections",
        "inter_node_links",
        "movements_by_approach",
        "approach_edges",
        "outgoing_edges",
    )
    for key in required:
        if key not in catalog:
            errors.append(f"missing key: {key}")
    if errors:
        return errors

    edges = list(catalog["edges"])
    connections = list(catalog["connections"])
    node_to_tls = dict(catalog["node_to_tls"])
    expected_hash = compute_topology_hash(edges, connections, node_to_tls)
    if catalog["topology_hash"] != expected_hash:
        errors.append(
            f"topology_hash mismatch: catalog={catalog['topology_hash']} expected={expected_hash}"
        )

    if dict(cfg.NODE_TO_TLS) != node_to_tls:
        errors.append("node_to_tls does not match config.NODE_TO_TLS")

    edge_ids = {str(e["id"]) for e in edges}
    for tls_id, dirs in cfg.APPROACH_EDGES.items():
        for direction, edge_id in dirs.items():
            if edge_id not in edge_ids:
                errors.append(f"approach edge missing: {tls_id}/{direction}={edge_id}")
    for tls_id, out_edges in cfg.OUTGOING_EDGES.items():
        for edge_id in out_edges:
            if edge_id not in edge_ids:
                errors.append(f"outgoing edge missing: {tls_id}/{edge_id}")

    for link_id, expected in INTER_NODE_LINKS.items():
        got = catalog["inter_node_links"].get(link_id)
        if not got:
            errors.append(f"inter_node_link missing: {link_id}")
            continue
        if got.get("edge") != expected["edge"]:
            errors.append(
                f"inter_node_link {link_id} edge={got.get('edge')} expected={expected['edge']}"
            )
        if expected["edge"] not in edge_ids:
            errors.append(f"inter_node_link edge not in net: {link_id}={expected['edge']}")

    for tls_id, dirs in cfg.APPROACH_EDGES.items():
        tls_moves = catalog["movements_by_approach"].get(tls_id) or {}
        for direction, edge_id in dirs.items():
            rows = tls_moves.get(direction)
            if rows is None:
                errors.append(f"movements missing for {tls_id}/{direction}")
                continue
            if not rows:
                errors.append(f"no connections for approach {tls_id}/{direction} edge={edge_id}")
            for row in rows:
                if row.get("from") != edge_id:
                    errors.append(
                        f"movement from mismatch {tls_id}/{direction}: {row.get('from')} != {edge_id}"
                    )

    return errors


def load_catalog(path: Path | str | None = None) -> Dict[str, Any]:
    catalog_path = Path(path) if path else DEFAULT_CATALOG_PATH
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Topology catalog not found: {catalog_path}")
    with catalog_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid topology catalog root in {catalog_path}")
    return data


def write_catalog(path: Path | str | None = None, catalog: Optional[Mapping[str, Any]] = None) -> Path:
    """Write catalog JSON; create parent dir and .gitkeep if needed."""
    catalog_path = Path(path) if path else DEFAULT_CATALOG_PATH
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    gitkeep = catalog_path.parent / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")

    data: MutableMapping[str, Any] = dict(catalog) if catalog is not None else build_catalog()
    errors = validate_catalog(data)
    if errors:
        raise ValueError("topology catalog validation failed:\n- " + "\n- ".join(errors))

    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    catalog_path.write_text(text, encoding="utf-8")
    return catalog_path
