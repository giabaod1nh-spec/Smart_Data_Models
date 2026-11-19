"""Generate the runtime topology from SUMO XML and a curated logical-node selection."""
from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

VISUALIZE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_NET_PATH = VISUALIZE_DIR / "Visualize" / "intersection.net.xml"
DEFAULT_CATALOG_PATH = VISUALIZE_DIR / "generated" / "network_topology_catalog.json"
DEFAULT_SELECTION_PATH = Path(__file__).with_name("selected_intersections.json")
CATALOG_SCHEMA_VERSION = "1.1.0"

DIR_TO_MOVEMENT = {
    "r": "right",
    "s": "straight",
    "l": "left",
    "t": "turnaround",
}

# Semantic inter-node corridor links (outgoing of upstream = incoming of downstream).
# The selected physical intersections are separated by intermediate OSM
# junctions, so there is no single edge that is a direct node-to-node link.
INTER_NODE_LINKS: Dict[str, Dict[str, str]] = {}


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
    location_el = root.find("location")
    junction_locations: Dict[str, Dict[str, float]] = {}
    if location_el is not None:
        conv = [float(value) for value in (location_el.get("convBoundary") or "").split(",")]
        orig = [float(value) for value in (location_el.get("origBoundary") or "").split(",")]
        if len(conv) == 4 and len(orig) == 4 and conv[2] != conv[0] and conv[3] != conv[1]:
            for junction_el in root.findall("junction"):
                junction_id = junction_el.get("id") or ""
                if not junction_id or junction_id.startswith(":"):
                    continue
                x = float(junction_el.get("x") or 0.0)
                y = float(junction_el.get("y") or 0.0)
                lng = orig[0] + ((x - conv[0]) / (conv[2] - conv[0])) * (orig[2] - orig[0])
                lat = orig[1] + ((y - conv[1]) / (conv[3] - conv[1])) * (orig[3] - orig[1])
                junction_locations[junction_id] = {
                    "x": round(x, 2),
                    "y": round(y, 2),
                    "lng": round(lng, 7),
                    "lat": round(lat, 7),
                }
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
                "lengthM": float(lanes[0].get("length")) if lanes else 0.0,
                "laneSpeedsMps": [float(lane.get("speed") or 0.0) for lane in lanes],
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
    signal_programs: Dict[str, List[Dict[str, Any]]] = {}
    for logic in root.findall("tlLogic"):
        signal_programs.setdefault(logic.get("id") or "", []).append({
            "programID": logic.get("programID"),
            "type": logic.get("type"),
            "offset": logic.get("offset"),
            "phases": [
                {"duration": phase.get("duration"), "state": phase.get("state")}
                for phase in logic.findall("phase")
            ],
        })
    return {
        "edges": edges,
        "connections": connections,
        "junction_locations": junction_locations,
        "junction_types": {
            j.get("id"): j.get("type") for j in root.findall("junction")
            if j.get("id") and not j.get("id").startswith(":")
        },
        "signal_programs": signal_programs,
        "source_net": str(net_path),
    }


def _hash_payload(
    edges: Sequence[Mapping[str, Any]],
    connections: Sequence[Mapping[str, Any]],
    node_to_tls: Mapping[str, str],
    approach_edges: Mapping[str, Mapping[str, str]],
    sensing_edges: Mapping[str, Mapping[str, Sequence[str]]],
    phase_name_to_index: Mapping[str, Mapping[str, int]],
    signal_program_hash: str,
) -> Dict[str, Any]:
    """Canonical runtime topology and selected signal behavior (no XML comments)."""
    return {
        "connections": list(connections),
        "edges": list(edges),
        "node_to_tls": dict(sorted(node_to_tls.items())),
        "approach_edges": approach_edges,
        "sensing_edges": sensing_edges,
        "phase_name_to_index": phase_name_to_index,
        "signal_program_hash": signal_program_hash,
    }


def compute_topology_hash(
    edges: Sequence[Mapping[str, Any]],
    connections: Sequence[Mapping[str, Any]],
    node_to_tls: Mapping[str, str],
    approach_edges: Mapping[str, Mapping[str, str]],
    sensing_edges: Mapping[str, Mapping[str, Sequence[str]]],
    phase_name_to_index: Mapping[str, Mapping[str, int]],
    signal_program_hash: str,
) -> str:
    return _sha256_text(_canonical_json(_hash_payload(
        edges, connections, node_to_tls, approach_edges, sensing_edges,
        phase_name_to_index, signal_program_hash,
    )))


def load_selection(path: Path | str = DEFAULT_SELECTION_PATH) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as stream:
        return json.load(stream)


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
    """Build full topology catalog from net XML and explicit logical selection."""
    parsed = parse_net(net_path)
    selection = load_selection()
    edges = parsed["edges"]
    connections = parsed["connections"]
    node_to_tls = {node: spec["primary_tls"] for node, spec in selection.items()}
    node_to_tls_groups = {
        node: [spec["primary_tls"], *spec.get("other_tls", [])]
        for node, spec in selection.items()
    }
    tls_to_node = {tls: node for node, group in node_to_tls_groups.items() for tls in group}
    grouped_tls_axes: Dict[str, Dict[str, List[str]]] = {}
    node_locations = {
        node_id: dict(parsed["junction_locations"][tls_id])
        for node_id, tls_id in node_to_tls.items()
        if tls_id in parsed["junction_locations"]
    }
    approach_edges = {spec["primary_tls"]: dict(spec["approach_edges"])
                      for spec in selection.values()}
    edge_by_id = _edge_index(edges)
    sensing_edges: Dict[str, Dict[str, List[str]]] = {}
    for tls, dirs in approach_edges.items():
        sensing_edges[tls] = {}
        for direction, approach in dirs.items():
            corridor = [approach]
            if float(edge_by_id[approach]["lengthM"]) < 15.0:
                predecessors = {
                    str(row["from"]) for row in connections
                    if row["to"] == approach and row["dir"] == "s"
                    and row["from"] in edge_by_id
                }
                if len(predecessors) == 1:
                    corridor.insert(0, next(iter(predecessors)))
            sensing_edges[tls][direction] = corridor
    outgoing_edges = {
        tls: sorted({e["id"] for e in edges if e["from"] == tls})
        for tls in node_to_tls.values()
    }
    phase_name_to_index = {
        node: dict(spec["phase_name_to_index"]) for node, spec in selection.items()
    }
    selected_programs = {
        tls: parsed["signal_programs"].get(tls, []) for tls in node_to_tls.values()
    }
    signal_program_hash = _sha256_text(_canonical_json(selected_programs))

    topology_hash = compute_topology_hash(
        edges, connections, node_to_tls, approach_edges, sensing_edges,
        phase_name_to_index, signal_program_hash,
    )
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
        "signal_program_hash": signal_program_hash,
        "signal_programs": selected_programs,
        "phase_name_to_index": phase_name_to_index,
        "junction_types": parsed["junction_types"],
        "node_to_tls": node_to_tls,
        "node_to_tls_groups": node_to_tls_groups,
        "tls_to_node": tls_to_node,
        "grouped_tls_axes": grouped_tls_axes,
        "node_locations": node_locations,
        "approach_edges": approach_edges,
        "sensing_edges": sensing_edges,
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
        "signal_program_hash",
        "signal_programs",
        "phase_name_to_index",
        "junction_types",
        "node_to_tls",
        "node_to_tls_groups",
        "tls_to_node",
        "grouped_tls_axes",
        "node_locations",
        "edges",
        "connections",
        "inter_node_links",
        "movements_by_approach",
        "approach_edges",
        "sensing_edges",
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
    expected_hash = compute_topology_hash(
        edges, connections, node_to_tls, catalog["approach_edges"],
        catalog["sensing_edges"], catalog["phase_name_to_index"],
        catalog["signal_program_hash"],
    )
    if catalog["topology_hash"] != expected_hash:
        errors.append(
            f"topology_hash mismatch: catalog={catalog['topology_hash']} expected={expected_hash}"
        )

    selection = load_selection()
    if {node: spec["primary_tls"] for node, spec in selection.items()} != node_to_tls:
        errors.append("node_to_tls does not match selected_intersections.json")
    expected_program_hash = _sha256_text(_canonical_json(catalog["signal_programs"]))
    if catalog["signal_program_hash"] != expected_program_hash:
        errors.append("signal_program_hash mismatch")

    node_to_tls_groups = catalog["node_to_tls_groups"]
    tls_to_node = catalog["tls_to_node"]
    for node_id, primary_tls in node_to_tls.items():
        tls_group = list(node_to_tls_groups.get(node_id) or [])
        if not tls_group:
            errors.append(f"node_to_tls_groups missing: {node_id}")
            continue
        if tls_group[0] != primary_tls:
            errors.append(
                f"primary TLS must be first in group: {node_id}={primary_tls} group={tls_group}"
            )
        for tls_id in tls_group:
            if tls_to_node.get(tls_id) != node_id:
                errors.append(
                    f"tls_to_node mismatch: {tls_id}={tls_to_node.get(tls_id)!r} expected={node_id!r}"
                )

    for node_id, axes in catalog["grouped_tls_axes"].items():
        group = set(node_to_tls_groups.get(node_id) or [])
        for axis, tls_ids in axes.items():
            unknown = set(tls_ids) - group
            if unknown:
                errors.append(
                    f"grouped_tls_axes outside TLS group: {node_id}/{axis}={sorted(unknown)}"
                )

    for node_id in node_to_tls:
        location = catalog["node_locations"].get(node_id)
        if not location:
            errors.append(f"node_locations missing: {node_id}")
        elif not all(key in location for key in ("x", "y", "lat", "lng")):
            errors.append(f"node_locations incomplete: {node_id}")

    edge_by_id = _edge_index(edges)
    edge_ids = set(edge_by_id)
    for tls_id, dirs in catalog["approach_edges"].items():
        if catalog["junction_types"].get(tls_id) != "traffic_light":
            errors.append(f"selected junction is not traffic_light: {tls_id}")
        programs = catalog["signal_programs"].get(tls_id) or []
        if len(programs) != 1 or len(programs[0].get("phases", [])) != 4:
            errors.append(f"selected TLS must have one four-phase program: {tls_id}")
        link_indexes = [c["linkIndex"] for c in connections if c["tl"] == tls_id]
        if not link_indexes:
            errors.append(f"selected TLS has no controlled connections: {tls_id}")
        elif programs:
            for phase in programs[0].get("phases", []):
                if len(phase.get("state") or "") <= max(link_indexes):
                    errors.append(f"TLS phase state too short for link indexes: {tls_id}")
        for direction, edge_id in dirs.items():
            edge = edge_by_id.get(edge_id)
            if edge is None:
                errors.append(f"approach edge missing: {tls_id}/{direction}={edge_id}")
            elif edge["to"] != tls_id:
                errors.append(f"approach does not enter selected TLS: {tls_id}/{direction}={edge_id}")
            corridor = catalog["sensing_edges"].get(tls_id, {}).get(direction, [])
            if not corridor or corridor[-1] != edge_id:
                errors.append(f"sensing corridor must end at selected approach: {tls_id}/{direction}")
            for sensing_edge in corridor:
                if sensing_edge not in edge_ids:
                    errors.append(f"sensing edge missing: {tls_id}/{direction}={sensing_edge}")
            if len(corridor) > 1:
                first = edge_by_id.get(corridor[0])
                if first and edge and first["numLanes"] != edge["numLanes"]:
                    errors.append(f"sensing lane count differs: {tls_id}/{direction}")
                if not any(c["from"] == corridor[0] and c["to"] == edge_id
                           and c["dir"] == "s" for c in connections):
                    errors.append(f"sensing edge is not a straight predecessor: {tls_id}/{direction}")
    for tls_id, out_edges in catalog["outgoing_edges"].items():
        for edge_id in out_edges:
            if edge_id not in edge_ids:
                errors.append(f"outgoing edge missing: {tls_id}/{edge_id}")
            elif edge_by_id[edge_id]["from"] != tls_id:
                errors.append(f"outgoing edge does not leave selected TLS: {tls_id}/{edge_id}")

    for node, spec in selection.items():
        tls_id = spec["primary_tls"]
        if catalog["approach_edges"].get(tls_id) != spec["approach_edges"]:
            errors.append(f"approach selection mismatch: {node}")
        names = catalog["phase_name_to_index"].get(node)
        if names != spec["phase_name_to_index"]:
            errors.append(f"phase selection mismatch: {node}")
        if names and sorted(names.values()) != [0, 1, 2, 3]:
            errors.append(f"phase indexes invalid: {node}")
        if not names or not catalog["signal_programs"].get(tls_id):
            continue
        phases = catalog["signal_programs"][tls_id][0]["phases"]
        for direction, approach in spec["approach_edges"].items():
            links = [c["linkIndex"] for c in connections
                     if c["tl"] == tls_id and c["from"] == approach and c["dir"] == "s"]
            if not links:
                errors.append(f"no straight controlled link: {node}/{direction}")
                continue
            green_name = ("NS" if direction in ("North", "South") else "EW") + "_GREEN"
            green_state = phases[names[green_name]]["state"]
            yellow_name = green_name.replace("GREEN", "YELLOW")
            yellow_state = phases[names[yellow_name]]["state"]
            opposite_name = ("EW" if green_name.startswith("NS") else "NS") + "_GREEN"
            opposite_state = phases[names[opposite_name]]["state"]
            if any(green_state[i] not in "Gg" or opposite_state[i] != "r" for i in links):
                errors.append(f"TLS phase does not serve {node}/{direction} correctly")
            if any(yellow_state[i] not in "yY" for i in links):
                errors.append(f"TLS yellow phase does not serve {node}/{direction} correctly")

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

    for tls_id, dirs in catalog["approach_edges"].items():
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


def verify_against_net(
    net_path: Path | str = DEFAULT_NET_PATH,
    catalog_path: Path | str = DEFAULT_CATALOG_PATH,
) -> List[str]:
    """Compare the generated catalog with the exact net used by SUMO."""
    catalog = load_catalog(catalog_path)
    errors = validate_catalog(catalog)
    current = build_catalog(net_path)
    for key in ("topology_hash", "signal_program_hash", "node_to_tls",
                "approach_edges", "sensing_edges", "outgoing_edges",
                "phase_name_to_index"):
        if catalog.get(key) != current.get(key):
            errors.append(f"catalog {key} differs from the current SUMO net/selection")
    return errors


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
