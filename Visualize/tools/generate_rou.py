"""Generate intersection.rou.xml with weighted composition + full O-D turn split."""
from __future__ import annotations

import sys
from pathlib import Path
from xml.etree import ElementTree as ET

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from configuration.demand_profiles import (
    CONTAINER_PARAMS,
    DIAGONAL_VEH_PER_HOUR,
    VTYPE_GUI,
    car_vtype_attrs,
    moto_vtype_attrs,
    split_flow,
)
from configuration.model_params import get_registry
from configuration.lane_policy import preferred_depart_lane

OUT = _ROOT / "Visualize" / "intersection.rou.xml"
NET = _ROOT / "Visualize" / "intersection.net.xml"

# The former synthetic-grid diagonals do not exist in the cropped OSM net.
DIAGONALS: list[tuple[str, str, str, str]] = []


def _attrs(d: dict) -> str:
    return " ".join(f'{k}="{v}"' for k, v in d.items())


def _vtype_block(vtype_id: str, attrs: dict, params: dict | None = None) -> list[str]:
    if params:
        lines = [f'    <vType id="{vtype_id}" {_attrs(attrs)}>']
        for k, v in params.items():
            lines.append(f'        <param key="{k}" value="{v}"/>')
        lines.append("    </vType>")
        return lines
    return [f'    <vType id="{vtype_id}" {_attrs(attrs)}/>']


def _scaled_vph(vph: float, weight: float) -> float:
    """Preserve low-volume classes when distributing them across turns.

    Rounding to an integer made truck/container/emergency flows disappear
    because their per-turn demand is below 1 veh/h. SUMO accepts fractional
    vehsPerHour values, so retain three decimals here.
    """
    return max(0.0, round(vph * weight, 3))


def _flow_probability(vph: float) -> float:
    """Convert expected vehicles/hour to SUMO per-second flow probability.

    Unlike ``vehsPerHour``, a probabilistic flow does not force the first
    vehicle of every flow to depart at ``begin=0``. With SUMO's fixed seed the
    resulting demand remains reproducible while avoiding the startup burst.
    """
    return max(0.0, min(1.0, float(vph) / 3600.0))


def _edge_lane_counts() -> dict[str, int]:
    root = ET.parse(NET).getroot()
    return {
        edge.attrib["id"]: len(edge.findall("lane"))
        for edge in root.findall("edge")
        if not edge.attrib.get("function")
    }


def main() -> None:
    reg = get_registry(reload=True)
    sources = reg.boundary_sources()
    lane_counts = _edge_lane_counts()
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<!-- Weighted demand + O-D turn split + soft VN mixed-traffic lane preference -->",
        '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">',
        "",
        "    <!-- Loại xe + sprite PNG (đường dẫn tương đối so với sumocfg) -->",
    ]
    lines.extend(_vtype_block("motorcycle", moto_vtype_attrs()))
    for vid, attrs in VTYPE_GUI.items():
        if vid == "car":
            attrs = car_vtype_attrs()
        params = CONTAINER_PARAMS if vid == "container" else None
        lines.extend(_vtype_block(vid, attrs, params))
    lines.append("")

    for source_id, src in sorted(sources.items()):
        fr = src["source_edge"]
        baseline = float(src["baseline_veh_per_hour"])
        trunk = split_flow(baseline)
        lines.append(
            f"    <!-- corridor {source_id}: {fr} @ {src['turn_at_tls']} "
            f"({baseline:.0f} veh/h baseline) -->"
        )
        for dest in src["destinations"]:
            mov = dest["movement"]
            to = dest["to_edge"]
            via = dest["turn_edge"]
            weight = float(dest["weight"])
            for vtype, vph in trunk.items():
                scaled = _scaled_vph(vph, weight)
                if scaled <= 0:
                    continue
                flow_id = f"{source_id}_{mov}_{vtype}"
                depart_lane = preferred_depart_lane(
                    vtype, mov, lane_counts.get(fr, 1)
                )
                probability = _flow_probability(scaled)
                lines.append(
                    f'    <flow id="{flow_id}" type="{vtype}" from="{fr}" to="{to}" '
                    f'via="{via}" begin="0" end="3600" probability="{probability:.9g}" '
                    f'departLane="{depart_lane}"/>'
                )
        lines.append("")

    if DIAGONALS:
        diag_vph = max(1, int(round(DIAGONAL_VEH_PER_HOUR / len(DIAGONALS))))
        lines.append("    <!-- light diagonal flows -->")
        for fid, fr, to, vtype in DIAGONALS:
            probability = _flow_probability(diag_vph)
            lines.append(
                f'    <flow id="{fid}" type="{vtype}" from="{fr}" to="{to}" '
                f'begin="0" end="3600" probability="{probability:.9g}"/>'
            )
    lines.append("</routes>")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} with turn-split flows for {len(sources)} boundary sources")


if __name__ == "__main__":
    main()
