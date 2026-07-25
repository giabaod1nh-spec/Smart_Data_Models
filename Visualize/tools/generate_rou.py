"""Generate intersection.rou.xml with weighted composition + GUI imgFile sprites."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from configuration.demand_profiles import (
    BASE_TRUNK_VEH_PER_HOUR,
    CONTAINER_PARAMS,
    DIAGONAL_VEH_PER_HOUR,
    VTYPE_GUI,
    moto_vtype_attrs,
    split_flow,
)

OUT = _ROOT / "Visualize" / "intersection.rou.xml"

TRUNKS = [
    ("ns_w", "N1J3", "J1S1"),
    ("sn_w", "S1J1", "J3N1"),
    ("ns_e", "N2J4", "J2S2"),
    ("sn_e", "S2J2", "J4N2"),
    ("we_s", "W1J1", "J2E1"),
    ("ew_s", "E1J2", "J1W1"),
    ("we_n", "W2J3", "J4E2"),
    ("ew_n", "E2J4", "J3W2"),
]

DIAGONALS = [
    ("x1", "N1J3", "J2E1", "car"),
    ("x2", "S1J1", "J3W2", "motorcycle"),
    ("x3", "W1J1", "J4N2", "motorcycle"),
    ("x4", "E1J2", "J1S1", "car"),
    ("x5", "N2J4", "J1W1", "motorcycle"),
    ("x6", "S2J2", "J4E2", "car"),
    ("x7", "W2J3", "J2S2", "motorcycle"),
    ("x8", "E2J4", "J3N1", "bus"),
]


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


def main() -> None:
    trunk = split_flow(BASE_TRUNK_VEH_PER_HOUR)
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<!-- Weighted demand (ADR-007) + GUI imgFile from Visualize/images/ -->",
        '<routes xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/routes_file.xsd">',
        "",
        "    <!-- Loại xe + sprite PNG (đường dẫn tương đối so với sumocfg) -->",
    ]
    lines.extend(_vtype_block("motorcycle", moto_vtype_attrs()))
    for vid, attrs in VTYPE_GUI.items():
        params = CONTAINER_PARAMS if vid == "container" else None
        lines.extend(_vtype_block(vid, attrs, params))
    lines.append("")

    for prefix, fr, to in TRUNKS:
        lines.append(f"    <!-- corridor {prefix}: {fr} -> {to} -->")
        for vtype, vph in trunk.items():
            if vph <= 0:
                continue
            lines.append(
                f'    <flow id="{prefix}_{vtype}" type="{vtype}" from="{fr}" to="{to}" '
                f'begin="0" end="3600" vehsPerHour="{vph}"/>'
            )
        lines.append("")

    diag_vph = max(1, int(round(DIAGONAL_VEH_PER_HOUR / len(DIAGONALS))))
    lines.append("    <!-- light diagonal flows -->")
    for fid, fr, to, vtype in DIAGONALS:
        lines.append(
            f'    <flow id="{fid}" type="{vtype}" from="{fr}" to="{to}" '
            f'begin="0" end="3600" vehsPerHour="{diag_vph}"/>'
        )
    lines.append("</routes>")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} with imgFile sprites; trunk={trunk}")


if __name__ == "__main__":
    main()
