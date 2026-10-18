"""Generate frontend SUMO geometry bundles from the active OSM net."""
from __future__ import annotations

import json
from pathlib import Path

from streaming.live_vehicle_stream import load_network_geometry


def main() -> None:
    repo = Path(__file__).resolve().parents[2]
    net = repo / "Visualize" / "Visualize" / "intersection.net.xml"
    geometry = load_network_geometry(net)
    payload = json.dumps(geometry, ensure_ascii=False, separators=(",", ":"))
    targets = (
        repo / "frontend" / "public" / "live-network.json",
        repo / "frontend" / "src" / "data" / "liveNetwork.json",
    )
    for target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        print(f"Wrote {target}")


if __name__ == "__main__":
    main()
