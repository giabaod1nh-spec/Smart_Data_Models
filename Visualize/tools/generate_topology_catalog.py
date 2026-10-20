"""Generate Visualize/generated/network_topology_catalog.json from intersection.net.xml."""
from __future__ import annotations

import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from topology.catalog import DEFAULT_CATALOG_PATH, build_catalog, write_catalog


def main() -> None:
    catalog = build_catalog()
    out = write_catalog(DEFAULT_CATALOG_PATH, catalog)
    contract_dir = ROOT.parent / "contracts" / "topology" / "fixtures"
    contract_catalog = contract_dir / "network_topology_catalog.example.json"
    contract_catalog.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    manifest_path = contract_dir / "run_manifest.example.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["topology_hash"] = catalog["topology_hash"]
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    n_edges = len(catalog["edges"])
    n_conn = len(catalog["connections"])
    n_links = len(catalog["inter_node_links"])
    print(
        f"Wrote {out} edges={n_edges} connections={n_conn} "
        f"inter_node_links={n_links} topology_hash={catalog['topology_hash']}"
    )


if __name__ == "__main__":
    main()
