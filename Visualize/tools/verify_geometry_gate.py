"""Validate the generated topology catalog against the current SUMO net."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from topology.catalog import (  # noqa: E402
    DEFAULT_CATALOG_PATH,
    DEFAULT_NET_PATH,
    build_catalog,
    load_catalog,
    validate_catalog,
)


def verify(net_path: Path, catalog_path: Path) -> list[str]:
    catalog = load_catalog(catalog_path)
    errors = validate_catalog(catalog)
    current = build_catalog(net_path)

    if catalog.get("topology_hash") != current.get("topology_hash"):
        errors.append(
            "catalog topology_hash does not match the current SUMO .net.xml; "
            "run tools/generate_topology_catalog.py"
        )
    if catalog.get("node_to_tls") != current.get("node_to_tls"):
        errors.append("catalog logical node mapping differs from current configuration")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--net", type=Path, default=DEFAULT_NET_PATH)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    errors = verify(args.net, args.catalog)
    report = {"pass": not errors, "errors": errors}
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if errors:
        print("TOPOLOGY GATE FAIL:")
        for error in errors:
            print(f"  - {error}")
        raise SystemExit(1)
    print("TOPOLOGY GATE PASS: catalog matches SUMO net and logical mappings")


if __name__ == "__main__":
    main()
