"""Read Orion-LD back and compare it with the latest APPLIED V1 observations."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

from integration.projector.v1_orion import V1Orion, local_attribute_name
from integration.projector.v1_projection import DYNAMIC, project, static_entities
from integration.projector.v1_runtime import ArchiveResolver

VIS = Path(__file__).resolve().parents[1]


def _normalized(entity: dict) -> dict:
    return {local_attribute_name(key): value for key, value in entity.items() if key != "@context"}


def _equal_attribute(name: str, actual: object, expected: object) -> bool:
    if name == "location" and isinstance(actual, dict) and isinstance(expected, dict):
        try:
            av = actual["value"]
            ev = expected["value"]
            return (actual["type"] == expected["type"] == "GeoProperty"
                    and av["type"] == ev["type"] == "Point"
                    and len(av["coordinates"]) == len(ev["coordinates"]) == 2
                    and all(abs(float(a) - float(e)) <= 0.000001 for a, e in zip(av["coordinates"], ev["coordinates"])))
        except (KeyError, TypeError, ValueError):
            return False
    return actual == expected


def verify(db_path: Path, runs_dir: Path, orion: V1Orion, run_id: str) -> dict:
    archives = ArchiveResolver(runs_dir)
    manifest = archives.manifest(run_id)
    catalog = archives.resolve(run_id).catalog
    with sqlite3.connect(db_path) as db:
        rows = db.execute("SELECT frozen_event FROM events WHERE run_id=? AND status='APPLIED' ORDER BY cycle", (run_id,)).fetchall()
    latest: dict[str, dict] = {}
    for (frozen,) in rows:
        event = json.loads(frozen)
        latest[event["intersectionId"]] = event
    errors: list[str] = []
    compared = 0
    for node, event in latest.items():
        projection = project(event, catalog)
        for expected in projection.entities:
            actual = orion.get(expected["id"])
            if actual is None:
                errors.append(f"missing entity {expected['id']}")
                continue
            compared += 1
            actual = _normalized(actual)
            expected = _normalized(expected)
            if actual.get("type") != expected["type"]:
                errors.append(f"type mismatch {expected['id']}")
            for name, value in expected.items():
                if name in ("id", "type"):
                    continue
                if not _equal_attribute(name, actual.get(name), value):
                    errors.append(f"{expected['id']}.{name}: {actual.get(name)!r} != {value!r}")
            for name in DYNAMIC[expected["type"]] - expected.keys():
                if name in actual:
                    errors.append(f"stale dynamic attribute {expected['id']}.{name}")
    for node in set(catalog["node_to_tls"]) - set(manifest["participatingIntersectionIds"]):
        for static in static_entities(catalog, node):
            actual = orion.get(static["id"])
            if actual is None:
                errors.append(f"excluded node missing static entity {static['id']}")
                continue
            compared += 1
            actual = _normalized(actual)
            expected = _normalized(static)
            for name, value in expected.items():
                if name in ("id", "type"):
                    continue
                if not _equal_attribute(name, actual.get(name), value):
                    errors.append(f"excluded node static mismatch {static['id']}.{name}")
            for name in DYNAMIC[static["type"]]:
                if name in actual:
                    errors.append(f"excluded node has stale dynamic attribute {static['id']}.{name}")
    return {"runId": run_id, "latestAppliedNodes": sorted(latest), "entitiesCompared": compared, "errors": errors, "ok": not errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path(os.getenv("V1_PROJECTOR_DB", str(VIS / "artifacts/projector/v1-production.sqlite3"))))
    parser.add_argument("--runs-dir", type=Path, default=Path(os.getenv("V1_RUNS_DIR", str(VIS / "artifacts/runs"))))
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    result = verify(args.db, args.runs_dir, V1Orion.from_env(), args.run_id)
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
