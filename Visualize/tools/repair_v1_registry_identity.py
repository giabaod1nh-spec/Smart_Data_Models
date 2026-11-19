"""Audit and repair Phase 3 run-manifest identity hashes after validation.

Some retained Phase 3 runs were activated before the current immutable
manifest hashing rule was finalized.  This tool never changes a manifest or
catalog.  It backs up the SQLite selector before replacing legacy hashes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.topology.v1_digest import topology_digest  # noqa: E402
from runtime.active_run_registry import manifest_identity  # noqa: E402

CONFIRM = "REPAIR-V1-RUN-IDENTITY"


def audit(db_path: Path, runs_dir: Path) -> list[dict]:
    with sqlite3.connect(db_path) as db:
        rows = db.execute("SELECT generation, run_id, manifest_identity, status FROM runs ORDER BY generation").fetchall()
    findings = []
    for generation, run_id, stored, status in rows:
        folder = runs_dir / run_id
        manifest = json.loads((folder / "run_manifest.json").read_text(encoding="utf-8"))
        catalog = json.loads((folder / "network_topology_catalog.json").read_text(encoding="utf-8"))
        if manifest.get("simulationRunId") != run_id or manifest.get("runGeneration") != generation:
            raise ValueError(f"run {run_id} has mismatched identity or generation")
        if manifest.get("topologyDigest") != topology_digest(catalog):
            raise ValueError(f"run {run_id} has mismatched archived topology digest")
        participants = manifest.get("participatingIntersectionIds")
        if not isinstance(participants, list) or not participants or not set(participants) <= set(catalog["node_to_tls"]):
            raise ValueError(f"run {run_id} has invalid participants")
        if manifest.get("runStatus") != status:
            raise ValueError(f"run {run_id} status differs between manifest and registry")
        findings.append({"generation": generation, "runId": run_id, "status": status,
                         "storedIdentity": stored, "verifiedIdentity": manifest_identity(manifest),
                         "needsRepair": stored != manifest_identity(manifest)})
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=ROOT / "artifacts/runs")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    db_path = args.runs_dir / "active_run.sqlite3"
    findings = audit(db_path, args.runs_dir)
    print(json.dumps(findings, indent=2))
    changes = [row for row in findings if row["needsRepair"]]
    if not args.execute:
        print(f"dry-run: {len(changes)} identity hashes require repair")
        return 0
    if args.confirm != CONFIRM:
        parser.error(f"execution requires --confirm {CONFIRM}")
    if any(row["status"] == "RUNNING" for row in findings):
        parser.error("refusing to edit a registry with a RUNNING simulation")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = args.runs_dir / f"active_run.pre-v1-projector.{stamp}.sqlite3"
    with sqlite3.connect(db_path) as source, sqlite3.connect(backup_path) as backup:
        source.backup(backup)
    checksum = hashlib.sha256(backup_path.read_bytes()).hexdigest()
    with sqlite3.connect(db_path, isolation_level=None) as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            for row in changes:
                result = db.execute("UPDATE runs SET manifest_identity=? WHERE generation=? AND manifest_identity=?",
                                    (row["verifiedIdentity"], row["generation"], row["storedIdentity"]))
                if result.rowcount != 1:
                    raise RuntimeError(f"concurrent registry change at generation {row['generation']}")
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
    print(f"repaired={len(changes)} backup={backup_path} sha256={checksum}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
