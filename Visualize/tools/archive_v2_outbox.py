"""Create a verified, read-only historical backup of the V2 SQLite Outbox."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


def archive(source: Path, destination: Path) -> dict:
    source = source.resolve()
    destination = destination.resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".backup-in-progress")
    if temporary.exists():
        raise FileExistsError(temporary)
    try:
        with closing(sqlite3.connect(str(source), timeout=30)) as old:
            with closing(sqlite3.connect(str(temporary), timeout=30)) as saved:
                old.backup(saved, pages=1000, sleep=0.1)
        with closing(sqlite3.connect(str(temporary))) as saved:
            integrity = saved.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise RuntimeError(f"V2 Outbox archive integrity check failed: {integrity}")
            statuses = dict(saved.execute(
                "SELECT status, COUNT(*) FROM kafka_outbox GROUP BY status"
            ).fetchall())
            total = sum(statuses.values())
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    digest = hashlib.sha256()
    with destination.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return {
        "source": str(source),
        "archive": str(destination),
        "archivedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "integrityCheck": integrity,
        "sha256": digest.hexdigest(),
        "totalRecords": total,
        "pendingRecords": sum(statuses.get(s, 0) for s in ("OUTBOXED", "QUEUED", "FAILED_RETRYABLE")),
        "ackedRecords": statuses.get("ACKED", 0),
        "statusDistribution": statuses,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = archive(args.source, args.destination)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
