"""Reset DE pipeline runtime state for clean E2E verification.

Does NOT drop schema, migrations, DDL, or views. Resets only:
Kafka topics, Raw/Bronze/Silver/Gold runtime data, SQLite checkpoints/ledgers/locks.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import clickhouse_connect  # noqa: E402

from de.bronze.config import get_settings  # noqa: E402

EVIDENCE_ROOT = _REPO / "artifacts" / "e2e"
TOPIC = "traffic.entity-events.v2"
KAFKA_CONTAINER = "smart-traffic-kafka"
SERVICES = (
    "de-gold-runtime",
    "de-silver-processor",
    "de-bronze-processor",
    "de-kafka-raw-consumer",
)
ARTIFACT_DIRS = (
    _REPO / "de" / "artifacts" / "kafka_raw",
    _REPO / "de" / "artifacts" / "bronze",
    _REPO / "de" / "artifacts" / "silver",
    _REPO / "de" / "artifacts" / "gold",
)


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ch_client():
    s = get_settings()
    return clickhouse_connect.get_client(
        host=s.clickhouse_host,
        port=s.clickhouse_port,
        username=s.clickhouse_user,
        password=s.clickhouse_password,
        database=s.clickhouse_database,
    )


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=_REPO, text=True, capture_output=True, check=check)


def stop_services(report: dict[str, Any]) -> None:
    result = _run(
        ["docker", "compose", "stop", "kafka", *SERVICES, "orion-projector"],
        check=False,
    )
    report["stop_services"] = {
        "cmd": ["docker", "compose", "stop", "kafka", *SERVICES, "orion-projector"],
        "returncode": result.returncode,
        "stderr": result.stderr[-2000:],
    }


def start_services(report: dict[str, Any]) -> None:
    result = _run(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "clickhouse",
            "kafka",
            "de-migrate",
            "de-kafka-raw-consumer",
            "de-bronze-processor",
            "de-silver-processor",
            "de-gold-runtime",
        ],
        check=False,
    )
    report["start_services"] = {
        "returncode": result.returncode,
        "stdout": result.stdout[-2000:],
        "stderr": result.stderr[-2000:],
    }


def delete_sqlite_artifacts(report: dict[str, Any]) -> None:
    removed: list[str] = []
    for base in ARTIFACT_DIRS:
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if path.is_file() and path.suffix in {".sqlite3", ".lock"} or path.name.endswith(
                (".sqlite3-wal", ".sqlite3-shm")
            ):
                try:
                    path.unlink()
                    removed.append(str(path.relative_to(_REPO)))
                except OSError as exc:
                    report.setdefault("sqlite_errors", []).append(f"{path}: {exc}")
    report["sqlite_removed"] = removed


def truncate_runtime_tables(report: dict[str, Any]) -> None:
    settings = get_settings()
    db = settings.clickhouse_database
    client = _ch_client()
    tables = [
        r[0]
        for r in client.query(
            "SELECT name FROM system.tables "
            "WHERE database={db:String} AND engine LIKE '%MergeTree%' "
            "AND name NOT LIKE '.inner%'",
            parameters={"db": db},
        ).result_rows
    ]
    truncated: list[str] = []
    for table in sorted(tables):
        if table.startswith(("kafka_", "bronze_", "silver_", "gold_")) or table == "raw_ngsi_notifications":
            client.command(f"TRUNCATE TABLE {db}.{table}")
            truncated.append(table)
    client.close()
    report["truncated_tables"] = truncated
    time.sleep(3.0)


def reset_kafka_data_dir(report: dict[str, Any]) -> None:
    """Wipe bind-mounted Kafka log dir (runtime data only)."""
    data_dir = _REPO / "data" / "kafka"
    removed = 0
    if data_dir.is_dir():
        for child in data_dir.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
                removed += 1
            except OSError as exc:
                report.setdefault("kafka_data_errors", []).append(f"{child}: {exc}")
    report["kafka_data_dir_reset"] = {"path": str(data_dir), "entries_removed": removed}


def reset_kafka_topics(report: dict[str, Any]) -> None:
    """Start a clean broker and recreate topics via kafka-init."""
    _run(["docker", "compose", "up", "-d", "kafka"], check=False)
    healthy = False
    for _ in range(36):
        proc = _run(
            ["docker", "inspect", "smart-traffic-kafka", "--format", "{{.State.Health.Status}}"],
            check=False,
        )
        if proc.stdout.strip() == "healthy":
            healthy = True
            break
        time.sleep(5.0)
    init = _run(["docker", "compose", "run", "--rm", "kafka-init"], check=False)
    report["kafka_reset"] = {
        "strategy": "wipe_data_dir_then_kafka_init",
        "kafka_healthy": healthy,
        "kafka_init_returncode": init.returncode,
        "kafka_init_stdout": init.stdout[-2000:],
        "kafka_init_stderr": init.stderr[-2000:],
    }


def reset_consumer_groups(report: dict[str, Any]) -> None:
    groups = ["de-kafka-raw-v2"]
    results = []
    for group in groups:
        result = _run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--delete-offsets",
                "--group",
                group,
            ],
            check=False,
        )
        results.append(
            {
                "group": group,
                "returncode": result.returncode,
                "stderr": result.stderr[-500:],
            }
        )
    report["consumer_groups"] = results


def reset_pipeline(*, dry_run: bool = False) -> dict[str, Any]:
    report: dict[str, Any] = {"started_at": _utc(), "dry_run": dry_run, "topic": TOPIC}
    if dry_run:
        report["note"] = "dry-run only; no mutations applied"
        return report

    stop_services(report)
    truncate_runtime_tables(report)
    delete_sqlite_artifacts(report)
    reset_kafka_data_dir(report)
    reset_kafka_topics(report)
    reset_consumer_groups(report)
    start_services(report)
    report["finished_at"] = _utc()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset DE pipeline runtime for E2E")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--evidence-dir",
        default=str(EVIDENCE_ROOT),
        help="Directory for reset evidence JSON",
    )
    args = parser.parse_args()
    evidence_dir = Path(args.evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = reset_pipeline(dry_run=args.dry_run)
    out = evidence_dir / f"reset_{stamp}.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"evidence": str(out), "truncated": len(report.get("truncated_tables", []))}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
