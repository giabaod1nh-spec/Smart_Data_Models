"""Bounded continuation of the stopped, fully delivered incident fixture."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.request import urlopen

import psutil

PROFILE = Path(__file__).resolve().parent
REPO = PROFILE.parent.parent
OUTBOX = PROFILE / "producer/kafka_outbox/v1-outbox.sqlite3"
PROJECT = "smart-perf-w2-paced-01"
SERVICES = ("clickhouse", "silver", "gold")
RETAINED = ("kafka", "raw", "bronze")
START_RESERVE = 2_500_000_000
MEMORY_FLOOR = 1024**3
SETUP_SECONDS = 30
DRAIN_SECONDS = 165
sys.path.insert(0, str(REPO))

from de.tools.gold_core_capacity import ENDPOINTS, MeasurementGuard


def docker(*args, timeout=8):
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True, timeout=timeout,
    ).stdout


def read_only(path):
    return closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True))


def fixtures():
    names = {service: f"{PROJECT}-{service}-1" for service in SERVICES + RETAINED}
    records = json.loads(docker("inspect", *names.values()))
    if len(records) != len(names):
        raise RuntimeError("incident fixture is incomplete")
    for service, record in zip(names, records, strict=True):
        labels = record["Config"]["Labels"]
        volumes = {mount.get("Name") for mount in record["Mounts"]}
        if (labels.get("com.docker.compose.project") != PROJECT
                or labels.get("com.docker.compose.service") != service
                or f"{PROJECT}_{service}" not in volumes or record["State"]["Status"] != "exited"):
            raise RuntimeError(f"incident container or volume mismatch for {service}")
    return names


def source_boundary(snapshot):
    reference = json.loads((snapshot / "state.json").read_text(encoding="utf-8"))
    with read_only(OUTBOX) as db:
        db.execute("BEGIN")
        counts = dict(db.execute("SELECT status,count(*) FROM kafka_outbox GROUP BY status"))
        inventory = db.execute("SELECT event_id,payload_json FROM kafka_outbox ORDER BY event_id").fetchall()
    digest = sha256(json.dumps(inventory, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    if counts != {"ACKED": 1535} or digest != reference["outbox_rows_digest"]:
        raise RuntimeError("incident Outbox differs from the immutable delivered source evidence")
    with read_only(snapshot / "gold.sqlite3") as db:
        (binding,) = db.execute("SELECT binding_id FROM core_binding WHERE namespace='live'").fetchone()
    return reference, binding


def endpoints():
    snapshots = {}
    for name in ("silver", "gold"):
        port, route = ENDPOINTS[name]
        try:
            with urlopen(f"http://127.0.0.1:{port}/{route}", timeout=0.5) as stream:
                snapshots[name] = json.load(stream)
        except (OSError, ValueError):
            snapshots[name] = None
    return snapshots


def run(log, snapshot, drain_seconds):
    def record(phase, **fields):
        headroom = psutil.virtual_memory().available
        print(json.dumps(dict(phase=phase, available=headroom, **fields,
                              utc=datetime.now(timezone.utc).isoformat())), file=log, flush=True)
        if headroom < MEMORY_FLOOR:
            raise RuntimeError("available host memory below 1 GiB")
        return headroom

    if record("preflight") < START_RESERVE:
        raise RuntimeError("available host memory below 2.5 GB before startup")
    names = fixtures()
    reference, binding = source_boundary(snapshot)
    expected_changes, expected_invalidations = reference["bronze_allocators"]
    missing_proofs = sum(row["source_crossed"] and row["gold"].get("state") != "COMPLETE"
                         for row in reference["prefix"]["window_progress"])
    started = []
    try:
        deadline = time.monotonic() + SETUP_SECONDS
        if record("verified-source", change=expected_changes, invalidation=expected_invalidations,
                  binding=binding, drain_seconds=drain_seconds) < START_RESERVE:
            raise RuntimeError("available host memory fell below the startup reserve")
        started.append(names["clickhouse"])
        docker("start", names["clickhouse"])
        while time.monotonic() < deadline:
            status = docker("inspect", "--format", "{{.State.Health.Status}}", names["clickhouse"]).strip()
            record("clickhouse-startup", health=status)
            if status == "healthy":
                break
            time.sleep(1)
        else:
            raise RuntimeError("ClickHouse exceeded its setup budget")
        for service in ("silver", "gold"):
            started.append(names[service])
            docker("start", names[service])
        guard = MeasurementGuard()
        while time.monotonic() < deadline:
            services = endpoints()
            measurement = guard.assess(services, wall_utc=time.time(), monotonic=time.monotonic())
            available = record("worker-startup", services=services, measurement=measurement)
            if measurement["stop_reason"]:
                raise RuntimeError(measurement["stop_reason"])
            gold = services["gold"] or {}
            progress = gold.get("progress", {})
            if measurement["valid"] and progress:
                if available < START_RESERVE:
                    raise RuntimeError("available host memory below 2.5 GB after startup")
                break
            time.sleep(1)
        else:
            raise RuntimeError("Silver and Gold exceeded the setup budget")

        drain_start = time.monotonic()
        while time.monotonic() - drain_start < drain_seconds:
            services = endpoints()
            measurement = guard.assess(services, wall_utc=time.time(), monotonic=time.monotonic())
            record("drain", elapsed=time.monotonic()-drain_start, services=services, measurement=measurement)
            if measurement["stop_reason"]:
                raise RuntimeError(measurement["stop_reason"])
            if measurement["valid"]:
                gold = services["gold"]
                progress = gold.get("progress", {})
                if (gold["source_binding_id"] != binding
                        or progress.get("bronze_change") != expected_changes
                        or progress.get("bronze_invalidation") != expected_invalidations):
                    raise RuntimeError("Gold source identity or upstream boundary changed")
                if (progress["silver_change"] < reference["silver_cursors"][0]
                        or progress["gold_change"] < reference["prefix"]["gold_cursors"]["CHANGE"]):
                    raise RuntimeError("Silver or Gold cursor regressed")
                metrics = gold["metrics"]
                pending = any(metrics.get(key, 0) for key in
                              ("pending_work", "running_work", "promotion_pending_runs"))
                if (progress["gold_change"] == expected_changes and not pending
                        and metrics.get("work_completed", 0) >= missing_proofs):
                    record("candidate-full-drain")
                    return
            time.sleep(2)
        record("drain-budget-reached")
    finally:
        if started:
            docker("stop", "--timeout", "25", *reversed(started), timeout=35)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    parser.add_argument("--reference", type=Path, default=PROFILE / "recovery-after")
    parser.add_argument("--drain-seconds", type=int, default=DRAIN_SECONDS,
                        help="wall limit for the authorized drain interval")
    arguments = parser.parse_args()
    if arguments.drain_seconds < 1:
        parser.error("--drain-seconds must be positive")
    with arguments.samples.open("x", encoding="utf-8") as log:
        run(log, arguments.reference.resolve(), arguments.drain_seconds)
