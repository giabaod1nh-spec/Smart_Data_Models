"""Bound the sealed fixture's read-only physical audit to the approved wall time."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import psutil


ROOT = Path(__file__).resolve().parent
CONTAINER = "smart-perf-w2-paced-01-clickhouse-1"
PROJECT = "smart-perf-w2-paced-01"
VOLUME = PROJECT + "_clickhouse"
RESERVE = 2_500_000_000
FLOOR = 1024 ** 3


def docker(*args, timeout=5):
    return subprocess.check_output(["docker", *args], timeout=timeout, text=True).strip()


def fingerprints():
    result = {}
    for path in ROOT.glob("*.sqlite3"):
        with path.open("rb") as stream:
            result[path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    if len(result) != 5:
        raise RuntimeError("Missing sealed SQLite copy")
    return result


def run(log):
    start = time.monotonic()
    deadline = start + 120

    def record(phase, **fields):
        available = psutil.virtual_memory().available
        print(json.dumps(dict(phase=phase, elapsed=time.monotonic() - start, available_bytes=available,
                              utc=datetime.now(timezone.utc).isoformat(), **fields)), file=log, flush=True)
        if available < FLOOR:
            raise RuntimeError("Host memory fell below 1 GiB")
        return available

    if record("preflight") < RESERVE:
        raise RuntimeError("Host memory below the 2.5 GB startup reserve")
    if docker("ps", "-q"):
        raise RuntimeError("Docker is not idle; leave existing services under owner control")
    item, = json.loads(docker("inspect", CONTAINER))
    if (item["State"]["Status"] != "exited"
            or item["Config"]["Labels"]["com.docker.compose.project"] != PROJECT
            or item["Config"]["Labels"]["com.docker.compose.service"] != "clickhouse"
            or not any(m.get("Name") == VOLUME and m["Destination"] == "/var/lib/clickhouse"
                       for m in item["Mounts"])):
        raise RuntimeError("Retained fixture identity differs")
    before = fingerprints()
    process = None
    started = False
    try:
        started = True
        docker("start", CONTAINER)
        readiness_deadline = min(deadline - 25, time.monotonic() + 25)
        while time.monotonic() < readiness_deadline:
            health = docker("inspect", "--format", "{{.State.Health.Status}}", CONTAINER)
            available = record("startup", health=health)
            if health == "healthy":
                if available < RESERVE:
                    raise RuntimeError("Host memory below the 2.5 GB post-start reserve")
                break
            time.sleep(1)
        else:
            raise RuntimeError("ClickHouse exceeded its readiness budget")
        with (ROOT / "physical-audit.json").open("x", encoding="utf-8") as stdout, (
                ROOT / "physical-audit.stderr.log").open("x", encoding="utf-8") as stderr:
            process = subprocess.Popen([sys.executable, str(ROOT / "audit-physical.py")],
                                       stdout=stdout, stderr=stderr, creationflags=subprocess.CREATE_NO_WINDOW)
            while process.poll() is None:
                if time.monotonic() >= deadline - 20:
                    raise RuntimeError("Audit stopped to reserve time for shutdown within 120 seconds")
                record("audit")
                time.sleep(1)
            record("audit-exit", exit_code=process.returncode)
    finally:
        try:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=3)
        finally:
            if started:
                docker("stop", "--timeout", "8", CONTAINER, timeout=13)
    if docker("ps", "-q"):
        raise RuntimeError("Docker is no longer idle after the audit")
    preserved = fingerprints() == before
    record("sealed", copies_unchanged=preserved, audit_exit=process.returncode)
    if not preserved:
        raise RuntimeError("A sealed database changed")
    if time.monotonic() > deadline:
        raise RuntimeError("Physical audit exceeded the approved wall clock limit")
    return process.returncode


if __name__ == "__main__":
    with (ROOT / "physical-audit-run.jsonl").open("x", encoding="utf-8") as log:
        raise SystemExit(run(log))
