"""Bounded realtime path verification (RT-F live smoke + RT-BLOCK-001 capture)."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

VIS = Path(__file__).resolve().parents[1]
REPO = VIS.parent


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _http_json(url: str, timeout: float = 5.0) -> tuple[int, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            code = resp.status
            raw = resp.read().decode("utf-8")
            return code, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = raw
        return e.code, body
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def kafka_watermarks(bootstrap: str, topic: str) -> Dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"rt-block-probe-{int(time.time())}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic=topic, timeout=10.0)
        t = md.topics.get(topic)
        if t is None:
            raise RuntimeError(f"topic missing: {topic}")
        tips: Dict[int, int] = {}
        for p in sorted(t.partitions.keys()):
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10.0)
            tips[int(p)] = int(hi)
        return tips
    finally:
        c.close()


def capture_rt_block_001(
    *,
    bootstrap: str,
    topic: str,
    sqlite_path: Path,
    group_id: str,
    projector_url: str,
) -> dict:
    import sqlite3

    evidence: dict[str, Any] = {
        "captured_at": _utc(),
        "topic": topic,
        "group_id": group_id,
        "sqlite_path": str(sqlite_path),
        "kafka_tips": kafka_watermarks(bootstrap, topic),
    }
    if sqlite_path.is_file():
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        evidence["sqlite_commits"] = [
            dict(r) for r in conn.execute("SELECT * FROM projector_partition_commits")
        ]
        evidence["sqlite_active_runs"] = [
            dict(r) for r in conn.execute("SELECT * FROM projector_active_runs")
        ]
        try:
            evidence["sqlite_runtime"] = [
                dict(r) for r in conn.execute("SELECT * FROM projector_runtime_state")
            ]
        except Exception as e:
            evidence["sqlite_runtime_error"] = str(e)
        conn.close()
    code, ready = _http_json(f"{projector_url}/ready")
    evidence["projector_ready"] = {"http": code, "body": ready}
    code, current = _http_json(f"{projector_url}/current-run")
    evidence["projector_current_run"] = {"http": code, "body": current}
    return evidence


def write_demo_fence(*, run_id: str, tips: Dict[int, int], out: Path, topic: str) -> None:
    manifest = {
        "targetSimulationRunId": run_id,
        "topic": topic,
        "partitions": [{"partition": p, "nextOffset": off} for p, off in sorted(tips.items())],
        "generatedAt": _utc(),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def wait_projector(
    url: str,
    *,
    want_ready: bool = True,
    want_current_run: bool = False,
    timeout_sec: float = 300.0,
) -> dict:
    deadline = time.time() + timeout_sec
    timeline = []
    while time.time() < deadline:
        code_r, ready = _http_json(f"{url}/ready")
        code_c, current = _http_json(f"{url}/current-run")
        snap = {
            "ts": _utc(),
            "ready_http": code_r,
            "ready": (ready or {}).get("ready") if isinstance(ready, dict) else None,
            "ready_reason": (ready or {}).get("ready_reason") if isinstance(ready, dict) else None,
            "lag": (ready or {}).get("health", {}).get("projector_lag_events") if isinstance(ready, dict) else None,
            "orion_apply": (ready or {}).get("health", {}).get("orion_apply_count") if isinstance(ready, dict) else None,
            "current_run_http": code_c,
            "current_run": current,
        }
        timeline.append(snap)
        ok_ready = code_r == 200 and isinstance(ready, dict) and ready.get("ready") is True
        ok_current = code_c == 200 and isinstance(current, dict) and current.get("simulationRunId")
        if want_ready and ok_ready and (not want_current_run or ok_current):
            return {"ok": True, "timeline": timeline, "final": snap}
        time.sleep(2.0)
    return {"ok": False, "timeline": timeline, "final": timeline[-1] if timeline else None}


def run_sumo(*, run_id: str, max_sim: float, evidence_dir: Path) -> dict:
    env = os.environ.copy()
    env.update(
        {
            "ORION_PUBLISH_ENABLED": "false",
            "KAFKA_OUTBOX_ENABLED": "true",
            "KAFKA_PUBLISH_ENABLED": "false",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
        }
    )
    cmd = [
        sys.executable,
        "-m",
        "app.traci_runner",
        "--no-gui",
        "--fast",
        "--nodes",
        "A,B,C,D",
        "--max-sim-time",
        str(max_sim),
        "--simulation-run-id",
        run_id,
    ]
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=VIS, env=env, text=True, capture_output=True)
    out = {
        "command": cmd,
        "run_id": run_id,
        "max_sim_time": max_sim,
        "returncode": proc.returncode,
        "wall_seconds": round(time.time() - t0, 2),
        "ok": proc.returncode == 0,
    }
    log = evidence_dir / "sumo_run.log"
    log.write_text(proc.stdout + "\n---STDERR---\n" + proc.stderr, encoding="utf-8")
    out["log"] = str(log)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Bounded realtime verification")
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--topic", default="traffic.entity-events.v2")
    p.add_argument("--projector-url", default="http://127.0.0.1:8093")
    p.add_argument("--group-id", default="projector-k5-production")
    p.add_argument(
        "--production-sqlite",
        default=str(VIS / "artifacts" / "projector" / "k5-production.sqlite3"),
    )
    p.add_argument("--max-sim-time", type=float, default=60.0)
    p.add_argument("--run-id", default="")
    p.add_argument("--skip-sumo", action="store_true")
    p.add_argument("--capture-only", action="store_true")
    args = p.parse_args()

    stamp = _utc().replace(":", "").replace("-", "")
    evidence_dir = REPO / "artifacts" / "realtime" / stamp
    evidence_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {"started_at": _utc(), "evidence_dir": str(evidence_dir)}

    report["rt_block_001"] = capture_rt_block_001(
        bootstrap=args.bootstrap,
        topic=args.topic,
        sqlite_path=Path(args.production_sqlite),
        group_id=args.group_id,
        projector_url=args.projector_url,
    )
    (evidence_dir / "rt_block_001.json").write_text(
        json.dumps(report["rt_block_001"], indent=2), encoding="utf-8"
    )

    if args.capture_only:
        print(json.dumps(report, indent=2))
        return 0

    run_id = args.run_id or str(uuid.uuid4())
    report["run_id"] = run_id
    tips = report["rt_block_001"]["kafka_tips"]
    demo_db = VIS / "artifacts" / "projector" / f"demo-{stamp}.sqlite3"
    fence_path = VIS / "artifacts" / "projector" / "fence" / f"demo_{stamp}.json"
    write_demo_fence(run_id=run_id, tips=tips, out=fence_path, topic=args.topic)
    report["demo"] = {"db": str(demo_db), "fence": str(fence_path), "kafka_tips": tips}

    print("Stopping compose orion-projector for demo run...", flush=True)
    subprocess.run(
        ["docker", "compose", "stop", "orion-projector"],
        cwd=REPO,
        check=False,
    )

    proj_cmd = [
        sys.executable,
        "tools/projector_live_consumer.py",
        "--no-shadow",
        "--namespace",
        "production",
        "--write-mode",
        "active",
        "--consumer-mode",
        "demo",
        "--start-offsets-file",
        str(fence_path),
        "--db",
        str(demo_db),
        "--bootstrap",
        args.bootstrap,
        "--health-host",
        "127.0.0.1",
        "--health-port",
        "8094",
    ]
    report["projector_cmd"] = proj_cmd
    log_path = evidence_dir / "projector_demo.log"
    proj_log = open(log_path, "w", encoding="utf-8")
    proj_proc = subprocess.Popen(
        proj_cmd,
        cwd=VIS,
        stdout=proj_log,
        stderr=subprocess.STDOUT,
        env={
            **os.environ,
            "PYTHONPATH": str(VIS),
            "ORION_URL": os.environ.get("ORION_URL", "http://127.0.0.1:1026"),
        },
    )
    report["projector_pid"] = proj_proc.pid
    demo_url = "http://127.0.0.1:8094"

    try:
        idle_wait = wait_projector(demo_url, want_ready=True, want_current_run=False, timeout_sec=60.0)
        report["idle_wait"] = idle_wait
        if not idle_wait.get("ok"):
            report["verdict"] = "FAIL"
            report["reason"] = "projector_demo_idle_timeout"
            (evidence_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(json.dumps(report, indent=2))
            return 1

        if not args.skip_sumo:
            report["sumo"] = run_sumo(run_id=run_id, max_sim=args.max_sim_time, evidence_dir=evidence_dir)

        active_wait = wait_projector(
            demo_url,
            want_ready=True,
            want_current_run=True,
            timeout_sec=max(600.0, args.max_sim_time * 4),
        )
        report["active_wait"] = active_wait
        code, final_current = _http_json(f"{demo_url}/current-run")
        report["final_current_run"] = {"http": code, "body": final_current}

        sumo_ok = report.get("sumo", {}).get("ok", True)
        report["verdict"] = (
            "PASS"
            if sumo_ok and active_wait.get("ok") and code == 200
            else "FAIL"
        )
    finally:
        proj_proc.terminate()
        try:
            proj_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proj_proc.kill()
        proj_log.close()
        subprocess.run(["docker", "compose", "start", "orion-projector"], cwd=REPO, check=False)

    report["finished_at"] = _utc()
    (evidence_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"verdict": report["verdict"], "evidence_dir": str(evidence_dir)}, indent=2))
    return 0 if report.get("verdict") == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
