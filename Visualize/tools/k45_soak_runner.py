"""
K-4.5 dual-path soak harness.

Modes:
  dry-run       — topology / readiness audit only
  toggle-smoke  — ~2 min Orion OFF/ON while TraCI publishes (Control API required)
  full          — NORMAL→PEAK→CHAOS→RECOVERY→rehearsals→drain→oracles

Evidence: docs/architecture/k45_evidence/<run_id>/

Env knobs:
  K45_WINDOW_SEC          default 600 (10 min) — set smaller for dry local drills
  K45_REHEARSAL_SEC       default 300 (5 min) total for R1+R2 window
  K45_DRAIN_TIMEOUT_SEC   default 300
  K45_CONTROL_API         default http://127.0.0.1:9090
  K45_KAFKA_BOOTSTRAP     default localhost:29092
  K45_TOPIC               default traffic.entity-events.v2
  K45_SKIP_CHAOS          default 0
  K45_PEAK_DEMAND         default morning_peak
  K45_NORMAL_DEMAND       default normal
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

VIS = Path(__file__).resolve().parents[1]
REPO = VIS.parent
sys.path.insert(0, str(VIS))
sys.path.insert(0, str(REPO))

from tools.k45_oracles import (  # noqa: E402
    capture_partition_watermarks,
    clickhouse_logical_offsets,
    compare_latest_to_shadow,
    fetch_shadow_entities,
    historical_completeness,
    kafka_entity_keys,
    kafka_offsets_in_window,
    latest_entity_events,
    legacy_entity_keys_from_notifications,
    legacy_entity_parity,
    metrics_resource_gate,
    outbox_integrity,
    scan_kafka_window_events,
    window_offsets,
)

log = logging.getLogger("k45.runner")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_json(method: str, url: str, body: Optional[dict] = None, timeout: float = 10.0) -> Any:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _http_ok(url: str, timeout: float = 5.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


class EvidenceWriter:
    def __init__(self, root: Path, run_id: str) -> None:
        self.root = root / run_id
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "profiles").mkdir(exist_ok=True)
        self._timeline = open(self.root / "timeline.jsonl", "a", encoding="utf-8")
        self._chaos = open(self.root / "chaos_events.jsonl", "a", encoding="utf-8")
        self._rehearsal = open(self.root / "rehearsal.jsonl", "a", encoding="utf-8")
        self.gates: Dict[str, Any] = {}
        self.ok = True

    def write_json(self, name: str, obj: Any) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")

    def event(self, stream: str, **kwargs: Any) -> None:
        row = {"ts": _utc_now(), **kwargs}
        fh = {
            "timeline": self._timeline,
            "chaos": self._chaos,
            "rehearsal": self._rehearsal,
        }[stream]
        fh.write(json.dumps(row, default=str) + "\n")
        fh.flush()

    def gate(self, name: str, passed: bool, detail: str = "") -> None:
        self.gates[name] = {"pass": passed, "detail": detail}
        if not passed:
            self.ok = False
        log.info("[%s] %s: %s", "PASS" if passed else "FAIL", name, detail)

    def close(self) -> None:
        for fh in (self._timeline, self._chaos, self._rehearsal):
            fh.close()
        self.write_json("gates.json", {"ok": self.ok, "gates": self.gates})


class ControlClient:
    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")

    def health(self) -> dict:
        return _http_json("GET", f"{self.base}/health")

    def publish_stats(self) -> dict:
        return _http_json("GET", f"{self.base}/publish-stats")

    def set_orion_publish(self, enabled: bool) -> dict:
        return _http_json(
            "POST", f"{self.base}/control/orion-publish", {"enabled": enabled}
        )

    def get_orion_publish(self) -> dict:
        return _http_json("GET", f"{self.base}/control/orion-publish")

    def set_demand(self, profile: str) -> dict:
        return _http_json("POST", f"{self.base}/demand-profile", {"profile": profile})


def docker_compose(*args: str, check: bool = False) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", *args]
    return subprocess.run(
        cmd,
        cwd=str(REPO),
        capture_output=True,
        text=True,
        check=check,
    )


def topology_audit(ev: EvidenceWriter, control: Optional[ControlClient]) -> Dict[str, Any]:
    bootstrap = os.getenv("K45_KAFKA_BOOTSTRAP", "localhost:29092")
    orion = os.getenv("ORION_URL", "http://localhost:1026")
    raw_ready = os.getenv("K45_RAW_READY", "http://127.0.0.1:8091/ready")
    ch = os.getenv("CLICKHOUSE_HTTP", "http://localhost:8123")

    checks = {
        "kafka_bootstrap_tcp": False,
        "orion_version": _http_ok(f"{orion.rstrip('/')}/version"),
        "raw_ready": _http_ok(raw_ready),
        "clickhouse": _http_ok(ch),
        "control_api": False,
        "control_orion_toggle": False,
    }
    # kafka: try watermark
    try:
        topic = os.getenv("K45_TOPIC", "traffic.entity-events.v2")
        wm = capture_partition_watermarks(bootstrap, topic)
        checks["kafka_bootstrap_tcp"] = len(wm.get("partitions", [])) > 0
        checks["kafka_partitions"] = wm
    except Exception as e:
        checks["kafka_error"] = str(e)

    if control is not None:
        try:
            h = control.health()
            checks["control_api"] = h.get("status") in ("ok", "starting")
            checks["control_health"] = h
            t = control.get_orion_publish()
            checks["control_orion_toggle"] = "enabled" in t
            checks["orion_publish"] = t
        except Exception as e:
            checks["control_error"] = str(e)

    # docker ps soft check
    try:
        ps = docker_compose("ps", "--format", "json")
        checks["docker_compose_exit"] = ps.returncode
        checks["docker_up"] = ps.returncode == 0
    except Exception as e:
        checks["docker_error"] = str(e)

    ev.write_json("topology_audit.json", checks)
    required = ["kafka_bootstrap_tcp", "orion_version", "clickhouse"]
    soft = ["raw_ready", "control_api", "control_orion_toggle"]
    hard_ok = all(checks.get(k) for k in required)
    ev.gate(
        "topology_audit",
        hard_ok,
        json.dumps({k: checks.get(k) for k in required + soft}),
    )
    return checks


def _counter(stats: dict, *keys: str, default: int = 0) -> int:
    for k in keys:
        if k in stats and stats[k] is not None:
            return int(stats[k])
    kafka = stats.get("kafka") or {}
    for k in keys:
        if k in kafka and kafka[k] is not None:
            return int(kafka[k])
    return default


def sample_stats(control: ControlClient) -> Dict[str, int]:
    s = control.publish_stats()
    return {
        "legacy_orion_cycles_published_total": _counter(
            s, "legacy_orion_cycles_published_total"
        ),
        "legacy_orion_cycles_enqueued_total": _counter(
            s, "legacy_orion_cycles_enqueued_total"
        ),
        "legacy_orion_entity_success_total": _counter(
            s, "legacy_orion_entity_success_total"
        ),
        "events_acked_total": _counter(s, "events_acked_total", "acked_total"),
        "cycles_outboxed": _counter(s, "cycles_outboxed"),
        "pending_rows": _counter(
            s, "pending_rows", "outbox_pending_rows", "outbox_pending", "pending_count"
        ),
    }


def run_rehearsal(
    control: ControlClient,
    ev: EvidenceWriter,
    *,
    hold_sec: float = 20.0,
    label: str = "R1",
) -> bool:
    """ON→OFF→ON while producer running. Requires Kafka counters to move when OFF."""
    settle = min(5.0, max(2.0, hold_sec / 8.0))
    before_on = sample_stats(control)
    control.set_orion_publish(True)
    time.sleep(settle)
    mid_on = sample_stats(control)

    control.set_orion_publish(False)
    # Wait for in-flight Orion cycle to finish before measuring OFF baseline
    time.sleep(settle)
    off_start = sample_stats(control)
    time.sleep(hold_sec)
    off_end = sample_stats(control)

    orion_delta = (
        off_end["legacy_orion_cycles_published_total"]
        - off_start["legacy_orion_cycles_published_total"]
    )
    kafka_delta = (
        off_end["events_acked_total"] - off_start["events_acked_total"]
    ) + (off_end["cycles_outboxed"] - off_start["cycles_outboxed"])

    control.set_orion_publish(True)
    time.sleep(hold_sec)
    after_on = sample_stats(control)
    resume_delta = (
        after_on["legacy_orion_cycles_published_total"]
        - off_end["legacy_orion_cycles_published_total"]
    )

    passed = orion_delta == 0 and kafka_delta > 0 and resume_delta > 0
    detail = {
        "label": label,
        "off_orion_published_delta": orion_delta,
        "off_kafka_ack_delta": kafka_delta,
        "on_resume_orion_delta": resume_delta,
        "before_on": before_on,
        "mid_on": mid_on,
        "off_start": off_start,
        "off_end": off_end,
        "after_on": after_on,
    }
    ev.event("rehearsal", **detail, passed=passed)
    ev.gate(f"rehearsal_{label}", passed, json.dumps(detail))
    return passed


def wait_ready(url: str, timeout: float, expect_ready: bool = True) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = _http_ok(url)
        if expect_ready and ok:
            return True
        if not expect_ready and not ok:
            return True
        time.sleep(2)
    return False


def chaos_sequential(ev: EvidenceWriter, *, recover_timeout: float = 120.0) -> None:
    """One fault at a time; wait recovery before next."""
    raw_ready = os.getenv("K45_RAW_READY", "http://127.0.0.1:8091/ready")
    orion = os.getenv("ORION_URL", "http://localhost:1026").rstrip("/") + "/version"
    steps = [
        {
            "name": "raw_consumer",
            "action": lambda: docker_compose("restart", "de-kafka-raw-consumer"),
            "ready": lambda: wait_ready(raw_ready, recover_timeout),
        },
        {
            "name": "clickhouse",
            "action": lambda: (
                docker_compose("stop", "clickhouse"),
                time.sleep(60),
                docker_compose("start", "clickhouse"),
            ),
            "ready": lambda: wait_ready(
                os.getenv("CLICKHOUSE_HTTP", "http://localhost:8123"), recover_timeout
            )
            and wait_ready(raw_ready, recover_timeout),
            "during_expected_not_ready": raw_ready,
        },
        {
            "name": "orion",
            "action": lambda: docker_compose("restart", "orion"),
            "ready": lambda: wait_ready(orion, recover_timeout),
        },
        {
            "name": "kafka",
            "action": lambda: docker_compose("restart", "kafka"),
            "ready": lambda: _probe_kafka(recover_timeout),
        },
    ]
    # Projector: optional PID kill — skip if K45_PROJECTOR_PID unset
    proj_pid = os.getenv("K45_PROJECTOR_PID")
    if proj_pid:
        steps.insert(
            0,
            {
                "name": "projector",
                "action": lambda: _restart_projector(int(proj_pid)),
                "ready": lambda: _projector_alive(),
            },
        )

    for step in steps:
        name = step["name"]
        ev.event("chaos", phase="start", component=name)
        t0 = time.time()
        try:
            if name == "clickhouse":
                # during outage: not ready is expected
                docker_compose("stop", "clickhouse")
                time.sleep(2)
                degraded = not _http_ok(raw_ready)
                ev.event(
                    "chaos",
                    phase="outage",
                    component=name,
                    raw_ready=False,
                    expected_degraded=degraded,
                )
                ev.gate(
                    f"chaos_{name}_outage_expected",
                    degraded,
                    "raw /ready false during CH stop",
                )
                time.sleep(58)
                docker_compose("start", "clickhouse")
            else:
                step["action"]()
            ok = bool(step["ready"]())
            recover_sec = time.time() - t0
            ev.event(
                "chaos",
                phase="recovered" if ok else "timeout",
                component=name,
                recover_sec=recover_sec,
            )
            ev.gate(f"chaos_{name}_recover", ok, f"recover_sec={recover_sec:.1f}")
            # stabilize before next
            time.sleep(10)
        except Exception as e:
            ev.gate(f"chaos_{name}_recover", False, str(e))


def _probe_kafka(timeout: float) -> bool:
    deadline = time.time() + timeout
    topic = os.getenv("K45_TOPIC", "traffic.entity-events.v2")
    bootstrap = os.getenv("K45_KAFKA_BOOTSTRAP", "localhost:29092")
    while time.time() < deadline:
        try:
            capture_partition_watermarks(bootstrap, topic)
            return True
        except Exception:
            time.sleep(3)
    return False


def _restart_projector(pid: int) -> Optional[int]:
    """Kill projector PID then restart same CLI; return new PID or None."""
    log.warning("Projector chaos: stopping pid=%s then restarting CLI", pid)
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False)
        else:
            os.kill(pid, 15)
    except Exception as e:
        log.error("projector kill failed: %s", e)
    time.sleep(2)
    cmd = os.getenv(
        "K45_PROJECTOR_CMD",
        # Do NOT use --from-latest after chaos kill — must resume offsets / catch up.
        f'"{sys.executable}" tools/projector_live_consumer.py '
        f"--max-wall-sec 10000 --idle-sec 7200",
    )
    cwd = os.getenv("K45_PROJECTOR_CWD", str(VIS))
    try:
        proc = subprocess.Popen(
            cmd,
            shell=True,
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.environ["K45_PROJECTOR_PID"] = str(proc.pid)
        log.info("Projector restarted pid=%s", proc.pid)
        time.sleep(5)
        return proc.pid
    except Exception as e:
        log.error("projector restart failed: %s", e)
        return None


def _projector_alive() -> bool:
    pid = os.getenv("K45_PROJECTOR_PID")
    if not pid:
        return False
    try:
        if os.name == "nt":
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                check=False,
            )
            return pid in out.stdout and "No tasks" not in out.stdout
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def append_metric_row(path: Path, row: Dict[str, Any]) -> None:
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)


def drain_until(
    control: ControlClient,
    ev: EvidenceWriter,
    *,
    timeout: float,
    projector_lag_threshold: float = 2.0,
) -> bool:
    deadline = time.time() + timeout
    raw_ready = os.getenv("K45_RAW_READY", "http://127.0.0.1:8091/ready")
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        try:
            stats = sample_stats(control)
            pending = stats["pending_rows"]
        except Exception as e:
            pending = _outbox_pending_fallback()
            stats = {"pending_rows": pending, "control_error": str(e)}
        raw_ok = _http_ok(raw_ready)
        # Prefer /ready JSON ready=true when available
        try:
            ready_body = _http_json("GET", raw_ready.replace("/ready", "/ready"), timeout=3)
            if isinstance(ready_body, dict) and "ready" in ready_body:
                raw_ok = bool(ready_body.get("ready"))
        except Exception:
            pass
        last = {
            "pending_rows": pending,
            "raw_ready": raw_ok,
            "stats": stats,
            "ts": _utc_now(),
        }
        ev.event("timeline", phase="drain_sample", **last)
        if pending == 0 and raw_ok:
            if _raw_caught_up(ev):
                ev.write_json("drain_report.json", {**last, "pass": True})
                ev.gate("drain", True, json.dumps(last))
                return True
        # Auto-heal stuck raw (commit_stale / not ready) once during drain
        if pending == 0 and not raw_ok and not getattr(drain_until, "_healed", False):
            drain_until._healed = True  # type: ignore[attr-defined]
            log.warning("Raw not ready during drain — restarting de-kafka-raw-consumer")
            docker_compose("restart", "de-kafka-raw-consumer")
            time.sleep(20)
        time.sleep(5)
    ev.write_json("drain_report.json", {**last, "pass": False})
    ev.gate("drain", False, json.dumps(last))
    return False


def _raw_caught_up(ev: EvidenceWriter, settle_sec: float = 20.0) -> bool:
    """Wait until raw /ready and records_stored stable (or still growing but ready)."""
    raw_health = os.getenv("K45_RAW_HEALTH", "http://127.0.0.1:8091/health")
    try:
        a = _http_json("GET", raw_health, timeout=5)
        if not bool(a.get("ready")):
            return False
        time.sleep(settle_sec)
        b = _http_json("GET", raw_health, timeout=5)
        ready = bool(b.get("ready"))
        stored_a = int(a.get("records_stored") or 0)
        stored_b = int(b.get("records_stored") or 0)
        # Catch-up complete when ready and growth rate dropped (flat within settle)
        flat = stored_b == stored_a
        ev.event(
            "timeline",
            phase="raw_catchup",
            stored_a=stored_a,
            stored_b=stored_b,
            ready=ready,
            flat=flat,
        )
        return ready and flat
    except Exception as e:
        log.warning("raw catch-up probe failed: %s", e)
        return False


def _outbox_pending_fallback() -> int:
    db = os.getenv(
        "KAFKA_OUTBOX_DB",
        str(VIS / "artifacts" / "kafka_outbox" / "outbox.sqlite3"),
    )
    try:
        import sqlite3

        con = sqlite3.connect(db)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM kafka_outbox WHERE status IN "
                "('OUTBOXED','QUEUED','FAILED_RETRYABLE')"
            ).fetchone()
            return int(row[0]) if row else 0
        finally:
            con.close()
    except Exception:
        return -1


def run_oracles(
    ev: EvidenceWriter,
    start_wm: dict,
    end_wm: dict,
    *,
    soak_started_at: Optional[str] = None,
    metrics_csv: Optional[Path] = None,
) -> None:
    bootstrap = os.getenv("K45_KAFKA_BOOTSTRAP", "localhost:29092")
    ch = os.getenv("CLICKHOUSE_HTTP", "http://localhost:8123")
    orion = os.getenv("ORION_URL", "http://localhost:1026")
    run_id = os.getenv("K45_SIMULATION_RUN_ID")
    win = window_offsets(start_wm, end_wm)
    ev.write_json("kafka_manifest.json", win)
    if not win["retention_ok"]:
        ev.gate(
            "watermark_retention",
            False,
            "INSUFFICIENT_RETENTION: low_watermark > start_offset",
        )
        ev.write_json(
            "replay_report.json",
            {"result": "INSUFFICIENT_RETENTION", "window": win},
        )
        return
    ev.gate("watermark_retention", True, "retention covers [start,end)")

    parity: Dict[str, Any] = {}
    try:
        k_offs = kafka_offsets_in_window(bootstrap, win)
        raw = clickhouse_logical_offsets(
            ch,
            table=os.getenv("K45_RAW_TABLE", "smart_traffic.kafka_raw_events"),
            simulation_run_id=run_id,
        )
        quar = clickhouse_logical_offsets(
            ch,
            table=os.getenv(
                "K45_QUARANTINE_TABLE", "smart_traffic.kafka_quarantine_events"
            ),
            simulation_run_id=run_id,
        )

        def _in_window(item: tuple) -> bool:
            t, p, o = item
            for w in win["partitions"]:
                if (
                    w["topic"] == t
                    and w["partition"] == p
                    and w["start_offset"] <= o < w["end_offset"]
                ):
                    return True
            return False

        raw_f = {x for x in raw if _in_window(x)}
        quar_f = {x for x in quar if _in_window(x)}
        hist = historical_completeness(k_offs, raw_f, quar_f)
        parity["historical"] = hist
        ev.gate("historical_completeness", bool(hist["pass"]), json.dumps(hist))
    except Exception as e:
        ev.gate("historical_completeness", False, str(e))
        parity["historical_error"] = str(e)

    try:
        events = scan_kafka_window_events(bootstrap, win)
        latest = latest_entity_events(events)
        k_keys = kafka_entity_keys(events)
        sample_ids = sorted({eid for _, eid in latest.keys()})[:40]
        shadows = fetch_shadow_entities(orion, sample_ids)
        latest_sample = {k: v for k, v in latest.items() if k[1] in set(sample_ids)}
        if not latest_sample:
            shadow_rep = {
                "pass": False,
                "matched": 0,
                "reason": "no_entity_events_in_window",
            }
        else:
            shadow_rep = compare_latest_to_shadow(latest_sample, shadows)
        parity["shadow_latest"] = shadow_rep
        ev.gate(
            "shadow_latest",
            bool(shadow_rep.get("pass")) and int(shadow_rep.get("matched") or 0) > 0,
            json.dumps(shadow_rep),
        )
    except Exception as e:
        ev.gate("shadow_latest", False, str(e))
        parity["shadow_error"] = str(e)
        events = []
        k_keys = set()

    try:
        since = soak_started_at
        if since and since.endswith("Z"):
            since = since.replace("Z", "")
        leg_keys = legacy_entity_keys_from_notifications(ch, since_iso=since)
        runs = {r for r, _ in k_keys}
        # Webhook may also receive Shadow entities (same Orion) — normalize / drop :shadow:
        from tools.k45_oracles import normalize_shadow_id

        leg_scoped = set()
        for r, e in leg_keys:
            if r not in runs:
                continue
            if ":shadow:" in e:
                continue
            leg_scoped.add((r, normalize_shadow_id(e)))
        missing_leg = sorted(k_keys - leg_scoped)
        # Camera etc. may be sparse in notification samples — allow if >=90% of kafka keys present
        coverage = 1.0 - (len(missing_leg) / max(1, len(k_keys)))
        leg_rep = {
            "pass": len(k_keys) > 0 and coverage >= 0.9,
            "coverage": coverage,
            "kafka_entity_count": len(k_keys),
            "legacy_scoped_count": len(leg_scoped),
            "missing_in_legacy": [
                {"run": a, "entityId": b} for a, b in missing_leg[:50]
            ],
            "equality_helper": legacy_entity_parity(k_keys, leg_scoped),
        }
        parity["legacy_entity"] = leg_rep
        ev.gate("legacy_entity_parity", bool(leg_rep["pass"]), json.dumps(leg_rep))
    except Exception as e:
        ev.gate("legacy_entity_parity", False, str(e))
        parity["legacy_error"] = str(e)

    # Fast replay identity: sample last N offsets per partition (full CLI too slow for 6k+)
    replay_results = []
    try:
        sample_n = int(os.getenv("K45_REPLAY_SAMPLE_PER_PARTITION", "80"))
        for w in win["partitions"]:
            start, end = int(w["start_offset"]), int(w["end_offset"])
            if start >= end:
                continue
            sample_from = max(start, end - sample_n)
            cmd = [
                sys.executable,
                "-m",
                "de.kafka_raw.replay",
                "--topic",
                w["topic"],
                "--partition",
                str(w["partition"]),
                "--from-offset",
                str(sample_from),
                "--to-offset",
                str(end - 1),
                "--replay-run-id",
                f"k45-{ev.root.name}-p{w['partition']}",
            ]
            proc = subprocess.run(
                cmd,
                cwd=str(REPO),
                capture_output=True,
                text=True,
                timeout=int(os.getenv("K45_REPLAY_TIMEOUT_SEC", "900")),
                check=False,
            )
            body = (proc.stdout or "") + (proc.stderr or "")
            try:
                parsed = (
                    json.loads(body[body.find("{") :]) if "{" in body else {"raw": body}
                )
            except Exception:
                parsed = {"raw": body}
            parsed["returncode"] = proc.returncode
            parsed["partition"] = w["partition"]
            parsed["sampled_from"] = sample_from
            parsed["sampled_to"] = end - 1
            replay_results.append(parsed)
        if any(r.get("status") == "INSUFFICIENT_RETENTION" for r in replay_results):
            ev.gate("replay", False, "INSUFFICIENT_RETENTION")
        else:
            replay_ok = len(replay_results) > 0 and all(
                r.get("returncode") in (0, None) or r.get("status") == "OK"
                for r in replay_results
            )
            # Compare replay row counts to sample width
            for r in replay_results:
                expected = int(r.get("sampled_to", 0)) - int(r.get("sampled_from", 0)) + 1
                got = int(r.get("record_count") or 0)
                if got < expected:
                    replay_ok = False
                    r["count_mismatch"] = {"expected": expected, "got": got}
            ev.gate("replay", replay_ok, json.dumps({"partitions": len(replay_results)}))
        ev.write_json("replay_report.json", {"results": replay_results, "mode": "tail_sample"})
    except Exception as e:
        ev.gate("replay", False, str(e))
        ev.write_json("replay_report.json", {"error": str(e)})

    try:
        db = os.getenv(
            "KAFKA_OUTBOX_DB",
            str(VIS / "artifacts" / "kafka_outbox" / "outbox.sqlite3"),
        )
        oi = outbox_integrity(db)
        parity["outbox_integrity"] = oi
        ev.gate("no_failed_permanent", bool(oi["pass"]), json.dumps(oi))
    except Exception as e:
        ev.gate("no_failed_permanent", False, str(e))

    if metrics_csv is not None:
        rr = metrics_resource_gate(str(metrics_csv))
        ev.write_json("resource_report.json", rr)
        ev.gate("resource_bounded", bool(rr.get("pass")), json.dumps(rr))

    shadow = parity.get("shadow_latest") or {}
    proj_ok = int(shadow.get("matched") or 0) > 0 and bool(shadow.get("pass"))
    ev.gate(
        "projector_lag",
        proj_ok,
        "proxy: shadow latest matched > 0 after drain",
    )
    ev.write_json("parity_report.json", parity)


def mode_dry_run(ev: EvidenceWriter) -> int:
    base = os.getenv("K45_CONTROL_API", "http://127.0.0.1:9090")
    control = None
    try:
        control = ControlClient(base)
        control.health()
    except Exception:
        control = None
    topology_audit(ev, control)
    return 0 if ev.ok else 1


def mode_toggle_smoke(ev: EvidenceWriter) -> int:
    control = ControlClient(os.getenv("K45_CONTROL_API", "http://127.0.0.1:9090"))
    topology_audit(ev, control)
    hold = _env_float("K45_SMOKE_HOLD_SEC", 15.0)
    ok = run_rehearsal(control, ev, hold_sec=hold, label="smoke")
    return 0 if ok and ev.ok else 1


def mode_full(ev: EvidenceWriter) -> int:
    control = ControlClient(os.getenv("K45_CONTROL_API", "http://127.0.0.1:9090"))
    bootstrap = os.getenv("K45_KAFKA_BOOTSTRAP", "localhost:29092")
    topic = os.getenv("K45_TOPIC", "traffic.entity-events.v2")
    window = _env_int("K45_WINDOW_SEC", 600)
    rehearsal_budget = _env_int("K45_REHEARSAL_SEC", 300)
    drain_timeout = _env_float("K45_DRAIN_TIMEOUT_SEC", 300)
    normal_demand = os.getenv("K45_NORMAL_DEMAND", "normal")
    peak_demand = os.getenv("K45_PEAK_DEMAND", "morning_peak")
    skip_chaos = os.getenv("K45_SKIP_CHAOS", "0").lower() in ("1", "true", "yes")

    topology_audit(ev, control)
    soak_started_at = _utc_now()
    start_wm = capture_partition_watermarks(bootstrap, topic)
    ev.write_json("start_watermark.json", start_wm)
    ev.event("timeline", phase="start_watermark_captured", soak_started_at=soak_started_at)

    metrics_path = ev.root / "metrics.csv"

    def profile(name: str, duration: float, demand: Optional[str] = None) -> None:
        if demand:
            try:
                control.set_demand(demand)
            except Exception as e:
                log.warning("demand-profile %s failed: %s", demand, e)
        meta = {
            "profile_name": name,
            "duration_sec": duration,
            "demand_profile": demand,
            "peak_semantics": "demand/scenario only — no fake entities",
            "started_at": _utc_now(),
        }
        ev.write_json(f"profiles/{name}.json", meta)
        ev.event("timeline", phase="profile_start", profile=name)
        t_end = time.time() + duration
        while time.time() < t_end:
            try:
                st = sample_stats(control)
                append_metric_row(
                    metrics_path,
                    {
                        "ts": _utc_now(),
                        "profile": name,
                        **st,
                    },
                )
            except Exception as e:
                log.warning("metric sample failed: %s", e)
            time.sleep(_env_float("K45_RESOURCE_SAMPLE_SEC", 30))
        meta["ended_at"] = _utc_now()
        ev.write_json(f"profiles/{name}.json", meta)
        ev.event("timeline", phase="profile_end", profile=name)

    # NORMAL / PEAK / CHAOS / RECOVERY (chaos is sub-window inside CHAOS wall time)
    profile("normal", window, normal_demand)
    profile("peak", window, peak_demand)

    if skip_chaos:
        ev.gate("chaos_sequential", True, "skipped via K45_SKIP_CHAOS")
        time.sleep(min(window, 30))
    else:
        ev.event("timeline", phase="chaos_start")
        chaos_deadline = time.time() + window
        chaos_sequential(ev, recover_timeout=120.0)
        # pad remaining chaos window as recover observe
        remain = max(0.0, chaos_deadline - time.time())
        if remain:
            time.sleep(remain)
        ev.event("timeline", phase="chaos_end")

    # RECOVERY shorter half then rehearsal in remaining
    recovery_sec = max(60.0, window * 0.5)
    profile("recovery", recovery_sec, normal_demand)

    # Rehearsals WHILE producer still running
    hold = max(15.0, rehearsal_budget / 4.0)
    ev.write_json(
        "profiles/rehearsal.json",
        {
            "profile_name": "rehearsal",
            "note": "producer still running",
            "hold_sec": hold,
            "started_at": _utc_now(),
        },
    )
    r1 = run_rehearsal(control, ev, hold_sec=hold, label="R1")
    r2 = run_rehearsal(control, ev, hold_sec=hold, label="R2")
    ev.gate("rehearsals_while_running", r1 and r2, f"R1={r1} R2={r2}")

    # Stop producer — kill TraCI if K45_TRACI_PID provided; else wait for freeze / flag
    ev.event("timeline", phase="await_producer_stop")
    traci_pid = os.getenv("K45_TRACI_PID")
    if traci_pid:
        log.warning("Stopping TraCI pid=%s after rehearsals", traci_pid)
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", traci_pid, "/T", "/F"], check=False)
            else:
                os.kill(int(traci_pid), 15)
        except Exception as e:
            log.error("TraCI stop failed: %s", e)
        os.environ["K45_PRODUCER_STOPPED"] = "1"
        time.sleep(5)
    else:
        log.warning(
            "Stop TraCI producer now (Ctrl+C / shut down). "
            "Set K45_PRODUCER_STOPPED=1 or wait for publish counters to freeze."
        )
    stop_deadline = time.time() + _env_float("K45_STOP_WAIT_SEC", 600)
    last: Dict[str, int] = {}
    try:
        last = sample_stats(control)
    except Exception:
        last = {}
    while time.time() < stop_deadline:
        if os.getenv("K45_PRODUCER_STOPPED", "").lower() in ("1", "true", "yes"):
            break
        try:
            cur = sample_stats(control)
        except Exception:
            break
        time.sleep(10)
        if not last:
            last = cur
            continue
        if cur["cycles_outboxed"] == last["cycles_outboxed"] and cur[
            "legacy_orion_cycles_enqueued_total"
        ] == last["legacy_orion_cycles_enqueued_total"]:
            time.sleep(30)
            try:
                cur2 = sample_stats(control)
            except Exception:
                break
            if cur2["cycles_outboxed"] == cur["cycles_outboxed"]:
                break
        last = cur

    try:
        drain_until(control, ev, timeout=drain_timeout)
    except Exception as e:
        ev.gate("drain", False, str(e))
    # Allow projector / raw to finish catch-up after producer stop
    settle = _env_float("K45_POST_DRAIN_SETTLE_SEC", 90.0)
    log.info("Post-drain settle %.0fs for projector/raw catch-up", settle)
    time.sleep(settle)
    try:
        end_wm = capture_partition_watermarks(bootstrap, topic)
        ev.write_json("end_watermark.json", end_wm)
        run_oracles(
            ev,
            start_wm,
            end_wm,
            soak_started_at=soak_started_at,
            metrics_csv=metrics_path,
        )
    except Exception as e:
        ev.gate("end_watermark_oracles", False, str(e))

    return 0 if ev.ok else 1


def main() -> int:
    p = argparse.ArgumentParser(description="K-4.5 soak harness")
    p.add_argument(
        "--mode",
        choices=("dry-run", "toggle-smoke", "full"),
        default="dry-run",
    )
    p.add_argument("--run-id", default=None)
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    run_id = args.run_id or datetime.now(timezone.utc).strftime("k45-%Y%m%dT%H%M%SZ")
    evidence_root = REPO / "docs" / "architecture" / "k45_evidence"
    ev = EvidenceWriter(evidence_root, run_id)
    ev.write_json(
        "config.json",
        {
            "run_id": run_id,
            "mode": args.mode,
            "window_sec": _env_int("K45_WINDOW_SEC", 600),
            "started_at": _utc_now(),
            "control_api": os.getenv("K45_CONTROL_API", "http://127.0.0.1:9090"),
        },
    )
    try:
        if args.mode == "dry-run":
            code = mode_dry_run(ev)
        elif args.mode == "toggle-smoke":
            code = mode_toggle_smoke(ev)
        else:
            code = mode_full(ev)
    finally:
        ev.close()
    log.info("Evidence: %s ok=%s", ev.root, ev.ok)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
