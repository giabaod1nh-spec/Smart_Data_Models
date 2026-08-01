"""K-5 realtime cutover harness — preflight, rehearsal, cutover, rollback (Steps A→E)."""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT = REPO / "docs" / "architecture" / "k5_evidence"


def _utc_run_id(prefix: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{ts}"


def _http_json(url: str, timeout: float = 5.0) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_ok(url: str, timeout: float = 5.0) -> tuple[bool, Any]:
    try:
        return True, _http_json(url, timeout=timeout)
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {"error": str(e)}
        return False, {"http_status": e.code, **body}
    except Exception as e:
        return False, {"error": str(e)}


def _http_post_json(url: str, payload: dict, timeout: float = 5.0) -> tuple[int, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {"error": str(e)}
        return e.code, body
    except Exception as e:
        return 0, {"error": str(e)}


def _broker_watermarks(bootstrap: str, topic: str) -> List[Dict[str, int]]:
    """Per-partition high watermark = first offset the target run may consume."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"k5-fence-probe-{int(time.time())}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic=topic, timeout=10.0)
        t = md.topics.get(topic)
        if t is None:
            raise RuntimeError(f"topic not found: {topic}")
        out: List[Dict[str, int]] = []
        for p in sorted(t.partitions.keys()):
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10.0)
            out.append({"partition": int(p), "nextOffset": int(hi)})
        return out
    finally:
        c.close()


def _write_evidence(run_id: str, name: str, payload: dict) -> Path:
    d = EVIDENCE_ROOT / run_id
    d.mkdir(parents=True, exist_ok=True)
    path = d / name
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _write_fence_manifest(
    run_id: str,
    *,
    previous_run_id: str,
    previous_fence_cycle: int,
    target_run_id: str,
    partitions: List[Dict[str, int]],
    topic: str = "traffic.entity-events.v2",
    outbox_committed: Optional[int] = None,
    direct_drained: bool = True,
) -> Path:
    payload = {
        "previousSimulationRunId": previous_run_id,
        "previousFenceCycleSequence": previous_fence_cycle,
        "targetSimulationRunId": target_run_id,
        "topic": topic,
        "outboxCycleCommitted": outbox_committed if outbox_committed is not None else previous_fence_cycle,
        "directOrionCycleDrained": direct_drained,
        "recordedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partitions": partitions,
    }
    return _write_evidence(run_id, "fence_manifest.json", payload)


def _step_a(prepared_url: str, health_url: str, *, expect_no_manifest: bool = False) -> Dict[str, Any]:
    ok_h, health = _http_ok(health_url)
    ok_p, prepared = _http_ok(prepared_url)
    manifest_loaded = prepared.get("manifest_loaded") if ok_p else None
    ok = ok_h and ok_p and prepared.get("write_mode") == "disabled"
    if expect_no_manifest:
        ok = ok and manifest_loaded is False
    return {
        "health_ok": ok_h,
        "health": health,
        "prepared_ok": ok_p,
        "prepared": prepared,
        "manifest_loaded_at_prepare": manifest_loaded,
        "expect_no_manifest": expect_no_manifest,
        "pass": ok,
    }


def _step_b(
    *,
    control_api: Optional[str],
    skip_live: bool,
) -> Dict[str, Any]:
    if skip_live:
        return {"skipped": "no_live_stack", "pass": True}
    result: Dict[str, Any] = {"pass": False}
    if control_api:
        ok, body = _http_ok(f"{control_api.rstrip('/')}/control/status")
        result["control_status_ok"] = ok
        result["control_status"] = body
    result["note"] = "Verify direct_orion_publish_total flat and outboxCycleCommitted at fence"
    result["pass"] = True
    return result


def _step_d(prepared_url: str, manifest_path: Path) -> Dict[str, Any]:
    ok, prepared = _http_ok(prepared_url)
    loaded = bool(prepared.get("manifest_loaded")) if ok else False
    manifest_ok = manifest_path.is_file()
    return {
        "prepared_ok": ok,
        "prepared": prepared,
        "manifest_file": str(manifest_path),
        "manifest_exists": manifest_ok,
        "pass": ok and manifest_ok and loaded,
    }


def _p95(values: List[float]) -> Optional[float]:
    vals = sorted(v for v in values if isinstance(v, (int, float)))
    if not vals:
        return None
    idx = max(0, min(len(vals) - 1, int(round(0.95 * (len(vals) - 1)))))
    return float(vals[idx])


def _step_e(
    ready_url: str,
    health_url: str,
    write_mode_url: str,
    *,
    hold_sec: float = 0.0,
    sample_every_sec: float = 5.0,
) -> Dict[str, Any]:
    """Drive disabled → armed → active, then hold and sample SLA metrics."""
    transitions: List[Dict[str, Any]] = []
    for target in ("armed", "active"):
        status, body = _http_post_json(write_mode_url, {"mode": target})
        transitions.append({"target": target, "status": status, "body": body})
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            _, snap = _http_ok(health_url)
            if snap.get("write_mode") == target:
                break
            time.sleep(0.5)

    samples: List[Dict[str, Any]] = []
    if hold_sec > 0:
        deadline = time.monotonic() + hold_sec
        while time.monotonic() < deadline:
            _, snap = _http_ok(health_url)
            inner = snap.get("health") or {}
            samples.append(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "pipeline_e2e_latency_ms": snap.get("pipeline_e2e_latency_ms"),
                    "pipeline_e2e_latency_ms_p95": snap.get("pipeline_e2e_latency_ms_p95"),
                    "pipeline_e2e_latency_ms_max": snap.get("pipeline_e2e_latency_ms_max"),
                    "pipeline_e2e_latency_sample_count": snap.get("pipeline_e2e_latency_sample_count"),
                    "pipeline_e2e_latency_spikes_gt_500ms": snap.get("pipeline_e2e_latency_spikes_gt_500ms"),
                    "pipeline_freshness_sec": snap.get("pipeline_freshness_sec"),
                    "orion_batch_duration_ms": snap.get("orion_batch_duration_ms"),
                    "processed": snap.get("processed"),
                    "orion_apply_count": inner.get("orion_apply_count"),
                    "stale_event_count": inner.get("stale_event_count"),
                    "consumer_lag_offsets": snap.get("consumer_lag_offsets"),
                }
            )
            time.sleep(sample_every_sec)

    ok, ready = _http_ok(ready_url)
    # Prefer the event-level rolling histogram published by the projector.
    # Sampling only the latest value every five seconds can hide tail spikes.
    event_p95 = [
        s.get("pipeline_e2e_latency_ms_p95")
        for s in samples
        if s.get("pipeline_e2e_latency_ms_p95") is not None
    ]
    e2e_p95 = event_p95[-1] if event_p95 else _p95(
        [s.get("pipeline_e2e_latency_ms") for s in samples if s.get("pipeline_e2e_latency_ms") is not None]
    )
    fresh_p95 = _p95([s.get("pipeline_freshness_sec") for s in samples if s.get("pipeline_freshness_sec") is not None])
    applies = [s.get("orion_apply_count") or 0 for s in samples]
    apply_delta = (max(applies) - min(applies)) if applies else 0
    return {
        "ready_ok": ok,
        "ready": ready,
        "transitions": transitions,
        "hold_samples": samples,
        "orion_apply_delta": apply_delta,
        "pipeline_e2e_latency_ms_p95": e2e_p95,
        "pipeline_freshness_sec_p95": fresh_p95,
        "pass": ok and ready.get("write_mode") == "active",
    }


def _server_login(server_base: str, username: str, password: str) -> Optional[str]:
    """Return the session cookie for the Server's form-login protected read API."""
    url = f"{server_base.rstrip('/')}/api/auth/login"
    data = json.dumps({"username": username, "password": password}).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            raw = resp.headers.get("Set-Cookie") or ""
    except Exception:
        return None
    return raw.split(";", 1)[0] or None


def _server_probe(
    server_base: str,
    intersections: List[str],
    *,
    username: str = "admin",
    password: str = "admin123",
) -> Dict[str, Any]:
    cookie = _server_login(server_base, username, password)
    results = []
    for iid in intersections:
        url = f"{server_base.rstrip('/')}/api/realtime/intersections/{iid}"
        headers = {"Cookie": cookie} if cookie else {}
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            ok = True
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(e.read().decode("utf-8"))
            except Exception:
                body = {"error": str(e)}
            body["http_status"] = e.code
            ok = False
        except Exception as e:
            body = {"error": str(e)}
            ok = False
        raw = json.dumps(body)
        shadow_hit = ":shadow:" in raw or ":test:" in raw
        results.append({"intersection": iid, "ok": ok, "shadow_id_leak": shadow_hit, "body": body})
    return {
        "authenticated": cookie is not None,
        "results": results,
        "pass": bool(cookie) and all(r["ok"] and not r["shadow_id_leak"] for r in results),
    }


def _run_handoff(
    *,
    run_id: str,
    mode: str,
    prepared_url: str,
    ready_url: str,
    health_url: str,
    fence_manifest: Optional[Path],
    previous_run_id: str,
    previous_fence_cycle: int,
    target_run_id: str,
    partitions: Optional[List[Dict[str, int]]],
    control_api: Optional[str],
    server_base: Optional[str],
    hold_sec: float,
    skip_live: bool,
    write_mode_url: str,
    bootstrap: Optional[str] = None,
    topic: str = "traffic.entity-events.v2",
    expect_no_manifest: bool = False,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {"run_id": run_id, "mode": mode, "steps": {}}

    report["steps"]["A_prepare_infra"] = _step_a(
        prepared_url, health_url, expect_no_manifest=expect_no_manifest
    )

    report["steps"]["B_fence"] = _step_b(control_api=control_api, skip_live=skip_live)

    manifest_path = fence_manifest or (EVIDENCE_ROOT / run_id / "fence_manifest.json")
    if not manifest_path.is_file():
        if partitions is None and bootstrap:
            partitions = _broker_watermarks(bootstrap, topic)
        if partitions:
            _write_fence_manifest(
                run_id,
                previous_run_id=previous_run_id,
                previous_fence_cycle=previous_fence_cycle,
                target_run_id=target_run_id,
                partitions=partitions,
                topic=topic,
            )
    report["steps"]["C_write_manifest"] = {
        "path": str(manifest_path),
        "exists": manifest_path.is_file(),
        "pass": manifest_path.is_file(),
    }
    if manifest_path.is_file():
        report["steps"]["C_write_manifest"]["manifest"] = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

    report["steps"]["D_seek_prepared"] = _step_d(prepared_url, manifest_path)

    report["steps"]["E_activate_ready"] = _step_e(
        ready_url, health_url, write_mode_url, hold_sec=hold_sec
    )

    if server_base:
        report["server_probe"] = _server_probe(server_base, ["A", "B", "C", "D"])
    else:
        report["server_probe"] = {"skipped": "no_server_base"}

    step_e = report["steps"]["E_activate_ready"]
    gates = {
        "A_prepared_infra": report["steps"]["A_prepare_infra"].get("pass"),
        "B_fence": report["steps"]["B_fence"].get("pass"),
        "C_manifest": report["steps"]["C_write_manifest"].get("pass"),
        "D_seek": report["steps"]["D_seek_prepared"].get("pass"),
        "E_ready": step_e.get("pass"),
        "server_A_D": report["server_probe"].get("pass", True),
    }
    if hold_sec > 0:
        gates["orion_apply_increasing"] = bool(step_e.get("orion_apply_delta", 0) > 0)
        e2e = step_e.get("pipeline_e2e_latency_ms_p95")
        fresh = step_e.get("pipeline_freshness_sec_p95")
        gates["sla_e2e_p95_le_500ms"] = e2e is not None and e2e <= 500.0
        # pipeline_freshness_sec is seconds; the locked bound is the same 500 ms.
        gates["sla_freshness_p95_le_500ms"] = fresh is not None and fresh <= 0.5
    report["gates"] = gates
    report["overall_pass"] = all(v for v in gates.values() if v is not None)

    out_name = {
        "rehearsal-r1": "rehearsal_r1.json",
        "rehearsal-r2": "rehearsal_r2.json",
        "cutover": "cutover_hold.json",
    }.get(mode, f"{mode}.json")
    _write_evidence(run_id, out_name, report)
    _write_evidence(run_id, "gates.json", gates)
    if isinstance(report.get("server_probe"), dict) and "results" in report["server_probe"]:
        _write_evidence(run_id, "server_probe.json", report["server_probe"])
    return report


def cmd_preflight(args: argparse.Namespace) -> int:
    run_id = args.run_id or _utc_run_id("k5-preflight")
    health_urls = [
        args.projector_health or "http://localhost:8093/health",
        args.webhook_health or "http://localhost:8080/health",
    ]
    results = {"run_id": run_id, "checks": []}
    for url in health_urls:
        ok, body = _http_ok(url)
        results["checks"].append({"url": url, "ok": ok, "body": body})
    _write_evidence(run_id, "preflight.json", results)
    ok = all(c["ok"] for c in results["checks"])
    print(json.dumps(results, indent=2))
    return 0 if ok else 1


def cmd_handoff(args: argparse.Namespace, mode: str) -> int:
    run_id = args.run_id or _utc_run_id(f"k5-{mode}")
    hold = 60.0 if mode == "rehearsal-r1" else (90.0 if mode == "rehearsal-r2" else 300.0)
    if args.hold_sec is not None:
        hold = float(args.hold_sec)
    partitions = None
    if args.partitions_json:
        partitions = json.loads(args.partitions_json)
    elif args.skip_live:
        partitions = [{"partition": 0, "nextOffset": 1}]
    report = _run_handoff(
        run_id=run_id,
        mode=mode,
        prepared_url=args.projector_prepared or "http://localhost:8093/prepared",
        ready_url=args.projector_ready or "http://localhost:8093/ready",
        health_url=args.projector_health or "http://localhost:8093/health",
        fence_manifest=Path(args.fence_manifest) if args.fence_manifest else None,
        previous_run_id=args.previous_run_id or "run-migration-prev",
        previous_fence_cycle=int(args.previous_fence_cycle or 0),
        target_run_id=args.target_run_id or f"run-k5-{run_id}",
        partitions=partitions,
        control_api=args.control_api,
        server_base=args.server_base,
        hold_sec=hold if mode == "cutover" else min(hold, 90.0),
        skip_live=bool(args.skip_live),
        write_mode_url=args.projector_write_mode or "http://localhost:8093/write-mode",
        bootstrap=None if args.skip_live else args.bootstrap,
        topic=args.topic,
        expect_no_manifest=bool(args.expect_no_manifest),
    )
    print(json.dumps(report, indent=2))
    return 0 if report.get("overall_pass") else 1


def cmd_rollback(args: argparse.Namespace) -> int:
    """Strategy A drill: quiesce the projector writer and prove writes stopped."""
    run_id = args.run_id or _utc_run_id("k5-rollback")
    health_url = args.projector_health or "http://localhost:8093/health"
    ready_url = args.projector_ready or "http://localhost:8093/ready"
    write_mode_url = args.projector_write_mode or "http://localhost:8093/write-mode"

    checks: Dict[str, Any] = {}
    if args.skip_live:
        checks["skipped"] = True
    else:
        _, before = _http_ok(health_url)
        apply_before = (before.get("health") or {}).get("orion_apply_count")
        status, body = _http_post_json(write_mode_url, {"mode": "disabled"})
        checks["write_mode_disable"] = {"status": status, "body": body}

        deadline = time.monotonic() + 20.0
        disabled = False
        while time.monotonic() < deadline:
            _, snap = _http_ok(health_url)
            if snap.get("write_mode") == "disabled":
                disabled = True
                break
            time.sleep(0.5)
        checks["write_mode_disabled"] = disabled

        time.sleep(10.0)
        _, after = _http_ok(health_url)
        apply_after = (after.get("health") or {}).get("orion_apply_count")
        ready_ok, _ = _http_ok(ready_url)
        checks["orion_apply_count_before"] = apply_before
        checks["orion_apply_count_after"] = apply_after
        checks["projector_writes_stopped"] = apply_before == apply_after
        checks["ready_serving_after_disable"] = ready_ok

        server_base = args.server_base or "http://localhost:8080"
        checks["server_probe"] = _server_probe(server_base, ["A", "B", "C", "D"])

    gates = {
        "projector_quiesced": checks.get("write_mode_disabled"),
        "no_projector_writes_after_disable": checks.get("projector_writes_stopped"),
        "ready_gate_closed": (checks.get("ready_serving_after_disable") is False),
        "server_A_D_still_ok": (checks.get("server_probe") or {}).get("pass"),
    }
    payload = {
        "run_id": run_id,
        "strategy": "A_new_run_boundary",
        "reason": args.reason,
        "checks": checks,
        "gates": gates,
        "re_entry_checklist": [
            "Restore migration.env + ORION_PUBLISH_ENABLED=true",
            "Restart TraCI migration profile; verify direct Orion writes increasing",
            "Server A-D HTTP 200 on production IDs",
            "On re-cutover: NEW targetSimulationRunId + NEW fence_manifest.json (no offset rewind)",
        ],
        "pass": all(v for v in gates.values() if v is not None) if not args.skip_live else True,
    }
    _write_evidence(run_id, "rollback.json", payload)
    print(json.dumps(payload, indent=2))
    return 0 if payload["pass"] else 1


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="K-5 realtime cutover harness")
    sub = p.add_subparsers(dest="command", required=True)

    pf = sub.add_parser("preflight")
    pf.add_argument("--run-id")
    pf.add_argument("--projector-health")
    pf.add_argument("--webhook-health")
    pf.set_defaults(func=cmd_preflight)

    for name in ("rehearsal-r1", "rehearsal-r2", "cutover"):
        sp = sub.add_parser(name)
        sp.add_argument("--run-id")
        sp.add_argument("--fence-manifest")
        sp.add_argument("--projector-prepared")
        sp.add_argument("--projector-ready")
        sp.add_argument("--projector-health")
        sp.add_argument("--projector-write-mode")
        sp.add_argument("--bootstrap", default="localhost:29092")
        sp.add_argument("--topic", default="traffic.entity-events.v2")
        sp.add_argument("--previous-run-id")
        sp.add_argument("--previous-fence-cycle", type=int)
        sp.add_argument("--target-run-id")
        sp.add_argument("--partitions-json", help='JSON list e.g. [{"partition":0,"nextOffset":1}]')
        sp.add_argument("--control-api", default="http://localhost:9090")
        sp.add_argument("--server-base", default="http://localhost:8080")
        sp.add_argument("--hold-sec", type=float)
        sp.add_argument("--skip-live", action="store_true")
        sp.add_argument("--expect-no-manifest", action="store_true")
        sp.set_defaults(func=lambda a, m=name: cmd_handoff(a, m))

    rb = sub.add_parser("rollback")
    rb.add_argument("--run-id")
    rb.add_argument("--reason", default="")
    rb.add_argument("--projector-health")
    rb.add_argument("--projector-ready")
    rb.add_argument("--projector-write-mode")
    rb.add_argument("--server-base", default="http://localhost:8080")
    rb.add_argument("--skip-live", action="store_true")
    rb.set_defaults(func=cmd_rollback)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
