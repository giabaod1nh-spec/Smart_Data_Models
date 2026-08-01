#!/usr/bin/env python3
"""Minimal live A/B collector for K-5 isolation (60–90s, no official hold).

Assumes TraCI producer + projector already running; polls health endpoints.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


def _get(url: str, timeout: float = 3.0) -> Any:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    try:
        return json.loads(body)
    except Exception:
        return {"raw": body, "http": getattr(resp, "status", None)}


def _p95(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    ordered = sorted(vals)
    return ordered[max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--case", required=True, choices=["A", "B", "C"])
    p.add_argument("--projector-health", default="http://127.0.0.1:8093/health")
    p.add_argument("--traci-publish-stats", default="http://127.0.0.1:9090/publish-stats")
    p.add_argument("--hold-sec", type=float, default=75.0)
    p.add_argument("--sample-sec", type=float, default=5.0)
    p.add_argument("--out", required=True)
    p.add_argument("--label", default="")
    p.add_argument("--ledger-path", default="")
    p.add_argument("--orion", default="off")
    args = p.parse_args()

    samples: List[dict] = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < args.hold_sec:
        row: Dict[str, Any] = {"t": round(time.monotonic() - t0, 2)}
        try:
            h = _get(args.projector_health)
            stage = (h.get("stage_latency") or {}) if isinstance(h, dict) else {}
            lag = h.get("consumer_lag_offsets") or {}
            row.update(
                {
                    "e2e_p95": h.get("pipeline_e2e_latency_ms_p95"),
                    "e2e_max": h.get("pipeline_e2e_latency_ms_max"),
                    "e2e_spikes_gt_500ms": h.get("pipeline_e2e_latency_spikes_gt_500ms"),
                    "e2e_n": h.get("pipeline_e2e_latency_sample_count"),
                    "sqlite_p95": (stage.get("sqlite_tx_ms") or {}).get("p95"),
                    "sqlite_max": (stage.get("sqlite_tx_ms") or {}).get("max"),
                    "apply_p95": (stage.get("apply_total_ms") or {}).get("p95"),
                    "apply_max": (stage.get("apply_total_ms") or {}).get("max"),
                    "apply_spikes_gt_500ms": (stage.get("apply_total_ms") or {}).get("spikes_gt_500ms"),
                    "orion_p95": (stage.get("orion_http_ms") or {}).get("p95"),
                    "orion_max": (stage.get("orion_http_ms") or {}).get("max"),
                    "orion_spikes_gt_500ms": (stage.get("orion_http_ms") or {}).get("spikes_gt_500ms"),
                    "buffer_wait_p95": (stage.get("buffer_wait_ms") or {}).get("p95"),
                    "buffer_wait_max": (stage.get("buffer_wait_ms") or {}).get("max"),
                    "capture_to_broker_p95": (stage.get("capture_to_broker_ms") or {}).get("p95"),
                    "capture_to_broker_max": (stage.get("capture_to_broker_ms") or {}).get("max"),
                    "btc_p95": (stage.get("broker_to_consumer_ms") or {}).get("p95"),
                    "partial": h.get("orion_partial_count"),
                    "node_partial": h.get("node_partial_count"),
                    "lag": lag,
                    "lag_sum": sum(int(v) for v in lag.values()) if isinstance(lag, dict) else None,
                    "processed": h.get("processed"),
                    "write_mode": h.get("write_mode"),
                }
            )
        except Exception as e:
            row["projector_error"] = str(e)
        try:
            ps = _get(args.traci_publish_stats)
            kafka = (ps.get("kafka") or {}) if isinstance(ps, dict) else {}
            row["outbox_p95"] = kafka.get("outbox_append_p95_ms")
            row["outbox_commit_p95"] = kafka.get("outbox_commit_p95_ms")
            row["outbox_pending"] = kafka.get("outbox_pending_rows")
        except Exception as e:
            row["traci_error"] = str(e)
        samples.append(row)
        time.sleep(args.sample_sec)

    def _last(key: str) -> Any:
        for s in reversed(samples):
            if s.get(key) is not None:
                return s.get(key)
        return None

    result = {
        "case": args.case,
        "label": args.label or f"live-{args.case}",
        "hold_sec": args.hold_sec,
        "ledger_path": args.ledger_path,
        "orion": args.orion,
        "final": {
            "e2e_p95": _last("e2e_p95"),
            "e2e_max": _last("e2e_max"),
            "e2e_spikes_gt_500ms": _last("e2e_spikes_gt_500ms"),
            "sqlite_p95": _last("sqlite_p95"),
            "sqlite_max": _last("sqlite_max"),
            "apply_p95": _last("apply_p95"),
            "apply_max": _last("apply_max"),
            "apply_spikes_gt_500ms": _last("apply_spikes_gt_500ms"),
            "orion_p95": _last("orion_p95"),
            "orion_max": _last("orion_max"),
            "orion_spikes_gt_500ms": _last("orion_spikes_gt_500ms"),
            "buffer_wait_p95": _last("buffer_wait_p95"),
            "buffer_wait_max": _last("buffer_wait_max"),
            "capture_to_broker_p95": _last("capture_to_broker_p95"),
            "capture_to_broker_max": _last("capture_to_broker_max"),
            "btc_p95": _last("btc_p95"),
            "outbox_p95": _last("outbox_p95"),
            "lag_sum": _last("lag_sum"),
            "partial": _last("partial"),
            "node_partial": _last("node_partial"),
        },
        "samples": samples,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"wrote": str(out), "final": result["final"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
