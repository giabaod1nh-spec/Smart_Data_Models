#!/usr/bin/env python3
"""Independent durable filesystem write/fsync benchmark (K-5 isolation)."""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


def _pct(vals: List[float], q: float) -> Optional[float]:
    if not vals:
        return None
    ordered = sorted(vals)
    return ordered[max(0, min(len(ordered) - 1, int(round(q * (len(ordered) - 1)))))]


def _dist(vals: List[float]) -> Dict[str, Any]:
    if not vals:
        return {"p50": None, "p95": None, "p99": None, "max": None, "sample_count": 0,
                "spikes_gt_100ms": 0, "spikes_gt_500ms": 0}
    return {
        "p50": _pct(vals, 0.50),
        "p95": _pct(vals, 0.95),
        "p99": _pct(vals, 0.99),
        "max": max(vals),
        "sample_count": len(vals),
        "spikes_gt_100ms": sum(1 for v in vals if v > 100.0),
        "spikes_gt_500ms": sum(1 for v in vals if v > 500.0),
    }


def _fsync_write(path: Path, payload: bytes) -> float:
    t0 = time.perf_counter()
    with open(path, "wb", buffering=0) as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    return (time.perf_counter() - t0) * 1000.0


def _seq_write(path: Path, total_bytes: int, chunk: int = 65536) -> float:
    payload = b"x" * chunk
    t0 = time.perf_counter()
    written = 0
    with open(path, "wb", buffering=0) as f:
        while written < total_bytes:
            n = min(chunk, total_bytes - written)
            f.write(payload[:n])
            written += n
        f.flush()
        os.fsync(f.fileno())
    return (time.perf_counter() - t0) * 1000.0


def run(path: Path, *, samples: int, label: str) -> Dict[str, Any]:
    path.mkdir(parents=True, exist_ok=True)
    small = b"k5-isolation-fsync-" + os.urandom(64)
    durable: List[float] = []
    for i in range(samples):
        durable.append(_fsync_write(path / f"small_{i}.bin", small))
    random_small: List[float] = []
    for i in range(samples):
        random_small.append(_fsync_write(path / f"rnd_{i % 17}_{i}.bin", os.urandom(256)))
    seq_ms = _seq_write(path / "seq_1mb.bin", 1_048_576)
    usage = shutil.disk_usage(str(path))
    return {
        "label": label,
        "path": str(path.resolve()),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "disk_total_bytes": usage.total,
        "disk_free_bytes": usage.free,
        "samples": samples,
        "distributions": {
            "small_durable_write_fsync_ms": _dist(durable),
            "random_small_durable_write_fsync_ms": _dist(random_small),
        },
        "sequential_1mib_write_fsync_ms": seq_ms,
        "notes": {
            "antivirus_state": "not modified; host may have realtime scanning",
            "docker_desktop_path": "D:" in str(path.resolve()) or "/host_mnt" in str(path),
        },
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--path", required=True)
    p.add_argument("--label", default="fs")
    p.add_argument("--samples", type=int, default=200)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    result = run(Path(args.path), samples=args.samples, label=args.label)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    d = result["distributions"]["small_durable_write_fsync_ms"]
    print(json.dumps({"wrote": str(out), "fsync_p95_ms": d["p95"], "fsync_max_ms": d["max"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
