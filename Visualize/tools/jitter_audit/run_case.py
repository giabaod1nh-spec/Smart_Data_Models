"""Execute one jitter audit case in-process with probes."""
from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import threading
import time
from pathlib import Path

VIS = Path(__file__).resolve().parents[2]
REPO = VIS.parent
for p in (str(VIS), str(REPO)):
    if p not in sys.path:
        sys.path.insert(0, p)

from tools.jitter_audit.probes import finalize_recorder, install_probes  # noqa: E402
from tools.jitter_audit.recorder import init_recorder  # noqa: E402
from tools.jitter_audit.runtime_verify import verify_runtime  # noqa: E402


def _sample_system(stop: threading.Event, out: list) -> None:
    try:
        import psutil
    except ImportError:
        return
    proc = psutil.Process()
    while not stop.wait(0.5):
        try:
            out.append(
                {
                    "t": time.time(),
                    "cpu_pct": proc.cpu_percent(interval=None),
                    "threads": proc.num_threads(),
                }
            )
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", required=True, choices=["A", "B", "C", "D", "E", "F", "PRIMARY", "D_GC"])
    ap.add_argument("--max-sim-time", type=float, default=180.0)
    ap.add_argument("--gui", action="store_true")
    ap.add_argument("--fast", action="store_true")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--seed-note", default="cfg.SIM_SEED unchanged")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / f"case_{args.case}.jsonl"
    meta_path = out_dir / f"case_{args.case}_meta.json"

    env = os.environ.copy()
    env.setdefault("SUMO_HOME", r"D:\SUMO")
    env["PATH"] = env["SUMO_HOME"] + r"\bin;" + env.get("PATH", "")
    env["AUDIT_CASE"] = args.case
    env["ARCHITECTURE_PROFILE"] = "final"
    env["PUBLISH_NODES"] = "A,B,C,D"
    env["KAFKA_BOOTSTRAP_SERVERS"] = env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    env["LOG_LEVEL"] = env.get("LOG_LEVEL", "WARNING")
    env["ORION_PERF_AUDIT"] = env.get("ORION_PERF_AUDIT", "0")

    noop_outbox = False
    disable_worker = False
    kafka_on = True
    gui = args.gui
    fast = args.fast

    if args.case == "A":
        kafka_on = False
        env["ARCHITECTURE_PROFILE"] = "none"
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "false"
        gui = True
        fast = False
    elif args.case == "B":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        noop_outbox = True
        gui = True
        fast = False
    elif args.case == "C":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        disable_worker = True
        gui = True
        fast = False
    elif args.case == "D":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        gui = True
        fast = False
    elif args.case == "E":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        env["LOG_LEVEL"] = "ERROR"
        env["ORION_PERF_AUDIT"] = "0"
        gui = True
        fast = False
    elif args.case == "F":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        gui = False
        fast = True
    elif args.case == "PRIMARY":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        gui = True
        fast = False
    elif args.case == "D_GC":
        env["ORION_PUBLISH_ENABLED"] = "false"
        env["KAFKA_OUTBOX_ENABLED"] = "true"
        gui = True
        fast = False
        env["AUDIT_DISABLE_GC"] = "1"

    for k, v in env.items():
        os.environ[k] = v

    rv = verify_runtime(env)
    if args.case not in ("A", "B", "C") and not rv.ok:
        meta_path.write_text(json.dumps({"runtime_verification": rv.to_dict()}, indent=2), encoding="utf-8")
        print("RUNTIME VERIFICATION FAILED:", rv.errors, file=sys.stderr)
        return 2

    if env.get("AUDIT_DISABLE_GC") == "1":
        gc.disable()

    init_recorder(jsonl_path)
    install_probes(noop_outbox=noop_outbox, disable_worker=disable_worker)

    sys_samples: list = []
    stop = threading.Event()
    th = threading.Thread(target=_sample_system, args=(stop, sys_samples), daemon=True)
    th.start()

    from app.traci_runner import build_parser, run  # noqa: WPS433

    ns = build_parser().parse_args(
        [
            "--gui" if gui else "--no-gui",
            *(["--fast"] if fast else []),
            "--no-orion",
            "--no-api",
            "--nodes",
            "A,B,C,D",
            "--max-sim-time",
            str(args.max_sim_time),
        ]
    )

    t0 = time.time()
    code = run(ns)
    stop.set()
    th.join(timeout=2.0)
    finalize_recorder()

    from tools.jitter_audit.recorder import get_recorder

    r = get_recorder()
    outbox_stats = r.outbox_stats if r else {}

    meta = {
        "case": args.case,
        "wall_sec": time.time() - t0,
        "max_sim_time": args.max_sim_time,
        "gui": gui,
        "fast": fast,
        "kafka_on": kafka_on,
        "noop_outbox": noop_outbox,
        "disable_worker": disable_worker,
        "runtime_verification": rv.to_dict(),
        "outbox_stats": outbox_stats,
        "system_samples": sys_samples[-120:],
        "seed_note": args.seed_note,
        "orion_perf_audit": env.get("ORION_PERF_AUDIT"),
        "log_level": env.get("LOG_LEVEL"),
        "exit_code": code,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
