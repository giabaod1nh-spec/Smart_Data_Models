#!/usr/bin/env python3
"""Generate docs/health_report.md with real PASS/WARNING/FAIL/NOT_CHECKED statuses."""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

VISUALIZE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(VISUALIZE))


def _status(ok: bool, *, skipped: bool = False, warn: bool = False) -> str:
    if skipped:
        return "NOT_CHECKED"
    if warn:
        return "WARNING"
    return "PASS" if ok else "FAIL"


def check_registry() -> tuple[str, str]:
    try:
        from configuration.model_params import get_registry

        reg = get_registry(reload=True)
        ok = (
            abs(reg.get_pcu("motorcycle") - 0.24) < 1e-9
            and reg.is_frozen
            and bool(reg.config_hash)
            and reg.profile_id == "hanoi_signalized_mixed_traffic_pce_024"
        )
        detail = f"profile={reg.profile_id} moto={reg.get_pcu('motorcycle')} hash={reg.config_hash[:12]}"
        return _status(ok), detail
    except Exception as e:
        return "FAIL", str(e)


def check_catalog_json() -> tuple[str, str]:
    path = VISUALIZE / "docs" / "parameter_catalog.json"
    if not path.is_file():
        return "FAIL", "parameter_catalog.json missing"
    try:
        from configuration.model_params import get_registry

        data = json.loads(path.read_text(encoding="utf-8"))
        reg = get_registry()
        ok = (
            data.get("schema_version") == reg.schema_version
            and data.get("config_hash") == reg.config_hash
            and any(
                p.get("key") == "pcu.motorcycle" and abs(float(p.get("value")) - 0.24) < 1e-9
                for p in data.get("parameters", [])
            )
        )
        return _status(ok), f"keys={len(data.get('parameters', []))}"
    except Exception as e:
        return "FAIL", str(e)


def check_ast_gate() -> tuple[str, str]:
    script = VISUALIZE / "tools" / "check_magic_numbers.py"
    if not script.is_file():
        return "FAIL", "check_magic_numbers.py missing"
    r = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(VISUALIZE),
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        return "PASS", "clean"
    return "FAIL", (r.stderr or r.stdout or "")[:500]


def check_dependency_doc() -> tuple[str, str]:
    path = VISUALIZE / "docs" / "parameter_dependency.md"
    if not path.is_file():
        return "FAIL", "missing"
    text = path.read_text(encoding="utf-8")
    needed = ["motorcycle", "trafficStatus", "K_JAM", "theoreticalSpeed"]
    missing = [n for n in needed if n.lower() not in text.lower()]
    return _status(not missing), f"missing={missing}" if missing else "ok"


def check_orion() -> tuple[str, str]:
    try:
        import requests

        r = requests.get("http://localhost:1026/version", timeout=2)
        return _status(r.status_code == 200), f"status={r.status_code}"
    except Exception as e:
        return "NOT_CHECKED", f"broker unreachable: {e}"


def check_traci_smoke() -> tuple[str, str]:
    # Health report does not run TraCI by default (slow). Mark NOT_CHECKED unless env set.
    import os

    if os.getenv("HEALTH_RUN_TRACI") != "1":
        return "NOT_CHECKED", "set HEALTH_RUN_TRACI=1 to execute"
    try:
        from simulation.backend import SumoBackend

        b = SumoBackend(use_gui=False)
        b.start()
        for _ in range(5):
            b.step()
        ok = bool(b.simulation_run_id) and bool(b.run_manifest)
        b.stop()
        return _status(ok), f"run_id={b.simulation_run_id}"
    except Exception as e:
        return "FAIL", str(e)


def main() -> None:
    checks = [
        ("ParameterRegistry load/freeze/moto 0.24", check_registry),
        ("parameter_catalog.json hash match", check_catalog_json),
        ("AST magic-number gate", check_ast_gate),
        ("parameter_dependency.md chains", check_dependency_doc),
        ("Orion broker", check_orion),
        ("TraCI short smoke", check_traci_smoke),
    ]
    rows = []
    for name, fn in checks:
        status, detail = fn()
        # Never mark PASS without a real check — NOT_CHECKED stays NOT_CHECKED
        if status == "PASS" and not detail:
            status = "FAIL"
            detail = "empty detail"
        rows.append((name, status, detail))

    generated_at = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Health Report",
        "",
        f"Generated: `{generated_at}`",
        "",
        "| Check | Status | Detail |",
        "| --- | --- | --- |",
    ]
    for name, status, detail in rows:
        safe = str(detail).replace("|", "/").replace("\n", " ")[:200]
        lines.append(f"| {name} | **{status}** | {safe} |")
    lines.append("")
    lines.append("Status vocabulary: PASS | WARNING | FAIL | NOT_CHECKED.")
    lines.append("NOT_CHECKED must never be reported as PASS.")
    lines.append("")
    out = VISUALIZE / "docs" / "health_report.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out)
    fails = [r for r in rows if r[1] == "FAIL"]
    raise SystemExit(1 if fails else 0)


if __name__ == "__main__":
    main()
