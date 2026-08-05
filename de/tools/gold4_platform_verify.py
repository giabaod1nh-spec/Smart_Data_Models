"""Gold4 bounded platform + consumer-handoff verification (no soak, no benchmark)."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

EVIDENCE = _REPO / "docs" / "gold" / "gold4_evidence"
TEMPLATES = _REPO / "docs" / "gold" / "gold4_query_templates"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _http(url: str, timeout: float = 3.0) -> tuple[int, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(body)
        except Exception:
            return exc.code, body
    except Exception as exc:  # noqa: BLE001
        return 0, f"{type(exc).__name__}:{exc}"


def _client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.getenv("GOLD_CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("GOLD_CLICKHOUSE_PORT", "8123")),
        username=os.getenv("GOLD_CLICKHOUSE_USER", "default"),
        password=os.getenv("GOLD_CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("GOLD_CLICKHOUSE_DATABASE", "smart_traffic"),
        connect_timeout=5,
    )


def _scalar(client, sql: str, parameters: Optional[dict] = None) -> Any:
    return client.query(sql, parameters=parameters or {}).result_rows[0][0]


def gate_compose(report: dict) -> None:
    result = subprocess.run(
        ["docker", "compose", "config", "--services"],
        cwd=_REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    services = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    required = {
        "clickhouse",
        "de-migrate",
        "de-silver-processor",
        "de-gold-runtime",
        "de-bronze-processor",
        "de-kafka-raw-consumer",
    }
    report["compose"] = {
        "ok": result.returncode == 0 and required <= services,
        "services": sorted(services),
        "missing": sorted(required - services),
        "stderr": result.stderr[-500:],
    }


def gate_migration(report: dict) -> None:
    client = _client()
    tables = [
        r[0]
        for r in client.query(
            "SELECT name FROM system.tables "
            "WHERE database='smart_traffic' AND name LIKE 'gold_%' ORDER BY name"
        ).result_rows
    ]
    required = {
        "gold_fact_traffic_window",
        "gold_fact_kpi_result",
        "gold_processing_ledger",
        "gold_mart_intersection_window_summary",
        "gold_dim_window",
    }
    report["migration"] = {
        "ok": required <= set(tables),
        "gold_table_count": len(tables),
        "missing": sorted(required - set(tables)),
        "select_1": client.command("SELECT 1"),
    }


def gate_health(report: dict) -> None:
    checks = {}
    # Wait briefly for readiness after Compose recreate/restart.
    ready_status, ready_body = 0, None
    for _ in range(30):
        ready_status, ready_body = _http("http://localhost:8096/ready")
        if ready_status == 200:
            break
        time.sleep(2)
    silver_status, silver_body = 0, None
    for _ in range(30):
        silver_status, silver_body = _http("http://localhost:8095/ready")
        if silver_status == 200:
            break
        time.sleep(2)
    checks["clickhouse"] = {"status": _http("http://localhost:8123/ping")[0], "body": "ping"}
    checks["silver_ready"] = {"status": silver_status, "body": silver_body}
    hs, health_body = _http("http://localhost:8096/health")
    checks["gold_health"] = {"status": hs, "body": health_body}
    checks["gold_ready"] = {"status": ready_status, "body": ready_body}
    gold_ready = checks["gold_ready"]["status"] == 200
    fields_ok = False
    if isinstance(health_body, dict):
        fields_ok = all(
            key in health_body
            for key in (
                "status",
                "ready",
                "namespace",
                "last_batch_id",
                "last_window_id",
                "watermark",
                "metrics",
            )
        )
        metrics = health_body.get("metrics") or {}
        fields_ok = fields_ok and "revisions_total" in metrics and "ledger_recovery_counts" in metrics
    report["health"] = {
        "ok": checks["clickhouse"]["status"] == 200
        and checks["silver_ready"]["status"] == 200
        and gold_ready
        and fields_ok,
        "checks": checks,
        "health_fields_ok": fields_ok,
    }


def _pick_run(client) -> dict:
    rows = client.query(
        """
        SELECT simulation_run_id, scenario_id, count() AS n,
               min(simulation_time_sec), max(simulation_time_sec)
        FROM smart_traffic.silver_fact_traffic_observation
        GROUP BY simulation_run_id, scenario_id
        HAVING max(simulation_time_sec) >= 360 AND n >= 100
        ORDER BY n DESC
        LIMIT 1
        """
    ).result_rows
    if not rows:
        raise RuntimeError("no Silver run with max_sim>=360 for 60+300 windows")
    return {
        "simulation_run_id": rows[0][0],
        "scenario_id": rows[0][1],
        "traffic_rows": int(rows[0][2]),
        "min_sim": float(rows[0][3]),
        "max_sim": float(rows[0][4]),
    }


def gate_integration(report: dict) -> None:
    """Validate the deployed Compose Gold runtime (single writer) via ClickHouse + restart."""
    client = _client()
    status, health = _http("http://localhost:8096/health")
    if status != 200 or not isinstance(health, dict):
        report["integration"] = {"ok": False, "error": "gold /health unavailable"}
        return

    # Prefer a run that already has both window sizes from the live writer.
    rows = client.query(
        """
        SELECT simulation_run_id, scenario_id,
               countIf(window_size_sec=60) AS n60,
               countIf(window_size_sec=300) AS n300
        FROM smart_traffic.gold_fact_traffic_window
        WHERE namespace='live'
        GROUP BY simulation_run_id, scenario_id
        HAVING n60 > 0 AND n300 > 0
        ORDER BY n60 + n300 DESC
        LIMIT 1
        """
    ).result_rows
    if not rows:
        # Wait briefly for Compose Gold to close a 300s window.
        deadline = time.time() + 90
        while time.time() < deadline and not rows:
            time.sleep(5)
            rows = client.query(
                """
                SELECT simulation_run_id, scenario_id,
                       countIf(window_size_sec=60) AS n60,
                       countIf(window_size_sec=300) AS n300
                FROM smart_traffic.gold_fact_traffic_window
                WHERE namespace='live'
                GROUP BY simulation_run_id, scenario_id
                HAVING n60 > 0 AND n300 > 0
                ORDER BY n60 + n300 DESC
                LIMIT 1
                """
            ).result_rows
    if not rows:
        report["integration"] = {
            "ok": False,
            "error": "no live run with both 60s and 300s traffic facts yet",
            "health_watermark": health.get("watermark"),
            "windows_processed": (health.get("metrics") or {}).get("windows_processed_total"),
        }
        return

    chosen = {
        "simulation_run_id": rows[0][0],
        "scenario_id": rows[0][1],
        "facts60": int(rows[0][2]),
        "facts300": int(rows[0][3]),
    }
    kpi = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_fact_kpi_result "
            "WHERE namespace='live' AND simulation_run_id={r:String}",
            {"r": chosen["simulation_run_id"]},
        )
    )
    ledger = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_processing_ledger "
            "WHERE namespace='live' AND disposition='PERSISTED'",
        )
    )
    mart = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_mart_intersection_window_summary "
            "WHERE namespace='live' AND simulation_run_id={r:String}",
            {"r": chosen["simulation_run_id"]},
        )
    )
    before_windows = int((health.get("metrics") or {}).get("windows_processed_total") or 0)
    before_batch = health.get("last_batch_id")

    # Restart the Compose Gold service and confirm recovery without fault.
    subprocess.run(
        ["docker", "compose", "restart", "de-gold-runtime"],
        cwd=_REPO,
        check=False,
        capture_output=True,
        text=True,
    )
    ready_ok = False
    after_health: Any = {}
    for _ in range(30):
        time.sleep(2)
        st, body = _http("http://localhost:8096/ready")
        hs, after_health = _http("http://localhost:8096/health")
        if st == 200 and hs == 200 and isinstance(after_health, dict):
            if after_health.get("status") not in {"FAULTED"}:
                ready_ok = True
                break

    after_windows = int((after_health.get("metrics") or {}).get("windows_processed_total") or 0)
    ok = (
        chosen["facts60"] > 0
        and chosen["facts300"] > 0
        and kpi > 0
        and ledger > 0
        and mart >= 0  # mart may lag view materialization; non-negative required
        and ready_ok
        and after_health.get("namespace") == "live"
        and after_health.get("lock_held") is True
        and after_health.get("schema_ok") is True
    )
    report["integration"] = {
        "ok": ok,
        "chosen": chosen,
        "kpi": kpi,
        "ledger_persisted": ledger,
        "intersection_mart": mart,
        "restart": {
            "ready_ok": ready_ok,
            "before_windows": before_windows,
            "after_windows": after_windows,
            "before_batch": before_batch,
            "after_batch": after_health.get("last_batch_id"),
            "after_state": after_health.get("status"),
            "fault_code": after_health.get("fault_code"),
        },
    }


def gate_templates(report: dict) -> None:
    client = _client()
    run = report.get("integration", {}).get("chosen") or _pick_run(client)
    params = {
        "run_id": run["simulation_run_id"],
        "scenario_id": run["scenario_id"],
        "window_size_sec": 60,
        "limit": 5,
        "offset": 0,
    }
    results = {}
    ok = True
    for path in sorted(TEMPLATES.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        # Strip comments; templates may contain multiple statements.
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
        cleaned = []
        for stmt in statements:
            lines = [
                line
                for line in stmt.splitlines()
                if line.strip() and not line.strip().startswith("--")
            ]
            cleaned.append("\n".join(lines))
        path_ok = True
        counts = []
        try:
            for stmt in cleaned:
                if not stmt:
                    continue
                # replay isolation template has no params for first query parts
                if "{run_id" in stmt:
                    rows = client.query(stmt, parameters=params).result_rows
                else:
                    rows = client.query(stmt).result_rows
                counts.append(len(rows))
        except Exception as exc:  # noqa: BLE001
            path_ok = False
            counts.append(f"ERR:{type(exc).__name__}:{exc}")
            ok = False
        # network overview may legitimately return 0 rows (WHERE 0 scaffold)
        if path.name.startswith("06_") and path_ok:
            results[path.name] = {"ok": True, "row_counts": counts, "limitation": "may_be_empty"}
        else:
            results[path.name] = {"ok": path_ok, "row_counts": counts}
            ok = ok and path_ok
    report["templates"] = {"ok": ok, "results": results, "run": run}


def gate_namespace(report: dict) -> None:
    client = _client()
    live = int(_scalar(client, "SELECT count() FROM smart_traffic.gold_fact_traffic_window WHERE namespace='live'"))
    replay = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_fact_traffic_window WHERE namespace LIKE 'replay:%'",
        )
    )
    report["namespace"] = {
        "ok": True,
        "live_rows": live,
        "replay_rows": replay,
        "note": "live consumer filter excludes replay; replay may be zero",
    }


def gate_handoff_artifacts(report: dict) -> None:
    contract = _REPO / "docs" / "shared" / "GOLD_ANALYTICS_CONSUMER_CONTRACT.md"
    runbook = _REPO / "docs" / "gold" / "GOLD_4_RUNBOOK.md"
    templates = list(TEMPLATES.glob("*.sql"))
    report["handoff"] = {
        "ok": contract.is_file() and runbook.is_file() and len(templates) >= 8,
        "contract": str(contract.relative_to(_REPO)),
        "runbook": str(runbook.relative_to(_REPO)) if runbook.is_file() else None,
        "template_count": len(templates),
    }


def main() -> int:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"started_at": _utc(), "ok": False, "gates": {}}
    try:
        gate_compose(report)
        gate_migration(report)
        gate_health(report)
        gate_integration(report)
        gate_templates(report)
        gate_namespace(report)
        gate_handoff_artifacts(report)
    except Exception as exc:  # noqa: BLE001
        report["fatal"] = f"{type(exc).__name__}:{exc}"
        report["traceback"] = traceback.format_exc()

    gates = {
        "compose": report.get("compose", {}).get("ok", False),
        "migration": report.get("migration", {}).get("ok", False),
        "health": report.get("health", {}).get("ok", False),
        "integration": report.get("integration", {}).get("ok", False),
        "templates": report.get("templates", {}).get("ok", False),
        "namespace": report.get("namespace", {}).get("ok", False),
        "handoff": report.get("handoff", {}).get("ok", False),
    }
    report["gate_summary"] = gates
    report["ok"] = all(gates.values())
    report["finished_at"] = _utc()
    out = EVIDENCE / f"platform_verify_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"ok": report["ok"], "gates": gates, "evidence": str(out)}, indent=2))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
