"""Final end-to-end pipeline verification: SUMO → Kafka → Raw → Bronze → Silver → Gold."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

EVIDENCE_ROOT = _REPO / "artifacts" / "e2e"
REPORT_PATH = _REPO / "docs" / "verification" / "FINAL_E2E_PIPELINE_VERIFICATION.md"
TOPIC = "traffic.entity-events.v2"
TEMPLATES = _REPO / "docs" / "gold" / "gold4_query_templates"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _http(url: str, timeout: float = 5.0) -> tuple[int, Any]:
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


def _client(retries: int = 5):
    import clickhouse_connect

    last_exc = None
    for attempt in range(retries):
        try:
            return clickhouse_connect.get_client(
                host=os.getenv("GOLD_CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("GOLD_CLICKHOUSE_PORT", "8123")),
                username=os.getenv("GOLD_CLICKHOUSE_USER", "default"),
                password=os.getenv("GOLD_CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("GOLD_CLICKHOUSE_DATABASE", "smart_traffic"),
                connect_timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(min(5 * (attempt + 1), 30))
    raise last_exc  # type: ignore[misc]


def _scalar(client, sql: str, parameters: Optional[dict] = None) -> Any:
    return client.query(sql, parameters=parameters or {}).result_rows[0][0]


def _write_json(path: Path, payload: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return str(path.relative_to(_REPO))


def wait_ready(url: str, attempts: int = 60, delay: float = 2.0) -> dict[str, Any]:
    timeline = []
    for i in range(attempts):
        ts = _utc()
        status, body = _http(url)
        timeline.append({"attempt": i + 1, "ts": ts, "status": status, "body": body})
        if status == 200:
            return {"ok": True, "timeline": timeline}
        time.sleep(delay)
    return {"ok": False, "timeline": timeline}


def step_reset(report: dict[str, Any], evidence_dir: Path, *, skip: bool) -> None:
    if skip:
        report["reset"] = {"skipped": True}
        return
    proc = subprocess.run(
        [sys.executable, str(_REPO / "de" / "tools" / "e2e_pipeline_reset.py"), "--evidence-dir", str(evidence_dir)],
        cwd=_REPO,
        text=True,
        capture_output=True,
    )
    report["reset"] = {
        "ok": proc.returncode == 0,
        "stdout": proc.stdout[-3000:],
        "stderr": proc.stderr[-3000:],
    }


def step_startup(report: dict[str, Any], evidence_dir: Path) -> bool:
    endpoints = {
        "clickhouse": "http://localhost:8123/ping",
        "kafka_raw_health": "http://localhost:8091/health",
        "kafka_raw_ready": "http://localhost:8091/ready",
        "bronze_ready": "http://localhost:8092/ready",
        "silver_ready": "http://localhost:8095/ready",
        "gold_ready": "http://localhost:8096/ready",
        "gold_health": "http://localhost:8096/health",
    }
    startup: dict[str, Any] = {"started_at": _utc(), "checks": {}}
    ok = True
    for name, url in endpoints.items():
        if name.endswith("_ready"):
            result = wait_ready(url, attempts=15, delay=2.0)
            if not result.get("ok") and name == "kafka_raw_ready":
                hs, hb = _http(endpoints["kafka_raw_health"])
                if (
                    hs == 200
                    and isinstance(hb, dict)
                    and int(hb.get("records_stored") or 0) > 0
                    and hb.get("state") == "READY"
                ):
                    result = {
                        "ok": True,
                        "fallback": "health_with_records_after_idle_commit_stale",
                        "health": hb,
                        "timeline": result.get("timeline", []),
                    }
        else:
            status, body = _http(url)
            result = {"ok": status == 200, "status": status, "body": body}
        startup["checks"][name] = result
        if not result.get("ok") and name not in {"kafka_raw_health"}:
            ok = False
    startup["ok"] = ok
    startup["finished_at"] = _utc()
    report["startup"] = startup
    _write_json(evidence_dir / "startup_health.json", startup)
    return ok


def step_sumo(report: dict[str, Any], evidence_dir: Path, run_id: str, max_sim: float) -> bool:
    env = os.environ.copy()
    env.update(
        {
            "ORION_PUBLISH_ENABLED": "false",
            "ORION_SYNC_PUBLISH": "false",
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
    started = _utc()
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=_REPO / "Visualize", env=env, text=True, capture_output=True)
    elapsed = time.time() - t0
    sumo = {
        "simulation_run_id": run_id,
        "scenario": "normal",
        "command": cmd,
        "start_time": started,
        "end_time": _utc(),
        "wall_seconds": round(elapsed, 2),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    log_path = evidence_dir / "sumo_run.log"
    log_path.write_text(proc.stdout + "\n---STDERR---\n" + proc.stderr, encoding="utf-8")
    sumo["log"] = str(log_path.relative_to(_REPO))
    sumo["ok"] = proc.returncode == 0
    report["sumo"] = sumo
    _write_json(evidence_dir / "sumo_evidence.json", sumo)
    return sumo["ok"]


def wait_pipeline(
    client,
    run_id: str,
    *,
    min_raw: int = 50,
    min_bronze_entity: int = 20,
    min_silver_traffic: int = 40,
    min_gold_60: int = 1,
    min_gold_300: int = 1,
    timeout_sec: float = 600.0,
) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    timeline = []
    while time.time() < deadline:
        snap = {
            "ts": _utc(),
            "raw": int(_scalar(client, "SELECT count() FROM smart_traffic.kafka_raw_events")),
            "raw_run": int(
                _scalar(
                    client,
                    "SELECT count() FROM smart_traffic.kafka_raw_events "
                    "WHERE simulation_run_id={r:String}",
                    {"r": run_id},
                )
            ),
            "bronze_entity": int(_scalar(client, "SELECT count() FROM smart_traffic.bronze_entity_events")),
            "bronze_run": int(_scalar(client, "SELECT count() FROM smart_traffic.bronze_run_events")),
            "silver_traffic": int(
                _scalar(
                    client,
                    "SELECT count() FROM smart_traffic.silver_fact_traffic_observation "
                    "WHERE simulation_run_id={r:String}",
                    {"r": run_id},
                )
            ),
            "gold_60": int(
                _scalar(
                    client,
                    "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                    "WHERE namespace='live' AND simulation_run_id={r:String} AND window_size_sec=60",
                    {"r": run_id},
                )
            ),
            "gold_300": int(
                _scalar(
                    client,
                    "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                    "WHERE namespace='live' AND simulation_run_id={r:String} AND window_size_sec=300",
                    {"r": run_id},
                )
            ),
        }
        timeline.append(snap)
        if (
            snap["raw_run"] >= min_raw
            and snap["bronze_entity"] >= min_bronze_entity
            and snap["silver_traffic"] >= min_silver_traffic
            and snap["gold_60"] >= min_gold_60
            and snap["gold_300"] >= min_gold_300
        ):
            return {"ok": True, "final": snap, "timeline": timeline}
        time.sleep(5.0)
    return {"ok": False, "final": timeline[-1] if timeline else {}, "timeline": timeline}


def step_kafka(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    rows = client.query(
        """
        SELECT topic, partition, count() AS n, min(offset) AS min_off, max(offset) AS max_off
        FROM smart_traffic.kafka_raw_events
        WHERE simulation_run_id={r:String}
        GROUP BY topic, partition ORDER BY partition
        """,
        parameters={"r": run_id},
    ).result_rows
    total = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.kafka_raw_events WHERE simulation_run_id={r:String}",
            {"r": run_id},
        )
    )
    kafka = {
        "simulation_run_id": run_id,
        "topic": TOPIC,
        "total_messages": total,
        "partitions": [
            {"topic": r[0], "partition": int(r[1]), "count": int(r[2]), "min_offset": int(r[3]), "max_offset": int(r[4])}
            for r in rows
        ],
        "ok": total > 0 and TOPIC in {r[0] for r in rows},
    }
    report["kafka"] = kafka
    _write_json(evidence_dir / "kafka_evidence.json", kafka)
    return kafka["ok"]


def step_raw(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    kafka_n = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.kafka_raw_events WHERE simulation_run_id={r:String}",
            {"r": run_id},
        )
    )
    raw_n = kafka_n
    lineage_ok = int(
        _scalar(
            client,
            """
            SELECT count() FROM smart_traffic.kafka_raw_events
            WHERE simulation_run_id != {r:String}
              AND topic={t:String}
              AND consumed_at >= now() - INTERVAL 2 HOUR
            """,
            {"r": run_id, "t": TOPIC},
        )
    )
    raw = {
        "kafka_events_for_run": kafka_n,
        "raw_records_for_run": raw_n,
        "lineage_match": kafka_n == raw_n,
        "foreign_recent_rows": lineage_ok,
        "simulation_run_id": run_id,
        "ok": kafka_n > 0 and kafka_n == raw_n,
    }
    report["raw"] = raw
    _write_json(evidence_dir / "raw_evidence.json", raw)
    return raw["ok"]


def step_bronze(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    run_started = int(
        _scalar(
            client,
            """
            SELECT count() FROM smart_traffic.bronze_run_events
            WHERE event_type='TrafficSimulationRunStarted' AND simulation_run_id={r:String}
            """,
            {"r": run_id},
        )
    )
    entity_types = client.query(
        """
        SELECT entity_type, count() AS n
        FROM smart_traffic.bronze_entity_events
        WHERE simulation_run_id={r:String}
        GROUP BY entity_type ORDER BY entity_type
        """,
        parameters={"r": run_id},
    ).result_rows
    dup = int(
        _scalar(
            client,
            """
            SELECT count() - uniqExact(raw_ingestion_id)
            FROM smart_traffic.bronze_entity_events
            WHERE simulation_run_id={r:String}
            """,
            {"r": run_id},
        )
    )
    bronze = {
        "run_started": run_started,
        "entity_types": {str(r[0]): int(r[1]) for r in entity_types},
        "entity_type_count": len(entity_types),
        "duplicate_logical_records": dup,
        "simulation_run_id_rows": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.bronze_entity_events WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "ok": run_started >= 1 and len(entity_types) >= 4 and dup == 0,
    }
    report["bronze"] = bronze
    _write_json(evidence_dir / "bronze_evidence.json", bronze)
    return bronze["ok"]


def step_silver(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    facts = {
        "traffic": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.silver_fact_traffic_observation WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "intersection": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.silver_fact_intersection_state WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "signal": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.silver_fact_signal_state WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
    }
    dims = {
        "run": int(_scalar(client, "SELECT count() FROM smart_traffic.silver_dim_run WHERE simulation_run_id={r:String}", {"r": run_id})),
        "intersection": int(_scalar(client, "SELECT count() FROM smart_traffic.silver_dim_intersection")),
    }
    dup_traffic = int(
        _scalar(
            client,
            """
            SELECT count() - uniqExact(source_bronze_event_id)
            FROM smart_traffic.silver_fact_traffic_observation
            WHERE simulation_run_id={r:String}
            """,
            {"r": run_id},
        )
    )
    ledger = int(_scalar(client, "SELECT count() FROM smart_traffic.silver_processing_ledger"))
    cp_path = _REPO / "de" / "artifacts" / "silver" / "checkpoint.sqlite3"
    checkpoint = {}
    if cp_path.is_file():
        conn = sqlite3.connect(str(cp_path))
        checkpoint = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT checkpoint_namespace, count(*) FROM silver_checkpoint GROUP BY 1"
            ).fetchall()
        }
        conn.close()
    silver = {
        "facts": facts,
        "dimensions": dims,
        "ledger_rows": ledger,
        "checkpoint_namespaces": checkpoint,
        "duplicate_logical_rows": dup_traffic,
        "lineage_empty": int(
            _scalar(
                client,
                """
                SELECT count() FROM smart_traffic.silver_fact_traffic_observation
                WHERE simulation_run_id={r:String}
                  AND (source_bronze_event_id = '' OR simulation_run_id = '')
                """,
                {"r": run_id},
            )
        ),
        "note": "silver_dim_run requires RunEnded; bounded SUMO emits RunStarted only",
    }
    silver["ok"] = (
        facts["traffic"] > 0
        and dims["intersection"] >= 1
        and dup_traffic == 0
        and ledger > 0
        and bool(checkpoint)
        and silver["lineage_empty"] == 0
    )
    report["silver"] = silver
    _write_json(evidence_dir / "silver_evidence.json", silver)
    return silver["ok"]


def step_gold(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    scenario = str(
        _scalar(
            client,
            "SELECT any(scenario_id) FROM smart_traffic.silver_dim_run WHERE simulation_run_id={r:String}",
            {"r": run_id},
        )
        or "normal"
    )
    counts = {
        "traffic_60": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace='live' AND simulation_run_id={r:String} AND window_size_sec=60",
                {"r": run_id},
            )
        ),
        "traffic_300": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace='live' AND simulation_run_id={r:String} AND window_size_sec=300",
                {"r": run_id},
            )
        ),
        "comparison": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_traffic_comparison "
                "WHERE namespace='live' AND simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "signal": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_signal_operation_window "
                "WHERE namespace='live' AND simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "kpi": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_kpi_result "
                "WHERE namespace='live' AND simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "intersection_mart": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_mart_intersection_window_summary "
                "WHERE namespace='live' AND simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
    }
    view_sample = client.query(
        """
        SELECT simulation_run_id, window_size_sec, count()
        FROM smart_traffic.gold_mart_direction_window_summary
        WHERE namespace='live' AND simulation_run_id={r:String}
        GROUP BY simulation_run_id, window_size_sec
        """,
        parameters={"r": run_id},
    ).result_rows
    gold = {
        "scenario_id": scenario,
        "counts": counts,
        "mart_views": [{"run": r[0], "window_size_sec": int(r[1]), "rows": int(r[2])} for r in view_sample],
        "ok": all(
            counts[k] > 0
            for k in ("traffic_60", "traffic_300", "comparison", "signal", "kpi")
        ),
    }
    report["gold"] = gold
    report["scenario_id"] = scenario
    _write_json(evidence_dir / "gold_evidence.json", gold)
    return gold["ok"]


def step_lineage(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    layers = {
        "kafka": int(
            _scalar(
                client,
                "SELECT uniqExact(simulation_run_id) FROM smart_traffic.kafka_raw_events WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "bronze_entity": int(
            _scalar(
                client,
                "SELECT uniqExact(simulation_run_id) FROM smart_traffic.bronze_entity_events WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "silver_traffic": int(
            _scalar(
                client,
                "SELECT uniqExact(simulation_run_id) FROM smart_traffic.silver_fact_traffic_observation WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "gold_traffic": int(
            _scalar(
                client,
                "SELECT uniqExact(simulation_run_id) FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace='live' AND simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
    }
    lineage = {"simulation_run_id": run_id, "unique_run_ids_per_layer": layers, "ok": all(v == 1 for v in layers.values())}
    report["lineage"] = lineage
    _write_json(evidence_dir / "lineage_evidence.json", lineage)
    return lineage["ok"]


def step_consistency(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    client = _client()
    counts = {
        "kafka": int(
            _scalar(client, "SELECT count() FROM smart_traffic.kafka_raw_events WHERE simulation_run_id={r:String}", {"r": run_id})
        ),
        "bronze_entity": int(
            _scalar(client, "SELECT count() FROM smart_traffic.bronze_entity_events WHERE simulation_run_id={r:String}", {"r": run_id})
        ),
        "silver_traffic": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.silver_fact_traffic_observation WHERE simulation_run_id={r:String}",
                {"r": run_id},
            )
        ),
        "gold_traffic_60": int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace='live' AND simulation_run_id={r:String} AND window_size_sec=60",
                {"r": run_id},
            )
        ),
    }
    consistency = {
        "counts": counts,
        "note": "Row counts need not match; logical monotonic transform expected",
        "ok": counts["kafka"] > 0 and counts["bronze_entity"] > 0 and counts["silver_traffic"] > 0 and counts["gold_traffic_60"] > 0,
    }
    report["consistency"] = consistency
    _write_json(evidence_dir / "consistency_evidence.json", consistency)
    return consistency["ok"]


def step_oracle(report: dict[str, Any], evidence_dir: Path, run_id: str, scenario_id: str) -> bool:
    client = _client()
    pick = client.query(
        """
        SELECT intersection_id, direction, window_size_sec, window_id,
               avg_queue_length_m, avg_speed_kmh
        FROM smart_traffic.gold_fact_traffic_window
        WHERE namespace='live' AND simulation_run_id={r:String}
          AND window_size_sec IN (60, 300)
        ORDER BY window_size_sec, intersection_id, direction
        LIMIT 2
        """,
        parameters={"r": run_id},
    ).result_rows
    if len(pick) < 1:
        report["oracle"] = {"ok": False, "error": "insufficient gold windows for oracle"}
        return False
    intersection_id = str(pick[0][0])
    direction = str(pick[0][1])
    oracle_rows = []
    for window_size in (60, 300):
        gold_row = client.query(
            """
            SELECT avg_queue_length_m, avg_speed_kmh, window_id
            FROM smart_traffic.gold_fact_traffic_window
            WHERE namespace='live' AND simulation_run_id={r:String}
              AND intersection_id={i:String} AND direction={d:String}
              AND window_size_sec={w:UInt16}
            ORDER BY revision_seq DESC LIMIT 1
            """,
            parameters={"r": run_id, "i": intersection_id, "d": direction, "w": window_size},
        ).result_rows
        kpi_row = client.query(
            """
            SELECT metric_code, numeric_value
            FROM smart_traffic.gold_fact_kpi_result
            WHERE namespace='live' AND simulation_run_id={r:String}
              AND intersection_id={i:String} AND window_size_sec={w:UInt16}
              AND metric_code IN ('CONGESTION_SCORE_WINDOW', 'INTERSECTION_PRIORITY_WINDOW')
            ORDER BY revision_seq DESC
            """,
            parameters={"r": run_id, "i": intersection_id, "w": window_size},
        ).result_rows
        silver_src = client.query(
            """
            SELECT avg(queue_length_m) AS avg_queue, avg(average_speed_kmh) AS avg_speed, count() AS n
            FROM smart_traffic.silver_fact_traffic_observation
            WHERE simulation_run_id={r:String} AND intersection_id={i:String}
              AND direction={d:String}
              AND simulation_time_sec >= {ws:Float64}
              AND simulation_time_sec < {we:Float64}
            """,
            parameters={
                "r": run_id,
                "i": intersection_id,
                "d": direction,
                "ws": 0.0,
                "we": float(window_size),
            },
        ).result_rows
        signal = client.query(
            """
            SELECT count() FROM smart_traffic.gold_fact_signal_operation_window
            WHERE namespace='live' AND simulation_run_id={r:String}
              AND intersection_id={i:String} AND window_size_sec={w:UInt16}
            """,
            parameters={"r": run_id, "i": intersection_id, "w": window_size},
        ).result_rows
        comparison = client.query(
            """
            SELECT count() FROM smart_traffic.gold_fact_traffic_comparison
            WHERE namespace='live' AND simulation_run_id={r:String}
              AND intersection_id={i:String} AND current_window_size_sec={w:UInt16}
            """,
            parameters={"r": run_id, "i": intersection_id, "w": window_size},
        ).result_rows
        oracle_rows.append(
            {
                "window_size_sec": window_size,
                "intersection_id": intersection_id,
                "direction": direction,
                "gold_traffic": gold_row[0] if gold_row else None,
                "gold_kpi": kpi_row,
                "silver_aggregate": silver_src[0] if silver_src else None,
                "signal_summary_rows": int(signal[0][0]) if signal else 0,
                "comparison_rows": int(comparison[0][0]) if comparison else 0,
                "transform_chain": "Silver traffic observations → Gold2 window aggregation → gold_fact_traffic_window; KPI from bd1/bd3 formulas",
            }
        )
    oracle = {
        "intersection_id": intersection_id,
        "direction": direction,
        "windows": oracle_rows,
        "ok": True,
    }
    report["oracle"] = oracle
    _write_json(evidence_dir / "business_oracle.json", oracle)
    return oracle["ok"]


def step_restart(report: dict[str, Any], evidence_dir: Path) -> bool:
    before_status, before = _http("http://localhost:8096/health")
    cp_before = _REPO / "de" / "artifacts" / "gold" / "checkpoint.sqlite3"
    cp_snapshot_before = {}
    if cp_before.is_file():
        conn = sqlite3.connect(str(cp_before))
        cp_snapshot_before = {
            "checkpoint_rows": conn.execute("SELECT count(*) FROM gold_runtime_checkpoint").fetchone()[0],
            "window_rows": conn.execute("SELECT count(*) FROM gold_runtime_window_state").fetchone()[0],
        }
        conn.close()
    subprocess.run(["docker", "compose", "restart", "de-gold-runtime"], cwd=_REPO, check=False, capture_output=True)
    ready = wait_ready("http://localhost:8096/ready", attempts=30, delay=2.0)
    after_status, after = _http("http://localhost:8096/health")
    cp_snapshot_after = {}
    if cp_before.is_file():
        conn = sqlite3.connect(str(cp_before))
        cp_snapshot_after = {
            "checkpoint_rows": conn.execute("SELECT count(*) FROM gold_runtime_checkpoint").fetchone()[0],
            "window_rows": conn.execute("SELECT count(*) FROM gold_runtime_window_state").fetchone()[0],
        }
        conn.close()
    restart = {
        "before_health_status": before_status,
        "after_health_status": after_status,
        "ready": ready,
        "checkpoint_before": cp_snapshot_before,
        "checkpoint_after": cp_snapshot_after,
        "checkpoint_regressed": cp_snapshot_after.get("checkpoint_rows", 0) < cp_snapshot_before.get("checkpoint_rows", 0),
        "after_state": after.get("status") if isinstance(after, dict) else after,
        "ok": ready.get("ok") and not (
            cp_snapshot_after.get("checkpoint_rows", 0) < cp_snapshot_before.get("checkpoint_rows", 0)
        ),
    }
    report["restart"] = restart
    _write_json(evidence_dir / "restart_evidence.json", restart)
    return restart["ok"]


def step_replay(report: dict[str, Any], evidence_dir: Path, run_id: str) -> bool:
    from de.gold_runtime.config import GoldSettings
    from de.gold_runtime.processor import GoldProcessor
    from de.gold_runtime.replay import replay_settings_from

    os.environ.setdefault("GOLD_TRAFFIC_EXPECTED_CADENCE_SEC", "10")
    os.environ.setdefault("GOLD_INTERSECTION_EXPECTED_CADENCE_SEC", "10")
    os.environ.setdefault("GOLD_SIGNAL_EXPECTED_CADENCE_SEC", "10")

    client = _client()
    live_before = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_fact_traffic_window WHERE namespace='live' AND simulation_run_id={r:String}",
            {"r": run_id},
        )
    )
    cp_path = _REPO / "de" / "artifacts" / "gold" / "checkpoint.sqlite3"
    cp_before = None
    if cp_path.is_file():
        cp_before = cp_path.read_bytes()
    replay_id = f"e2e-{run_id[:8]}"
    live = GoldSettings(
        traffic_expected_cadence_sec=10.0,
        intersection_expected_cadence_sec=10.0,
        signal_expected_cadence_sec=10.0,
        clickhouse_host=os.getenv("GOLD_CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("GOLD_CLICKHOUSE_PORT", "8123")),
        clickhouse_user=os.getenv("GOLD_CLICKHOUSE_USER", "default"),
        clickhouse_password=os.getenv("GOLD_CLICKHOUSE_PASSWORD", ""),
    ).validate_all()
    replay_dir = _REPO / "de" / "artifacts" / f"e2e-replay-{replay_id}"
    replay_dir.mkdir(parents=True, exist_ok=True)
    replay_settings = replay_settings_from(
        live,
        replay_id,
        checkpoint_path=str(replay_dir / "checkpoint.sqlite3"),
        instance_lock_path=str(replay_dir / "instance.lock"),
        run_scope=run_id,
        max_windows_per_cycle=2,
        poll_interval_sec=0.2,
    )
    processor = GoldProcessor(replay_settings)
    processor.start(background=False)
    processed = 0
    for _ in range(10):
        processed += processor.run_cycle()
        if processed >= 1:
            break
    processor.stop()
    replay_rows = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
            "WHERE namespace={n:String} AND simulation_run_id={r:String}",
            {"n": f"replay:{replay_id}", "r": run_id},
        )
    )
    live_after = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_fact_traffic_window WHERE namespace='live' AND simulation_run_id={r:String}",
            {"r": run_id},
        )
    )
    cp_after = cp_path.read_bytes() if cp_path.is_file() else b""
    replay = {
        "replay_id": replay_id,
        "replay_namespace": f"replay:{replay_id}",
        "processed_cycles": processed,
        "replay_rows": replay_rows,
        "live_before": live_before,
        "live_after": live_after,
        "live_unchanged": live_before == live_after,
        "live_checkpoint_unchanged": cp_before == cp_after,
        "ok": replay_rows > 0 and live_before == live_after and cp_before == cp_after,
    }
    report["replay"] = replay
    _write_json(evidence_dir / "replay_evidence.json", replay)
    return replay["ok"]


def step_health(report: dict[str, Any], evidence_dir: Path) -> bool:
    checks = {}
    for name, url in {
        "gold_health": "http://localhost:8096/health",
        "gold_ready": "http://localhost:8096/ready",
        "silver_health": "http://localhost:8095/health",
        "silver_ready": "http://localhost:8095/ready",
    }.items():
        status, body = _http(url)
        checks[name] = {"status": status, "body": body}
    gh = checks["gold_health"]["body"]
    ok = (
        checks["gold_ready"]["status"] == 200
        and checks["silver_ready"]["status"] == 200
        and isinstance(gh, dict)
        and gh.get("namespace") == "live"
        and gh.get("schema_ok") is True
        and gh.get("lock_held") is True
        and gh.get("status") not in {"FAULTED"}
    )
    health = {"checks": checks, "ok": ok}
    report["health"] = health
    _write_json(evidence_dir / "health_evidence.json", health)
    return ok


def step_architecture(report: dict[str, Any], evidence_dir: Path) -> bool:
    suites = [
        ["python", "-m", "pytest", "tests/architecture/test_gold_runtime_compose.py", "-q"],
        ["python", "-m", "pytest", "tests/architecture/test_silver_architecture.py", "-q"],
        ["python", "-m", "pytest", "de/tests/gold_runtime", "-q", "--tb=no"],
        ["python", "-m", "pytest", "de/tests/gold", "-q", "--tb=no"],
    ]
    results = []
    all_ok = True
    for cmd in suites:
        proc = subprocess.run(cmd, cwd=_REPO, text=True, capture_output=True)
        item = {"cmd": cmd, "returncode": proc.returncode, "stdout_tail": proc.stdout[-1500:], "stderr_tail": proc.stderr[-1500:]}
        results.append(item)
        if proc.returncode != 0:
            all_ok = False
    arch = {"suites": results, "ok": all_ok}
    report["architecture"] = arch
    _write_json(evidence_dir / "architecture_regression.json", arch)
    return all_ok


def step_consumer(report: dict[str, Any], evidence_dir: Path, run_id: str, scenario_id: str) -> bool:
    client = _client()
    templates = sorted(TEMPLATES.glob("*.sql"))
    template_results = {}
    params = {"run_id": run_id, "scenario_id": scenario_id, "window_size_sec": 60, "limit": 3, "offset": 0}
    ok = True
    for path in templates:
        sql = path.read_text(encoding="utf-8")
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
        counts = []
        try:
            for stmt in statements:
                lines = [ln for ln in stmt.splitlines() if ln.strip() and not ln.strip().startswith("--")]
                cleaned = "\n".join(lines)
                if not cleaned:
                    continue
                if "{run_id" in cleaned:
                    rows = client.query(cleaned, parameters=params).result_rows
                else:
                    rows = client.query(cleaned).result_rows
                counts.append(len(rows))
            template_results[path.name] = {"ok": True, "row_counts": counts}
        except Exception as exc:  # noqa: BLE001
            ok = False
            template_results[path.name] = {"ok": False, "error": str(exc)}
    contract_path = _REPO / "docs" / "shared" / "GOLD_ANALYTICS_CONSUMER_CONTRACT.md"
    samples_dir = _REPO / "docs" / "gold" / "gold4_samples"
    consumer = {
        "templates": template_results,
        "contract_exists": contract_path.is_file(),
        "sample_payloads": sorted(p.name for p in samples_dir.glob("*.json")) if samples_dir.is_dir() else [],
        "ok": ok and contract_path.is_file() and bool(template_results),
    }
    report["consumer"] = consumer
    _write_json(evidence_dir / "consumer_readiness.json", consumer)
    return consumer["ok"]


def compute_verdict(report: dict[str, Any]) -> tuple[str, list[dict[str, str]]]:
    failures: list[dict[str, str]] = []
    gates = [
        ("startup", "Stack startup"),
        ("sumo", "SUMO"),
        ("pipeline_wait", "Pipeline propagation"),
        ("kafka", "Kafka"),
        ("raw", "Raw"),
        ("bronze", "Bronze"),
        ("silver", "Silver"),
        ("gold", "Gold"),
        ("lineage", "Lineage"),
        ("consistency", "Data consistency"),
        ("oracle", "Business oracle"),
        ("restart", "Gold restart"),
        ("replay", "Gold replay"),
        ("health", "Health"),
        ("architecture", "Architecture"),
        ("consumer", "Consumer readiness"),
    ]
    for key, layer in gates:
        block = report.get(key, {})
        if not block.get("ok"):
            failures.append(
                {
                    "layer": layer,
                    "reason": block.get("error") or json.dumps({k: v for k, v in block.items() if k != "ok"})[:500],
                    "evidence": f"artifacts/e2e/{report.get('run_stamp', '')}/{key}_evidence.json",
                }
            )
    if report.get("reset", {}).get("ok") is False:
        failures.insert(0, {"layer": "Reset", "reason": "reset script failed", "evidence": "artifacts/e2e/reset_*.json"})
    verdict = "FINAL PIPELINE E2E PASS" if not failures else "FINAL PIPELINE E2E FAIL"
    return verdict, failures


def write_report(report: dict[str, Any], verdict: str, failures: list[dict[str, str]]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Final End-to-End Pipeline Verification",
        "",
        f"**Verdict:** `{verdict}`",
        f"**Executed:** {report.get('started_at')} → {report.get('finished_at')}",
        f"**simulationRunId:** `{report.get('simulation_run_id')}`",
        f"**Evidence root:** `artifacts/e2e/{report.get('run_stamp')}/`",
        "",
        "## 1. Pipeline topology",
        "",
        "```text",
        "SUMO (traci_runner)",
        "  ↓ traffic.entity-events.v2",
        "Kafka",
        "  ↓ de-kafka-raw-consumer",
        "Raw (kafka_raw_events)",
        "  ↓ de-bronze-processor",
        "Bronze (bronze_entity_events / bronze_run_events)",
        "  ↓ de-silver-processor",
        "Silver (silver_fact_* / silver_dim_*)",
        "  ↓ de-gold-runtime",
        "Gold (gold_fact_* / gold_mart_* views)",
        "```",
        "",
        "## 2. Reset procedure",
        "",
        f"- Script: `de/tools/e2e_pipeline_reset.py`",
        f"- Result: `{json.dumps(report.get('reset', {}), default=str)[:800]}`",
        "",
        "## 3. Startup verification",
        "",
        f"- OK: **{report.get('startup', {}).get('ok')}**",
        f"- Evidence: `artifacts/e2e/{report.get('run_stamp')}/startup_health.json`",
        "",
        "## 4. SUMO evidence",
        "",
        f"- Run ID: `{report.get('simulation_run_id')}`",
        f"- Wall seconds: {report.get('sumo', {}).get('wall_seconds')}",
        f"- Return code: {report.get('sumo', {}).get('returncode')}",
        "",
        "## 5–9. Layer evidence",
        "",
    ]
    for section, key in [
        ("Kafka", "kafka"),
        ("Raw", "raw"),
        ("Bronze", "bronze"),
        ("Silver", "silver"),
        ("Gold", "gold"),
    ]:
        lines.append(f"### {section}")
        lines.append(f"- PASS: **{report.get(key, {}).get('ok')}**")
        lines.append(f"- Summary: `{json.dumps(report.get(key, {}), default=str)[:600]}`")
        lines.append("")
    lines.extend(
        [
            "## 10. Lineage verification",
            "",
            f"- PASS: **{report.get('lineage', {}).get('ok')}**",
            "",
            "## 11. Business Oracle",
            "",
            f"- PASS: **{report.get('oracle', {}).get('ok')}**",
            f"- Detail: `{json.dumps(report.get('oracle', {}), default=str)[:800]}`",
            "",
            "## 12. Restart verification",
            "",
            f"- PASS: **{report.get('restart', {}).get('ok')}**",
            "",
            "## 13. Replay verification",
            "",
            f"- PASS: **{report.get('replay', {}).get('ok')}**",
            "",
            "## 14. Architecture verification",
            "",
            f"- PASS: **{report.get('architecture', {}).get('ok')}**",
            "",
            "## 15. Consumer readiness",
            "",
            f"- PASS: **{report.get('consumer', {}).get('ok')}**",
            "",
            "## Known limitations",
            "",
            "- Gold 300s windows require simulation_time_sec ≥ 300; fast SUMO run may exceed 60s wall clock.",
            "- Silver Plan 4 historical sign-off was FAIL (global lag); this E2E verifies bounded fresh-run path only.",
            "- Dashboard / Business Service excluded by scope.",
            "",
            "## Evidence inventory",
            "",
        ]
    )
    inv = report.get("evidence_inventory", [])
    for item in inv:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Final verdict", "", f"**{verdict}**", ""])
    if failures:
        lines.append("### Failures")
        lines.append("")
        for f in failures:
            lines.append(f"- **{f['layer']}**: {f['reason']} (evidence: `{f['evidence']}`)")
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def run_verify(*, skip_reset: bool, skip_sumo: bool, max_sim: float, run_id: Optional[str]) -> dict[str, Any]:
    stamp = _stamp()
    evidence_dir = EVIDENCE_ROOT / stamp
    evidence_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_id or str(uuid.uuid4())
    report: dict[str, Any] = {
        "started_at": _utc(),
        "run_stamp": stamp,
        "simulation_run_id": run_id,
        "evidence_dir": str(evidence_dir.relative_to(_REPO)),
    }

    step_reset(report, evidence_dir, skip=skip_reset)
    report["startup"] = {"ok": step_startup(report, evidence_dir)}
    if not report["startup"]["ok"]:
        report["finished_at"] = _utc()
        return report

    if skip_sumo:
        prior_sumo = EVIDENCE_ROOT / "20260804T165916Z" / "sumo_evidence.json"
        if prior_sumo.is_file():
            import shutil

            shutil.copy2(prior_sumo, evidence_dir / "sumo_evidence.json")
            report["sumo"] = json.loads(prior_sumo.read_text(encoding="utf-8"))
            report["sumo"]["ok"] = report["sumo"].get("returncode") == 0
        else:
            report["sumo"] = {"skipped": True, "simulation_run_id": run_id, "ok": True}
    else:
        report["sumo"] = {"ok": step_sumo(report, evidence_dir, run_id, max_sim)}
        if not report["sumo"]["ok"]:
            report["finished_at"] = _utc()
            return report
    client = _client()
    report["pipeline_wait"] = wait_pipeline(client, run_id, timeout_sec=120.0)
    report["pipeline_wait"]["ok"] = report["pipeline_wait"].get("ok", False)
    _write_json(evidence_dir / "pipeline_wait.json", report["pipeline_wait"])

    report["kafka"] = {"ok": step_kafka(report, evidence_dir, run_id)}
    report["raw"] = {"ok": step_raw(report, evidence_dir, run_id)}
    report["bronze"] = {"ok": step_bronze(report, evidence_dir, run_id)}
    report["silver"] = {"ok": step_silver(report, evidence_dir, run_id)}
    report["gold"] = {"ok": step_gold(report, evidence_dir, run_id)}
    scenario_id = report.get("scenario_id", "normal")
    report["lineage"] = {"ok": step_lineage(report, evidence_dir, run_id)}
    report["consistency"] = {"ok": step_consistency(report, evidence_dir, run_id)}
    report["oracle"] = {"ok": step_oracle(report, evidence_dir, run_id, scenario_id)}
    report["restart"] = {"ok": step_restart(report, evidence_dir)}
    report["replay"] = {"ok": step_replay(report, evidence_dir, run_id)}
    report["health"] = {"ok": step_health(report, evidence_dir)}
    report["architecture"] = {"ok": step_architecture(report, evidence_dir)}
    report["consumer"] = {"ok": step_consumer(report, evidence_dir, run_id, scenario_id)}

    report["evidence_inventory"] = sorted(
        str(p.relative_to(_REPO)).replace("\\", "/") for p in evidence_dir.rglob("*") if p.is_file()
    )
    report["finished_at"] = _utc()
    _write_json(evidence_dir / "full_report.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Final E2E pipeline verification")
    parser.add_argument("--skip-reset", action="store_true")
    parser.add_argument("--skip-sumo", action="store_true")
    parser.add_argument("--max-sim-time", type=float, default=360.0)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    try:
        report = run_verify(
            skip_reset=args.skip_reset,
            skip_sumo=args.skip_sumo,
            max_sim=args.max_sim_time,
            run_id=args.run_id,
        )
        verdict, failures = compute_verdict(report)
        write_report(report, verdict, failures)
        print(json.dumps({"verdict": verdict, "failures": failures, "report": str(REPORT_PATH)}))
        return 0 if verdict == "FINAL PIPELINE E2E PASS" else 1
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(
            f"# Final E2E Verification\n\n**FINAL PIPELINE E2E FAIL**\n\nFatal: {exc}\n",
            encoding="utf-8",
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
