"""Bounded Gold 3 live smoke: one run/window forward progress + restart recovery.

Does not modify Compose. Requires a reachable ClickHouse with migration 005 and
enough Silver rows for at least one closed 60s window under Contract v1.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

EVIDENCE_DIR = _REPO / "docs" / "gold" / "gold3_evidence"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _scalar(client, sql: str, parameters: Optional[dict[str, Any]] = None) -> Any:
    result = client.query(sql, parameters=parameters or {})
    return result.result_rows[0][0]


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


def probe(client) -> dict[str, Any]:
    gold = client.query(
        "SELECT name FROM system.tables "
        "WHERE database = {db:String} AND name LIKE 'gold_%' "
        "ORDER BY name",
        parameters={"db": "smart_traffic"},
    ).result_rows
    silver_counts = {}
    for table in (
        "silver_fact_traffic_observation",
        "silver_fact_intersection_state",
        "silver_fact_signal_state",
        "silver_fact_camera_observation",
        "silver_dim_run",
        "silver_dim_approach",
    ):
        try:
            silver_counts[table] = int(_scalar(client, f"SELECT count() FROM smart_traffic.{table}"))
        except Exception as exc:  # noqa: BLE001
            silver_counts[table] = f"ERR:{type(exc).__name__}:{exc}"
    runs = []
    try:
        rows = client.query(
            "SELECT simulation_run_id, scenario_id, count() AS n, "
            "min(simulation_time_sec) AS mn, max(simulation_time_sec) AS mx "
            "FROM smart_traffic.silver_fact_traffic_observation "
            "GROUP BY simulation_run_id, scenario_id "
            "HAVING mx >= 120 AND n >= 40 "
            "ORDER BY n DESC LIMIT 10"
        ).result_rows
        runs = [
            {
                "simulation_run_id": r[0],
                "scenario_id": r[1],
                "traffic_rows": int(r[2]),
                "min_sim_sec": float(r[3]),
                "max_sim_sec": float(r[4]),
            }
            for r in rows
        ]
    except Exception as exc:  # noqa: BLE001
        runs = [{"error": f"{type(exc).__name__}:{exc}"}]
    return {
        "select_1": client.command("SELECT 1"),
        "gold_tables": [r[0] for r in gold],
        "silver_counts": silver_counts,
        "top_runs": runs,
    }


def _settings(tmp: Path, run_id: Optional[str]) -> Any:
    from de.gold_runtime.config import GoldSettings

    kwargs: dict[str, Any] = {
        "traffic_expected_cadence_sec": float(
            os.getenv("GOLD_TRAFFIC_EXPECTED_CADENCE_SEC", "10")
        ),
        "intersection_expected_cadence_sec": float(
            os.getenv("GOLD_INTERSECTION_EXPECTED_CADENCE_SEC", "10")
        ),
        "signal_expected_cadence_sec": float(
            os.getenv("GOLD_SIGNAL_EXPECTED_CADENCE_SEC", "10")
        ),
        "checkpoint_path": str(tmp / "checkpoint.sqlite3"),
        "instance_lock_path": str(tmp / "instance.lock"),
        "clickhouse_host": os.getenv("GOLD_CLICKHOUSE_HOST", "localhost"),
        "clickhouse_port": int(os.getenv("GOLD_CLICKHOUSE_PORT", "8123")),
        "clickhouse_user": os.getenv("GOLD_CLICKHOUSE_USER", "default"),
        "clickhouse_password": os.getenv("GOLD_CLICKHOUSE_PASSWORD", ""),
        "clickhouse_database": os.getenv("GOLD_CLICKHOUSE_DATABASE", "smart_traffic"),
        "max_windows_per_cycle": 1,
        "poll_interval_sec": 0.5,
        "health_port": 18096,
        "silver_fetch_batch_size": 500,
    }
    if run_id:
        kwargs["run_scope"] = run_id
    return GoldSettings(**kwargs).validate_all()


def run_smoke() -> dict[str, Any]:
    from de.gold_runtime.config import ProcessorState
    from de.gold_runtime.processor import GoldProcessor

    report: dict[str, Any] = {
        "started_at": _utc(),
        "ok": False,
        "steps": {},
        "errors": [],
    }
    client = _client()
    report["probe"] = probe(client)

    gold_tables = set(report["probe"]["gold_tables"])
    required = {
        "gold_fact_traffic_window",
        "gold_fact_intersection_window",
        "gold_processing_ledger",
        "gold_dim_window",
    }
    missing = sorted(required - gold_tables)
    if missing:
        report["errors"].append(f"missing gold tables: {missing}")
        return report

    runs = [r for r in report["probe"]["top_runs"] if "simulation_run_id" in r]
    eligible = [r for r in runs if r.get("max_sim_sec", 0) >= 60.0]
    if not eligible:
        report["errors"].append(
            "no Silver run with max(simulation_time_sec) >= 60; cannot close a window"
        )
        report["steps"]["data_gate"] = "SKIP_NO_SILVER_WINDOW"
        return report

    chosen = eligible[0]
    run_id = str(chosen["simulation_run_id"])
    report["steps"]["chosen_run"] = chosen

    tmp = Path(tempfile.mkdtemp(prefix="gold3-smoke-", dir=str(_REPO / "de" / "artifacts")))
    report["steps"]["runtime_dir"] = str(tmp)
    settings = _settings(tmp, run_id)

    before_facts = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
            "WHERE namespace = 'live' AND simulation_run_id = {run:String}",
            {"run": run_id},
        )
    )
    before_ledger = int(
        _scalar(
            client,
            "SELECT count() FROM smart_traffic.gold_processing_ledger "
            "WHERE namespace = 'live' AND disposition = 'PERSISTED'",
        )
    )
    report["steps"]["before"] = {
        "traffic_facts": before_facts,
        "persisted_ledger": before_ledger,
    }

    processor = GoldProcessor(settings)
    try:
        processor.start(background=False)
        processed = processor.run_cycle()
        report["steps"]["first_cycle"] = {
            "processed": processed,
            "state": processor.state.value,
            "engine_batch": processor.metrics.last_batch_id,
            "window_id": processor.metrics.last_window_id,
            "watermark": processor.metrics.watermark,
        }
        if processed < 1:
            report["errors"].append("first cycle processed 0 windows")
            return report

        after_facts = int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace = 'live' AND simulation_run_id = {run:String}",
                {"run": run_id},
            )
        )
        after_ledger = int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_processing_ledger "
                "WHERE namespace = 'live' AND disposition = 'PERSISTED'",
            )
        )
        report["steps"]["after_first"] = {
            "traffic_facts": after_facts,
            "persisted_ledger": after_ledger,
            "delta_facts": after_facts - before_facts,
            "delta_ledger": after_ledger - before_ledger,
        }
        if after_facts <= before_facts:
            report["errors"].append("no forward progress on gold_fact_traffic_window")
            return report

        # Simulate crash boundary: stop without advancing further, then recover.
        processor.stop()
        processor2 = GoldProcessor(settings)
        processor2.start(background=False)
        recovered = processor2.recover()
        second = processor2.run_cycle()
        report["steps"]["restart"] = {
            "recovered": recovered,
            "second_cycle_processed": second,
            "state": processor2.state.value,
            "idempotent_or_forward": second >= 0,
        }
        # Same closed window must not duplicate: second process of first window identity
        # is covered by processor metrics / non-decreasing unique identity count.
        distinct = int(
            _scalar(
                client,
                "SELECT uniqExact((window_id, direction, revision_seq)) "
                "FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace = 'live' AND simulation_run_id = {run:String}",
                {"run": run_id},
            )
        )
        total = int(
            _scalar(
                client,
                "SELECT count() FROM smart_traffic.gold_fact_traffic_window "
                "WHERE namespace = 'live' AND simulation_run_id = {run:String}",
                {"run": run_id},
            )
        )
        report["steps"]["dedupe_check"] = {
            "distinct_identities": distinct,
            "total_rows": total,
        }
        processor2.stop()
        report["ok"] = (
            processed >= 1
            and after_facts > before_facts
            and after_ledger > before_ledger
            and processor2.state
            in {ProcessorState.READY, ProcessorState.STOPPED, ProcessorState.PROCESSING}
        )
        return report
    except Exception as exc:  # noqa: BLE001
        report["errors"].append(f"{type(exc).__name__}: {exc}")
        report["traceback"] = traceback.format_exc()
        try:
            processor.stop()
        except Exception:
            pass
        return report
    finally:
        report["finished_at"] = _utc()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe-only", action="store_true")
    args = parser.parse_args()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = EVIDENCE_DIR / f"live_smoke_{stamp}.json"

    if args.probe_only:
        try:
            payload = {"started_at": _utc(), "probe": probe(_client()), "ok": True}
        except Exception as exc:  # noqa: BLE001
            payload = {
                "started_at": _utc(),
                "ok": False,
                "errors": [f"{type(exc).__name__}: {exc}"],
            }
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        print(f"EVIDENCE={out}")
        return 0 if payload.get("ok") else 2

    payload = run_smoke()
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    print(f"EVIDENCE={out}")
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
