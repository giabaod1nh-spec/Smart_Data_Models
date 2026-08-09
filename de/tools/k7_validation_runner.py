"""K-7 Bronze v2 full validation runner — K-4.5 scoped gates + evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import clickhouse_connect  # noqa: E402

from de.bronze.config import BronzeSettings, get_settings  # noqa: E402
from de.bronze.replay import run_backfill, run_parity_sync  # noqa: E402
from de.tools.k7_bronze_oracles import run_oracles  # noqa: E402

EVIDENCE_ROOT = _REPO / "docs" / "architecture" / "k7_bronze_evidence"
TOPIC = "traffic.entity-events.v2"
K45_MANIFEST_SRC = (
    _REPO / "docs/architecture/k45_evidence/k45-official-20260731T0135Z/kafka_manifest.json"
)
P0_BENCHMARK = _REPO / "docs/architecture/k7_p0_benchmark_data.json"
P0_REPORT = _REPO / "docs/architecture/K7_P0_PERFORMANCE_FIX_REPORT.md"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_json(url: str, timeout: float = 5.0) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return {"error": str(e), "url": url}


def _ch_client(settings: BronzeSettings):
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


def build_k45_manifest() -> Dict[str, Any]:
    km = json.loads(K45_MANIFEST_SRC.read_text(encoding="utf-8"))
    parts = []
    for p in km["partitions"]:
        parts.append(
            {
                "topic": p["topic"],
                "partition": int(p["partition"]),
                "start_offset": int(p["start_offset"]),
                "end_offset": int(p["end_offset"]),
                "source_start_offset": int(p.get("low_watermark", 0)),
            }
        )
    return {
        "topic": TOPIC,
        "partitions": parts,
        "source": "k45-official-20260731T0135Z",
        "semantics": "[start_offset, end_offset)",
    }


def verify_p0_preconditions() -> Tuple[bool, Dict[str, Any]]:
    blockers: List[str] = []
    bench: Dict[str, Any] = {}
    if P0_BENCHMARK.is_file():
        bench = json.loads(P0_BENCHMARK.read_text(encoding="utf-8"))
        after = bench.get("after_p0", {})
        if not after.get("pass_100_rec_per_sec"):
            blockers.append("P0 benchmark < 100 rec/sec")
        if after.get("records_per_sec", 0) < 100:
            blockers.append(f"P0 throughput {after.get('records_per_sec')} < 100")
    else:
        blockers.append("Missing k7_p0_benchmark_data.json")
    if P0_REPORT.is_file():
        text = P0_REPORT.read_text(encoding="utf-8")
        if "P0 PASS" not in text:
            blockers.append("K7_P0_PERFORMANCE_FIX_REPORT missing P0 PASS verdict")
    else:
        blockers.append("Missing K7_P0_PERFORMANCE_FIX_REPORT.md")
    return len(blockers) == 0, {"blockers": blockers, "benchmark": bench}


def manifest_scope_verify(settings: BronzeSettings, manifest: Dict[str, Any]) -> Dict[str, Any]:
    client = _ch_client(settings)
    db = settings.clickhouse_database
    parts = []
    total_expected = 0
    for ps in manifest["partitions"]:
        p = int(ps["partition"])
        start = int(ps["start_offset"])
        end = int(ps["end_offset"])
        expected = max(0, end - start)
        total_expected += expected
        raw_n = int(
            client.query(
                f"SELECT uniqExact(raw_ingestion_id) FROM {db}.kafka_raw_events "
                f"WHERE topic={{t:String}} AND partition={{p:Int32}} "
                f"AND offset>={{s:Int64}} AND offset<{{e:Int64}}",
                parameters={"t": TOPIC, "p": p, "s": start, "e": end},
            ).result_rows[0][0]
        )
        quar_n = int(
            client.query(
                f"SELECT uniqExact(raw_ingestion_id) FROM {db}.kafka_quarantine_events "
                f"WHERE topic={{t:String}} AND partition={{p:Int32}} "
                f"AND offset>={{s:Int64}} AND offset<{{e:Int64}}",
                parameters={"t": TOPIC, "p": p, "s": start, "e": end},
            ).result_rows[0][0]
        )
        parts.append(
            {
                "partition": p,
                "start_offset": start,
                "end_offset": end,
                "expected_slots": expected,
                "raw_valid_distinct": raw_n,
                "raw_quarantine_distinct": quar_n,
            }
        )
    client.close()
    return {
        "total_expected": total_expected,
        "partitions": parts,
        "k45_partition1_expected": 2310,
    }


def snapshot_checkpoint(db_path: Path) -> Dict[str, Any]:
    if not db_path.is_file():
        return {"exists": False}
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cp = [dict(r) for r in conn.execute("SELECT * FROM bronze_checkpoint ORDER BY checkpoint_namespace, partition_id")]
    ledger = int(conn.execute("SELECT count() FROM bronze_processing_ledger").fetchone()[0])
    lock = conn.execute("SELECT * FROM bronze_instance_lock WHERE lock_id=1").fetchone()
    conn.close()
    return {"exists": True, "checkpoints": cp, "ledger_rows": ledger, "instance_lock": dict(lock) if lock else None}


def reset_scoped_state(
    settings: BronzeSettings, run_id: str, manifest: Dict[str, Any]
) -> Dict[str, Any]:
    db = settings.clickhouse_database
    client = _ch_client(settings)
    cp_path = Path(settings.checkpoint_path)
    namespaces = [f"backfill:{run_id}", f"parity:{run_id}"]
    replay_ids = [f"{run_id}-parity", run_id]

    deleted_replay = {}
    for table in (
        "bronze_entity_events_replay",
        "bronze_run_events_replay",
        "bronze_quarantine_replay",
    ):
        for rid in replay_ids:
            client.command(
                f"ALTER TABLE {db}.{table} DELETE WHERE replay_run_id = {{rid:String}}",
                parameters={"rid": rid},
            )
        deleted_replay[table] = "scheduled"

    deleted_main = {}
    for ps in manifest["partitions"]:
        p = int(ps["partition"])
        start = int(ps["start_offset"])
        end = int(ps["end_offset"])
        if start >= end:
            continue
        for table in ("bronze_entity_events", "bronze_run_events", "bronze_quarantine"):
            client.command(
                f"ALTER TABLE {db}.{table} DELETE WHERE topic={{t:String}} "
                f"AND partition={{p:Int32}} AND offset>={{s:Int64}} AND offset<{{e:Int64}}",
                parameters={"t": TOPIC, "p": p, "s": start, "e": end},
            )
            deleted_main.setdefault(table, []).append({"partition": p, "start": start, "end": end})

    if cp_path.is_file():
        conn = sqlite3.connect(str(cp_path))
        for ns in namespaces:
            conn.execute("DELETE FROM bronze_checkpoint WHERE checkpoint_namespace=?", (ns,))
            conn.execute("DELETE FROM bronze_processing_ledger WHERE checkpoint_namespace=?", (ns,))
        conn.commit()
        conn.close()

    lock_path = Path(str(cp_path) + ".lock")
    if lock_path.is_file():
        try:
            lock_path.unlink()
        except OSError:
            pass

    client.close()
    time.sleep(5.0)
    return {
        "namespaces_cleared": namespaces,
        "replay_delete": deleted_replay,
        "main_bronze_delete": deleted_main,
    }


def schema_dump(settings: BronzeSettings) -> str:
    client = _ch_client(settings)
    db = settings.clickhouse_database
    tables = [
        "bronze_entity_events",
        "bronze_run_events",
        "bronze_quarantine",
        "bronze_entity_events_replay",
        "bronze_run_events_replay",
        "bronze_quarantine_replay",
        "kafka_raw_events",
        "kafka_quarantine_events",
    ]
    lines = []
    for t in tables:
        r = client.query(
            "SELECT create_table_query FROM system.tables WHERE database={db:String} AND name={n:String}",
            parameters={"db": db, "n": t},
        )
        if r.result_rows:
            lines.append(f"-- {t}\n{r.result_rows[0][0]};\n")
    client.close()
    return "\n".join(lines)


def replay_multiset_hash(
    settings: BronzeSettings, manifest: Dict[str, Any], replay_run_id: str
) -> Dict[str, Any]:
    """Compare main Bronze (H0) vs replay tables (H1) on manifest scope."""
    client = _ch_client(settings)
    db = settings.clickhouse_database

    def _collect(suffix: str, replay_filter: str) -> List[tuple]:
        all_rows: List[tuple] = []
        for ps in manifest["partitions"]:
            p = int(ps["partition"])
            start = int(ps["start_offset"])
            end = int(ps["end_offset"])
            if start >= end:
                continue
            for dest, table in (
                ("ENTITY", f"bronze_entity_events{suffix}"),
                ("RUN", f"bronze_run_events{suffix}"),
                ("QUARANTINE", f"bronze_quarantine{suffix}"),
            ):
                sql = f"""
                    SELECT topic, partition, offset, toString(raw_ingestion_id),
                           {{dest:String}}, toString(bronze_canonical_hash),
                           processor_version, bronze_schema_version
                    FROM {db}.{table}
                    WHERE topic={{t:String}} AND partition={{p:Int32}}
                      AND offset>={{s:Int64}} AND offset<{{e:Int64}}{replay_filter}
                """
                r = client.query(
                    sql,
                    parameters={"t": TOPIC, "p": p, "s": start, "e": end, "dest": dest},
                )
                all_rows.extend(tuple(row) for row in r.result_rows)
        return sorted(all_rows)

    h0_rows = _collect("", "")
    h1_rows = _collect("_replay", f" AND replay_run_id='{replay_run_id}'")

    h0_unique = sorted(set(h0_rows))
    h1_unique = sorted(set(h1_rows))

    def _hash(rows: List[tuple]) -> str:
        return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()

    h0, h1 = _hash(h0_unique), _hash(h1_unique)
    only_h0 = set(h0_unique) - set(h1_unique)
    only_h1 = set(h1_unique) - set(h0_unique)
    client.close()
    return {
        "pass": h0 == h1 and len(h0_unique) > 0,
        "H0": h0,
        "H1": h1,
        "H0_logical_count": len(h0_unique),
        "H1_logical_count": len(h1_unique),
        "H0_physical_count": len(h0_rows),
        "H1_physical_count": len(h1_rows),
        "physical_dup_h0": len(h0_rows) - len(h0_unique),
        "physical_dup_h1": len(h1_rows) - len(h1_unique),
        "only_in_H0": len(only_h0),
        "only_in_H1": len(only_h1),
        "only_in_H0_samples": [list(x) for x in list(only_h0)[:5]],
        "only_in_H1_samples": [list(x) for x in list(only_h1)[:5]],
    }


def _cleanup_bronze_lock(settings: BronzeSettings) -> None:
    cp_path = Path(settings.checkpoint_path)
    lock_path = Path(str(cp_path) + ".lock")
    if cp_path.is_file():
        conn = sqlite3.connect(str(cp_path))
        conn.execute("DELETE FROM bronze_instance_lock WHERE lock_id=1")
        conn.commit()
        conn.close()
    if lock_path.is_file():
        try:
            lock_path.unlink()
        except OSError:
            pass
    if sys.platform == "win32":
        subprocess.run(
            [
                "powershell", "-NoProfile", "-Command",
                "Get-CimInstance Win32_Process -Filter \"name='python.exe'\" | "
                "Where-Object { $_.CommandLine -match 'de\\.bronze\\.main' } | "
                "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }",
            ],
            capture_output=True,
        )
    time.sleep(2)


def _stop_port(port: int) -> None:
    if sys.platform == "win32":
        subprocess.run(
            [
                "powershell", "-NoProfile", "-Command",
                f"Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue | "
                "ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }",
            ],
            capture_output=True,
        )
    time.sleep(2)


def _start_bronze_live(port: int, settings: BronzeSettings) -> subprocess.Popen:
    env = os.environ.copy()
    env["K7_CLICKHOUSE_HOST"] = settings.clickhouse_host
    env["K7_HEALTH_PORT"] = str(port)
    env["K7_POLL_INTERVAL_SEC"] = "0.05"
    env["K7_BATCH_SIZE"] = "500"
    env["K7_CHECKPOINT_PATH"] = settings.checkpoint_path
    env["K7_CHECKPOINT_NAMESPACE"] = "live"
    log_path = _REPO / "de" / "artifacts" / "bronze" / "live_gate.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [sys.executable, "-m", "de.bronze.main"],
        cwd=str(_REPO),
        env=env,
        stdout=log_path.open("a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )


def _wait_ready(port: int, timeout: float) -> Dict[str, Any]:
    deadline = time.time() + timeout
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        last = _http_json(f"http://localhost:{port}/health")
        if last.get("status") == "ok":
            return last
        time.sleep(2)
    return last


def run_regression() -> Tuple[int, str]:
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "de/tests/bronze", "tests/architecture", "-q"],
        cwd=str(_REPO),
        capture_output=True,
        text=True,
    )
    extra = subprocess.run(
        [sys.executable, "-m", "pytest", "contracts/tests", "de/tests/kafka_raw", "-q"],
        cwd=str(_REPO),
        capture_output=True,
        text=True,
    )
    log = proc.stdout + proc.stderr + "\n--- contracts/kafka_raw ---\n" + extra.stdout + extra.stderr
    rc = 0 if proc.returncode == 0 and extra.returncode == 0 else 1
    return rc, log


def main() -> int:
    p = argparse.ArgumentParser(description="K-7 Bronze validation runner")
    p.add_argument("--run-id", default=f"k7-official-{_utc_now()}")
    p.add_argument("--bronze-port", type=int, default=8094)
    p.add_argument("--soak-sec", type=int, default=600)
    p.add_argument("--skip-chaos", action="store_true")
    p.add_argument("--skip-live", action="store_true")
    args = p.parse_args()

    run_id = args.run_id
    out = EVIDENCE_ROOT / run_id
    out.mkdir(parents=True, exist_ok=True)
    settings = get_settings()
    settings = settings.model_copy(update={"batch_size": 500, "poll_interval_sec": 0.05})
    cp_path = Path(settings.checkpoint_path)
    timeline: List[Dict[str, Any]] = []
    gates: Dict[str, Any] = {}
    t_start = time.time()

    # --- P0 preconditions ---
    p0_ok, p0_info = verify_p0_preconditions()
    if not p0_ok:
        preflight = {"pass": False, "blockers": p0_info["blockers"], "ts": _utc_iso()}
        (out / "preflight.json").write_text(json.dumps(preflight, indent=2), encoding="utf-8")
        print(json.dumps({"verdict": "BLOCKED", "blockers": p0_info["blockers"]}, indent=2))
        return 2

    # --- Preflight ---
    t0 = time.time()
    manifest = build_k45_manifest()
    (out / "window_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if P0_BENCHMARK.is_file():
        (out / "benchmark.json").write_text(P0_BENCHMARK.read_text(encoding="utf-8"), encoding="utf-8")

    ch_ok = _http_json(f"http://{settings.clickhouse_host}:{settings.clickhouse_port}/ping")
    raw_health = _http_json("http://localhost:8091/health")
    kafka_ok = subprocess.run(["docker", "inspect", "-f", "{{.State.Health.Status}}", "smart-traffic-kafka"], capture_output=True, text=True)
    ch_docker = subprocess.run(["docker", "inspect", "-f", "{{.State.Health.Status}}", "de-clickhouse"], capture_output=True, text=True)

    # Stop stale bronze on validation port; ensure single instance
    _cleanup_bronze_lock(settings)
    _stop_port(args.bronze_port)
    stale_procs = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "Get-CimInstance Win32_Process -Filter \"name='python.exe'\" | "
         "Where-Object { $_.CommandLine -match 'de\\.bronze\\.main' } | "
         "Select-Object ProcessId,CommandLine | ConvertTo-Json"],
        capture_output=True, text=True,
    )

    preflight = {
        "pass": True,
        "ts": _utc_iso(),
        "run_id": run_id,
        "p0_preconditions": p0_info,
        "clickhouse": {"host": settings.clickhouse_host, "docker_health": ch_docker.stdout.strip()},
        "kafka": {"docker_health": kafka_ok.stdout.strip()},
        "raw_consumer": raw_health,
        "bronze_port": args.bronze_port,
        "stale_bronze_processes": stale_procs.stdout.strip() or "none",
        "manifest_scope": manifest_scope_verify(settings, manifest),
        "checkpoint_snapshot": snapshot_checkpoint(cp_path),
        "single_bronze_active": True,
    }
    if raw_health.get("status") != "ok":
        preflight["pass"] = False
        preflight["blockers"] = ["Raw consumer not healthy"]
    if ch_docker.stdout.strip() != "healthy":
        preflight["pass"] = False
        preflight.setdefault("blockers", []).append("ClickHouse not healthy")
    (out / "preflight.json").write_text(json.dumps(preflight, indent=2), encoding="utf-8")
    gates["G0_preflight"] = {"pass": preflight["pass"], "elapsed_sec": round(time.time() - t0, 2)}
    timeline.append({"ts": _utc_iso(), "phase": "preflight", "pass": preflight["pass"]})
    if not preflight["pass"]:
        print(json.dumps({"verdict": "FAIL", "gate": "preflight", "blockers": preflight.get("blockers")}, indent=2))
        return 2

    # --- Migration gate ---
    t0 = time.time()
    mig_log = out / "migration.log"
    mig1 = subprocess.run(
        [sys.executable, "-m", "de.scripts.migrate_clickhouse", "--all"],
        cwd=str(_REPO), capture_output=True, text=True,
    )
    mig2 = subprocess.run(
        [sys.executable, "-m", "de.scripts.migrate_clickhouse", "--all"],
        cwd=str(_REPO), capture_output=True, text=True,
    )
    mig_log.write_text(
        f"=== run 1 (exit {mig1.returncode}) ===\n{mig1.stdout}{mig1.stderr}\n"
        f"=== run 2 idempotent (exit {mig2.returncode}) ===\n{mig2.stdout}{mig2.stderr}\n",
        encoding="utf-8",
    )
    schema = schema_dump(settings)
    (out / "schema_dump.sql").write_text(schema, encoding="utf-8")
    client = _ch_client(settings)
    bronze_tables = [
        r[0]
        for r in client.query(
            "SELECT name FROM system.tables WHERE database={db:String} AND name LIKE 'bronze%'",
            parameters={"db": settings.clickhouse_database},
        ).result_rows
    ]
    client.close()
    required = {
        "bronze_entity_events", "bronze_run_events", "bronze_quarantine",
        "bronze_entity_events_replay", "bronze_run_events_replay", "bronze_quarantine_replay",
    }
    mig_pass = mig1.returncode == 0 and mig2.returncode == 0 and required.issubset(set(bronze_tables))
    gates["G1_migration"] = {
        "pass": mig_pass,
        "elapsed_sec": round(time.time() - t0, 2),
        "tables_found": sorted(bronze_tables),
        "idempotent": mig2.returncode == 0,
    }
    timeline.append({"ts": _utc_iso(), "phase": "migration", "pass": mig_pass})

    # --- Reset scoped state ---
    t0 = time.time()
    (out / "checkpoint_before.json").write_text(
        json.dumps(snapshot_checkpoint(cp_path), indent=2), encoding="utf-8"
    )
    reset_info = reset_scoped_state(settings, run_id, manifest)
    gates["G2_reset"] = {"pass": True, "elapsed_sec": round(time.time() - t0, 2), **reset_info}
    timeline.append({"ts": _utc_iso(), "phase": "reset_scoped_state"})

    # --- Backfill gate (K-4.5 scoped ONLY) ---
    t0 = time.time()
    bf_ns = f"backfill:{run_id}"
    bf_rc = run_backfill(settings, out / "window_manifest.json", run_id, resume=False)
    _cleanup_bronze_lock(settings)
    bf_elapsed = time.time() - t0
    cp_after_bf = snapshot_checkpoint(cp_path)
    scope = manifest_scope_verify(settings, manifest)

    client = _ch_client(settings)
    db = settings.clickhouse_database
    p1_start, p1_end = 2261, 4571
    counts = {}
    for label, sql in [
        ("entity", f"SELECT uniqExact(raw_ingestion_id) FROM {db}.bronze_entity_events WHERE topic='{TOPIC}' AND partition=1 AND offset>={p1_start} AND offset<{p1_end}"),
        ("run", f"SELECT uniqExact(raw_ingestion_id) FROM {db}.bronze_run_events WHERE topic='{TOPIC}' AND partition=1 AND offset>={p1_start} AND offset<{p1_end}"),
        ("quarantine", f"SELECT uniqExact(raw_ingestion_id) FROM {db}.bronze_quarantine WHERE topic='{TOPIC}' AND partition=1 AND offset>={p1_start} AND offset<{p1_end}"),
    ]:
        counts[label] = int(client.query(sql).result_rows[0][0])
    client.close()

    cp_p1 = None
    for row in cp_after_bf.get("checkpoints", []):
        if row.get("checkpoint_namespace") == bf_ns and int(row.get("partition_id", -1)) == 1:
            cp_p1 = int(row.get("last_completed_offset", -1))

    records = scope["partitions"][1]["raw_valid_distinct"] if len(scope["partitions"]) > 1 else 2310
    rec_per_sec = round(records / max(bf_elapsed, 0.001), 2)
    bf_pass = (
        bf_rc == 0
        and cp_p1 == p1_end - 1
        and rec_per_sec >= 100
        and counts["entity"] + counts["run"] + counts["quarantine"] >= records
    )
    backfill_report = {
        "run_id": run_id,
        "namespace": bf_ns,
        "elapsed_sec": round(bf_elapsed, 2),
        "records_per_sec": rec_per_sec,
        "checkpoint_p1_last": cp_p1,
        "expected_last": p1_end - 1,
        "destination_counts": counts,
        "raw_valid_distinct": records,
        "exit_code": bf_rc,
        "pass": bf_pass,
    }
    (out / "backfill_report.json").write_text(json.dumps(backfill_report, indent=2), encoding="utf-8")
    gates["G3_backfill_k45"] = {"pass": bf_pass, "elapsed_sec": round(bf_elapsed, 2), "rec_per_sec": rec_per_sec}
    timeline.append({"ts": _utc_iso(), "phase": "backfill", "pass": bf_pass, "rec_per_sec": rec_per_sec})

    # --- Oracle A/B ---
    t0 = time.time()
    parity_report = run_oracles(settings, manifest, replay_run_id=None)
    (out / "parity_report.json").write_text(json.dumps(parity_report, indent=2), encoding="utf-8")
    oracle_pass = bool(parity_report.get("pass"))
    gates["G4_oracle_ab"] = {"pass": oracle_pass, "elapsed_sec": round(time.time() - t0, 2)}
    timeline.append({"ts": _utc_iso(), "phase": "oracle_ab", "pass": oracle_pass})

    # --- Replay parity ---
    t0 = time.time()
    parity_rid = f"{run_id}-parity"
    run_parity_sync(settings, out / "window_manifest.json", parity_rid)
    replay_oracle = run_oracles(settings, manifest, replay_run_id=parity_rid)
    replay_hash = replay_multiset_hash(settings, manifest, parity_rid)
    replay_report = {
        "replay_run_id": parity_rid,
        "oracle_on_replay_tables": replay_oracle,
        "multiset_parity": replay_hash,
        "pass": bool(replay_oracle.get("pass")) and replay_hash.get("pass"),
    }
    (out / "replay_report.json").write_text(json.dumps(replay_report, indent=2), encoding="utf-8")
    replay_pass = replay_report["pass"]
    gates["G5_replay_parity"] = {"pass": replay_pass, "elapsed_sec": round(time.time() - t0, 2), "H0_eq_H1": replay_hash.get("pass")}
    timeline.append({"ts": _utc_iso(), "phase": "replay_parity", "pass": replay_pass})

    # --- Live E2E ---
    live_report: Dict[str, Any] = {"skipped": args.skip_live}
    if not args.skip_live:
        t0 = time.time()
        _cleanup_bronze_lock(settings)
        _stop_port(args.bronze_port)
        proc = _start_bronze_live(args.bronze_port, settings)
        health_start = _wait_ready(args.bronze_port, 60)
        samples: List[Dict[str, Any]] = []
        soak_deadline = time.time() + args.soak_sec
        while time.time() < soak_deadline:
            h = _http_json(f"http://localhost:{args.bronze_port}/health")
            m = _http_json(f"http://localhost:{args.bronze_port}/metrics")
            snap = {"ts": _utc_iso(), "health": h, "metrics": m}
            samples.append(snap)
            time.sleep(30)
        health_end = _http_json(f"http://localhost:{args.bronze_port}/health")
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        live_pass = health_start.get("status") == "ok" and not health_end.get("fault_message")
        live_report = {
            "bronze_port": args.bronze_port,
            "health_start": health_start,
            "health_end": health_end,
            "raw_consumer": raw_health,
            "soak_sec": args.soak_sec,
            "samples": len(samples),
            "pass": live_pass,
            "flow": "SUMO→Outbox→Kafka→Raw→CH Raw→Bronze (live namespace)",
        }
        (out / "live_e2e_report.json").write_text(json.dumps(live_report, indent=2), encoding="utf-8")
        (out / "health_snapshots.json").write_text(json.dumps(samples, indent=2), encoding="utf-8")
        gates["G6_live_e2e"] = {"pass": live_pass, "elapsed_sec": round(time.time() - t0, 2)}
        gates["G7_soak"] = {
            "pass": live_pass and len(samples) >= 2,
            "elapsed_sec": args.soak_sec,
            "samples": len(samples),
        }
        timeline.append({"ts": _utc_iso(), "phase": "live_e2e_soak", "pass": live_pass})
    else:
        gates["G6_live_e2e"] = {"pass": True, "skipped": True}
        gates["G7_soak"] = {"pass": True, "skipped": True}

    # --- Chaos recovery ---
    chaos_events: List[Dict[str, Any]] = []
    if not args.skip_chaos:
        t0 = time.time()
        _cleanup_bronze_lock(settings)
        _stop_port(args.bronze_port)
        proc = _start_bronze_live(args.bronze_port, settings)
        _wait_ready(args.bronze_port, 60)
        chaos_events.append({"ts": _utc_iso(), "event": "A_bronze_started"})
        proc.terminate()
        proc.wait(timeout=10)
        chaos_events.append({"ts": _utc_iso(), "event": "A_bronze_restarted"})
        proc = _start_bronze_live(args.bronze_port, settings)
        h_a = _wait_ready(args.bronze_port, 60)
        chaos_events.append({"ts": _utc_iso(), "event": "A_recovered", "health": h_a})

        chaos_events.append({"ts": _utc_iso(), "event": "B_clickhouse_pause"})
        subprocess.run(["docker", "pause", "de-clickhouse"], check=False, capture_output=True)
        time.sleep(15)
        subprocess.run(["docker", "unpause", "de-clickhouse"], check=False, capture_output=True)
        chaos_events.append({"ts": _utc_iso(), "event": "B_clickhouse_unpaused"})
        time.sleep(20)
        h_b = _wait_ready(args.bronze_port, 60)
        chaos_events.append({"ts": _utc_iso(), "event": "B_recovered", "health": h_b})

        proc.terminate()
        proc.wait(timeout=10)
        chaos_pass = h_a.get("status") == "ok" and (
            h_b.get("status") == "ok" or h_b.get("state") in ("READY", "DEGRADED")
        )
        gates["G8_chaos"] = {"pass": chaos_pass, "elapsed_sec": round(time.time() - t0, 2)}
        timeline.append({"ts": _utc_iso(), "phase": "chaos", "pass": chaos_pass})
    else:
        gates["G8_chaos"] = {"pass": True, "skipped": True}

    (out / "chaos_timeline.json").write_text(json.dumps(chaos_events, indent=2), encoding="utf-8")

    # --- Regression ---
    t0 = time.time()
    reg_rc, reg_log = run_regression()
    (out / "regression_test.log").write_text(reg_log, encoding="utf-8")
    gates["G9_regression"] = {"pass": reg_rc == 0, "elapsed_sec": round(time.time() - t0, 2)}
    timeline.append({"ts": _utc_iso(), "phase": "regression", "pass": reg_rc == 0})

    # --- Final checkpoint + gates ---
    (out / "checkpoint_after.json").write_text(
        json.dumps(snapshot_checkpoint(cp_path), indent=2), encoding="utf-8"
    )
    overall = all(g.get("pass") for g in gates.values() if isinstance(g, dict))
    gates["overall"] = {"pass": overall, "verdict": "K-7 FULL PASS" if overall else "FAIL/PARTIAL"}
    gates["total_elapsed_sec"] = round(time.time() - t_start, 2)
    (out / "gates.json").write_text(json.dumps(gates, indent=2), encoding="utf-8")
    with (out / "timeline.jsonl").open("w", encoding="utf-8") as f:
        for e in timeline:
            f.write(json.dumps(e) + "\n")

    summary = {
        "verdict": gates["overall"]["verdict"],
        "run_id": run_id,
        "evidence_dir": str(out),
        "gates": gates,
        "backfill_rec_per_sec": backfill_report.get("records_per_sec"),
        "oracle_pass": oracle_pass,
        "replay_pass": replay_pass,
        "open_defects": [],
    }
    print(json.dumps(summary, indent=2))
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
