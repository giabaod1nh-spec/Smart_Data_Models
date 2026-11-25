"""Local, isolated SUMO -> actual Gold baseline. See the Phase 4A-Core runbook."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from urllib.request import urlopen

REPO = Path(__file__).resolve().parents[2]
ENDPOINTS = {"raw": (18097, "metrics"), "bronze": (18098, "metrics"),
             "silver": (18095, "health"), "gold": (18096, "health")}
WORKER_IMAGE = "smart_data_models-de-silver-processor:latest"


def prepare(root, deps):
    """Create a fresh project spec. All writable stores belong to this trial."""
    root.mkdir(parents=True, exist_ok=False)
    producer_path = root / "producer"
    (producer_path / "runs").mkdir(parents=True)
    mounts = [f"{REPO / 'de'}:/code/de:ro", f"{REPO / 'contracts'}:/code/contracts:ro",
              f"{deps}:/app/acceptance-deps:ro"]
    archives = f"{producer_path}:/app/Visualize/artifacts:ro"
    cache = "/app/de/artifacts"
    base_env = {"PYTHONPATH": "/app/acceptance-deps:/code", "PYTHONDONTWRITEBYTECODE": "1"}
    services = {}

    def service(module, args=(), *, env=None, volumes=(), port=None, depends=None):
        return dict(image=WORKER_IMAGE, working_dir="/code", environment=dict(base_env, **(env or {})),
                    volumes=[*mounts, *volumes], command=["python", "-m", module, *args],
                    **({"ports": [f"127.0.0.1:{port[0]}:{port[1]}"]} if port else {}),
                    **({"depends_on": depends} if depends else {}))

    def health(command):
        return dict(test=["CMD-SHELL", command], interval="3s", timeout="5s", retries=30)

    services["clickhouse"] = dict(image="clickhouse/clickhouse-server:24.3",
        ports=["127.0.0.1:18124:8123"], environment={"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"},
        volumes=["clickhouse:/var/lib/clickhouse"], healthcheck=health("wget -qO- http://localhost:8123/ping"))
    services["kafka"] = dict(image="apache/kafka:3.8.1", ports=["127.0.0.1:39092:39092"],
        environment=dict(KAFKA_NODE_ID="1", KAFKA_PROCESS_ROLES="broker,controller",
            KAFKA_LISTENERS="INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:39092",
            KAFKA_ADVERTISED_LISTENERS="INTERNAL://kafka:9092,EXTERNAL://localhost:39092",
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
            KAFKA_INTER_BROKER_LISTENER_NAME="INTERNAL", KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
            KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093", KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1",
            KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1", KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1",
            KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"),
        volumes=["kafka:/var/lib/kafka/data"],
        healthcheck=health("/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null"))
    services["topic"] = dict(image="apache/kafka:3.8.1",
        depends_on={"kafka": {"condition": "service_healthy"}},
        entrypoint=["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--create",
                    "--if-not-exists", "--topic", "traffic.observations.v1", "--partitions", "3",
                    "--replication-factor", "1", "--config", "retention.ms=604800000",
                    "--config", "retention.bytes=1073741824"])
    services["migrate"] = service("de.scripts.migrate_clickhouse", ("--v1-bronze",),
        env={"K4_CLICKHOUSE_HOST": "clickhouse"}, depends={"clickhouse": {"condition": "service_healthy"}})
    services["silver-migrate"] = service("de.scripts.migrate_clickhouse", ("--v1-silver",),
        env={"K4_CLICKHOUSE_HOST": "clickhouse"},
        depends={"migrate": {"condition": "service_completed_successfully"}})
    services["reader-grants"] = dict(image="clickhouse/clickhouse-server:24.3",
        entrypoint=["clickhouse-client", "--host", "clickhouse", "--multiquery", "--query",
            "CREATE USER IF NOT EXISTS gold_acceptance_reader IDENTIFIED WITH no_password; "
            "GRANT SELECT ON smart_traffic.* TO gold_acceptance_reader;"],
        depends_on={"silver-migrate": {"condition": "service_completed_successfully"}})
    services["raw"] = service("de.kafka_raw.v1_main",
        env=dict(DE_V1_RAW_BOOTSTRAP_SERVERS="kafka:9092", DE_V1_RAW_CLICKHOUSE_HOST="clickhouse",
                 DE_V1_RAW_LEDGER_PATH=f"{cache}/kafka_raw_v1/ledger.sqlite3"),
        volumes=[f"raw:{cache}/kafka_raw_v1"], port=(18097, 8097),
        depends={item: {"condition": "service_completed_successfully"} for item in ("migrate", "topic")})
    services["bronze"] = service("de.bronze.main",
        env=dict(DE_BRONZE_CLICKHOUSE_HOST="clickhouse", DE_BRONZE_RAW_LEDGER_PATH="/raw/ledger.sqlite3",
                 DE_BRONZE_CHECKPOINT_PATH=f"{cache}/bronze_v1/checkpoint.sqlite3"),
        volumes=["raw:/raw:ro", f"bronze:{cache}/bronze_v1"], port=(18098, 8098),
        depends={"raw": {"condition": "service_started"}})
    shared = dict(SILVER_CLICKHOUSE_HOST="clickhouse", SILVER_V1_ARCHIVE_ROOT="/app/Visualize/artifacts",
                  SILVER_CHECKPOINT_PATH=f"{cache}/silver_v1/checkpoint.sqlite3",
                  SILVER_V1_BRONZE_PATH=f"{cache}/bronze_v1/checkpoint.sqlite3")
    services["silver"] = service("de.silver.main", env=shared,
        volumes=[f"bronze:{cache}/bronze_v1:ro", f"silver:{cache}/silver_v1", archives], port=(18095, 8095),
        depends={"bronze": {"condition": "service_started"},
                 "silver-migrate": {"condition": "service_completed_successfully"}})
    services["gold"] = service("de.gold_runtime.main",
        env=dict(shared, SILVER_CLICKHOUSE_USER="gold_acceptance_reader",
                 GOLD_CHECKPOINT_PATH=f"{cache}/gold_v1/checkpoint.sqlite3",
                 GOLD_INSTANCE_LOCK_PATH=f"{cache}/gold_v1/instance.lock"),
        volumes=[f"bronze:{cache}/bronze_v1:ro", f"silver:{cache}/silver_v1:ro",
                 f"gold:{cache}/gold_v1", archives], port=(18096, 8096),
        depends={"silver": {"condition": "service_started"},
                 "reader-grants": {"condition": "service_completed_successfully"}})
    services["gold"]["stop_grace_period"] = "90s"
    spec = dict(services=services, volumes={name: {} for name in
                ("clickhouse", "kafka", "raw", "bronze", "silver", "gold")})
    (root / "compose.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")


def produce(root, run_id, duration, realtime, *, sumo_step=1.0):
    artifacts = root / "producer"
    os.environ.update(SUMO_STEP_LENGTH=str(sumo_step), KAFKA_BOOTSTRAP_SERVERS="localhost:39092",
                      KAFKA_PUBLISH_ENABLED="false", KAFKA_OUTBOX_ENABLED="false")
    sys.path.insert(0, str(REPO / "Visualize"))
    from configuration import config
    config.ARTIFACTS_DIR = config.ARTIFACT_ROOT = artifacts
    config.V1_OUTBOX_DB = artifacts / "kafka_outbox/v1-outbox.sqlite3"
    from app.traci_runner import build_parser, run
    args = ["--no-gui", "--no-orion", "--no-api", "--v1-observations", "--v1-publish",
            "--nodes", "A,B,C,D,M", "--simulation-run-id", run_id, "--max-sim-time", str(duration),
            "--log-level", "WARNING"]
    if realtime:
        args.append("--realtime")
    (root / "source-profile.json").write_text(json.dumps(dict(
        run_id=run_id, sumo_step_seconds=config.SUMO_STEP_LENGTH, simulation_seconds=duration,
        observation_cache_seconds=float(os.getenv("OBSERVATION_INTERVAL_SEC", "1.0")),
        publish_interval_seconds=config.PUBLISH_INTERVAL, realtime=realtime, arguments=args,
        v1_topic=config.V1_KAFKA_TOPIC, bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        outbox=str(config.V1_OUTBOX_DB)), indent=2), encoding="utf-8")
    return run(build_parser().parse_args(args))


def outbox(path):
    if not path.exists():
        return None
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.5) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        table = db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                           "AND name='kafka_outbox'").fetchone()
        if not table:
            return None
        rows, total_bytes, sim = db.execute("""SELECT count(*), coalesce(sum(length(CAST(payload_json AS BLOB))),0),
            max(json_extract(payload_json,'$.simulationTimeSeconds')) FROM kafka_outbox""").fetchone()
        pending, pending_bytes, oldest = db.execute("""SELECT count(*),
            coalesce(sum(length(CAST(payload_json AS BLOB))),0), min(created_at) FROM kafka_outbox
            WHERE status IN ('OUTBOXED','QUEUED','FAILED_RETRYABLE')""").fetchone()
        oldest_age = None
        if oldest:
            oldest_age = (datetime.now(timezone.utc)
                          - datetime.fromisoformat(oldest.replace("Z", "+00:00"))).total_seconds()
        acked = db.execute("SELECT count(*) FROM kafka_outbox WHERE status='ACKED'").fetchone()[0]
        return dict(rows=rows, bytes=total_bytes, simulation_seconds=sim, acked=acked,
                    pending=pending, pending_bytes=pending_bytes, oldest_pending_seconds=oldest_age)


class MeasurementGuard:
    """Distinguish short sampling gaps from a run that has lost its evidence."""

    def __init__(self, *, missing_budget_sec=15, snapshot_budget_sec=60, allowed_outages=()):
        self.missing_budget_sec = missing_budget_sec
        self.snapshot_budget_sec = snapshot_budget_sec
        self.allowed_outages = frozenset(allowed_outages)
        self.invalid_since = {}

    def assess(self, services, *, wall_utc, monotonic):
        invalid = {}
        terminal = []
        for name, snapshot in services.items():
            reason = self._reason(name, snapshot, wall_utc)
            if reason is None:
                self.invalid_since.pop(name, None)
                continue
            invalid[name] = reason
            self.invalid_since.setdefault(name, monotonic)
            if reason.startswith("terminal"):
                terminal.append(name)
        sustained = [name for name in invalid if name not in self.allowed_outages
                     and monotonic - self.invalid_since[name] >= self.missing_budget_sec]
        failures = terminal or sustained
        return dict(valid=not invalid, invalid=invalid, stop_reason=(
            "; ".join(f"{name}: {invalid[name]}" for name in failures) if failures else None))

    def _reason(self, name, snapshot, wall_utc):
        if not isinstance(snapshot, dict):
            return "endpoint unavailable"
        states = {value.strip().upper() for field in ("state", "status")
                  if isinstance(value := snapshot.get(field), str) and value.strip()}
        terminal = states & {"FAULTED", "FAILED"}
        if terminal:
            return "terminal state " + ", ".join(sorted(terminal))
        faults = snapshot.get("fault_status")
        if isinstance(faults, dict) and any(isinstance(fault, dict) and fault.get("retryable") is False
                                            for fault in faults.values()):
            return "terminal reported fault"
        if "STOPPED" in states:
            return "worker stopped" if name in self.allowed_outages else "terminal state STOPPED"
        if snapshot.get("worker_alive") is False or snapshot.get("consumer_thread_alive") is False:
            return "worker not alive"
        if snapshot.get("metrics_stale") is True:
            return "metrics flagged stale"
        if states & {"STARTING", "STOPPING"} or not states:
            return "endpoint has no running snapshot"
        if name in {"silver", "gold"}:
            try:
                captured = datetime.fromisoformat(snapshot["snapshot_at"])
                if captured.tzinfo is None:
                    raise ValueError("timestamp must include its timezone")
                age = wall_utc - captured.timestamp()
            except (KeyError, TypeError, ValueError, OverflowError):
                return "snapshot timestamp missing or invalid"
            if age > self.snapshot_budget_sec:
                return "snapshot too old"
            if age < -5:
                return "snapshot clock ahead of collector"
        return None


def sample(root, seconds, *, source_only=False, allowed_outages=()):
    try:
        import psutil
    except ImportError:
        psutil = None
    guard = MeasurementGuard(allowed_outages=allowed_outages)
    deadline = time.monotonic() + seconds
    with (root / "samples.jsonl").open("a", encoding="utf-8") as output:
        while time.monotonic() < deadline:
            start = time.monotonic()
            row = {"wall_utc": time.time(), "services": {}}
            if psutil:
                memory = psutil.virtual_memory()
                row["host"] = dict(available_memory_bytes=memory.available, cpu_pct=psutil.cpu_percent())
            for name, (port, route) in (() if source_only else ENDPOINTS.items()):
                try:
                    with urlopen(f"http://127.0.0.1:{port}/{route}", timeout=1) as response:
                        row["services"][name] = json.load(response)
                except (OSError, ValueError):
                    row["services"][name] = None
            try:
                row["outbox"] = outbox(root / "producer/kafka_outbox/v1-outbox.sqlite3")
            except sqlite3.Error:
                row["outbox"] = None
            row["measurement"] = guard.assess(row["services"], wall_utc=time.time(), monotonic=time.monotonic())
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
            if row["measurement"]["stop_reason"]:
                print(row["measurement"]["stop_reason"], file=sys.stderr)
                return 2
            time.sleep(max(0, 2 - (time.monotonic() - start)))
    return 0


def verify_pipeline(outbox_path, bronze_path, gold_path):
    """Offline acceptance oracle over isolated stores. No production imports."""
    from contextlib import ExitStack

    with ExitStack() as stack:
        outbox_db, bronze_db, gold_db = [stack.enter_context(sqlite3.connect(
            Path(path).resolve().as_uri() + "?mode=ro", uri=True)) for path in
            (outbox_path, bronze_path, gold_path)]
        # Begin with Gold's prefix so upstream snapshots can account for it
        # while writers are still draining. This harness measures conflict-free load.
        gold_db.execute("BEGIN")
        gold_cursors = dict(gold_db.execute("SELECT kind,sequence FROM core_cursor"))
        bronze_db.execute("BEGIN")
        canonical = dict(bronze_db.execute("SELECT event_id,state FROM v1_identity WHERE namespace='live'"))
        outbox_db.execute("BEGIN")
        outbox_rows = dict(outbox_db.execute(
            "SELECT event_id,payload_json FROM kafka_outbox WHERE status='ACKED'"))
        unacked_outbox = outbox_db.execute(
            "SELECT count(*) FROM kafka_outbox WHERE status IS NOT 'ACKED'").fetchone()[0]
        changes = list(bronze_db.execute("""SELECT change_sequence,event_id,change_type
            FROM v1_changes WHERE namespace='live' ORDER BY change_sequence"""))
        accepted_prefix = changes[:gold_cursors["CHANGE"]]
        inventory = [(seq, event, "CONFLICTED" if kind == "CONFLICTED" else
                      "SUPERSEDED_BY_CONFLICT" if canonical[event] == "CONFLICTED" else "BUNDLE_VALID")
                     for seq, event, kind in accepted_prefix]
        actual = list(gold_db.execute("""SELECT sequence,event_id,disposition FROM core_transition
            WHERE kind='CHANGE' ORDER BY sequence"""))
        expected_membership = set()
        for _, event_id, disposition in inventory:
            if disposition == "BUNDLE_VALID":
                item = json.loads(outbox_rows[event_id])
                # Use the producer envelope coordinates rather than Gold's window helper.
                tick = int(item["simulationTimeSeconds"])
                for width in (60, 300):
                    start = tick - tick % width
                    expected_membership.add((event_id, item["simulationRunId"], width, start, start + width))
        actual_membership = {(event_id, json.loads(identity)["simulation_run_id"], duration, lo, hi)
            for event_id, identity, duration, lo, hi in gold_db.execute("""SELECT m.event_id,r.identity,
                w.duration,w.start,w.end FROM core_membership m JOIN core_window w ON w.id=m.window_id
                JOIN core_run r ON r.id=w.run_key""")}
        sequence_coverage = (len(actual) == gold_cursors["CHANGE"] and actual == inventory
                             and all(seq == idx for idx, (seq, _, _) in enumerate(actual, 1)))
        receipts = list(gold_db.execute("""SELECT after_sequence,through_sequence,id FROM core_receipt
            WHERE kind='CHANGE' ORDER BY after_sequence"""))
        receipt_coverage = ((not receipts and gold_cursors["CHANGE"] == 0) or bool(
            receipts and receipts[0][0] == 0 and receipts[-1][1] == gold_cursors["CHANGE"]
            and all(prev[1] == row[0] for prev, row in zip(receipts, receipts[1:]))))
        membership_match = actual_membership == expected_membership
        # Classify windows against producer progress, including those that are
        # still OPEN or absent in Gold because its observed clock is behind.
        source_clock = {}
        source_windows = {}
        for event_id, payload in outbox_rows.items():
            event = json.loads(payload)
            run = event["simulationRunId"]
            tick = event["simulationTimeSeconds"]
            source_clock[run] = max(tick, source_clock.get(run, tick))
            if canonical.get(event_id) == "CONFLICTED":
                continue
            for width in (60, 300):
                lo = int(tick) // width * width
                source_windows.setdefault((run, width, lo, lo + width), set()).add(event_id)
        gold_windows = {}
        for identity, observed, width, lo, hi, eligible, state, count, eligible_at, completed_at in gold_db.execute("""
                SELECT r.identity,r.observed_time,w.duration,w.start,w.end,w.eligible,g.state,
                       g.input_count,g.eligible_at,g.completed_at
                FROM core_window w JOIN core_run r ON r.id=w.run_key
                LEFT JOIN core_work g ON g.window_id=w.id AND g.generation=w.generation"""):
            key = (json.loads(identity)["simulation_run_id"], width, lo, hi)
            gold_windows[key] = dict(observed_time=observed, eligible=bool(eligible), state=state,
                                     count=count, eligible_at=eligible_at, completed_at=completed_at)
        window_progress = []
        for (run, width, lo, hi), members in sorted(source_windows.items()):
            gold_window = gold_windows.get((run, width, lo, hi), {})
            window_progress.append(dict(
                run_id=run, duration=width, start=lo, end=hi, source_time=source_clock[run],
                source_crossed=source_clock[run] >= hi, expected_members=len(members),
                gold=gold_window))
        eligible_work_drained = all(
            entry["gold"].get("state") == "COMPLETE"
            and entry["gold"].get("count") == entry["expected_members"]
            for entry in window_progress if entry["source_crossed"])
        return dict(acked_outbox=len(outbox_rows), unacked_outbox=unacked_outbox,
                    bronze_identities=len(canonical),
                    gold_cursors=gold_cursors, membership_rows=len(actual_membership),
                    pending_source_events=len(outbox_rows.keys()-canonical.keys()),
                    bronze_unknown_events=len(canonical.keys()-outbox_rows.keys()),
                    exact_sequence_coverage=sequence_coverage, receipt_coverage=receipt_coverage,
                    membership_match=membership_match, receipts=len(receipts),
                    eligible_work_drained=eligible_work_drained, window_progress=window_progress,
                    membership_identity_sha256=sha256(json.dumps(sorted(actual_membership),
                        ensure_ascii=False, separators=(",", ":")).encode()).hexdigest(),
                    full_drain=(unacked_outbox == 0 and set(outbox_rows) == set(canonical)
                                and len(changes) == gold_cursors["CHANGE"]))


def probe_retained_reads():
    """Short read-only profile on a frozen, already verified isolated run."""
    from itertools import groupby

    from de.gold_runtime.silver_readers import CanonicalGoldSource
    from de.silver.canonical_reader import CanonicalReadSession
    from de.silver.config import SilverSettings
    from de.silver.repositories import V1_DIRECTION_TABLES

    with CanonicalReadSession(SilverSettings()) as session:
        gold = CanonicalGoldSource(session)
        boundary = gold.discover(None)
        if boundary.verified_change_prefix < 48:
            raise RuntimeError("retained profile requires at least 48 verified sequences")
        tables = sorted(V1_DIRECTION_TABLES)
        evidence = session.silver.execute("""
            SELECT source_sequence,target_table,expected_json FROM silver_v1_obligation
            WHERE namespace='live' AND source_kind='changes' AND source_sequence BETWEEN 1 AND 16
            AND status='RECONCILED' AND target_table IN (?,?)
            ORDER BY source_sequence,target_table,row_key LIMIT 513""", tables).fetchall()
        if not evidence or len(evidence) > 512:
            raise RuntimeError("unexpected retained direction evidence size")
        groups = [
            (table, [json.loads(row["expected_json"]) for row in group])
            for (_, table), group in groupby(evidence, key=lambda row: (row["source_sequence"], row["target_table"]))
        ]
        repo = session.silver_repo
        timings = {"single": [], "group": []}
        for order in (("single", "group"), ("group", "single")):
            for mode in order:
                started = time.monotonic()
                for table, rows in groups:
                    found = (repo.reconcile_v1_directions(table, rows) if mode == "group"
                             else [repo.reconcile_v1_row(table, row) for row in rows])
                    if not all(found):
                        raise RuntimeError("retained directional evidence does not match")
                timings[mode].append(time.monotonic() - started)
        reads = []
        started = time.monotonic()
        for sequence in range(1, 49):
            read_start = time.monotonic()
            page = gold.changes(boundary.source_binding_id, sequence - 1, entries=1)
            if (page.entry_count != 1 or page.entries[0].effective_disposition != "BUNDLE_VALID"
                    or page.entries[0].change_sequence != sequence):
                raise RuntimeError("retained Gold member failed verification")
            reads.append(dict(sequence=sequence, seconds=time.monotonic() - read_start,
                              bytes=page.encoded_bytes))
            if time.monotonic() - started > 40:
                break
        return dict(direction_rows_per_pass=len(evidence), direction_groups_per_pass=len(groups),
                    direction_wall_seconds=timings, canonical_member_reads=reads,
                    frozen_change_prefix=boundary.verified_change_prefix,
                    frozen_invalidation_prefix=boundary.verified_invalidation_prefix)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "produce", "sample", "oracle", "probe"))
    parser.add_argument("root", type=Path)
    parser.add_argument("--deps", type=Path, default=Path(".pytest_cache/phase4a-deps"))
    parser.add_argument("--run-id", default="phase4a-core-capacity-a")
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--realtime", action="store_true")
    parser.add_argument("--sumo-step", type=float, choices=(0.01, 1.0), default=1.0)
    parser.add_argument("--source-only", action="store_true", help="Sample the producer and host without probing DE ports")
    parser.add_argument("--allow-outage", choices=tuple(ENDPOINTS), action="append", default=[],
                        help="Allow this endpoint to be unavailable in an intentional outage experiment")
    parser.add_argument("--require-window", type=int, choices=(60, 300), action="append", default=[],
                        help="Require a source-crossed completed proof for this duration in the oracle")
    args = parser.parse_args()
    root = args.root.resolve()
    if args.action == "prepare":
        prepare(root, args.deps.resolve(strict=True))
    elif args.action == "produce":
        return produce(root, args.run_id, args.seconds, args.realtime, sumo_step=args.sumo_step)
    elif args.action == "sample":
        return sample(root, args.seconds, source_only=args.source_only, allowed_outages=args.allow_outage)
    elif args.action == "probe":
        print(json.dumps(probe_retained_reads(), sort_keys=True))
    else:
        evidence = verify_pipeline(root / "kafka_outbox/v1-outbox.sqlite3",
            os.environ["SILVER_V1_BRONZE_PATH"], os.environ["GOLD_CHECKPOINT_PATH"])
        crossed = {window["duration"] for window in evidence["window_progress"] if window["source_crossed"]}
        evidence["missing_required_durations"] = sorted(set(args.require_window) - crossed)
        print(json.dumps(evidence, sort_keys=True))
        return 0 if not evidence["missing_required_durations"] and all(evidence[key] for key in (
            "exact_sequence_coverage", "receipt_coverage", "membership_match", "full_drain",
            "eligible_work_drained")) else 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
