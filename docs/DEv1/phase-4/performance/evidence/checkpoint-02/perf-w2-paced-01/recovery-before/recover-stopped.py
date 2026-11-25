"""Resume the original isolated project and Outbox without starting SUMO."""
from base64 import b64decode
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
from urllib.request import urlopen

from confluent_kafka import TopicCollection
from confluent_kafka.admin import AdminClient
import psutil


ROOT = Path(__file__).resolve().parent
PROJECT = "smart-perf-w2-paced-01"
PROFILE = ROOT.parent
OUTBOX = PROFILE / "producer/kafka_outbox/v1-outbox.sqlite3"
REPO = PROFILE.parent.parent
TOPIC = "traffic.observations.v1"
BROKER = "localhost:39092"
BACKENDS = ("clickhouse", "kafka")
WORKERS = ("raw", "bronze", "silver", "gold")
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Visualize"))

from de.tools.gold_core_capacity import ENDPOINTS, MeasurementGuard
from integration.kafka.v1_publisher import V1DurablePublisher


def snapshot_db(name):
    return closing(sqlite3.connect((ROOT / f"{name}.sqlite3").as_uri() + "?immutable=1", uri=True))


def docker(*args, timeout=20):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True, timeout=timeout).stdout


def capture_services():
    snapshots = {}
    for name, (port, route) in ENDPOINTS.items():
        try:
            with urlopen(f"http://127.0.0.1:{port}/{route}", timeout=0.5) as stream:
                snapshots[name] = json.load(stream)
        except (OSError, ValueError):
            snapshots[name] = None
    return snapshots


def run(log):
    def record(phase, **fields):
        available = psutil.virtual_memory().available
        row = dict(phase=phase, utc=datetime.now(timezone.utc).isoformat(), available=available, **fields)
        print(json.dumps(row, sort_keys=True), file=log, flush=True)
        if available < 1024**3:
            raise RuntimeError("host headroom below 1 GiB runtime floor")
        return available

    if record("preflight") < 2_500_000_000:
        raise RuntimeError("host headroom below 2.5 GB setup reserve")
    names = [f"{PROJECT}-{service}-1" for service in BACKENDS + WORKERS]
    ids = json.loads(docker("inspect", *names))
    for service, name, data in zip(BACKENDS + WORKERS, names, ids, strict=True):
        labels = data["Config"]["Labels"]
        mounted = {item.get("Name") for item in data["Mounts"]}
        if (labels.get("com.docker.compose.project") != PROJECT or
                labels.get("com.docker.compose.service") != service or
                f"{PROJECT}_{service}" not in mounted or data["State"]["Status"] != "exited"):
            raise RuntimeError(f"incident container or volume mismatch for {name}")
    with snapshot_db("raw") as db:
        (original_identity,) = db.execute("SELECT DISTINCT topic_identity FROM partition_progress").fetchone()
    with snapshot_db("outbox") as db:
        ack_offsets = db.execute(
            "SELECT kafka_partition,min(kafka_offset),max(kafka_offset) FROM kafka_outbox "
            "WHERE status='ACKED' GROUP BY kafka_partition",
        ).fetchall()
        expected_counts = Counter(dict(db.execute("SELECT status,count(*) FROM kafka_outbox GROUP BY status")))
    if not OUTBOX.is_file():
        raise RuntimeError("incident Outbox missing")
    with closing(sqlite3.connect(OUTBOX.as_uri() + "?mode=ro", uri=True)) as db:
        actual_counts = Counter(dict(db.execute("SELECT status,count(*) FROM kafka_outbox GROUP BY status")))
        actual_digest = sha256(json.dumps(db.execute(
            "SELECT event_id,payload_json FROM kafka_outbox ORDER BY event_id").fetchall(),
            ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    expected_digest = json.loads((ROOT / "state.json").read_text())["outbox_rows_digest"]
    if actual_counts != expected_counts or actual_digest != expected_digest:
        raise RuntimeError("incident Outbox differs from its saved status boundary")
    expected_binding = next(
        row["services"]["gold"]["source_binding_id"]
        for line in (PROFILE / "samples.jsonl").read_text(encoding="utf-8").splitlines()
        if (row := json.loads(line))["services"].get("gold", {})
        and row["services"]["gold"].get("source_binding_id")
    )
    started = []
    publisher = None
    try:
        setup_deadline = time.monotonic() + 65
        for name in names[:2]:
            docker("start", name)
            started.append(name)
        admin = AdminClient({"bootstrap.servers": BROKER, "log_level": 0})
        while True:
            record("wait-broker")
            try:
                desc = admin.describe_topics(TopicCollection([TOPIC]), request_timeout=3)[TOPIC].result(4)
                break
            except Exception:
                if time.monotonic() >= setup_deadline - 12:
                    raise
                time.sleep(1)
        description = docker(
            "exec", names[1], "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092",
            "--describe", "--topic", TOPIC, timeout=12,
        )
        match = re.search(r"\bTopicId:\s*(\S+)", description)
        broker_identity = match[1] if match else ""
        retained_uuid = b64decode(original_identity + "==", altchars=b"-_", validate=True)
        broker_uuid = b64decode(broker_identity + "==", altchars=b"-_", validate=True)
        record("broker-identity-check", expected=original_identity, broker_reported=broker_identity,
               host_reported=str(desc.topic_id))
        if len(retained_uuid) != 16 or broker_uuid != retained_uuid or len(desc.partitions) != 3:
            raise RuntimeError(f"retained broker topic identity mismatch: {broker_identity}")
        identity = original_identity
        bounds_by_time = {}
        for bound in ("earliest", "latest"):
            result = docker("exec", names[1], "/opt/kafka/bin/kafka-get-offsets.sh",
                            "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--time", bound, timeout=12)
            items = re.findall(rf"^{re.escape(TOPIC)}:(\d+):(\d+)$", result, re.MULTILINE)
            bounds_by_time[bound] = {int(partition): int(offset) for partition, offset in items}
        bounds = {partition: (bounds_by_time["earliest"][partition], bounds_by_time["latest"][partition])
                  for partition, _, _ in ack_offsets}
        if any(not bounds[part][0] <= lo <= hi < bounds[part][1] for part, lo, hi in ack_offsets):
            raise RuntimeError("broker does not retain the original ACKed offsets")
        if record("broker-verified", topic_identity=identity, bounds=bounds) < 2_500_000_000:
            raise RuntimeError("host headroom below 2.5 GB before starting workers")
        for name in names[2:]:
            docker("start", name)
            started.append(name)
        guard = MeasurementGuard()
        while time.monotonic() < setup_deadline:
            services = capture_services()
            measurement = guard.assess(services, wall_utc=time.time(), monotonic=time.monotonic())
            available = record("wait-workers", services=services, measurement=measurement)
            if measurement["stop_reason"]:
                raise RuntimeError(measurement["stop_reason"])
            if measurement["valid"]:
                if (services["raw"].get("topic_identity") != identity or
                        services["gold"].get("source_binding_id") != expected_binding):
                    raise RuntimeError("worker source binding differs from retained evidence")
                if available < 2_500_000_000:
                    raise RuntimeError("host headroom below 2.5 GB after starting workers")
                break
            time.sleep(1)
        else:
            raise RuntimeError("worker setup budget exhausted")
        publisher = V1DurablePublisher(db_path=OUTBOX, bootstrap_servers=BROKER, client_id=PROJECT + "-recover")
        publisher.start()
        drain_start = time.monotonic()
        while time.monotonic() - drain_start < 130:
            services = capture_services()
            measurement = guard.assess(services, wall_utc=time.time(), monotonic=time.monotonic())
            health = publisher.health()
            record("drain", elapsed=time.monotonic() - drain_start, outbox=health,
                   services=services, measurement=measurement)
            if measurement["stop_reason"]:
                raise RuntimeError(measurement["stop_reason"])
            if publisher.is_faulted or health["outbox_failed_permanent_count"]:
                raise RuntimeError("permanent Outbox recovery failure")
            time.sleep(2)
        record("drain-budget-reached")
    finally:
        try:
            if publisher:
                publisher.stop(flush_timeout_sec=3)
        finally:
            if started:
                docker("stop", "--timeout", "20", *reversed(started), timeout=40)


if __name__ == "__main__":
    suffix = sys.argv[1] if len(sys.argv) > 1 else "initial"
    with (ROOT / f"recovery-{suffix}.samples.jsonl").open("x", encoding="utf-8") as evidence:
        run(evidence)
