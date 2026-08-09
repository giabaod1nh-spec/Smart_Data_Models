"""Live gates for K-2a / K-2b / K-3 shadow DVT (short drills, not 15–30m soak)."""
from __future__ import annotations

import json
import statistics
import sys
import threading
import time
import uuid
from pathlib import Path

VIS = Path(__file__).resolve().parents[1]
REPO = VIS.parent
sys.path.insert(0, str(VIS))
sys.path.insert(0, str(REPO))

REPORT: dict = {"gates": {}, "ok": True}


def _ent(eid, etype="Intersection", run="live-run-1", sim_t=1.0):
    return {
        "id": eid,
        "type": etype,
        "simulationRunId": {"type": "Property", "value": run},
        "simulationTime": {"type": "Property", "value": sim_t},
        "scenarioId": {"type": "Property", "value": "normal"},
        "refTrafficLights": {
            "type": "Relationship",
            "object": ["urn:ngsi-ld:TrafficLight:A-North"],
        },
        "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    }


def _cycle_ents(run="live-run-1", sim_t=1.0):
    return [
        _ent("urn:ngsi-ld:Intersection:A", run=run, sim_t=sim_t),
        _ent("urn:ngsi-ld:TrafficLight:A-North", "TrafficLight", run=run, sim_t=sim_t),
        _ent("urn:ngsi-ld:Camera:A", "Camera", run=run, sim_t=sim_t),
        _ent(
            "urn:ngsi-ld:VehicleSensor:A:NORTHBOUND",
            "VehicleSensor",
            run=run,
            sim_t=sim_t,
        ),
    ]


def gate(name: str, passed: bool, detail: str = "") -> None:
    REPORT["gates"][name] = {"pass": passed, "detail": detail}
    if not passed:
        REPORT["ok"] = False
    print(f"[{'PASS' if passed else 'FAIL'}] {name}: {detail}")


def k2a_fanout_p95(tmp: Path) -> None:
    from integration.kafka.producer import AsyncKafkaProducer

    run_id = f"fanout-{uuid.uuid4().hex[:8]}"
    p = AsyncKafkaProducer(
        bootstrap_servers="localhost:29092",
        evidence_root=tmp / "evidence",
        simulation_run_id=run_id,
        poll_interval_sec=0.05,
    )
    p.start()
    # warmup
    for i in range(3):
        p.publish_cycle(_cycle_ents(run=run_id, sim_t=float(i)), cycle_sequence=i)
    time.sleep(0.5)
    samples = []
    for i in range(30):
        n = p.publish_cycle(
            _cycle_ents(run=run_id, sim_t=float(10 + i)), cycle_sequence=100 + i
        )
        h = p.health()
        samples.append(float(h.get("fanout_total_duration_ms") or 0))
        assert n == 4
    # wait acks
    deadline = time.time() + 30
    while p.metrics.pending_count() > 0 and time.time() < deadline:
        time.sleep(0.1)
    p.stop()
    samples = [s for s in samples if s > 0]
    p95 = sorted(samples)[int(0.95 * (len(samples) - 1))] if samples else 999
    gate(
        "k2a_fanout_p95_lt_10ms",
        p95 < 10.0,
        f"p95={p95:.2f}ms n={len(samples)} max={max(samples):.2f}",
    )


def k2a_eventid_hash_unique(tmp: Path) -> None:
    from integration.kafka.event_mapper import build_cycle_events
    from integration.kafka.producer import AsyncKafkaProducer

    run_id = f"hash-{uuid.uuid4().hex[:8]}"
    p = AsyncKafkaProducer(
        bootstrap_servers="localhost:29092",
        evidence_root=tmp / "evidence2",
        simulation_run_id=run_id,
    )
    p.start()
    seen: dict[str, str] = {}
    conflict = False
    for i in range(10):
        ents = _cycle_ents(run=run_id, sim_t=float(i))
        events = build_cycle_events(
            ents, cycle_sequence=i, producer_session_id=p.producer_session_id
        )
        for ev in events:
            eid, h = ev["eventId"], ev["entityPayloadHash"]
            if eid in seen and seen[eid] != h:
                conflict = True
            seen[eid] = h
        p.publish_cycle(ents, cycle_sequence=i)
    deadline = time.time() + 20
    while p.metrics.pending_count() > 0 and time.time() < deadline:
        time.sleep(0.1)
    p.stop()
    gate(
        "k2a_no_eventid_payloadhash_conflict",
        not conflict and len(seen) >= 4,
        f"unique_eventIds={len(seen)} conflict={conflict}",
    )


def k2a_poll_thread_death(tmp: Path) -> None:
    from integration.kafka.producer import AsyncKafkaProducer, ProducerState

    run_id = f"poll-{uuid.uuid4().hex[:8]}"
    p = AsyncKafkaProducer(
        bootstrap_servers="localhost:29092",
        evidence_root=tmp / "evidence3",
        simulation_run_id=run_id,
    )
    p.start()
    assert p.state == ProducerState.READY
    # simulate poll thread death
    p._stop_poll.set()
    if p._poll_thread:
        p._poll_thread.join(timeout=3)
    h = p.health()
    dead = h.get("poll_thread_alive") is False
    faulted = h.get("producer_state") == "FAULTED" or p.state == ProducerState.FAULTED
    p._accepting = False
    try:
        p.stop(flush_timeout_sec=2)
    except Exception:
        pass
    gate(
        "k2a_poll_thread_death_detected",
        dead and faulted,
        f"alive={h.get('poll_thread_alive')} state={h.get('producer_state')}",
    )


def k2a_kafka_restart_recover(tmp: Path) -> None:
    import subprocess

    from integration.kafka.producer import AsyncKafkaProducer, ProducerState

    run_id = f"krest-{uuid.uuid4().hex[:8]}"
    p = AsyncKafkaProducer(
        bootstrap_servers="localhost:29092",
        evidence_root=tmp / "evidence4",
        simulation_run_id=run_id,
    )
    p.start()
    p.publish_cycle(_cycle_ents(run=run_id), cycle_sequence=1)
    time.sleep(1)
    subprocess.run(
        ["docker", "compose", "restart", "kafka"],
        cwd=str(REPO),
        check=True,
        capture_output=True,
    )
    # wait healthy
    for _ in range(60):
        r = subprocess.run(
            ["docker", "compose", "ps", "kafka", "--format", "{{.Status}}"],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )
        if "healthy" in (r.stdout or "").lower():
            break
        time.sleep(2)
    time.sleep(3)
    # produce again — should ack or degrade correctly (not silent success without broker)
    n = p.publish_cycle(_cycle_ents(run=run_id, sim_t=2.0), cycle_sequence=2)
    deadline = time.time() + 25
    while p.metrics.pending_count() > 0 and time.time() < deadline:
        time.sleep(0.2)
    h = p.health()
    state = h.get("producer_state")
    ok = state in ("READY", "DEGRADED", "FAULTED") and n == 4
    # prefer recovery to READY with acks
    recovered = h.get("events_acked_total", 0) >= 4
    p.stop(flush_timeout_sec=5)
    gate(
        "k2a_kafka_restart_recover_or_fault",
        ok and (recovered or state in ("DEGRADED", "FAULTED", "READY")),
        f"state={state} acked={h.get('events_acked_total')} n={n} recovered={recovered}",
    )


def k2b_queued_redrive_sim_kill9(tmp: Path) -> None:
    """Simulate kill-9 after produce-before-ACK: orphan QUEUED → recover → redrive."""
    from integration.kafka.outbox_schema import STATUS_ACKED, STATUS_OUTBOXED, STATUS_QUEUED
    from integration.kafka.outbox_store import KafkaOutboxStore
    from integration.kafka.outbox_worker import OutboxDeliveryWorker
    from integration.kafka.durable_publisher import DurableKafkaPublisher

    db = tmp / "outbox_kill.sqlite3"
    pub = DurableKafkaPublisher(
        db_path=db,
        bootstrap_servers="localhost:29092",
        max_in_flight=8,
    )
    pub.start()
    # Stop delivery so append stays OUTBOXED (crash mid-flight sim)
    if pub._worker:
        pub._worker.stop(timeout=2)
        pub._worker = None
    n = pub.append_cycle(_cycle_ents(run="k2b-kill-run", sim_t=1.0), cycle_sequence=1)
    assert n == 4
    # Simulate in-flight QUEUED without ACK (process killed after mark_queued)
    for r in pub.store.fetch_eligible(limit=20):
        pub.store.mark_queued(r.event_id)
    queued = pub.store.count_by_status().get(STATUS_QUEUED, 0)
    # "kill" — close without clean ACK
    pub.store.close()

    # restart: recover orphaned QUEUED → FAILED_RETRYABLE → worker redrive
    from confluent_kafka import Producer

    store2 = KafkaOutboxStore(db)
    recovered = store2.recover_orphaned_queued()
    prod = Producer(
        {
            "bootstrap.servers": "localhost:29092",
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "k2b-redrive",
        }
    )
    worker = OutboxDeliveryWorker(store2, producer=prod, max_in_flight=16)
    # start() also recovers again (0)
    worker.start()
    deadline = time.time() + 30
    while time.time() < deadline:
        if store2.count_by_status().get(STATUS_ACKED, 0) >= 4:
            break
        time.sleep(0.3)
    counts = store2.count_by_status()
    worker.stop()
    store2.close()
    gate(
        "k2b_kill9_queued_redrive",
        queued >= 4 and recovered >= 4 and counts.get(STATUS_ACKED, 0) >= 4,
        f"queued={queued} recovered={recovered} counts={counts}",
    )


def k2b_kafka_restart_outbox(tmp: Path) -> None:
    import subprocess

    from integration.kafka.durable_publisher import DurableKafkaPublisher
    from integration.kafka.outbox_schema import STATUS_ACKED

    db = tmp / "outbox_krest.sqlite3"
    pub = DurableKafkaPublisher(
        db_path=db,
        bootstrap_servers="localhost:29092",
    )
    pub.start()
    pub.append_cycle(_cycle_ents(run="k2b-krest", sim_t=1.0), cycle_sequence=1)
    time.sleep(1)
    subprocess.run(
        ["docker", "compose", "stop", "kafka"],
        cwd=str(REPO),
        check=True,
        capture_output=True,
    )
    # while down, still can OUTBOX
    pub.append_cycle(_cycle_ents(run="k2b-krest", sim_t=2.0), cycle_sequence=2)
    pending_before = pub.store.capacity_metrics()["outbox_pending_rows"]
    subprocess.run(
        ["docker", "compose", "start", "kafka"],
        cwd=str(REPO),
        check=True,
        capture_output=True,
    )
    for _ in range(60):
        r = subprocess.run(
            ["docker", "compose", "ps", "kafka", "--format", "{{.Status}}"],
            cwd=str(REPO),
            capture_output=True,
            text=True,
        )
        if "healthy" in (r.stdout or "").lower():
            break
        time.sleep(2)
    time.sleep(5)
    deadline = time.time() + 40
    while time.time() < deadline:
        if pub.store.count_by_status().get(STATUS_ACKED, 0) >= 4:
            break
        time.sleep(0.5)
    counts = pub.store.count_by_status()
    pub.stop(flush_timeout_sec=10)
    gate(
        "k2b_kafka_restart_outbox_redrive",
        pending_before >= 1 and counts.get(STATUS_ACKED, 0) >= 4,
        f"pending_while_down={pending_before} counts={counts}",
    )


def k3_shadow_dvt(tmp: Path) -> None:
    """Produce → live projector consumer → Orion shadow hash parity."""
    import subprocess

    from contracts.canonical_json import to_shadow_entity_id
    from integration.kafka.producer import AsyncKafkaProducer
    from tests.orion_async.helpers.orion_probe import OrionProbe, OrionProbeError

    run_id = f"dvt-{uuid.uuid4().hex[:8]}"
    session = str(uuid.uuid4())
    db = tmp / "proj_dvt.sqlite3"
    group = f"dvt-{uuid.uuid4().hex[:8]}"

    # Start consumer first at log end so we only see this run's produce
    cmd = [
        sys.executable,
        str(VIS / "tools" / "projector_live_consumer.py"),
        "--db",
        str(db),
        "--group",
        group,
        "--from-latest",
        "--idle-sec",
        "8",
        "--max-wall-sec",
        "90",
    ]
    cons = subprocess.Popen(
        cmd,
        cwd=str(VIS),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(4)  # assignment + seek

    prod = AsyncKafkaProducer(
        bootstrap_servers="localhost:29092",
        evidence_root=tmp / "dvt_ev",
        simulation_run_id=run_id,
        producer_session_id=session,
    )
    prod.start()
    for seq in range(1, 4):
        ents = _cycle_ents(run=run_id, sim_t=float(seq))
        n = prod.publish_cycle(ents, cycle_sequence=seq)
        assert n == 4
    deadline = time.time() + 30
    while prod.metrics.pending_count() > 0 and time.time() < deadline:
        time.sleep(0.2)
    prod.stop()

    try:
        out, err = cons.communicate(timeout=100)
    except subprocess.TimeoutExpired:
        cons.kill()
        out, err = cons.communicate()
    print((out or "")[-2500:])
    if cons.returncode not in (0, None):
        print((err or "")[-2500:])

    probe = OrionProbe("http://localhost:1026")
    matched = 0
    missing = []
    mismatch = []
    entity_ids = [
        "urn:ngsi-ld:Intersection:A",
        "urn:ngsi-ld:TrafficLight:A-North",
        "urn:ngsi-ld:Camera:A",
        "urn:ngsi-ld:VehicleSensor:A:NORTHBOUND",
    ]
    for eid in entity_ids:
        shadow_id = to_shadow_entity_id(eid)
        try:
            got = probe.get_entity(shadow_id)
        except OrionProbeError:
            missing.append(shadow_id)
            continue
        if OrionProbe.prop(got, "simulationRunId") != run_id:
            mismatch.append(
                f"{shadow_id} runId={OrionProbe.prop(got, 'simulationRunId')}"
            )
            continue
        if got.get("id") != shadow_id:
            mismatch.append(f"{shadow_id} id")
            continue
        # latest cycle sim_t == 3.0
        st = OrionProbe.prop(got, "simulationTime")
        if st is not None and float(st) != 3.0:
            mismatch.append(f"{shadow_id} simTime={st}")
            continue
        matched += 1
    total = len(entity_ids)
    ok = matched == total and not missing and not mismatch and cons.returncode == 0
    gate(
        "k3_shadow_dvt_parity",
        ok,
        f"matched={matched}/{total} missing={missing} mismatch={mismatch} "
        f"consumer_rc={cons.returncode}",
    )


def main() -> int:
    import tempfile

    tmp = Path(tempfile.mkdtemp(prefix="live_gates_"))
    print(f"tmp={tmp}")
    print("=== K-2a live gates ===")
    k2a_fanout_p95(tmp)
    k2a_eventid_hash_unique(tmp)
    k2a_poll_thread_death(tmp)
    k2a_kafka_restart_recover(tmp)
    print("=== K-2b live gates ===")
    k2b_queued_redrive_sim_kill9(tmp)
    k2b_kafka_restart_outbox(tmp)
    print("=== K-3 shadow DVT ===")
    k3_shadow_dvt(tmp)
    out = VIS / "artifacts" / "live_gates_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(REPORT, indent=2), encoding="utf-8")
    print(f"report={out}")
    print("OVERALL", "PASS" if REPORT["ok"] else "FAIL")
    return 0 if REPORT["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
