"""Actual Gold Core against real Bronze/Silver SQLite and disposable ClickHouse."""
from dataclasses import asdict
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from contracts.traffic_observation import build_observation
from de.gold_runtime.config import GoldCoreSettings
from de.gold_runtime.main import build_processor
from de.gold_runtime.replay import CoreReplayBoundaryChanged, CoreReplayManifest
from de.gold_runtime.silver_readers import discovery_boundary
from de.silver.incremental_contract import CanonicalFeedError, canonical_bytes
from de.tests.gold_runtime.test_core_foundation import drain, release
from de.tests.silver.test_incremental_reader import authority
from de.tests.silver.v1_fixtures import snapshot_v1

pytestmark = pytest.mark.skipif(os.environ.get("GOLD_CORE_STAGE_B") != "1",
                                reason="requires isolated ClickHouse 24.3")


def options(fixture, label):
    root = fixture.path / label
    return GoldCoreSettings(checkpoint_path=root / "checkpoint.sqlite3",
        instance_lock_path=root / "instance.lock", replay_root=root / "replay",
        min_free_bytes=0, max_entries=2, idle_poll_sec=0.02)


def publish(fixture, manifest, tick, cycle, scenario="NORMAL", conflict=False):
    context = fixture.archive.resolve(manifest["simulationRunId"], manifest["topologyDigest"])
    snap = snapshot_v1()
    sample = next(iter(snap["directions"].values()))
    directions = {name: dict(sample) for name in context.directions["A"]}
    if conflict:
        next(iter(directions.values()))["vehicle_count"] += 10
    snap.update(node_id="A", simulation_run_id=context.run_id, topology_digest=context.topology_digest,
                directions=directions, colors={key: "green" for key in directions},
                scenario=scenario, simulation_time_sec=tick)
    return fixture.publish(build_observation(snap, cycle_sequence=cycle, simulation_step=cycle * 100),
                           conflict=conflict)


def evidence(name, payload):
    directory = os.environ.get("GOLD_CORE_EVIDENCE")
    if directory:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        (path / (name + ".json")).write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


def independent_membership(fixture, coordinates):
    expected = set()
    for event_id, event in coordinates.items():
        identity = fixture.bronze.current("live", event_id)
        if identity["state"] != "ACCEPTED":
            continue
        run, tick = event
        for width in (60, 300):
            floor = int(tick) - int(tick) % width
            expected.add((event_id, run, width, floor, floor + width))
    return expected


def observed_membership(db):
    return {(eid, json.loads(identity)["simulation_run_id"], duration, start, end)
        for eid, identity, duration, start, end in db.execute("""SELECT m.event_id,r.identity,w.duration,w.start,w.end
          FROM core_membership m JOIN core_window w ON w.id=m.window_id JOIN core_run r ON r.id=w.run_key""")}


def test_stage_b_real_oracle_restart_late_conflict_and_replay(authority):
    fixture, first, second = authority
    retained = publish(fixture, first, 135.75, 1)
    superseded = publish(fixture, first, 135, 2)
    other = publish(fixture, second, 135.5, 1)
    progress = publish(fixture, first, 360, 3)
    publish(fixture, first, 135, 2, conflict=True)
    fixture.apply()
    coordinates = {retained: (first["simulationRunId"], 135.75), superseded: (first["simulationRunId"], 135),
                   other: (second["simulationRunId"], 135.5), progress: (first["simulationRunId"], 360)}

    settings = options(fixture, "gold-live")
    page = fixture.changes(max_entries=1)
    encoded = canonical_bytes(page)
    import rfc8785
    assert rfc8785.dumps(json.loads(encoded)) == encoded
    evidence("canonical_receipt_sample", dict(complete_sha256=sha256(encoded).hexdigest(),
        envelope=json.loads(encoded), encoded_bytes=len(encoded)))
    p, lock = build_processor(settings, silver=fixture.worker.settings)
    sample_start = time.monotonic()
    try:
        drain(p)
        db = p.store.conn
        assert p.store.cursors() == {"CHANGE": 5, "INVALIDATION": 1}
        changes = [tuple(row) for row in db.execute("""SELECT sequence,event_id,disposition
            FROM core_transition WHERE kind='CHANGE' ORDER BY sequence""")]
        independent_changes = [(row["change_sequence"], row["event_id"],
            "CONFLICTED" if row["change_type"] == "CONFLICTED" else
            "SUPERSEDED_BY_CONFLICT" if fixture.bronze.current("live", row["event_id"])["state"] == "CONFLICTED"
            else "BUNDLE_VALID") for row in fixture.bronze.db.execute("SELECT * FROM v1_changes ORDER BY change_sequence")]
        assert changes == independent_changes
        assert observed_membership(db) == independent_membership(fixture, coordinates)
        target = db.execute("""SELECT w.id FROM core_membership m JOIN core_window w ON w.id=m.window_id
            WHERE m.event_id=? AND w.duration=60""", (retained,)).fetchone()[0]
        original = db.execute("SELECT * FROM core_work WHERE window_id=? AND state='COMPLETE'", (target,)).fetchone()
        materialization = p.materialize(original)
        inputs = tuple(materialization)
        assert [bundle.event_id for bundle in inputs] == [retained]
        # Compare canonical facts against independent frozen Silver obligation rows.
        facts = [json.loads(row[0]) for row in fixture.silver.conn.execute("""SELECT expected_json
            FROM silver_v1_obligation WHERE source_kind='changes' AND source_sequence=1
            AND target_table LIKE '%fact_v1'""")]
        actual = (inputs[0].intersection, *inputs[0].traffic, *inputs[0].signal)
        assert sorted(map(canonical_bytes, facts)) == sorted(map(canonical_bytes, actual))
        assert original["input_hash"] == materialization.input_hash
    finally:
        release(p, lock)

    p, lock = build_processor(settings, silver=fixture.worker.settings)
    try:
        assert p.run_cycle()
        materialization = p.materialize(original)
        assert [bundle.event_id for bundle in materialization] == [retained]
        assert materialization.input_hash == original["input_hash"]
        late = publish(fixture, first, 135.125, 4, scenario="PEAK")
        fixture.apply()
        coordinates[late] = (first["simulationRunId"], 135.125)
        drain(p)
        db = p.store.conn
        assert observed_membership(db) == independent_membership(fixture, coordinates)
        work = db.execute("""SELECT * FROM core_work WHERE window_id=? AND state='COMPLETE'
                            ORDER BY generation DESC LIMIT 1""", (target,)).fetchone()
        assert work["generation"] > original["generation"] and work["input_count"] == 2
        assert work["input_hash"] != original["input_hash"]
        frozen = p.discover()
        live_binding = frozen.source_binding_id
        source_boundary = discovery_boundary(frozen)
        run = fixture.archive.resolve(first["simulationRunId"], first["topologyDigest"])
        manifest = CoreReplayManifest(live_binding, source_boundary, run.run_id, run.generation,
                                      run.topology_digest, run.manifest_identity, 0, 400)
        replay_file = fixture.path / "fixture-replay.json"
        replay_file.write_text(json.dumps(asdict(manifest)), encoding="utf-8")
        assert not p.ledger.finish(original, "stale", 17)
        p._publish_core()
        evidence("real_oracle_restart", dict(cursors=p.store.cursors(), changes=changes,
            membership=sorted(observed_membership(db)), input_hash_before=original["input_hash"],
            input_hash_after=work["input_hash"], measured_sec=time.monotonic()-sample_start,
            resources=p.health_snapshot()["metrics"]))
    finally:
        release(p, lock)

    live_before = sha256(settings.checkpoint_path.read_bytes()).hexdigest()
    replay_settings = settings.model_copy(update=dict(replay_manifest=replay_file))
    replay, replay_lock = build_processor(replay_settings, silver=fixture.worker.settings)
    try:
        drain(replay)
        assert replay.namespace == manifest.namespace and replay.store.db_path != settings.checkpoint_path
        assert replay.store.conn.execute("SELECT status FROM core_replay").fetchone()[0] == "COMPLETE"
        assert {row[0] for row in replay.store.conn.execute("SELECT id FROM core_event")} == {retained, late, progress}
    finally:
        release(replay, replay_lock)
    assert sha256(settings.checkpoint_path.read_bytes()).hexdigest() == live_before

    publish(fixture, first, 450, 5)
    fixture.apply()
    replay, replay_lock = build_processor(replay_settings, silver=fixture.worker.settings)
    try:
        with pytest.raises(CoreReplayBoundaryChanged):
            replay.run_cycle()
        assert replay.store.conn.execute("SELECT status FROM core_replay").fetchone()[0] == "COMPLETE"
    finally:
        release(replay, replay_lock)


def child_environment(settings, fixture):
    silver = fixture.worker.settings
    return dict(os.environ, SILVER_CLICKHOUSE_HOST=silver.clickhouse_host,
        SILVER_CLICKHOUSE_PORT=str(silver.clickhouse_port),
        SILVER_CLICKHOUSE_DATABASE=silver.clickhouse_database,
        SILVER_V1_BRONZE_PATH=silver.v1_bronze_path,
        SILVER_CHECKPOINT_PATH=silver.checkpoint_path, SILVER_V1_ARCHIVE_ROOT=silver.v1_archive_root,
        GOLD_CHECKPOINT_PATH=str(settings.checkpoint_path), GOLD_INSTANCE_LOCK_PATH=str(settings.instance_lock_path),
        GOLD_MIN_FREE_BYTES="0", GOLD_IDLE_POLL_SEC="0.02")


def test_stage_b_real_ack_crash_barriers(authority):
    fixture, run, _ = authority
    event = publish(fixture, run, 135, 1)
    fixture.apply()
    (publish(fixture, run, 135, 1, conflict=True))
    fixture.apply()
    snapshots = []
    for kind in ("INVALIDATION", "CHANGE"):
        for moment, exit_code in (("before_commit", 73), ("after_commit", 74)):
            settings = options(fixture, f"gold-{kind}-{moment}")
            code = """import os
from de.gold_runtime.config import GoldCoreSettings
from de.gold_runtime.main import build_processor
p, lock = build_processor(GoldCoreSettings())
p.barrier = lambda kind, moment: os._exit(int(os.environ['PROBE_EXIT'])) if (
    (kind, moment) == (os.environ['PROBE_KIND'], os.environ['PROBE_MOMENT'])) else None
p.run()
"""
            env = child_environment(settings, fixture)
            env.update(PROBE_KIND=kind, PROBE_MOMENT=moment, PROBE_EXIT=str(exit_code))
            child = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, timeout=45)
            assert child.returncode == exit_code, child.stderr.decode(errors="replace")
            with sqlite3.connect(settings.checkpoint_path) as db:
                before = dict(db.execute("SELECT kind,sequence FROM core_cursor"))
                suppressions = db.execute("SELECT count(*) FROM core_suppression").fetchone()[0]
                assert before == {"CHANGE": 2 if kind == "CHANGE" and exit_code == 74 else 0,
                    "INVALIDATION": 0 if kind == "INVALIDATION" and exit_code == 73 else 1}
                assert suppressions == before["INVALIDATION"]
            p, lock = build_processor(settings, silver=fixture.worker.settings)
            try:
                drain(p)
                assert p.store.cursors() == {"CHANGE": 2, "INVALIDATION": 1}
                assert p.store.conn.execute("SELECT event_id FROM core_suppression").fetchone()[0] == event
                assert p.store.conn.execute("SELECT count(*) FROM core_membership").fetchone()[0] == 0
                snapshots.append(dict(kind=kind, barrier=moment, exit=exit_code, durable=before,
                                      recovered=p.store.cursors()))
            finally:
                release(p, lock)
    evidence("real_crash_barriers", snapshots)



def test_stage_b_entrypoint_readonly_and_legacy_preservation(authority):
    from uuid import uuid4
    import socket
    import clickhouse_connect
    from clickhouse_connect.driver.exceptions import DatabaseError
    from de.kafka_raw.clickhouse_repository import _split

    fixture, run, _ = authority
    publish(fixture, run, 59.999, 1)
    publish(fixture, run, 300, 2)
    fixture.apply()
    server = fixture.bronze_repo.client
    database = fixture.worker.settings.clickhouse_database
    legacy_sql = (Path(__file__).resolve().parents[2] / "migrations/005_create_gold_m1.sql").read_text(encoding="utf-8")
    for statement in _split(legacy_sql.replace("smart_traffic", database)):
        server.command(statement)
    user = "stage_b_reader_" + uuid4().hex[:12]
    server.command(f"CREATE USER {user} IDENTIFIED WITH no_password")
    try:
        server.command(f"GRANT SELECT ON {database}.* TO {user}")
        client = clickhouse_connect.get_client(host=fixture.worker.settings.clickhouse_host, port=fixture.worker.settings.clickhouse_port,
                                               username=user, database=database)
        try:
            client.query(f"SELECT count() FROM {database}.gold_dim_metric_definition")
            with pytest.raises(DatabaseError, match="ACCESS_DENIED|Not enough privileges"):
                client.command(f"INSERT INTO {database}.gold_dim_metric_definition "
                               f"SELECT * FROM {database}.gold_dim_metric_definition WHERE 0")
        finally:
            client.close()

        def sentinel():
            results = {}
            tables = server.query("SELECT name FROM system.tables WHERE database={database:String} "
                                  "AND startsWith(name,'gold_') AND engine!='View'",
                                  parameters={"database": database}).result_rows
            for (table,) in tables:
                results[table] = tuple(server.query(f"SELECT count(), sum(cityHash64(tuple(*))) "
                                                    f"FROM {database}.{table}").result_rows[0])
            return results

        before = sentinel()
        settings = options(fixture, "entrypoint")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env = child_environment(settings, fixture)
        env.update(SILVER_CLICKHOUSE_USER=user, GOLD_HEALTH_PORT=str(port), GOLD_HEALTH_HOST="127.0.0.1")
        child = subprocess.Popen([sys.executable, "-m", "de.gold_runtime.main"],
                                 env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        ready = None
        try:
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline and child.poll() is None:
                try:
                    with urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as handle:
                        sample = json.load(handle)
                    if (sample.get("cursors", {}).get("CHANGE") == 2 and
                            sample["metrics"].get("work_completed") == 2):
                        with urlopen(f"http://127.0.0.1:{port}/ready", timeout=1) as handle:
                            ready = json.load(handle)
                        break
                except (OSError, HTTPError):
                    pass
                time.sleep(0.1)
            assert ready and ready["ready"] and not ready["analytics_ready"]
        finally:
            # Windows cannot deliver POSIX SIGTERM with Popen. Test controlled
            # shutdown separately using the actual processor's request API.
            if child.poll() is None:
                child.terminate()
            _, err = child.communicate(timeout=12)
        assert sentinel() == before
        evidence("entrypoint_isolation", dict(ready=ready, health=sample,
                                              v2_inventory_preserved=True,
                                              legacy_table_count=len(before)))
    finally:
        server.command(f"DROP USER {user}")


def test_stage_b_real_retention_gap_and_dirty_race(authority):
    from de.gold_runtime.processor import CoreWorkChanged

    fixture, run, _ = authority
    event = publish(fixture, run, 135, 1)
    publish(fixture, run, 360, 2)
    fixture.apply()
    p, lock = build_processor(options(fixture, "dirty-race"), silver=fixture.worker.settings)
    try:
        assert p.run_cycle()
        assert p._changes()
        assert p.ledger.promote(10) > 0
        work = p.ledger.claim()
        stream = p.materialize(work)
        assert next(stream).event_id == event
        publish(fixture, run, 135, 1, conflict=True)
        fixture.apply()
        p.discover()
        assert p._invalidations()
        with pytest.raises(CoreWorkChanged):
            next(stream)
        assert not p.ledger.finish(work, "obsolete", 1)
        drain(p)
        assert p.store.cursors() == {"CHANGE": 3, "INVALIDATION": 1}
        assert p.store.conn.execute("SELECT count(*) FROM core_membership WHERE event_id=?",
                                    (event,)).fetchone()[0] == 0
        later = publish(fixture, run, 450, 3)
        fixture.apply()
        # Corruption is confined to the disposable authority.
        fixture.bronze.db.execute("DELETE FROM v1_changes WHERE event_id=?", (later,))
        with pytest.raises(CanonicalFeedError) as rejected:
            p.run_cycle()
        assert rejected.value.code == "RETENTION_GAP"
        assert p.store.cursors() == {"CHANGE": 3, "INVALIDATION": 1}
    finally:
        release(p, lock)


def test_stage_b_actual_invalidation_progress_when_prefix_blocked(authority):
    fixture, run, _ = authority
    event = publish(fixture, run, 135, 1)
    fixture.apply()
    publish(fixture, run, 135, 1, conflict=True)
    # Silver budgets invalidations first. With a one-entry pass, the conflict
    # tombstone is applied while the conflict's change sequence still lags.
    fixture.apply(limit=1)
    assert fixture.silver.v1_cursors("live")["change_cursor"] == 1
    p, lock = build_processor(options(fixture, "blocked-prefix"), silver=fixture.worker.settings)
    try:
        assert p.run_cycle()
        assert p.run_cycle()
        assert p.store.cursors() == {"CHANGE": 0, "INVALIDATION": 1}
        assert p._faults["change"]["code"] == "PREFIX_BLOCKED"
        assert p.store.conn.execute("SELECT event_id FROM core_suppression").fetchone()[0] == event
        fixture.apply()
        time.sleep(p.settings.retry_initial_sec + 0.02)
        drain(p)
        assert p.store.cursors() == {"CHANGE": 2, "INVALIDATION": 1}
        assert p.store.conn.execute("SELECT count(*) FROM core_membership").fetchone()[0] == 0
        evidence("real_independent_invalidation", dict(block="PREFIX_BLOCKED", blocked_change_cursor=0,
            independent_invalidation_cursor=1, recovered=p.store.cursors()))
    finally:
        release(p, lock)



@pytest.mark.parametrize("invalidation_first", (False, True))
def test_stage_b_actual_conflict_after_valid_receipt(authority, invalidation_first):
    fixture, run, _ = authority
    event = publish(fixture, run, 135, 1)
    fixture.apply()
    p, lock = build_processor(options(fixture, "receipt-race"), silver=fixture.worker.settings)
    try:
        assert p.run_cycle()
        page = p.source.changes(p.binding_id, 0)
        assert page.entries[0].effective_disposition == "BUNDLE_VALID"
        assert page.receipt.safe_invalidation_through_sequence == 0
        p.pending_change = page
        p.pending_change_started = time.monotonic()
        publish(fixture, run, 135, 1, conflict=True)
        fixture.apply()
        p.discover()
        if invalidation_first:
            assert p._invalidations()
        assert p._changes()
        db = p.store.conn
        assert db.execute("SELECT count(*) FROM core_membership").fetchone()[0] == (
            0 if invalidation_first else 2)
        drain(p)
        assert p.store.cursors() == {"CHANGE": 2, "INVALIDATION": 1}
        assert db.execute("SELECT count(*) FROM core_membership").fetchone()[0] == 0
        assert db.execute("SELECT disposition FROM core_transition WHERE kind='CHANGE' AND sequence=1").fetchone()[0] == "BUNDLE_VALID"
        evidence(f"real_receipt_race_{int(invalidation_first)}", dict(
            invalidation_first=invalidation_first, original_receipt=page.receipt.receipt_id,
            event_id=event, final=p.store.cursors(), active_memberships=0))
    finally:
        release(p, lock)

