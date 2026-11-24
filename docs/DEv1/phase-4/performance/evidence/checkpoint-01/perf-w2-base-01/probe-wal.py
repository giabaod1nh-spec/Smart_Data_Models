"""Keep WAL sidecars available on the stopped disposable stores during the read probe."""
from contextlib import ExitStack, closing
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time

import de.gold_runtime.silver_readers as gold_readers
from de.silver.canonical_reader import CanonicalReadSession
from de.silver.config import SilverSettings
from de.silver.incremental_contract import canonical_hash
from de.silver.metrics import Metrics
from de.tools.gold_core_capacity import probe_retained_reads


def replay_retained(settings):
    from de.gold_runtime.config import GoldCoreSettings
    from de.gold_runtime.main import build_processor
    from de.gold_runtime.replay import CoreReplayManifest
    from de.tools.gold_core_capacity import verify_pipeline

    original = GoldCoreSettings().checkpoint_path
    root = Path("/app/perf-replay")
    with CanonicalReadSession(settings) as session:
        gold = gold_readers.CanonicalGoldSource(session)
        discovery = gold.discover(None)
        entry = gold.changes(discovery.source_binding_id, 0, entries=1).entries[0]
    manifest = CoreReplayManifest(
        discovery.source_binding_id, gold_readers.discovery_boundary(discovery), entry.run_id,
        entry.run_generation, entry.topology_digest, entry.manifest_identity, 0, 90,
    )
    manifest_path = root / "manifest.json"
    with manifest_path.open("x", encoding="utf-8") as output:
        json.dump(asdict(manifest), output)
    core = GoldCoreSettings(replay_root=root, replay_manifest=manifest_path)
    processor, lock = build_processor(core, silver=settings)
    state = processor.store.db_path
    started = time.monotonic()
    timer = threading.Timer(150, processor.request_shutdown)
    timer.start()
    try:
        code = processor.run()
        runtime_seconds = time.monotonic() - started
        status = processor.store.conn.execute("SELECT status FROM core_replay").fetchone()[0]
        metrics = processor.health_snapshot()["metrics"]
        oracle = verify_pipeline(Path("/app/Visualize/artifacts/kafka_outbox/v1-outbox.sqlite3"),
                                 settings.v1_bronze_path, state)
        sql = """SELECT work.input_hash FROM core_work work JOIN core_window w ON w.id=work.window_id
                 WHERE w.duration=60 AND w.start=0 AND w.generation=work.generation AND work.state='COMPLETE'"""
        result = processor.store.conn.execute(sql).fetchone()
        with closing(sqlite3.connect(original.as_uri() + "?mode=rw", uri=True)) as live:
            live.execute("PRAGMA query_only=ON")
            reference = live.execute(sql).fetchone()
        identical = bool(result and reference and result[0] == reference[0])
        evidence = dict(status=status, code=code, runtime_seconds=runtime_seconds, oracle=oracle,
                        input_hash_matches_retained_original=identical, metrics=metrics)
        print(json.dumps(evidence, sort_keys=True), flush=True)
        if code or status != "COMPLETE" or not identical or not all(oracle[key] for key in (
                "receipt_coverage", "exact_sequence_coverage", "membership_match",
                "full_drain", "eligible_work_drained")):
            raise SystemExit(2)
    finally:
        timer.cancel()
        lock.release()
        processor.store.close()


def profile_gold(settings):
    metrics = Metrics()

    def measure(obj, method, label):
        call = getattr(obj, method)
        setattr(obj, method, lambda *args, **kwargs: metrics.timed(label, call, *args, **kwargs))

    with CanonicalReadSession(settings) as session:
        gold = gold_readers.CanonicalGoldSource(session)
        boundary = gold.discover(None)
        measure(gold, "changes", "gold_change_page")
        measure(gold_readers, "verify_core_page", "gold_receipt_verification")
        for method in ("_capture_feed", "_run_identity", "_verified_bundle", "_check_feed_end",
                       "_validity_obligation", "_obligations", "_read_change_entry"):
            measure(session, method, method.removeprefix("_"))
        measure(session.archive_resolver, "resolve", "archive")
        measure(session.bronze_repo, "read_valid_observation", "bronze_valid_observation")
        measure(session.silver_repo, "fetch_v1_event_rows", "silver_fact_query")
        measure(session.silver_repo, "reconcile_v1_row", "reconcile_single")
        measure(session.silver_repo, "reconcile_v1_directions", "reconcile_group")
        if "--single" in sys.argv:
            session.silver_repo.reconcile_v1_directions = None
        count = 0
        page_hashes = []
        started = time.monotonic()
        for sequence in range(1, min(boundary.verified_change_prefix, 32) + 1):
            page = gold.changes(boundary.source_binding_id, sequence - 1, entries=1)
            if page.entry_count != 1 or page.entries[0].effective_disposition != "BUNDLE_VALID":
                raise RuntimeError("unexpected retained canonical disposition")
            count += 1
            page_hashes.append(canonical_hash(page))
            if time.monotonic() - started > 30:
                break
        return dict(count=count, wall_seconds=time.monotonic() - started, page_hashes=page_hashes,
                    operations=metrics.snapshot()["journal_operations"])


settings = SilverSettings()
with ExitStack() as stack:
    for path in (settings.v1_bronze_path, settings.checkpoint_path):
        connection = stack.enter_context(closing(
            sqlite3.connect(Path(path).resolve().as_uri() + "?mode=rw", uri=True)))
        connection.execute("PRAGMA query_only=ON")
        connection.execute("SELECT name FROM sqlite_schema LIMIT 1").fetchone()
    if "--replay-proof" in sys.argv:
        replay_retained(settings)
    else:
        result = profile_gold(settings) if "--gold-path" in sys.argv else probe_retained_reads()
        print(json.dumps(result, sort_keys=True))
