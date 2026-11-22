"""Silver Plan 3 — isolated replay CLI."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import (
    DestinationMode,
    SilverConfigError,
    SilverSettings,
    replay_namespace,
    validate_namespace_id,
)
from de.silver.instance_lock import InstanceLock
from de.silver.processor import SilverProcessor
from de.silver.source_binding import SourceBoundaryChangedError, SourceVerificationError, verify_bronze_source
from de.silver.archive_resolver import ArchiveResolver, ArchiveIntegrityError
from de.silver.lifecycle_refresher import LifecycleRefresher
from de.silver.repositories import SilverClickHouseRepository
from de.bronze.checkpoint_store import V1SemanticReader
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.bronze import MIGRATION_VERSION as BRONZE_VERSION
from datetime import datetime, timezone


REQUIRED_MANIFEST_VERSION = "silver-replay-v1"


def canonical_manifest_hash(doc: dict) -> str:
    payload = json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def logical_replay_hash(store: SilverCheckpointStore, namespace: str) -> str:
    """Hash canonical logical rows without processing timestamps or replay ID."""
    rows = []
    for table, encoded in store.conn.execute(
            "SELECT target_table,expected_json FROM silver_v1_obligation WHERE namespace=? "
            "AND target_table!='__LOCAL_SKIP__' AND status='RECONCILED'",
            (namespace,)):
        value = json.loads(encoded)
        for key in ("created_at", "processed_at", "observed_at", "replay_run_id"):
            value.pop(key, None)
        rows.append((table, value))
    encoded = json.dumps(sorted(rows, key=lambda item: (item[0],
        json.dumps(item[1], sort_keys=True))), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def validate_manifest(doc: dict, run_id: str) -> str:
    validate_namespace_id(run_id)
    if not isinstance(doc, dict):
        raise SilverConfigError("replay manifest must be an object")
    if doc.get("manifest_version") != REQUIRED_MANIFEST_VERSION:
        raise SilverConfigError("invalid manifest_version")
    if doc.get("replay_run_id") != run_id:
        raise SilverConfigError("replay_run_id mismatch")
    required = ("source_namespace", "source_binding", "change_high_water",
                "invalidation_high_water", "simulation_run_id", "run_generation",
                "topology_digest", "manifest_identity", "manifest_file_hash",
                "catalog_file_hash", "reader_version", "processor_version", "schema_version")
    if any(key not in doc for key in required):
        raise SilverConfigError("V1 replay manifest lacks frozen source/archive identity")
    if doc["source_namespace"] != "live" or not isinstance(doc["source_binding"], dict):
        raise SilverConfigError("V1 replay requires live Bronze source binding")
    if any(type(doc[key]) is not int or doc[key] < 0 for key in
           ("change_high_water", "invalidation_high_water")):
        raise SilverConfigError("invalid frozen V1 journal high-water")
    if type(doc['run_generation']) is not int or doc['run_generation'] < 1:
        raise SilverConfigError('invalid run generation')
    run = doc['simulation_run_id']
    if not isinstance(run, str) or not run or run in {'.', '..'} or any(c in run for c in '/\\\0'):
        raise SilverConfigError('invalid archived run id')
    for key in ('manifest_identity', 'manifest_file_hash', 'catalog_file_hash'):
        if not isinstance(doc[key], str) or not re.fullmatch('[0-9a-f]{64}', doc[key]):
            raise SilverConfigError('invalid ' + key)
    if not isinstance(doc['topology_digest'], str) or not re.fullmatch('v1:sha256:[0-9a-f]{64}', doc['topology_digest']):
        raise SilverConfigError('invalid topology digest')
    for key, version in (('reader_version', '1'), ('processor_version', '1'), ('schema_version', '008')):
        if doc[key] != version:
            raise SilverConfigError('unsupported ' + key)
    binding = doc['source_binding']
    if (binding.get('namespace') != 'live' or binding.get('bronze_version') != BRONZE_VERSION or
            not isinstance(binding.get('source_path'), str) or not binding['source_path'] or
            not isinstance(binding.get('schema_fingerprint'), str) or
            not re.fullmatch('[0-9a-f]{64}', binding['schema_fingerprint'])):
        raise SilverConfigError('invalid frozen source binding')
    identity = binding.get('file_identity')
    if not isinstance(identity, list) or len(identity) != 2 or any(type(v) is not int or v < 0 for v in identity):
        raise SilverConfigError('invalid source file identity')
    return canonical_manifest_hash(doc)


def verify_v1_replay_boundary(doc: dict, source_path: Path) -> dict:
    try:
        current = verify_bronze_source(source_path, doc["source_namespace"])
    except SourceVerificationError as exc:
        if exc.code == 'AUTHORITY_UNAVAILABLE':
            raise
        raise SourceBoundaryChangedError() from exc
    stable_keys = ("namespace", "source_path", "schema_fingerprint", "bronze_version", "file_identity")
    if (any(current.get(key) != doc["source_binding"].get(key) for key in stable_keys) or
            current["change_high_water"] != doc["change_high_water"] or
            current["invalidation_high_water"] != doc["invalidation_high_water"]):
        raise SourceBoundaryChangedError()
    return current


def build_replay_settings(base: SilverSettings, run_id: str) -> SilverSettings:
    return SilverSettings(
        clickhouse_host=base.clickhouse_host,
        clickhouse_port=base.clickhouse_port,
        clickhouse_user=base.clickhouse_user,
        clickhouse_password=base.clickhouse_password,
        clickhouse_database=base.clickhouse_database,
        checkpoint_path=str(Path(base.checkpoint_path).parent / "replay" / run_id / "checkpoint.sqlite3"),
        v1_bronze_path=base.v1_bronze_path,
        v1_source_namespace=base.v1_source_namespace,
        v1_archive_root=base.v1_archive_root,
        v1_replay_run_filter="",
        namespace=replay_namespace(run_id),
        destination_mode=DestinationMode.REPLAY.value,
        replay_run_id=run_id,
        topic_allowlist=base.topic_allowlist,
        batch_size=base.batch_size,
        poll_interval_sec=base.poll_interval_sec,
        health_port=base.health_port,
    )


def _abort_interrupted_replay(settings: SilverSettings, run_id: str, manifest_hash: str) -> None:
    """Persist preflight boundary failure only for this existing RUNNING manifest."""
    path = Path(settings.checkpoint_path)
    if not path.is_file():
        return
    lock = InstanceLock(path, settings.namespace)
    lock.acquire()
    try:
        db = sqlite3.connect(path)
        try:
            with db:
                db.execute("UPDATE silver_v1_replay_state SET status='ABORTED',updated_at=? "
                    "WHERE replay_run_id=? AND manifest_hash=? AND status='RUNNING'",
                    (datetime.now(timezone.utc).isoformat(), run_id, manifest_hash))
        finally:
            db.close()
    finally:
        lock.release()


def run_replay(manifest_path: Path, run_id: str, *, resume: bool = False,
               base_settings: SilverSettings | None = None) -> dict:
    doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    mhash = validate_manifest(doc, run_id)
    base = base_settings or SilverSettings()
    settings = build_replay_settings(base, run_id)
    settings.v1_replay_run_filter = doc["simulation_run_id"]
    settings.validate_mode_guards()
    archive = ArchiveResolver(Path(settings.v1_archive_root))
    try:
        verify_v1_replay_boundary(doc, Path(settings.v1_bronze_path))
        context = archive.resolve(doc["simulation_run_id"], doc["topology_digest"],
                                  doc["run_generation"])
        if (context.manifest_identity != doc["manifest_identity"] or
                context.manifest_file_hash != doc["manifest_file_hash"] or
                context.catalog_file_hash != doc["catalog_file_hash"]):
            raise SourceBoundaryChangedError()
    except (SourceBoundaryChangedError, ArchiveIntegrityError) as exc:
        _abort_interrupted_replay(settings, run_id, mhash)
        raise SourceBoundaryChangedError() from exc
    lock = InstanceLock(Path(settings.checkpoint_path), settings.namespace)
    lock.acquire()
    store = SilverCheckpointStore(Path(settings.checkpoint_path))
    repo = SilverClickHouseRepository(settings)
    bronze_settings = ObservationSettings(
        checkpoint_path=settings.v1_bronze_path,
        clickhouse_host=settings.clickhouse_host, clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user, clickhouse_password=settings.clickhouse_password,
        clickhouse_database=settings.clickhouse_database,
        clickhouse_secure=settings.clickhouse_secure)
    bronze_repo = BronzeClickHouseRepository(bronze_settings)
    source_reader = None
    try:
        store.open()
        prior = store.conn.execute("SELECT manifest_hash,status,logical_hash FROM silver_v1_replay_state "
                                   "WHERE replay_run_id=?", (run_id,)).fetchone()
        if prior:
            if not resume or prior[0] != mhash or prior[1] == "ABORTED":
                raise SilverConfigError("replay ID cannot resume with this manifest/state")
            if prior[1] == "COMPLETE":
                return dict(replay_run_id=run_id, status="COMPLETE", idempotent=True,
                            manifest_hash=mhash, logical_hash=prior[2])
        elif resume:
            raise SilverConfigError("resume requested but replay ID is unknown")
        else:
            store.conn.execute("INSERT INTO silver_v1_replay_state VALUES (?,?,?,?,?)",
                (run_id, mhash, "RUNNING", None, datetime.now(timezone.utc).isoformat()))
        store.bind_v1_source(settings.namespace, doc["source_binding"])
        repo.connect()
        repo.verify_v1_schema(replay=True)
        bronze_repo.connect()
        source_reader = V1SemanticReader(settings.v1_bronze_path)
        lifecycle = LifecycleRefresher(Path(settings.checkpoint_path), archive)
        processor = SilverProcessor(settings, repo=repo, checkpoint=store, lock_held=True)
        try:
            processed = processor.run_v1_journal_pass(source_reader, bronze_repo,
                archive, lifecycle, batch_size=min(settings.batch_size, 100),
                frozen_boundary=doc)
            lifecycle.poll()
            while lifecycle.project_pending(repo, replay_run_id=run_id):
                pass
            verify_v1_replay_boundary(doc, Path(settings.v1_bronze_path))
        except (SourceVerificationError, ArchiveIntegrityError) as exc:
            if isinstance(exc, SourceVerificationError) and exc.code == 'AUTHORITY_UNAVAILABLE':
                raise
            store.conn.execute("UPDATE silver_v1_replay_state SET status='ABORTED',updated_at=? "
                               "WHERE replay_run_id=?", (datetime.now(timezone.utc).isoformat(), run_id))
            raise SourceBoundaryChangedError() from exc
        except Exception as exc:
            # A source transition can surface first as a failed point read or
            # output operation inside a batch. Recheck the frozen authority
            # before treating that failure as resumable.
            try:
                verify_v1_replay_boundary(doc, Path(settings.v1_bronze_path))
            except SourceBoundaryChangedError as changed:
                store.conn.execute("UPDATE silver_v1_replay_state SET status='ABORTED',updated_at=? "
                                   "WHERE replay_run_id=?", (datetime.now(timezone.utc).isoformat(), run_id))
                raise changed from exc
            except SourceVerificationError:
                # An unavailable authority cannot prove a permanent change.
                raise exc
            raise
        cursors = store.v1_cursors(settings.namespace)
        if (cursors["change_cursor"] != doc["change_high_water"] or
                cursors["invalidation_cursor"] != doc["invalidation_high_water"] or
                store.pending_v1_obligations(settings.namespace)):
            raise SilverConfigError("V1 replay did not reconcile frozen boundary")
        logical_hash = logical_replay_hash(store, settings.namespace)
        store.conn.execute("UPDATE silver_v1_replay_state SET status='COMPLETE',logical_hash=?,"
                           "updated_at=? WHERE replay_run_id=?",
                           (logical_hash, datetime.now(timezone.utc).isoformat(), run_id))
        return dict(replay_run_id=run_id, status="COMPLETE", idempotent=False,
                    manifest_hash=mhash, logical_hash=logical_hash,
                    source_entries_processed=processed,
                    change_cursor=cursors["change_cursor"],
                    invalidation_cursor=cursors["invalidation_cursor"])
    finally:
        if source_reader is not None:
            source_reader.close()
        bronze_repo.close()
        repo.close()
        store.close()
        lock.release()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="de.silver.replay")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    report = run_replay(Path(args.manifest), args.run_id, resume=args.resume)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
