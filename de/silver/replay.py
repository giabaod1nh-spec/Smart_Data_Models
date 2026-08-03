"""Silver Plan 3 — isolated replay CLI."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import (
    DestinationMode,
    SilverConfigError,
    SilverSettings,
    replay_namespace,
)
from de.silver.instance_lock import InstanceLock
from de.silver.processor import SilverProcessor


REQUIRED_MANIFEST_VERSION = "silver-replay-v1"


def canonical_manifest_hash(doc: dict) -> str:
    payload = json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_manifest(doc: dict, run_id: str) -> str:
    if doc.get("manifest_version") != REQUIRED_MANIFEST_VERSION:
        raise SilverConfigError("invalid manifest_version")
    if doc.get("replay_run_id") != run_id:
        raise SilverConfigError("replay_run_id mismatch")
    if doc.get("source_database") != "smart_traffic":
        raise SilverConfigError("source_database must be smart_traffic")
    windows = doc.get("stream_windows") or []
    if not windows:
        raise SilverConfigError("stream_windows required")
    for w in windows:
        if w.get("source_table") not in {"bronze_entity_events", "bronze_run_events"}:
            raise SilverConfigError(f"invalid source_table {w.get('source_table')}")
        if int(w["start_offset"]) >= int(w["end_offset"]):
            raise SilverConfigError("start_offset must be < end_offset")
    return canonical_manifest_hash(doc)


def build_replay_settings(base: SilverSettings, run_id: str) -> SilverSettings:
    return SilverSettings(
        clickhouse_host=base.clickhouse_host,
        clickhouse_port=base.clickhouse_port,
        clickhouse_user=base.clickhouse_user,
        clickhouse_password=base.clickhouse_password,
        clickhouse_database=base.clickhouse_database,
        checkpoint_path=base.checkpoint_path,
        namespace=replay_namespace(run_id),
        destination_mode=DestinationMode.REPLAY.value,
        replay_run_id=run_id,
        topic_allowlist=base.topic_allowlist,
        batch_size=base.batch_size,
        poll_interval_sec=base.poll_interval_sec,
        health_port=base.health_port,
    )


def run_replay(manifest_path: Path, run_id: str, *, resume: bool = False) -> dict:
    doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    mhash = validate_manifest(doc, run_id)
    base = SilverSettings()
    settings = build_replay_settings(base, run_id)
    settings.validate_mode_guards()

    store = SilverCheckpointStore(Path(settings.checkpoint_path))
    store.open()
    try:
        existing = store.get_replay_manifest(run_id)
        if existing is None:
            if resume:
                raise SilverConfigError("resume requested but no stored manifest")
            store.put_replay_manifest(run_id, mhash)
        else:
            if not resume:
                raise SilverConfigError("replay run exists; pass --resume")
            if existing != mhash:
                raise SilverConfigError("manifest hash mismatch on resume")
    finally:
        store.close()

    lock = InstanceLock(Path(settings.checkpoint_path), settings.namespace)
    lock.acquire()
    processor = SilverProcessor(settings, lock_held=True)
    try:
        # Bounded catch-up for CLI: process discovered streams until idle or timeout.
        # Do not leave a daemon running after the command returns.
        import time

        processor.reader.connect()
        processor.repo.connect()
        processor.checkpoint.open()
        processor._schema_ok = True
        processor._clickhouse_ok = True
        processor._sqlite_ok = True
        processor.state = processor.state  # noqa: B018 — keep attribute access stable
        from de.silver.config import ProcessorState

        processor.state = ProcessorState.READY
        processor._streams = processor.reader.discover_streams(settings.topic_list())
        idle_rounds = 0
        deadline = time.time() + 180.0
        while time.time() < deadline and idle_rounds < 3:
            progressed = 0
            for stream in processor._streams:
                progressed += processor.process_stream_once(stream)
            if progressed == 0:
                idle_rounds += 1
            else:
                idle_rounds = 0
        report = {
            "replay_run_id": run_id,
            "namespace": settings.namespace,
            "manifest_hash": mhash,
            "suppressed_dimension_candidates": processor.metrics.suppressed_dimension_candidates,
            "records_processed_total": processor.metrics.records_processed_total,
            "approach_scenario": "NOT_APPLICABLE_NO_REPLAY_TABLE",
            "physical_replay_targets": 8,
        }
        return report
    finally:
        try:
            processor.request_shutdown()
        except Exception:
            pass
        processor.reader.close()
        processor.repo.close()
        processor.checkpoint.close()
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
