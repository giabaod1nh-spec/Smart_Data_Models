"""K-7 Bronze replay/backfill CLI."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.bronze.checkpoint_store import CheckpointStore  # noqa: E402
from de.bronze.clickhouse_repository import BronzeClickHouseRepository  # noqa: E402
from de.bronze.config import BronzeSettings, get_settings  # noqa: E402
from de.bronze.lineage_resolver import LineageResolver  # noqa: E402
from de.bronze.models import ResolveKind, WindowManifest  # noqa: E402
from de.bronze.payload_codec import decode_payload  # noqa: E402
from de.bronze.processor import BronzeProcessor  # noqa: E402
from de.bronze.transformer import BronzeTransformer  # noqa: E402
from de.bronze.validator import BronzeValidator, ValidationOutcome  # noqa: E402

log = logging.getLogger("de.bronze.replay")


def _manifest_bounds(manifest: Dict[str, Any]) -> Dict[Tuple[str, int], int]:
    topic = manifest["topic"]
    bounds: Dict[Tuple[str, int], int] = {}
    for part_spec in manifest["partitions"]:
        part = int(part_spec["partition"])
        bounds[(topic, part)] = int(part_spec["end_offset"])
    return bounds


def init_backfill_checkpoint(
    settings: BronzeSettings, manifest: Dict[str, Any], run_id: str
) -> None:
    wm = WindowManifest.from_dict(manifest)
    cp_store = CheckpointStore(Path(settings.checkpoint_path))
    namespace = f"backfill:{run_id}"
    for part_spec in wm.partitions:
        topic = part_spec.get("topic", wm.topic)
        partition = int(part_spec["partition"])
        start = int(part_spec["start_offset"])
        source_start = int(part_spec.get("source_start_offset", start))
        if start < source_start:
            raise RuntimeError(
                f"INSUFFICIENT_RETENTION partition={partition} start={start} source_start={source_start}"
            )
        cp_store.init_checkpoint(
            namespace=namespace,
            topic=topic,
            partition=partition,
            source_start_offset=source_start,
            last_completed_offset=start - 1,
            start_mode="explicit",
            processor_name=settings.processor_name,
            processor_version=settings.processor_version,
            bronze_schema_version=settings.bronze_schema_version,
        )
    cp_store.close()


def rebuild_checkpoint(settings: BronzeSettings, manifest_path: Path, namespace: str) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    wm = WindowManifest.from_dict(manifest)
    repo = BronzeClickHouseRepository(settings)
    repo.connect()
    cp_store = CheckpointStore(Path(settings.checkpoint_path))
    results = []
    for part_spec in wm.partitions:
        topic = part_spec.get("topic", wm.topic)
        partition = int(part_spec["partition"])
        source_start = int(part_spec.get("source_start_offset", part_spec["start_offset"]))
        cp = cp_store.get(namespace, topic, partition)
        if cp and (
            cp.processor_version != settings.processor_version
            or cp.processor_name != settings.processor_name
        ):
            raise RuntimeError("VERSION_MISMATCH on rebuild")
        start = int(part_spec["start_offset"])
        end = int(part_spec["end_offset"])
        sql_offsets = f"""
            SELECT offset FROM {settings.clickhouse_database}.bronze_entity_events
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
              AND processor_name={{pname:String}} AND processor_version={{pver:String}}
            UNION ALL
            SELECT offset FROM {settings.clickhouse_database}.bronze_run_events
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
              AND processor_name={{pname:String}} AND processor_version={{pver:String}}
            UNION ALL
            SELECT offset FROM {settings.clickhouse_database}.bronze_quarantine
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
              AND processor_name={{pname:String}} AND processor_version={{pver:String}}
            UNION ALL
            SELECT offset FROM {settings.clickhouse_database}.kafka_quarantine_events
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
        """
        r = repo.client.query(
            sql_offsets,
            parameters={
                "topic": topic,
                "part": partition,
                "start": start,
                "end": end,
                "pname": settings.processor_name,
                "pver": settings.processor_version,
            },
        )
        completed = sorted({int(row[0]) for row in r.result_rows})
        expected = source_start
        last = source_start - 1
        completed_set = set(completed)
        while expected in completed_set:
            last = expected
            expected += 1
        if cp:
            cp_store.advance(namespace, topic, partition, last)
        results.append(
            {
                "topic": topic,
                "partition": partition,
                "last_completed": last,
                "source_start": source_start,
            }
        )
    repo.close()
    cp_store.close()
    return {"status": "OK", "partitions": results}


def run_backfill(settings: BronzeSettings, manifest_path: Path, run_id: str, *, resume: bool = True) -> int:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cp_path = Path(settings.checkpoint_path)
    namespace = f"backfill:{run_id}"
    if not resume and cp_path.is_file():
        import sqlite3

        conn = sqlite3.connect(str(cp_path))
        conn.execute("DELETE FROM bronze_checkpoint WHERE checkpoint_namespace=?", (namespace,))
        conn.execute("DELETE FROM bronze_processing_ledger WHERE checkpoint_namespace=?", (namespace,))
        conn.commit()
        conn.close()
    needs_init = True
    if cp_path.is_file():
        import sqlite3

        conn = sqlite3.connect(str(cp_path))
        row = conn.execute(
            "SELECT count() FROM bronze_checkpoint WHERE checkpoint_namespace=?",
            (namespace,),
        ).fetchone()
        conn.close()
        needs_init = not row or int(row[0]) == 0
    if needs_init:
        init_backfill_checkpoint(settings, manifest, run_id)
    ns_settings = settings.model_copy(update={"checkpoint_namespace": f"backfill:{run_id}"})
    bounds = _manifest_bounds(manifest)
    processor = BronzeProcessor(
        ns_settings,
        write_main_tables=True,
        max_offset_exclusive=bounds,
    )
    processor.start()
    deadline = time.time() + 7200.0
    try:
        while time.time() < deadline:
            done = True
            for (topic, part), end in bounds.items():
                cp = processor.checkpoint.get(f"backfill:{run_id}", topic, part)
                if cp is None or cp.last_completed_offset < end - 1:
                    done = False
                    break
            if done:
                break
            time.sleep(2.0)
    finally:
        processor.stop()
    return 0


def _process_record_to_replay(
    *,
    repo: BronzeClickHouseRepository,
    resolver: LineageResolver,
    validator: BronzeValidator,
    transformer: BronzeTransformer,
    topic: str,
    partition: int,
    offset: int,
    run_id: str,
) -> None:
    resolved = resolver.resolve(topic, partition, offset)
    if resolved.kind == ResolveKind.END_OF_AVAILABLE_DATA:
        return
    if resolved.kind == ResolveKind.OFFSET_GAP_WAIT:
        raise RuntimeError(f"OFFSET_GAP at {topic}:{partition}:{offset}")
    if resolved.kind == ResolveKind.RAW_QUARANTINE_SKIPPED:
        return

    raw = resolved.raw_row
    assert raw is not None
    try:
        event, _ = decode_payload(raw)
    except Exception as e:
        outcome = ValidationOutcome(
            False,
            "QUARANTINE",
            error_code="PAYLOAD_DECODE_FAILED",
            error_detail=str(e),
            failure_stage="PARSE",
        )
        qrow = transformer.transform(raw, {}, outcome).quarantine_row
        if qrow:
            repo.insert_quarantine_batch([qrow], replay_run_id=run_id)
        return

    outcome = validator.validate(event)
    upstream_dup = False
    if outcome.ok and outcome.kind == "ENTITY":
        upstream_dup = repo.event_id_exists_at_different_offset(
            str(event.get("eventId")),
            str(event.get("entityPayloadHash")),
            topic,
            partition,
            offset,
        )
    result = transformer.transform(raw, event, outcome, upstream_duplicate=upstream_dup)
    if result.entity_row:
        repo.insert_entity_batch([result.entity_row], replay_run_id=run_id)
    elif result.run_row:
        repo.insert_run_batch([result.run_row], replay_run_id=run_id)
    elif result.quarantine_row:
        repo.insert_quarantine_batch([result.quarantine_row], replay_run_id=run_id)


def run_parity_sync(settings: BronzeSettings, manifest_path: Path, run_id: str) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bounds = _manifest_bounds(manifest)
    parity_ns = f"parity:{run_id}"
    wm = WindowManifest.from_dict(manifest)
    cp_store = CheckpointStore(Path(settings.checkpoint_path))
    for part_spec in wm.partitions:
        topic = part_spec.get("topic", wm.topic)
        partition = int(part_spec["partition"])
        start = int(part_spec["start_offset"])
        source_start = int(part_spec.get("source_start_offset", start))
        cp_store.init_checkpoint(
            namespace=parity_ns,
            topic=topic,
            partition=partition,
            source_start_offset=source_start,
            last_completed_offset=start - 1,
            start_mode="explicit",
            processor_name=settings.processor_name,
            processor_version=settings.processor_version,
            bronze_schema_version=settings.bronze_schema_version,
        )
    cp_store.close()

    ns_settings = settings.model_copy(update={"checkpoint_namespace": parity_ns})
    processor = BronzeProcessor(
        ns_settings,
        write_main_tables=False,
        replay_run_id=run_id,
        max_offset_exclusive=bounds,
    )
    processor.start()
    deadline = time.time() + 7200.0
    try:
        while time.time() < deadline:
            done = True
            for (topic, part), end in bounds.items():
                cp = processor.checkpoint.get(parity_ns, topic, part)
                if cp is None or cp.last_completed_offset < end - 1:
                    done = False
                    break
            if done:
                break
            time.sleep(2.0)
    finally:
        processor.stop()


def run_parity(settings: BronzeSettings, manifest_path: Path, run_id: str) -> int:
    from de.tools.k7_bronze_oracles import run_oracles

    run_parity_sync(settings, manifest_path, run_id)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    report = run_oracles(settings, manifest, replay_run_id=run_id)
    out = Path("docs/architecture/k7_bronze_evidence") / run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "parity_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return 0 if report.get("pass") else 1


def main() -> int:
    p = argparse.ArgumentParser(description="K-7 Bronze replay/backfill")
    p.add_argument("--backfill", action="store_true")
    p.add_argument("--parity", action="store_true")
    p.add_argument("--rebuild-checkpoint", action="store_true")
    p.add_argument("--manifest", required=False)
    p.add_argument("--run-id", default=None)
    p.add_argument("--namespace", default="live")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    run_id = args.run_id or f"k7-{uuid.uuid4().hex[:12]}"
    if args.rebuild_checkpoint:
        if not args.manifest:
            log.error("--manifest required")
            return 2
        report = rebuild_checkpoint(settings, Path(args.manifest), args.namespace)
        print(json.dumps(report, indent=2))
        return 0
    if args.parity:
        if not args.manifest:
            log.error("--manifest required")
            return 2
        return run_parity(settings, Path(args.manifest), run_id)
    if args.backfill:
        if not args.manifest:
            log.error("--manifest required")
            return 2
        return run_backfill(settings, Path(args.manifest), run_id)
    log.error("specify --backfill, --parity, or --rebuild-checkpoint")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
