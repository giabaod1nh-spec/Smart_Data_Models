"""K-7 Bronze parity oracles — executable ClickHouse gates."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from de.bronze.config import BronzeSettings
from de.bronze.clickhouse_repository import BronzeClickHouseRepository


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("ascii", errors="replace")
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    return value


def run_oracles(
    settings: BronzeSettings,
    manifest: Dict[str, Any],
    *,
    replay_run_id: str | None = None,
) -> Dict[str, Any]:
    repo = BronzeClickHouseRepository(settings)
    repo.connect()
    db = settings.clickhouse_database
    topic = manifest["topic"]
    report: Dict[str, Any] = {"pass": True, "partitions": []}

    for part_spec in manifest["partitions"]:
        partition = int(part_spec["partition"])
        start = int(part_spec["start_offset"])
        end = int(part_spec["end_offset"])
        part_report = _oracle_partition(repo, db, topic, partition, start, end, replay_run_id)
        report["partitions"].append(part_report)
        if not part_report.get("pass"):
            report["pass"] = False

    repo.close()
    return _json_safe(report)


def _oracle_partition(
    repo: BronzeClickHouseRepository,
    db: str,
    topic: str,
    partition: int,
    start: int,
    end: int,
    replay_run_id: str | None,
) -> Dict[str, Any]:
    entity_table = "bronze_entity_events_replay" if replay_run_id else "bronze_entity_events"
    run_table = "bronze_run_events_replay" if replay_run_id else "bronze_run_events"
    quar_table = "bronze_quarantine_replay" if replay_run_id else "bronze_quarantine"
    replay_filter = f" AND replay_run_id='{replay_run_id}'" if replay_run_id else ""

    source_count = repo.client.query(
        f"""
        SELECT uniqExact(raw_ingestion_id) FROM {db}.kafka_raw_events
        WHERE topic={{topic:String}} AND partition={{part:Int32}}
          AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
        """,
        parameters={"topic": topic, "part": partition, "start": start, "end": end},
    ).result_rows[0][0]

    dest_count = repo.client.query(
        f"""
        SELECT uniqExact(raw_ingestion_id) FROM (
            SELECT raw_ingestion_id FROM {db}.{entity_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
            UNION ALL
            SELECT raw_ingestion_id FROM {db}.{run_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
            UNION ALL
            SELECT raw_ingestion_id FROM {db}.{quar_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
        )
        """,
        parameters={"topic": topic, "part": partition, "start": start, "end": end},
    ).result_rows[0][0]

    missing = repo.client.query(
        f"""
        SELECT s.raw_ingestion_id FROM (
            SELECT raw_ingestion_id FROM {db}.kafka_raw_events
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
        ) s
        LEFT JOIN (
            SELECT raw_ingestion_id FROM {db}.{entity_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}{replay_filter}
            UNION ALL
            SELECT raw_ingestion_id FROM {db}.{run_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}{replay_filter}
            UNION ALL
            SELECT raw_ingestion_id FROM {db}.{quar_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}{replay_filter}
        ) d ON s.raw_ingestion_id = d.raw_ingestion_id
        WHERE d.raw_ingestion_id = '' OR d.raw_ingestion_id IS NULL
        LIMIT 20
        """,
        parameters={"topic": topic, "part": partition, "start": start, "end": end},
    ).result_rows

    overlap = repo.client.query(
        f"""
        SELECT raw_ingestion_id, uniqExact(dest) AS c FROM (
            SELECT raw_ingestion_id, 'ENTITY' AS dest FROM {db}.{entity_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
            UNION ALL
            SELECT raw_ingestion_id, 'RUN' FROM {db}.{run_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
            UNION ALL
            SELECT raw_ingestion_id, 'QUARANTINE' FROM {db}.{quar_table}
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
        )
        GROUP BY raw_ingestion_id
        HAVING c > 1
        LIMIT 20
        """,
        parameters={"topic": topic, "part": partition, "start": start, "end": end},
    ).result_rows

    physical_dup = repo.client.query(
        f"""
        SELECT raw_ingestion_id, count() AS c FROM {db}.{entity_table}
        WHERE topic={{topic:String}} AND partition={{part:Int32}}
          AND offset >= {{start:Int64}} AND offset < {{end:Int64}}{replay_filter}
        GROUP BY raw_ingestion_id HAVING c > 1
        LIMIT 10
        """,
        parameters={"topic": topic, "part": partition, "start": start, "end": end},
    ).result_rows

    raw_q = repo.client.query(
        f"""
        SELECT uniqExact(raw_ingestion_id) FROM {db}.kafka_quarantine_events
        WHERE topic={{topic:String}} AND partition={{part:Int32}}
          AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
        """,
        parameters={"topic": topic, "part": partition, "start": start, "end": end},
    ).result_rows[0][0]

    logical_ok = int(source_count) == int(dest_count)
    part_pass = logical_ok and len(missing) == 0 and len(overlap) == 0

    return {
        "topic": topic,
        "partition": partition,
        "start_offset": start,
        "end_offset": end,
        "pass": part_pass,
        "logical_totality": {
            "pass": logical_ok,
            "source_distinct": int(source_count),
            "dest_distinct": int(dest_count),
        },
        "missing_ids": {"pass": len(missing) == 0, "count": len(missing), "samples": missing[:5]},
        "overlap": {"pass": len(overlap) == 0, "count": len(overlap), "samples": overlap[:5]},
        "physical_duplicates": {"count": len(physical_dup), "samples": physical_dup[:5]},
        "raw_quarantine_distinct": int(raw_q),
        "raw_total_distinct": int(source_count) + int(raw_q),
    }


if __name__ == "__main__":
    import sys
    from pathlib import Path

    from de.bronze.config import get_settings

    manifest_path = Path(sys.argv[1])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rep = run_oracles(get_settings(), manifest)
    print(json.dumps(rep, indent=2))
