"""RT-BLOCK-001 production offset reconciliation (capture → backup → reconcile → verify)."""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

REPO = Path(__file__).resolve().parents[2]
VIS = REPO / "Visualize"
DEFAULT_SQLITE = VIS / "artifacts" / "projector" / "k5-production.sqlite3"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def kafka_watermarks(bootstrap: str, topic: str) -> Dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"rt-reconcile-probe-{int(datetime.now().timestamp())}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic=topic, timeout=10.0)
        t = md.topics.get(topic)
        if t is None:
            raise RuntimeError(f"topic missing: {topic}")
        tips: Dict[int, int] = {}
        for p in sorted(t.partitions.keys()):
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10.0)
            tips[int(p)] = int(hi)
        return tips
    finally:
        c.close()


def kafka_group_commits(bootstrap: str, topic: str, group_id: str) -> Dict[int, Optional[int]]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic=topic, timeout=10.0)
        t = md.topics.get(topic)
        if t is None:
            raise RuntimeError(f"topic missing: {topic}")
        parts = [TopicPartition(topic, p) for p in sorted(t.partitions.keys())]
        committed = c.committed(parts, timeout=10.0)
        out: Dict[int, Optional[int]] = {}
        for tp in committed:
            off = int(tp.offset) if tp.offset >= 0 else None
            out[int(tp.partition)] = off
        return out
    finally:
        c.close()


def read_sqlite_commits(sqlite_path: Path, topic: str) -> Dict[int, Optional[int]]:
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT partition, committed_offset FROM projector_partition_commits WHERE topic = ?",
        (topic,),
    ).fetchall()
    conn.close()
    return {int(r["partition"]): int(r["committed_offset"]) for r in rows}


def capture_state(
    *,
    bootstrap: str,
    topic: str,
    group_id: str,
    sqlite_path: Path,
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "captured_at": _utc(),
        "bootstrap": bootstrap,
        "topic": topic,
        "group_id": group_id,
        "sqlite_path": str(sqlite_path),
        "kafka_watermarks": kafka_watermarks(bootstrap, topic),
        "kafka_group_commits": kafka_group_commits(bootstrap, topic, group_id),
        "sqlite_commits": read_sqlite_commits(sqlite_path, topic),
    }
    tips = state["kafka_watermarks"]
    sqlite = state["sqlite_commits"]
    broker = state["kafka_group_commits"]
    issues = []
    for part in sorted(set(list(tips.keys()) + list(sqlite.keys()) + list(broker.keys()))):
        sql_last = sqlite.get(part)
        br_next = broker.get(part)
        end = tips.get(part)
        if sql_last is not None and end is not None and sql_last >= end:
            issues.append(
                {
                    "partition": part,
                    "type": "sqlite_ahead_or_at_broker_end",
                    "sqlite_last": sql_last,
                    "broker_end": end,
                }
            )
        if sql_last is not None and br_next is not None and br_next > sql_last + 1:
            issues.append(
                {
                    "partition": part,
                    "type": "broker_ahead_of_sqlite_authority",
                    "sqlite_last": sql_last,
                    "broker_next": br_next,
                    "expected_broker_next": sql_last + 1,
                }
            )
    state["issues"] = issues
    return state


def backup_sqlite(sqlite_path: Path, evidence_dir: Path) -> Path:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    stamp = _utc().replace(":", "").replace("-", "")
    dest = evidence_dir / f"k5-production.sqlite3.backup.{stamp}"
    shutil.copy2(sqlite_path, dest)
    for suffix in ("-wal", "-shm"):
        wal = Path(str(sqlite_path) + suffix)
        if wal.is_file():
            shutil.copy2(wal, evidence_dir / f"{dest.name}{suffix}")
    return dest


def reconcile_sqlite_generation(
    sqlite_path: Path,
    topic: str,
    watermarks: Dict[int, int],
    *,
    dry_run: bool,
) -> list[dict[str, Any]]:
    """Clamp SQLite last-processed when ahead of current log generation."""
    actions: list[dict[str, Any]] = []
    commits = read_sqlite_commits(sqlite_path, topic)
    conn = sqlite3.connect(sqlite_path)
    try:
        for part, end in sorted(watermarks.items()):
            sql_last = commits.get(part)
            if sql_last is None:
                continue
            max_last = int(end) - 1 if int(end) > 0 else None
            if max_last is not None and sql_last >= int(end):
                new_last = max_last
                actions.append(
                    {
                        "partition": part,
                        "action": "clamp_sqlite_last",
                        "from": sql_last,
                        "to": new_last,
                        "broker_end": int(end),
                    }
                )
                if not dry_run:
                    conn.execute(
                        """
                        INSERT INTO projector_partition_commits (topic, partition, committed_offset, updated_at)
                        VALUES (?, ?, ?, datetime('now'))
                        ON CONFLICT(topic, partition) DO UPDATE SET
                            committed_offset = excluded.committed_offset,
                            updated_at = excluded.updated_at
                        """,
                        (topic, part, new_last),
                    )
                    deleted = conn.execute(
                        """
                        DELETE FROM projector_event_ledger
                        WHERE topic = ? AND partition = ? AND offset > ?
                        """,
                        (topic, part, new_last),
                    ).rowcount
                    if deleted:
                        actions.append(
                            {
                                "partition": part,
                                "action": "trim_ledger_beyond_generation",
                                "deleted_rows": deleted,
                                "max_offset_kept": new_last,
                            }
                        )
        if not dry_run and actions:
            conn.commit()
    finally:
        conn.close()
    return actions


def reset_group_offsets(
    *,
    bootstrap: str,
    topic: str,
    group_id: str,
    targets: Dict[int, int],
    dry_run: bool,
) -> list[str]:
    """Reset consumer group to explicit next offsets via kafka-consumer-groups in Docker."""
    cmds_run: list[str] = []
    for part, next_off in sorted(targets.items()):
        topic_spec = f"{topic}:{part}"
        cmd = [
            "docker",
            "exec",
            "smart-traffic-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--group",
            group_id,
            "--reset-offsets",
            "--to-offset",
            str(next_off),
            "--topic",
            topic_spec,
            "--execute" if not dry_run else "--dry-run",
        ]
        cmds_run.append(" ".join(cmd))
        if dry_run:
            continue
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"reset failed p={part} off={next_off}: {proc.stderr or proc.stdout}"
            )
    return cmds_run


def build_reconcile_plan(state: dict[str, Any]) -> dict[str, Any]:
    topic = state["topic"]
    tips = state["kafka_watermarks"]
    sqlite = state["sqlite_commits"]
    broker = state["kafka_group_commits"]
    sqlite_clamps: list[dict] = []
    group_resets: Dict[int, int] = {}

    effective_sqlite = dict(sqlite)
    for part, end in tips.items():
        sql_last = effective_sqlite.get(part)
        if sql_last is not None and sql_last >= end and end > 0:
            new_last = end - 1
            sqlite_clamps.append({"partition": part, "from": sql_last, "to": new_last})
            effective_sqlite[part] = new_last

    for part in sorted(set(list(tips.keys()) + list(effective_sqlite.keys()))):
        sql_last = effective_sqlite.get(part)
        br_next = broker.get(part)
        if sql_last is None:
            target = tips.get(part, 0)
            if br_next != target:
                group_resets[part] = target
        else:
            expected = sql_last + 1
            if br_next is None or br_next != expected:
                group_resets[part] = expected

    return {
        "sqlite_clamps": sqlite_clamps,
        "group_resets": group_resets,
        "effective_sqlite_after_clamp": effective_sqlite,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Production offset reconciliation")
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--topic", default="traffic.entity-events.v2")
    p.add_argument("--group-id", default="projector-k5-production")
    p.add_argument("--sqlite", default=str(DEFAULT_SQLITE))
    p.add_argument("--evidence-dir", default="")
    p.add_argument("--capture-only", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--execute", action="store_true")
    args = p.parse_args()

    sqlite_path = Path(args.sqlite)
    if not sqlite_path.is_file():
        print(f"SQLite not found: {sqlite_path}", file=sys.stderr)
        return 2

    stamp = _utc().replace(":", "").replace("-", "")
    evidence_dir = (
        Path(args.evidence_dir)
        if args.evidence_dir
        else REPO / "artifacts" / "realtime" / "rt_block_001" / stamp
    )
    evidence_dir.mkdir(parents=True, exist_ok=True)

    before = capture_state(
        bootstrap=args.bootstrap,
        topic=args.topic,
        group_id=args.group_id,
        sqlite_path=sqlite_path,
    )
    (evidence_dir / "before.json").write_text(json.dumps(before, indent=2), encoding="utf-8")
    print(json.dumps({"phase": "capture", "issues": before["issues"]}, indent=2))

    if args.capture_only:
        return 0

    backup_path = backup_sqlite(sqlite_path, evidence_dir)
    before["backup_path"] = str(backup_path)
    (evidence_dir / "backup.json").write_text(
        json.dumps({"backup": str(backup_path), "at": _utc()}, indent=2),
        encoding="utf-8",
    )

    plan = build_reconcile_plan(before)
    (evidence_dir / "plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")
    print(json.dumps({"phase": "plan", "plan": plan}, indent=2))

    if not args.execute and not args.dry_run:
        print("Use --dry-run or --execute to apply reconciliation", file=sys.stderr)
        return 0

    sqlite_actions = reconcile_sqlite_generation(
        sqlite_path,
        args.topic,
        before["kafka_watermarks"],
        dry_run=args.dry_run,
    )
    group_cmds = reset_group_offsets(
        bootstrap=args.bootstrap,
        topic=args.topic,
        group_id=args.group_id,
        targets=plan["group_resets"],
        dry_run=args.dry_run,
    )

    result = {
        "at": _utc(),
        "dry_run": args.dry_run,
        "sqlite_actions": sqlite_actions,
        "group_reset_commands": group_cmds,
    }
    (evidence_dir / "apply.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    if args.dry_run:
        return 0

    after = capture_state(
        bootstrap=args.bootstrap,
        topic=args.topic,
        group_id=args.group_id,
        sqlite_path=sqlite_path,
    )
    (evidence_dir / "after.json").write_text(json.dumps(after, indent=2), encoding="utf-8")
    ok = len(after["issues"]) == 0
    print(json.dumps({"phase": "verify", "ok": ok, "issues": after["issues"]}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
