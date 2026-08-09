"""LEGACY_INTERNAL_ONLY: replay Raw-v1 notifications for rollback diagnostics.

This module is not an operator-facing Historical replay entrypoint after K-6.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from de.webhook.config import Settings, get_settings
from de.webhook.domain.canonical_hash import canonical_hash
from de.webhook.infrastructure.clickhouse_client import ClickHouseClient
from de.webhook.infrastructure.raw_repository import ClickHouseRawRepository


class ReplayRepository(Protocol):
    async def get_payload_raw(
        self, notification_id: str, subscription_id: str
    ) -> str | None: ...

    def iter_by_time_range(
        self, from_ts: datetime, to_ts: datetime
    ): ...


@dataclass(frozen=True)
class ReplaySummary:
    from_ts: datetime
    to_ts: datetime
    dry_run: bool
    records: int
    integrity_ok: int
    integrity_fail: int

    @property
    def exit_code(self) -> int:
        return 0 if self.integrity_fail == 0 else 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "from": self.from_ts.isoformat(),
            "to": self.to_ts.isoformat(),
            "dry_run": self.dry_run,
            "records": self.records,
            "integrity_ok": self.integrity_ok,
            "integrity_fail": self.integrity_fail,
        }


def _parse_ts(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def run_replay(
    repo: ReplayRepository,
    from_ts: datetime,
    to_ts: datetime,
    *,
    dry_run: bool = False,
) -> ReplaySummary:
    count = 0
    integrity_ok = 0
    integrity_fail = 0

    async for record in repo.iter_by_time_range(from_ts, to_ts):
        count += 1
        payload_raw = await repo.get_payload_raw(
            record.notification_id, record.subscription_id
        )
        if payload_raw is None:
            integrity_fail += 1
            continue
        parsed = json.loads(payload_raw)
        recomputed = canonical_hash(parsed)
        if recomputed == record.payload_hash:
            integrity_ok += 1
        else:
            integrity_fail += 1
        if not dry_run:
            print(
                json.dumps(
                    {
                        "ingestion_id": record.ingestion_id,
                        "notification_id": record.notification_id,
                        "subscription_id": record.subscription_id,
                        "payload_hash": record.payload_hash,
                        "entity_count": record.entity_count,
                        "received_at": record.received_at.isoformat(),
                    },
                    ensure_ascii=True,
                )
            )

    return ReplaySummary(
        from_ts=from_ts,
        to_ts=to_ts,
        dry_run=dry_run,
        records=count,
        integrity_ok=integrity_ok,
        integrity_fail=integrity_fail,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="LEGACY_INTERNAL_ONLY: replay Raw-v1 NGSI notifications"
    )
    parser.add_argument("--from", dest="from_ts", required=True, help="ISO8601 start timestamp")
    parser.add_argument("--to", dest="to_ts", required=True, help="ISO8601 end timestamp")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without side effects")
    args = parser.parse_args(argv)

    settings = get_settings()
    ch = ClickHouseClient(settings)
    ch.connect()
    ch.run_migration()
    repo = ClickHouseRawRepository(ch, settings.clickhouse_database)

    from_ts = _parse_ts(args.from_ts)
    to_ts = _parse_ts(args.to_ts)

    try:
        summary = asyncio.run(
            run_replay(repo, from_ts, to_ts, dry_run=args.dry_run)
        )
    finally:
        ch.close()

    print(json.dumps(summary.as_dict(), ensure_ascii=True), file=sys.stderr)
    return summary.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
