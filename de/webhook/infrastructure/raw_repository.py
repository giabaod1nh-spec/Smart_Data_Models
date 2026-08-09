"""ClickHouse implementation of RawRepository."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from de.webhook.domain.models import RawNotificationRecord
from de.webhook.infrastructure.clickhouse_client import ClickHouseClient


class ClickHouseRawRepository:
    TABLE = "raw_ngsi_notifications"

    def __init__(self, ch: ClickHouseClient, database: str) -> None:
        self._ch = ch
        self._database = database
        self._table = f"{database}.{self.TABLE}"

    async def exists(self, notification_id: str, subscription_id: str) -> bool:
        sql = f"""
            SELECT count() AS cnt
            FROM {self._table}
            WHERE notification_id = {{nid:String}}
              AND subscription_id = {{sid:String}}
        """
        result = await self._ch.run_sync(
            self._ch.query,
            sql,
            {"nid": notification_id, "sid": subscription_id},
        )
        return int(result.first_row[0]) > 0

    async def insert(self, record: RawNotificationRecord, payload_raw: str) -> None:
        sql = f"""
            INSERT INTO {self._table} (
                ingestion_id,
                notification_id,
                subscription_id,
                payload_hash,
                contract_version,
                source_type,
                received_at,
                notified_at,
                entity_count,
                payload_size_bytes,
                payload_raw,
                ingestion_status,
                source_ip,
                request_id
            ) VALUES
        """
        notified_at = record.notified_at
        if notified_at is not None and notified_at.tzinfo is None:
            notified_at = notified_at.replace(tzinfo=timezone.utc)

        row = [
            record.ingestion_id,
            record.notification_id,
            record.subscription_id,
            record.payload_hash,
            record.contract_version,
            record.source_type,
            record.received_at,
            notified_at,
            record.entity_count,
            record.payload_size_bytes,
            payload_raw,
            record.ingestion_status,
            record.source_ip,
            record.request_id,
        ]
        columns = [
            "ingestion_id",
            "notification_id",
            "subscription_id",
            "payload_hash",
            "contract_version",
            "source_type",
            "received_at",
            "notified_at",
            "entity_count",
            "payload_size_bytes",
            "payload_raw",
            "ingestion_status",
            "source_ip",
            "request_id",
        ]
        await self._ch.run_sync(
            self._ch.client.insert,
            self._table,
            [row],
            column_names=columns,
        )

    async def count_rows(self) -> int:
        result = await self._ch.run_sync(
            self._ch.query,
            f"SELECT count() FROM {self._table}",
        )
        return int(result.first_row[0])

    async def get_payload_raw(
        self, notification_id: str, subscription_id: str
    ) -> Optional[str]:
        sql = f"""
            SELECT payload_raw
            FROM {self._table}
            WHERE notification_id = {{nid:String}}
              AND subscription_id = {{sid:String}}
            ORDER BY received_at DESC
            LIMIT 1
        """
        result = await self._ch.run_sync(
            self._ch.query,
            sql,
            {"nid": notification_id, "sid": subscription_id},
        )
        if not result.result_rows:
            return None
        return str(result.first_row[0])

    async def iter_by_time_range(
        self, from_ts: datetime, to_ts: datetime
    ) -> AsyncIterator[RawNotificationRecord]:
        sql = f"""
            SELECT
                ingestion_id,
                notification_id,
                subscription_id,
                payload_hash,
                contract_version,
                source_type,
                received_at,
                notified_at,
                entity_count,
                payload_size_bytes,
                ingestion_status,
                source_ip,
                request_id
            FROM {self._table}
            WHERE received_at >= {{from_ts:DateTime64(3, 'UTC')}}
              AND received_at <= {{to_ts:DateTime64(3, 'UTC')}}
            ORDER BY received_at ASC
        """
        result = await self._ch.run_sync(
            self._ch.query,
            sql,
            {"from_ts": from_ts, "to_ts": to_ts},
        )
        for row in result.result_rows:
            yield RawNotificationRecord(
                ingestion_id=str(row[0]),
                notification_id=str(row[1]),
                subscription_id=str(row[2]),
                payload_hash=str(row[3]),
                contract_version=str(row[4]),
                source_type=str(row[5]),
                received_at=row[6],
                notified_at=row[7],
                entity_count=int(row[8]),
                payload_size_bytes=int(row[9]),
                ingestion_status=str(row[10]),
                source_ip=str(row[11]),
                request_id=str(row[12]),
            )
