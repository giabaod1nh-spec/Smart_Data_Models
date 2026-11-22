"""ClickHouse read (Raw) + write (Bronze) repositories."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set

import clickhouse_connect
from de.kafka_raw.v1_ledger_store import IntegrityFault

from de.bronze import MIGRATION_VERSION
from de.bronze.config import BronzeSettings
from de.bronze.models import RawRow

log = logging.getLogger(__name__)

ENTITY_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "broker_timestamp",
    "raw_consumed_at", "event_id", "event_type", "contract_version", "event_version",
    "source", "producer_id", "producer_session_id", "simulation_run_id",
    "simulation_time", "scenario_id", "node_id", "cycle_sequence", "entity_sequence",
    "cycle_entity_count", "node_entity_count", "captured_at", "entity_id", "entity_type",
    "entity_payload_hash", "entity_payload_json", "upstream_duplicate_event_id",
    "event_payload_json", "bronze_canonical_hash", "bronze_ingestion_id",
    "processor_name", "processor_version", "bronze_schema_version",
    "source_contract_version", "processed_at", "validation_status", "migration_version",
]

RUN_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "broker_timestamp",
    "raw_consumed_at", "event_type", "contract_version", "event_version", "source",
    "producer_id", "producer_session_id", "simulation_run_id", "started_at",
    "scenario_id", "event_payload_json", "bronze_canonical_hash", "bronze_ingestion_id",
    "processor_name", "processor_version", "bronze_schema_version",
    "source_contract_version", "processed_at", "validation_status", "migration_version",
]

QUAR_COLS = [
    "topic", "partition", "offset", "raw_ingestion_id", "broker_timestamp",
    "raw_consumed_at", "event_id", "event_type", "simulation_run_id",
    "failure_stage", "error_code", "error_detail", "retryable", "payload_encoding",
    "payload_reference", "payload_bytes_hash", "bronze_canonical_hash",
    "processor_name", "processor_version", "bronze_schema_version",
    "quarantined_at", "migration_version",
]


class BronzeClickHouseRepository:
    def __init__(self, settings: BronzeSettings) -> None:
        self.settings = settings
        self.database = settings.clickhouse_database
        self._client: Any = None

    def connect(self) -> None:
        self._client = clickhouse_connect.get_client(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_user,
            password=self.settings.clickhouse_password,
            database=self.database,
            secure=self.settings.clickhouse_secure,
            connect_timeout=self.settings.clickhouse_connect_timeout,
            send_receive_timeout=self.settings.clickhouse_query_timeout,
            settings={"async_insert": 0, "wait_end_of_query": 1},
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("ClickHouse not connected")
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    def reconcile_v1_outputs(self, obligations: list[dict]) -> list[dict]:
        """Verify complete immutable output content, returning only missing rows."""
        from datetime import datetime, timezone
        allowed = {
            'bronze_observations_v1': ('namespace','event_id','semantic_revision'),
            'bronze_delivery_dispositions_v1': ('namespace','raw_ingestion_id'),
            'bronze_conflict_evidence_v1': ('namespace','conflict_id'),
            'bronze_quarantine_v1': ('namespace','raw_ingestion_id'),
            'bronze_invalidations_v1': ('namespace','invalidation_id'),
        }
        missing = []
        for obligation in obligations:
            table, expected = obligation['table'], obligation['row']
            if table not in allowed or not expected.get('namespace'):
                raise ValueError('unapproved Bronze V1 destination or namespace')
            identity = allowed[table]
            predicates = []
            params = {}
            for index, key in enumerate(identity):
                name = 'p'+str(index)
                typ = 'UInt64' if key == 'semantic_revision' else 'String'
                predicates.append(key+'={'+name+':'+typ+'}')
                params[name] = expected[key]
            result = self.client.query('SELECT * FROM '+self.database+'.'+table+
                                       ' WHERE '+' AND '.join(predicates), parameters=params)
            if not result.result_rows:
                missing.append(obligation)
                continue
            for values in result.result_rows:
                actual = dict(zip(result.column_names, values))
                for key, value in expected.items():
                    cell = actual.get(key)
                    if isinstance(cell, bytes):
                        cell = cell.decode('ascii')
                    if isinstance(cell, datetime):
                        if cell.tzinfo is None:
                            cell = cell.replace(tzinfo=timezone.utc)
                        value = datetime.fromisoformat(value.replace('Z','+00:00')) if isinstance(value,str) else value
                    if cell != value:
                        raise IntegrityFault('Bronze immutable output mismatch: '+table+'.'+key)
        return missing

    def persist_v1_outputs(self, obligations: list[dict]):
        """Retry only missing obligations and positively reconcile acknowledgements."""
        from datetime import datetime
        missing = self.reconcile_v1_outputs(obligations)
        for obligation in missing:
            row = dict(obligation['row'])
            for key in ('processed_at','created_at','quarantined_at','captured_at'):
                if isinstance(row.get(key), str):
                    row[key] = datetime.fromisoformat(row[key].replace('Z','+00:00'))
            try:
                self.client.insert(self.database+'.'+obligation['table'],
                    [list(row.values())], column_names=list(row), settings={'async_insert':0})
            except Exception:
                # A lost response may mean the insert succeeded. Reconciliation
                # must still fail if any content differs or any obligation is absent.
                if self.reconcile_v1_outputs([obligation]):
                    raise
        if self.reconcile_v1_outputs(obligations):
            raise RuntimeError('incomplete Bronze persistence obligations')

    def read_valid_observations(self, state_path, namespace, after=None, limit=500, simulation_run_id=None):
        """One current-state SQLite snapshot per keyset page; no time travel."""
        import sqlite3
        import json
        import hashlib
        from pathlib import Path
        from contracts.traffic_observation import canonical_json, validate_observation
        if not namespace or not 1 <= limit <= 1000:
            raise ValueError('bounded page and explicit namespace required')
        db = sqlite3.connect(Path(state_path).resolve().as_uri()+'?mode=ro',uri=True)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            sql = "SELECT * FROM v1_identity WHERE namespace=? AND state='ACCEPTED'"
            args = [namespace]
            if after:
                sql += ' AND event_id>?'
                args.append(after)
            if simulation_run_id:
                sql += ' AND simulation_run_id=?'
                args.append(simulation_run_id)
            selected = db.execute(sql+' ORDER BY event_id LIMIT ?',(*args,limit)).fetchall()
            result = []
            for identity in selected:
                q = self.client.query('SELECT * FROM '+self.database+'.bronze_observations_v1 WHERE namespace={ns:String} AND event_id={eid:String} AND semantic_revision={rev:UInt64}',
                    parameters=dict(ns=namespace,eid=identity['event_id'],rev=identity['revision']))
                if not q.result_rows:
                    raise RuntimeError('missing accepted observation mirror')
                canonical = None
                for values in q.result_rows:
                    row = dict(zip(q.column_names,values))
                    for key,value in list(row.items()):
                        if isinstance(value,bytes):
                            row[key] = value.decode('ascii')
                    event = json.loads(row['observation_envelope_json'])
                    validate_observation(event)
                    hashed = hashlib.sha256(canonical_json(dict(observation=event,normalization_version=row['normalization_version'],bronze_schema_version=row['bronze_schema_version'])).encode()).hexdigest()
                    if hashed != identity['content_hash'] or row['bronze_canonical_hash'] != hashed:
                        raise RuntimeError('accepted observation content mismatch')
                    # Compare every field against the originally frozen obligation.
                    intent = db.execute('SELECT expected_json FROM v1_intents WHERE namespace=? AND raw_id=? AND processing_status=\'COMPLETED\'',(namespace,identity['first_raw_id'])).fetchone()
                    if intent is None:
                        raise RuntimeError('accepted observation has incomplete intent')
                    obligations = json.loads(intent[0])
                    if self.reconcile_v1_outputs(obligations):
                        raise RuntimeError('accepted observation lacks required lineage')
                    if canonical is not None and canonical != event:
                        raise RuntimeError('conflicting accepted observation retries')
                    canonical = event
                result.append(dict(event=canonical,semantic_revision=identity['revision'],change_sequence=identity['change_sequence'],bronze_canonical_hash=identity['content_hash']))
            return dict(rows=result,next_cursor=selected[-1]['event_id'] if selected else after)
        finally:
            db.close()

    def read_valid_observation(self, namespace, event_id, expected_revision, *, state_path=None):
        """Read one current ACCEPTED event with its frozen Bronze output obligations.

        The SQLite transaction pins semantic identity while the ClickHouse mirror
        is checked. A second identity check after reconciliation rejects a source
        transition committed during the external read.
        """
        import hashlib
        import json
        import sqlite3
        from pathlib import Path
        from contracts.traffic_observation import canonical_json, validate_observation

        if not namespace or not event_id or int(expected_revision) < 1:
            raise ValueError('namespace, event ID and positive revision required')
        authority_path = state_path or self.settings.checkpoint_path
        db = sqlite3.connect(Path(authority_path).resolve().as_uri() + '?mode=ro', uri=True,
                             isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            identity = db.execute(
                'SELECT * FROM v1_identity WHERE namespace=? AND event_id=?',
                (namespace, event_id)).fetchone()
            if identity is None or identity['state'] != 'ACCEPTED' or identity['revision'] != expected_revision:
                return None
            primary = db.execute(
                "SELECT * FROM v1_intents WHERE namespace=? AND raw_id=? AND processing_status='COMPLETED'",
                (namespace, identity['first_raw_id'])).fetchone()
            if primary is None:
                raise RuntimeError('accepted observation has no completed primary delivery')
            obligations = json.loads(primary['expected_json'])
            query = self.client.query(
                'SELECT * FROM ' + self.database + '.bronze_observations_v1 '
                'WHERE namespace={ns:String} AND event_id={eid:String} AND semantic_revision={rev:UInt64}',
                parameters=dict(ns=namespace, eid=event_id, rev=expected_revision))
            if not query.result_rows or self.reconcile_v1_outputs(obligations):
                raise RuntimeError('accepted observation mirror or lineage missing')
            event = None
            for values in query.result_rows:
                row = dict(zip(query.column_names, values))
                row = {key: value.decode('ascii') if isinstance(value, bytes) else value
                       for key, value in row.items()}
                candidate = json.loads(row['observation_envelope_json'])
                validate_observation(candidate)
                digest = hashlib.sha256(canonical_json(dict(
                    observation=candidate,
                    normalization_version=row['normalization_version'],
                    bronze_schema_version=row['bronze_schema_version'])).encode()).hexdigest()
                if (digest != identity['content_hash'] or
                        row['bronze_canonical_hash'] != digest or
                        candidate['eventId'] != event_id or
                        candidate['payloadHash'] != identity['payload_hash']):
                    raise RuntimeError('accepted observation content mismatch')
                if event is not None and event != candidate:
                    raise RuntimeError('conflicting accepted observation mirrors')
                event = candidate
            db.execute('COMMIT')
            # The CH query is outside SQLite's transaction domain. Reject a
            # concurrently committed conflict before returning the envelope.
            latest = db.execute('SELECT state,revision,content_hash FROM v1_identity '
                                'WHERE namespace=? AND event_id=?',
                                (namespace, event_id)).fetchone()
            if (latest is None or latest['state'] != 'ACCEPTED' or
                    latest['revision'] != expected_revision or
                    latest['content_hash'] != identity['content_hash']):
                return None
            return dict(event=event, semantic_revision=expected_revision,
                        change_sequence=identity['change_sequence'],
                        bronze_canonical_hash=identity['content_hash'],
                        primary_delivery=dict(primary))
        finally:
            if db.in_transaction:
                db.execute('ROLLBACK')
            db.close()

    def verify_tables(self) -> bool:
        for table in (
            "kafka_raw_events",
            "kafka_quarantine_events",
            "bronze_entity_events",
            "bronze_run_events",
            "bronze_quarantine",
        ):
            r = self.client.query(
                "SELECT count() FROM system.tables WHERE database={db:String} AND name={n:String}",
                parameters={"db": self.database, "n": table},
            )
            if not r.result_rows or int(r.result_rows[0][0]) < 1:
                return False
        return True

    def fetch_raw_one(self, topic: str, partition: int, offset: int) -> Optional[RawRow]:
        """Point lookup — not for processor hot path (use fetch_raw_batch)."""
        rows = self.fetch_raw_batch(topic, partition, offset, offset + 1, 1)
        return rows[0] if rows else None

    def fetch_raw_batch(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: int,
        batch_size: int,
    ) -> List[RawRow]:
        if start_offset >= end_offset or batch_size <= 0:
            return []
        sql = f"""
            SELECT topic, partition, offset, raw_ingestion_id, broker_timestamp, consumed_at,
                   payload_encoding, payload_stored, payload_bytes_hash,
                   event_id, event_type, simulation_run_id
            FROM {self.database}.kafka_raw_events
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
            ORDER BY offset
            LIMIT {{lim:UInt32}}
        """
        r = self.client.query(
            sql,
            parameters={
                "topic": topic,
                "part": int(partition),
                "start": int(start_offset),
                "end": int(end_offset),
                "lim": int(batch_size),
            },
        )
        return [_raw_from_row(row, r.column_names) for row in r.result_rows]

    def fetch_raw_quarantine_one(
        self, topic: str, partition: int, offset: int
    ) -> Optional[Dict[str, Any]]:
        """Point lookup — not for processor hot path (use fetch_raw_quarantine_batch)."""
        rows = self.fetch_raw_quarantine_batch(topic, partition, offset, offset + 1, 1)
        return rows[0] if rows else None

    def fetch_raw_quarantine_batch(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: int,
        batch_size: int,
    ) -> List[Dict[str, Any]]:
        if start_offset >= end_offset or batch_size <= 0:
            return []
        sql = f"""
            SELECT topic, partition, offset, raw_ingestion_id, payload_bytes_hash
            FROM {self.database}.kafka_quarantine_events
            WHERE topic={{topic:String}} AND partition={{part:Int32}}
              AND offset >= {{start:Int64}} AND offset < {{end:Int64}}
            ORDER BY offset
            LIMIT {{lim:UInt32}}
        """
        r = self.client.query(
            sql,
            parameters={
                "topic": topic,
                "part": int(partition),
                "start": int(start_offset),
                "end": int(end_offset),
                "lim": int(batch_size),
            },
        )
        cols = r.column_names
        out: List[Dict[str, Any]] = []
        for row in r.result_rows:
            d = {cols[i]: row[i] for i in range(len(cols))}
            if "raw_ingestion_id" in d:
                d["raw_ingestion_id"] = _as_hex64(d["raw_ingestion_id"])
            if "payload_bytes_hash" in d:
                d["payload_bytes_hash"] = _as_hex64(d["payload_bytes_hash"])
            out.append(d)
        return out

    def source_max_offset(self, topic: str, partition: int) -> Optional[int]:
        sql = f"""
            SELECT max(offset) FROM (
                SELECT offset FROM {self.database}.kafka_raw_events
                WHERE topic={{topic:String}} AND partition={{part:Int32}}
                UNION ALL
                SELECT offset FROM {self.database}.kafka_quarantine_events
                WHERE topic={{topic:String}} AND partition={{part:Int32}}
            )
        """
        r = self.client.query(
            sql, parameters={"topic": topic, "part": int(partition)}
        )
        if not r.result_rows or r.result_rows[0][0] is None:
            return None
        return int(r.result_rows[0][0])

    def min_source_offset(self, topic: str, partition: int) -> Optional[int]:
        sql = f"""
            SELECT min(offset) FROM (
                SELECT offset FROM {self.database}.kafka_raw_events
                WHERE topic={{topic:String}} AND partition={{part:Int32}}
                UNION ALL
                SELECT offset FROM {self.database}.kafka_quarantine_events
                WHERE topic={{topic:String}} AND partition={{part:Int32}}
            )
        """
        r = self.client.query(
            sql, parameters={"topic": topic, "part": int(partition)}
        )
        if not r.result_rows or r.result_rows[0][0] is None:
            return None
        return int(r.result_rows[0][0])

    def find_existing_raw_ingestion_ids(self, ids: Sequence[str]) -> Set[str]:
        out: Set[str] = set()
        if not ids:
            return out
        uniq = list(dict.fromkeys(ids))
        for i in range(0, len(uniq), 500):
            chunk = uniq[i : i + 500]
            for table in (
                "bronze_entity_events",
                "bronze_run_events",
                "bronze_quarantine",
            ):
                sql = f"""
                    SELECT raw_ingestion_id FROM {self.database}.{table}
                    WHERE has({{ids:Array(String)}}, toString(raw_ingestion_id))
                """
                r = self.client.query(sql, parameters={"ids": chunk})
                for row in r.result_rows:
                    out.add(str(row[0]))
        return out

    def event_id_exists_at_different_offset(
        self, event_id: str, entity_payload_hash: str, topic: str, partition: int, offset: int
    ) -> bool:
        dups = self.upstream_duplicate_offsets(
            [(event_id, entity_payload_hash, topic, partition, offset)]
        )
        return offset in dups

    def upstream_duplicate_offsets(
        self,
        entities: Sequence[tuple[str, str, str, int, int]],
    ) -> Set[int]:
        """Return batch offsets where (event_id, hash) exists at a different CH row."""
        if not entities:
            return set()
        event_ids = list({e[0] for e in entities})
        sql = f"""
            SELECT event_id, entity_payload_hash, topic, partition, offset
            FROM {self.database}.bronze_entity_events
            WHERE event_id IN {{ids:Array(String)}}
        """
        r = self.client.query(sql, parameters={"ids": event_ids})
        locations: Dict[tuple[str, str], List[tuple[str, int, int]]] = {}
        for row in r.result_rows:
            eid = _as_hex64(row[0]) if row[0] is not None else str(row[0])
            eh = _as_hex64(row[1]) if row[1] is not None else str(row[1])
            key = (eid, eh)
            locations.setdefault(key, []).append((str(row[2]), int(row[3]), int(row[4])))
        dup_offsets: Set[int] = set()
        for eid, eh, topic, part, off in entities:
            for t, p, o in locations.get((eid, eh), []):
                if not (t == topic and p == part and o == off):
                    dup_offsets.add(int(off))
                    break
        return dup_offsets

    def insert_entity_batch(
        self, rows: Sequence[Dict[str, Any]], *, replay_run_id: Optional[str] = None
    ) -> None:
        if not rows:
            return
        table = (
            f"{self.database}.bronze_entity_events_replay"
            if replay_run_id
            else f"{self.database}.bronze_entity_events"
        )
        cols = list(ENTITY_COLS)
        data = [[_cell(r, c) for c in ENTITY_COLS] for r in rows]
        if replay_run_id:
            cols = cols + ["replay_run_id"]
            data = [row + [replay_run_id] for row in data]
        self.client.insert(table, data, column_names=cols)

    def insert_run_batch(
        self, rows: Sequence[Dict[str, Any]], *, replay_run_id: Optional[str] = None
    ) -> None:
        if not rows:
            return
        table = (
            f"{self.database}.bronze_run_events_replay"
            if replay_run_id
            else f"{self.database}.bronze_run_events"
        )
        cols = list(RUN_COLS)
        data = [[_cell(r, c) for c in RUN_COLS] for r in rows]
        if replay_run_id:
            cols = cols + ["replay_run_id"]
            data = [row + [replay_run_id] for row in data]
        self.client.insert(table, data, column_names=cols)

    def insert_quarantine_batch(
        self, rows: Sequence[Dict[str, Any]], *, replay_run_id: Optional[str] = None
    ) -> None:
        if not rows:
            return
        table = (
            f"{self.database}.bronze_quarantine_replay"
            if replay_run_id
            else f"{self.database}.bronze_quarantine"
        )
        cols = list(QUAR_COLS)
        data = [[_cell(r, c) for c in QUAR_COLS] for r in rows]
        if replay_run_id:
            cols = cols + ["replay_run_id"]
            data = [row + [replay_run_id] for row in data]
        self.client.insert(table, data, column_names=cols)


FIXED64_COLS = frozenset(
    {
        "raw_ingestion_id",
        "payload_bytes_hash",
        "event_id",
        "entity_payload_hash",
        "bronze_canonical_hash",
        "bronze_ingestion_id",
    }
)


def _as_hex64(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("ascii")
    text = str(value)
    if text.startswith("b'") and text.endswith("'"):
        return text[2:-1]
    if text.startswith('b"') and text.endswith('"'):
        return text[2:-1]
    return text


def _raw_from_row(row: tuple, cols: List[str]) -> RawRow:
    d = {cols[i]: row[i] for i in range(len(cols))}
    return RawRow(
        topic=str(d["topic"]),
        partition=int(d["partition"]),
        offset=int(d["offset"]),
        raw_ingestion_id=_as_hex64(d["raw_ingestion_id"]),
        broker_timestamp=d["broker_timestamp"],
        consumed_at=d["consumed_at"],
        payload_encoding=str(d["payload_encoding"]),
        payload_stored=str(d["payload_stored"]),
        payload_bytes_hash=_as_hex64(d["payload_bytes_hash"]),
        event_id=_as_hex64(d["event_id"]) if d.get("event_id") else None,
        event_type=d.get("event_type"),
        simulation_run_id=d.get("simulation_run_id"),
    )


def _cell(row: Dict[str, Any], col: str) -> Any:
    if col == "migration_version":
        return row.get(col) or MIGRATION_VERSION
    v = row.get(col)
    if col in FIXED64_COLS and v is not None:
        return _as_hex64(v)
    if isinstance(v, datetime):
        return v
    return v
