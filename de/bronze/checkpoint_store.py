"""SQLite checkpoint + processing ledger (namespace-aware)."""
from __future__ import annotations

import sqlite3
import json
import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS bronze_instance_lock (
    lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1),
    holder_pid INTEGER NOT NULL,
    holder_host TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze_checkpoint (
    checkpoint_namespace TEXT NOT NULL DEFAULT 'live',
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    last_completed_offset INTEGER NOT NULL DEFAULT -1,
    source_start_offset INTEGER NOT NULL DEFAULT -1,
    start_mode TEXT NOT NULL DEFAULT 'earliest',
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    bronze_schema_version TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (checkpoint_namespace, topic, partition_id)
);

CREATE TABLE IF NOT EXISTS bronze_processing_ledger (
    checkpoint_namespace TEXT NOT NULL DEFAULT 'live',
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    raw_ingestion_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    status TEXT NOT NULL,
    bronze_ingestion_id TEXT,
    payload_hash TEXT,
    last_error TEXT,
    completed_at TEXT NOT NULL,
    PRIMARY KEY (checkpoint_namespace, topic, partition_id, offset_value),
    UNIQUE (checkpoint_namespace, raw_ingestion_id)
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class CheckpointRow:
    checkpoint_namespace: str
    topic: str
    partition_id: int
    last_completed_offset: int
    source_start_offset: int
    start_mode: str
    processor_name: str
    processor_version: str
    bronze_schema_version: str
    updated_at: str


class CheckpointStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get(
        self, namespace: str, topic: str, partition: int
    ) -> Optional[CheckpointRow]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM bronze_checkpoint
                WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                """,
                (namespace, topic, int(partition)),
            ).fetchone()
            if not row:
                return None
            return CheckpointRow(
                checkpoint_namespace=row["checkpoint_namespace"],
                topic=row["topic"],
                partition_id=int(row["partition_id"]),
                last_completed_offset=int(row["last_completed_offset"]),
                source_start_offset=int(row["source_start_offset"]),
                start_mode=row["start_mode"],
                processor_name=row["processor_name"],
                processor_version=row["processor_version"],
                bronze_schema_version=row["bronze_schema_version"],
                updated_at=row["updated_at"],
            )

    def init_checkpoint(
        self,
        *,
        namespace: str,
        topic: str,
        partition: int,
        source_start_offset: int,
        last_completed_offset: int,
        start_mode: str,
        processor_name: str,
        processor_version: str,
        bronze_schema_version: str,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO bronze_checkpoint (
                    checkpoint_namespace, topic, partition_id,
                    last_completed_offset, source_start_offset, start_mode,
                    processor_name, processor_version, bronze_schema_version, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(checkpoint_namespace, topic, partition_id) DO NOTHING
                """,
                (
                    namespace,
                    topic,
                    int(partition),
                    int(last_completed_offset),
                    int(source_start_offset),
                    start_mode,
                    processor_name,
                    processor_version,
                    bronze_schema_version,
                    now,
                ),
            )

    def advance(
        self, namespace: str, topic: str, partition: int, last_completed: int
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE bronze_checkpoint
                SET last_completed_offset=?, updated_at=?
                WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                """,
                (int(last_completed), now, namespace, topic, int(partition)),
            )

    def is_complete(
        self, namespace: str, topic: str, partition: int, offset: int
    ) -> bool:
        return offset in self.is_complete_batch(namespace, topic, partition, [offset])

    def is_complete_batch(
        self,
        namespace: str,
        topic: str,
        partition: int,
        offsets: List[int],
    ) -> set[int]:
        if not offsets:
            return set()
        with self._lock:
            placeholders = ",".join("?" * len(offsets))
            rows = self._conn.execute(
                f"""
                SELECT offset_value FROM bronze_processing_ledger
                WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                  AND offset_value IN ({placeholders})
                """,
                (namespace, topic, int(partition), *map(int, offsets)),
            ).fetchall()
            return {int(r[0]) for r in rows}

    def mark_complete(
        self,
        *,
        namespace: str,
        topic: str,
        partition: int,
        offset: int,
        raw_ingestion_id: str,
        destination: str,
        status: str,
        payload_hash: Optional[str] = None,
        bronze_ingestion_id: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO bronze_processing_ledger (
                    checkpoint_namespace, topic, partition_id, offset_value,
                    raw_ingestion_id, destination, status,
                    bronze_ingestion_id, payload_hash, last_error, completed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,NULL,?)
                ON CONFLICT(checkpoint_namespace, topic, partition_id, offset_value) DO UPDATE SET
                    raw_ingestion_id=excluded.raw_ingestion_id,
                    destination=excluded.destination,
                    status=excluded.status,
                    bronze_ingestion_id=excluded.bronze_ingestion_id,
                    payload_hash=excluded.payload_hash,
                    completed_at=excluded.completed_at
                """,
                (
                    namespace,
                    topic,
                    int(partition),
                    int(offset),
                    raw_ingestion_id,
                    destination,
                    status,
                    bronze_ingestion_id,
                    payload_hash,
                    now,
                ),
            )

    def commit_batch(
        self,
        namespace: str,
        topic: str,
        partition: int,
        entries: List[Dict[str, Any]],
        last_completed: int,
    ) -> None:
        now = _utc_now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for e in entries:
                    self._conn.execute(
                        """
                        INSERT INTO bronze_processing_ledger (
                            checkpoint_namespace, topic, partition_id, offset_value,
                            raw_ingestion_id, destination, status,
                            bronze_ingestion_id, payload_hash, last_error, completed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,NULL,?)
                        ON CONFLICT(checkpoint_namespace, topic, partition_id, offset_value)
                        DO UPDATE SET
                            status=excluded.status,
                            completed_at=excluded.completed_at
                        """,
                        (
                            namespace,
                            topic,
                            int(partition),
                            int(e["offset"]),
                            e["raw_ingestion_id"],
                            e["destination"],
                            e["status"],
                            e.get("bronze_ingestion_id"),
                            e.get("payload_hash"),
                            now,
                        ),
                    )
                self._conn.execute(
                    """
                    UPDATE bronze_checkpoint
                    SET last_completed_offset=?, updated_at=?
                    WHERE checkpoint_namespace=? AND topic=? AND partition_id=?
                    """,
                    (int(last_completed), now, namespace, topic, int(partition)),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise


V1_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS v1_checkpoint (
 namespace TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
 topic_identity TEXT NOT NULL, source_binding TEXT NOT NULL,
 next_source_offset INTEGER NOT NULL, safe_next_offset INTEGER NOT NULL,
 generation INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(namespace,topic,partition_id),
 CHECK(next_source_offset <= safe_next_offset));
CREATE TABLE IF NOT EXISTS v1_identity (
 namespace TEXT NOT NULL, event_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('PENDING','ACCEPTED','CONFLICTED')),
 revision INTEGER NOT NULL, envelope_hash TEXT NOT NULL, payload_hash TEXT NOT NULL,
 content_hash TEXT NOT NULL, first_raw_id TEXT NOT NULL,
 simulation_run_id TEXT NOT NULL, intersection_id TEXT NOT NULL,
 cycle_sequence INTEGER NOT NULL, simulation_time_seconds REAL NOT NULL,
 topology_digest TEXT NOT NULL, change_sequence INTEGER,
 PRIMARY KEY(namespace,event_id), UNIQUE(namespace,change_sequence));
CREATE TABLE IF NOT EXISTS v1_sequence (
 namespace TEXT PRIMARY KEY, next_change INTEGER NOT NULL DEFAULT 1,
 next_invalidation INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS v1_changes (
 namespace TEXT NOT NULL, change_sequence INTEGER NOT NULL,
 event_id TEXT NOT NULL, revision INTEGER NOT NULL,
 change_type TEXT NOT NULL CHECK(change_type IN ('ACCEPTED','CONFLICTED')),
 previous_state TEXT, new_state TEXT NOT NULL, content_hash TEXT NOT NULL,
 invalidation_id TEXT, created_at TEXT NOT NULL,
 PRIMARY KEY(namespace,change_sequence), UNIQUE(namespace,event_id,revision));
CREATE TABLE IF NOT EXISTS v1_intents (
 namespace TEXT NOT NULL, raw_id TEXT NOT NULL,
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_value INTEGER NOT NULL,
 topic_identity TEXT NOT NULL, event_id TEXT, disposition TEXT NOT NULL,
 processing_status TEXT NOT NULL CHECK(processing_status IN
 ('PREPARED','PENDING_PERSISTENCE','PERSISTENCE_CONFIRMED','COMPLETED','FAILED_RETRYABLE','FAILED_INTEGRITY')),
 expected_json TEXT NOT NULL, expected_hash TEXT NOT NULL,
 revision INTEGER, attempt_count INTEGER NOT NULL DEFAULT 0,
 last_error TEXT, prepared_at TEXT NOT NULL, completed_at TEXT,
 PRIMARY KEY(namespace,raw_id), UNIQUE(namespace,topic,partition_id,offset_value));
CREATE TABLE IF NOT EXISTS v1_conflicts (
 namespace TEXT NOT NULL, conflict_id TEXT NOT NULL, event_id TEXT NOT NULL,
 first_raw_id TEXT NOT NULL, conflicting_raw_id TEXT NOT NULL,
 previous_hash TEXT NOT NULL, conflicting_hash TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(namespace,conflict_id));
CREATE TABLE IF NOT EXISTS v1_invalidations (
 namespace TEXT NOT NULL, invalidation_id TEXT NOT NULL, invalidation_sequence INTEGER NOT NULL,
 event_id TEXT NOT NULL, revision INTEGER NOT NULL,
 first_raw_id TEXT NOT NULL, conflicting_raw_id TEXT NOT NULL,
 previous_hash TEXT NOT NULL, conflicting_hash TEXT NOT NULL,
 mirror_status TEXT NOT NULL DEFAULT 'PENDING', created_at TEXT NOT NULL,
 PRIMARY KEY(namespace,invalidation_id), UNIQUE(namespace,invalidation_sequence));
CREATE TABLE IF NOT EXISTS v1_faults (
 namespace TEXT NOT NULL, raw_id TEXT NOT NULL, detail TEXT NOT NULL,
 created_at TEXT NOT NULL, PRIMARY KEY(namespace,raw_id));
CREATE INDEX IF NOT EXISTS v1_intent_status ON v1_intents(namespace,processing_status);
CREATE INDEX IF NOT EXISTS v1_change_event ON v1_changes(namespace,event_id,revision);
"""


class V1SemanticStore:
    """One local SQLite authority for V1 semantic and processing state."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(V1_SCHEMA)

    def close(self):
        self.db.close()

    def current(self, namespace: str, event_id: str):
        return self.db.execute(
            "SELECT * FROM v1_identity WHERE namespace=? AND event_id=?",
            (namespace, event_id)).fetchone()

    def next_sequence(self, namespace: str, kind: str) -> int:
        if not self.db.in_transaction:
            raise RuntimeError('sequence allocation requires publication transaction')
        if kind not in ("change", "invalidation"):
            raise ValueError("unknown sequence")
        self.db.execute("INSERT OR IGNORE INTO v1_sequence(namespace) VALUES (?)", (namespace,))
        column = "next_" + kind
        row = self.db.execute(
            f"UPDATE v1_sequence SET {column}={column}+1 WHERE namespace=? RETURNING {column}-1",
            (namespace,)).fetchone()
        return int(row[0])

    def begin(self):
        self.db.execute("BEGIN IMMEDIATE")

    def commit(self):
        self.db.execute("COMMIT")

    def rollback(self):
        self.db.execute("ROLLBACK")

    def pending(self, namespace: str):
        return self.db.execute("SELECT * FROM v1_intents WHERE namespace=? AND processing_status!='COMPLETED' ORDER BY prepared_at,raw_id", (namespace,)).fetchall()

    def mark_attempt(self, namespace, raw_id, error=None, integrity=False):
        state = 'FAILED_INTEGRITY' if integrity else ('FAILED_RETRYABLE' if error else 'PENDING_PERSISTENCE')
        self.begin()
        try:
            intent = self.db.execute('SELECT processing_status FROM v1_intents WHERE namespace=? AND raw_id=?',(namespace,raw_id)).fetchone()
            if not intent or intent[0] in ('COMPLETED','FAILED_INTEGRITY'):
                raise RuntimeError('intent cannot be retried')
            # Count a persistence attempt once, when it starts. Recording its
            # outcome must not increment the same attempt a second time.
            increment = 1 if error is None and not integrity else 0
            self.db.execute('UPDATE v1_intents SET processing_status=?,attempt_count=attempt_count+?,last_error=? WHERE namespace=? AND raw_id=?',(state,increment,error,namespace,raw_id))
            if integrity:
                self.db.execute('INSERT OR IGNORE INTO v1_faults VALUES (?,?,?,?)',(namespace,raw_id,error,_utc_now()))
            self.commit()
        except BaseException:
            self.rollback()
            raise

    def read_page(self, namespace, kind, after=0, limit=500, high_water=None):
        """Read one durable journal page under a read transaction."""
        if kind not in ('changes','invalidations') or not 1 <= limit <= 1000:
            raise ValueError('invalid journal/page size')
        table = 'v1_'+kind
        sequence = 'change_sequence' if kind == 'changes' else 'invalidation_sequence'
        self.db.execute('BEGIN')
        try:
            latest = self.db.execute(f'SELECT coalesce(max({sequence}),0) FROM {table} WHERE namespace=?',(namespace,)).fetchone()[0]
            boundary = latest if high_water is None else min(high_water,latest)
            rows = [dict(r) for r in self.db.execute(f'SELECT * FROM {table} WHERE namespace=? AND {sequence}>? AND {sequence}<=? ORDER BY {sequence} LIMIT ?',(namespace,after,boundary,limit)).fetchall()]
            self.commit()
            cursor = rows[-1][sequence] if rows else after
            return dict(rows=rows,next_cursor=cursor,high_water=boundary,has_more=cursor<boundary)
        except BaseException:
            self.rollback()
            raise

    def prepare(self, namespace, raw, event, envelope_hash, content_hash, outputs):
        """Freeze one semantic operation; callers recover prior operations first.

        outputs(disposition, revision, conflict, invalidation) creates immutable
        destination/content obligations inside the preparation transaction.
        """
        from contracts.traffic_observation import canonical_json
        rid = raw['raw_ingestion_id']
        self.begin()
        try:
            old = self.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND raw_id=?', (namespace,rid)).fetchone()
            if old:
                self.commit()
                return dict(old)
            eid = event['eventId'] if event else None
            identity = self.current(namespace,eid) if eid else None
            if eid and self.db.execute("SELECT 1 FROM v1_intents WHERE namespace=? AND event_id=? AND processing_status!='COMPLETED'", (namespace,eid)).fetchone():
                raise RuntimeError('recover earlier semantic intent before classification')
            revision = identity['revision'] if identity else 1
            conflict = invalidation = None
            now = _utc_now()
            if not event:
                disposition = 'RAW_QUARANTINE_SKIPPED'
            elif identity is None:
                disposition = 'ACCEPTED_NEW'
                self.db.execute('INSERT INTO v1_identity VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (namespace,eid,'PENDING',1,envelope_hash,event['payloadHash'],content_hash,rid,
                     event['simulationRunId'],event['intersectionId'],event['cycleSequence'],
                     event['simulationTimeSeconds'],event['topologyDigest'],None))
            elif identity['state'] == 'CONFLICTED':
                disposition = 'ALREADY_CONFLICTED'
            elif identity['envelope_hash'] == envelope_hash and identity['payload_hash'] == event['payloadHash'] and identity['content_hash'] == content_hash:
                disposition = 'SEMANTIC_DUPLICATE'
            else:
                disposition = 'SEMANTIC_CONFLICT'
                revision += 1
                cid = hashlib.sha256(canonical_json([namespace,eid,revision,rid,envelope_hash]).encode()).hexdigest()
                iid = hashlib.sha256(('invalidation-1|'+cid).encode()).hexdigest()
                sequence = self.next_sequence(namespace,'change')
                inv_sequence = self.next_sequence(namespace,'invalidation')
                conflict = dict(conflict_id=cid,event_id=eid,first_raw_id=identity['first_raw_id'],
                    conflicting_raw_id=rid,previous_hash=identity['envelope_hash'],conflicting_hash=envelope_hash,created_at=now)
                invalidation = dict(invalidation_id=iid,invalidation_sequence=inv_sequence,
                    event_id=eid,revision=revision,first_raw_id=identity['first_raw_id'],conflicting_raw_id=rid,
                    previous_hash=identity['envelope_hash'],conflicting_hash=envelope_hash,created_at=now,
                    simulation_run_id=identity['simulation_run_id'],intersection_id=identity['intersection_id'],
                    cycle_sequence=identity['cycle_sequence'],simulation_time_seconds=identity['simulation_time_seconds'],
                    topology_digest=identity['topology_digest'])
                self.db.execute('INSERT INTO v1_conflicts VALUES (?,?,?,?,?,?,?,?)',
                    (namespace,cid,eid,identity['first_raw_id'],rid,identity['envelope_hash'],envelope_hash,now))
                self.db.execute('INSERT INTO v1_invalidations VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (namespace,iid,inv_sequence,eid,revision,identity['first_raw_id'],rid,identity['envelope_hash'],envelope_hash,'PENDING',now))
                self.db.execute("UPDATE v1_identity SET state='CONFLICTED',revision=?,change_sequence=? WHERE namespace=? AND event_id=? AND state='ACCEPTED'",
                    (revision,sequence,namespace,eid))
                self.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (namespace,sequence,eid,revision,'CONFLICTED','ACCEPTED','CONFLICTED',identity['content_hash'],iid,now))
            expected = canonical_json(outputs(disposition,revision,conflict,invalidation))
            self.db.execute('''INSERT INTO v1_intents
                (namespace,raw_id,topic,partition_id,offset_value,topic_identity,event_id,disposition,
                 processing_status,expected_json,expected_hash,revision,prepared_at)
                VALUES (?,?,?,?,?,?,?,?,'PREPARED',?,?,?,?)''',
                (namespace,rid,raw['topic'],raw['partition'],raw['offset'],raw['topic_identity'],eid,
                 disposition,expected,hashlib.sha256(expected.encode()).hexdigest(),revision,now))
            self.commit()
            return dict(self.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND raw_id=?',(namespace,rid)).fetchone())
        except BaseException:
            self.rollback()
            raise

    def complete(self, namespace, raw_id, confirmed_output_hash):
        """Publish ACCEPTED only after caller positively reconciles all outputs."""
        self.begin()
        try:
            intent = self.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND raw_id=?',(namespace,raw_id)).fetchone()
            if not intent or intent['expected_hash'] != confirmed_output_hash:
                raise RuntimeError('persistence confirmation does not match frozen obligations')
            if intent['processing_status'] == 'FAILED_INTEGRITY':
                raise RuntimeError('integrity-faulted intent cannot complete')
            if intent['processing_status'] == 'COMPLETED':
                self.commit()
                return
            now = _utc_now()
            if intent['disposition'] == 'ACCEPTED_NEW':
                identity = self.current(namespace,intent['event_id'])
                if identity['state'] != 'PENDING':
                    raise RuntimeError('reserved observation no longer eligible for acceptance')
                sequence = self.next_sequence(namespace,'change')
                self.db.execute("UPDATE v1_identity SET state='ACCEPTED',change_sequence=? WHERE namespace=? AND event_id=?",(sequence,namespace,intent['event_id']))
                self.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
                    (namespace,sequence,intent['event_id'],intent['revision'],'ACCEPTED','PENDING','ACCEPTED',identity['content_hash'],None,now))
            self.db.execute("UPDATE v1_intents SET processing_status='COMPLETED',completed_at=?,last_error=NULL WHERE namespace=? AND raw_id=?",(now,namespace,raw_id))
            if intent['disposition'] == 'SEMANTIC_CONFLICT':
                self.db.execute("UPDATE v1_invalidations SET mirror_status='MIRRORED' WHERE namespace=? AND conflicting_raw_id=?",(namespace,raw_id))
            self.commit()
        except BaseException:
            self.rollback()
            raise

    def advance_v1(self, namespace, topic, partition, identity, binding, generation, expected_next, new_next, safe):
        """CAS a cursor after the caller established complete Raw range coverage.

        This rejects known incomplete intents in [expected_next,new_next), but it
        cannot prove that the caller discovered every Raw delivery. The bounded
        logical Raw reader owns that source-completeness proof.
        """
        self.begin()
        try:
            if new_next < expected_next or new_next > safe:
                raise RuntimeError('unsafe checkpoint boundary')
            if self.db.execute("SELECT 1 FROM v1_intents WHERE namespace=? AND topic=? AND partition_id=? AND offset_value>=? AND offset_value<? AND processing_status!='COMPLETED'",(namespace,topic,partition,expected_next,new_next)).fetchone():
                raise RuntimeError('incomplete persistence obligations')
            updated = self.db.execute('''UPDATE v1_checkpoint SET next_source_offset=?,safe_next_offset=?,generation=generation+1
                WHERE namespace=? AND topic=? AND partition_id=? AND topic_identity=? AND source_binding=?
                AND generation=? AND next_source_offset=? AND safe_next_offset<=?''',
                (new_next,safe,namespace,topic,partition,identity,binding,generation,expected_next,safe))
            if updated.rowcount != 1:
                raise RuntimeError('checkpoint CAS failed')
            self.commit()
        except BaseException:
            self.rollback()
            raise


class V1SemanticReader(V1SemanticStore):
    """Downstream read-only access without schema initialization or WAL writes."""
    def __init__(self, path):
        self.path = Path(path)
        self.db = sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA query_only=ON')

    def read_delivery_lineage(self, namespace, event_id, after_raw_id='', limit=500):
        if not 1 <= limit <= 1000:
            raise ValueError('bounded lineage page required')
        return [dict(r) for r in self.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND event_id=? AND raw_id>? ORDER BY raw_id LIMIT ?',
            (namespace,event_id,after_raw_id,limit)).fetchall()]

    def read_invalidation_provenance(self, namespace, invalidation_id):
        """Return authority-owned invalidation context and both source deliveries."""
        row=self.db.execute('''SELECT v.*,i.simulation_run_id,i.intersection_id,
            i.cycle_sequence,i.simulation_time_seconds,i.topology_digest,i.content_hash,
            i.change_sequence
            FROM v1_invalidations v JOIN v1_identity i
              ON i.namespace=v.namespace AND i.event_id=v.event_id
            WHERE v.namespace=? AND v.invalidation_id=?''',(namespace,invalidation_id)).fetchone()
        if row is None:
            return None
        result=dict(row)
        original=self.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND raw_id=?',(namespace,row['first_raw_id'])).fetchone()
        conflicting=self.db.execute('SELECT * FROM v1_intents WHERE namespace=? AND raw_id=?',(namespace,row['conflicting_raw_id'])).fetchone()
        if original is None or conflicting is None:
            raise RuntimeError('invalidation source lineage missing')
        result['original_delivery']=dict(original)
        result['conflicting_delivery']=dict(conflicting)
        return result
