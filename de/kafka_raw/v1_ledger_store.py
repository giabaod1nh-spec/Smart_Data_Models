"""Durable physical delivery ledger and read-only downstream confirmation."""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

class IntegrityFault(RuntimeError):
    pass

class V1LedgerReader:
    """Read-only downstream connection; never initializes schema or changes Raw state."""
    def __init__(self, path):
        self.db=sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True)
        self.db.row_factory=sqlite3.Row
        self.db.execute("PRAGMA query_only=ON")

    def close(self):
        self.db.close()

class V1LedgerStore:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS deliveries(
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_value INTEGER NOT NULL,
 raw_ingestion_id TEXT NOT NULL UNIQUE, physical_record_hash TEXT NOT NULL,
 destination TEXT NOT NULL CHECK(destination IN ('RAW','QUARANTINE')),
 state TEXT NOT NULL CHECK(state IN ('RECEIVED','PERSISTENCE_ATTEMPTED','PERSISTENCE_CONFIRMED','COMMIT_ELIGIBLE','COMMITTED','FAILED_RETRYABLE','FAILED_INTEGRITY')),
 row_json TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
 batch_id TEXT, confirmed_hash TEXT, last_error TEXT, updated_at TEXT NOT NULL,
 PRIMARY KEY(topic,partition_id,offset_value),
 CHECK(state NOT IN ('PERSISTENCE_CONFIRMED','COMMIT_ELIGIBLE','COMMITTED')
       OR (confirmed_hash IS NOT NULL AND confirmed_hash=physical_record_hash)));
CREATE TABLE IF NOT EXISTS partition_progress(
 topic TEXT NOT NULL, partition_id INTEGER NOT NULL, topic_identity TEXT NOT NULL,
 committed_next_offset INTEGER, safe_next_offset INTEGER,
 PRIMARY KEY(topic,partition_id));
CREATE TABLE IF NOT EXISTS batches(
 batch_id TEXT PRIMARY KEY,state TEXT NOT NULL,attempt_count INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS integrity_faults(
 raw_ingestion_id TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS delivery_state ON deliveries(topic,partition_id,state,offset_value);
""")

    def get(self, topic, partition, offset):
        r = self.db.execute("SELECT * FROM deliveries WHERE topic=? AND partition_id=? AND offset_value=?",
                            (topic, partition, offset)).fetchone()
        return dict(r) if r else None

    def receive_batch(self, records, batch_id):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("INSERT OR IGNORE INTO batches VALUES (?,'RECEIVED',0)", (batch_id,))
            for classified in records:
                row = classified.row
                old = self.get(row["topic"], row["partition"], row["offset"])
                if old and (old["physical_record_hash"] != row["physical_record_hash"] or old["destination"] != classified.destination):
                    raise IntegrityFault("physical delivery changed")
                self.db.execute("""INSERT OR IGNORE INTO deliveries
(topic,partition_id,offset_value,raw_ingestion_id,physical_record_hash,destination,state,row_json,batch_id,updated_at)
VALUES(?,?,?,?,?,?,'RECEIVED',?,?,?)""",
                    (row["topic"],row["partition"],row["offset"],row["raw_ingestion_id"],
                     row["physical_record_hash"],classified.destination,
                     json.dumps(row,default=str),batch_id,self.now()))
            self.db.execute("COMMIT")
        except BaseException as exc:
            self.db.execute("ROLLBACK")
            if isinstance(exc,IntegrityFault):
                self.db.execute("INSERT INTO integrity_faults VALUES(?,?,?)",("receipt-conflict",str(exc),self.now()))
            raise

    def transition(self, ids, state, error=None):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for rid in ids:
                old = self.db.execute("SELECT * FROM deliveries WHERE raw_ingestion_id=?", (rid,)).fetchone()
                if old is None:
                    raise IntegrityFault("unknown delivery")
                if old["state"] == "FAILED_INTEGRITY":
                    raise IntegrityFault("faulted delivery")
                confirmed = old["confirmed_hash"]
                if state == "COMMITTED" and old["state"] not in ("COMMIT_ELIGIBLE","COMMITTED","PERSISTENCE_CONFIRMED"):
                    raise IntegrityFault("commit requires confirmed eligible delivery")
                if state == "PERSISTENCE_CONFIRMED":
                    confirmed = old["physical_record_hash"]
                if state in ("COMMIT_ELIGIBLE","COMMITTED") and not confirmed:
                    raise IntegrityFault("unconfirmed commit")
                self.db.execute("""UPDATE deliveries SET state=?,confirmed_hash=?,last_error=?,updated_at=?,
attempt_count=attempt_count+? WHERE raw_ingestion_id=?""",
                    (state,confirmed,error,self.now(),int(state=="PERSISTENCE_ATTEMPTED"),rid))
                self.db.execute("UPDATE batches SET state=?,attempt_count=attempt_count+? WHERE batch_id=?",
                                (state,int(state=="PERSISTENCE_ATTEMPTED"),old["batch_id"]))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    @staticmethod
    def now():
        return datetime.now(timezone.utc).isoformat()

    def close(self):
        self.db.close()

