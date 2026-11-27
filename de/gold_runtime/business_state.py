"""Stage A durable handoff for the isolated v2 Gold store.

The active Core entrypoint still opens its v1 store. Use a disposable checkpoint
and the existing instance lease before constructing a BusinessState.
"""

import hashlib
import json
import shutil
import time

from de.gold_runtime.checkpoint_store import GoldCoreStore, CoreStateFault
from de.silver.incremental_contract import canonical_bytes, canonical_hash

_PROJECTION_LIMIT = 256 * 1024
_RESULT_LIMIT = _MANIFEST_LIMIT = 32 * 1024
_RESERVATION = _PROJECTION_LIMIT + _RESULT_LIMIT + _MANIFEST_LIMIT
_TASK_LIMIT = 16
_PENDING_LIMIT = 8 * 1024 * 1024
_PROTECTED_FREE = (64 + 32) * 1024 * 1024

_SCHEMA = """
CREATE TABLE business_reservation (
 window_id TEXT NOT NULL,
 generation INTEGER NOT NULL,
 lease_token TEXT NOT NULL,
 reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>0),
 PRIMARY KEY(window_id,generation),
 FOREIGN KEY(window_id,generation) REFERENCES core_work(window_id,generation)
);
CREATE TABLE business_hold (
 window_id TEXT NOT NULL,
 generation INTEGER NOT NULL,
 reason TEXT NOT NULL,
 since REAL NOT NULL,
 PRIMARY KEY(window_id,generation),
 FOREIGN KEY(window_id,generation) REFERENCES core_work(window_id,generation)
);
CREATE INDEX business_hold_by_reason ON business_hold(reason,since,window_id);
CREATE TABLE business_obligation (
 id TEXT PRIMARY KEY, window_id TEXT NOT NULL, generation INTEGER NOT NULL,
 work_id TEXT NOT NULL, input_hash TEXT NOT NULL, binding_id TEXT NOT NULL, scope_id TEXT NOT NULL,
 definition_set TEXT NOT NULL, source_semantics TEXT NOT NULL, coverage_policy TEXT NOT NULL,
 population_digest TEXT NOT NULL, metadata_digest TEXT NOT NULL, codec INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('PENDING','COMPUTED','SUPERSEDED')),
 projection BLOB NOT NULL, projection_hash TEXT NOT NULL,
 remaining_reserve INTEGER NOT NULL CHECK(remaining_reserve>=0), created_at REAL NOT NULL,
 UNIQUE(window_id,generation,scope_id,population_digest,metadata_digest,codec),
 FOREIGN KEY(window_id,generation) REFERENCES core_work(window_id,generation)
);
CREATE INDEX business_obligation_queue ON business_obligation(state,created_at,id);
CREATE TABLE business_scope (
 id TEXT PRIMARY KEY, next_revision INTEGER NOT NULL CHECK(next_revision>0),
 result_id TEXT, epoch INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE business_result (
 id TEXT PRIMARY KEY, obligation_id TEXT NOT NULL UNIQUE REFERENCES business_obligation(id),
 scope_id TEXT NOT NULL REFERENCES business_scope(id), revision INTEGER NOT NULL,
 payload BLOB NOT NULL, payload_hash TEXT NOT NULL,
 row_manifest BLOB NOT NULL, manifest_hash TEXT NOT NULL, codec INTEGER NOT NULL,
 UNIQUE(scope_id,revision)
);
CREATE TABLE business_publication (
 result_id TEXT PRIMARY KEY REFERENCES business_result(id),
 state TEXT NOT NULL CHECK(state IN ('PENDING','WRITING','PUBLISHED','BLOCKED','INVALIDATED')),
 attempts INTEGER NOT NULL DEFAULT 0, claim_token TEXT, retry_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX business_publication_queue ON business_publication(state,retry_at,result_id);
CREATE TABLE business_metadata_cursor (
 run_key TEXT NOT NULL, stream TEXT NOT NULL, schedule_id TEXT NOT NULL,
 sequence INTEGER NOT NULL, digest TEXT, settled_ordinal INTEGER NOT NULL, pending_digest TEXT,
 PRIMARY KEY(run_key,stream)
);
CREATE TABLE business_metadata_receipt (
 digest TEXT PRIMARY KEY, run_key TEXT NOT NULL, payload BLOB NOT NULL
);
CREATE TABLE business_metadata_budget (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), receipt_count INTEGER NOT NULL, receipt_bytes INTEGER NOT NULL
);
INSERT INTO business_metadata_budget VALUES (1,0,0);
CREATE TABLE business_observation_schedule (
 run_key TEXT PRIMARY KEY, schedule_id TEXT NOT NULL, schedule BLOB NOT NULL,
 lifecycle TEXT NOT NULL, cutoff INTEGER, start_tick INTEGER
);
CREATE TABLE business_observation_slot (
 run_key TEXT NOT NULL, ordinal INTEGER NOT NULL, receipt_digest TEXT NOT NULL REFERENCES business_metadata_receipt(digest),
 outcome BLOB NOT NULL, PRIMARY KEY(run_key,ordinal)
);
CREATE TABLE business_schedule_recheck (
 run_key TEXT PRIMARY KEY, receipt_digest TEXT NOT NULL REFERENCES business_metadata_receipt(digest),
 next_due_tick INTEGER NOT NULL
);
CREATE TABLE business_measurement (
 event_id TEXT PRIMARY KEY, run_key TEXT NOT NULL, revision INTEGER NOT NULL,
 digest TEXT NOT NULL, payload BLOB NOT NULL
);
CREATE TABLE business_metadata_dirty (
 window_id TEXT PRIMARY KEY REFERENCES core_window(id), receipt_digest TEXT NOT NULL REFERENCES business_metadata_receipt(digest)
);
"""


class IsolatedBusinessStore(GoldCoreStore):
    """Opt-in additive schema; the production Core opener rejects this version."""

    schema_version = 2

    def _upgrade(self, db, version):
        if version == 1:
            db.executescript(f"BEGIN IMMEDIATE; {_SCHEMA} PRAGMA user_version=2; COMMIT;")


def _digest(payload):
    return hashlib.sha256(payload).hexdigest()


def _verify_canonical(payload, limit):
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise CoreStateFault("Business payload is absent or exceeds its byte budget")
    try:
        decoded = json.loads(payload)
        if canonical_bytes(decoded) != payload:
            raise ValueError("noncanonical bytes")
    except (ValueError, UnicodeError, TypeError, OverflowError) as exc:
        raise CoreStateFault("Business payload is not canonical JSON") from exc
    return _digest(payload)


class BusinessState:
    def __init__(self, store, namespace, lease_token):
        if not isinstance(store, IsolatedBusinessStore):
            raise CoreStateFault("Business state requires an explicit isolated v2 store")
        self.store = store
        self.namespace = namespace
        self.lease_token = lease_token

    def _tx(self):
        return self.store.transaction(self.namespace, self.lease_token)

    def recover_reservations(self):
        """Release only unsealed claims from old leases or generations."""
        with self._tx() as db:
            return db.execute("""
                DELETE FROM business_reservation WHERE
                lease_token<>? OR generation<>(SELECT generation FROM core_window WHERE id=window_id)
            """, (self.lease_token,)).rowcount

    def hold_unsealed(self, work, *, reason="RESOURCE_LIMIT"):
        with self._tx() as db:
            key = work["window_id"], work["generation"]
            removed = db.execute("""
                DELETE FROM business_reservation
                WHERE window_id=? AND generation=? AND lease_token=?
            """, (*key, self.lease_token))
            if removed.rowcount:
                db.execute("INSERT OR IGNORE INTO business_hold VALUES (?,?,?,?)",
                           (*key, reason, time.time()))

    def recheck_waiting_schedule(self, *, limit=16):
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ValueError("invalid schedule recheck bound")
        with self._tx() as db:
            intent = db.execute("SELECT * FROM business_schedule_recheck ORDER BY run_key LIMIT 1").fetchone()
            if intent is None:
                return 0
            rows = db.execute("""
                SELECT w.id,w.generation FROM business_hold h
                JOIN core_window w ON w.id=h.window_id AND w.generation=h.generation
                JOIN business_observation_schedule s ON s.run_key=w.run_key
                WHERE w.run_key=? AND h.reason IN ('WAITING','UNSUPPORTED_SCHEDULE')
                  AND s.start_tick IS NOT NULL
                  AND (w.end * 1000000<=? OR w.start * 1000000<s.start_tick OR s.cutoff IS NOT NULL)
                ORDER BY h.reason,h.since,h.window_id LIMIT ?
            """, (intent["run_key"], intent["next_due_tick"], limit)).fetchall()
            db.executemany("DELETE FROM business_hold WHERE window_id=? AND generation=?", (tuple(row) for row in rows))
            if len(rows) < limit:
                db.execute("DELETE FROM business_schedule_recheck WHERE run_key=?", (intent["run_key"],))
            return len(rows)

    def reserve_next(self):
        """Admit one proof traversal. Older unsealed work precedes metadata recomputation."""
        if shutil.disk_usage(self.store.db_path.parent).free <= _PROTECTED_FREE + _RESERVATION:
            return None
        with self._tx() as db:
            active = db.execute("SELECT count(*) FROM business_reservation").fetchone()[0]
            if active:
                return None
            pending, used = db.execute("""
                SELECT count(*), coalesce(sum(length(o.projection)+o.remaining_reserve+
                    coalesce(length(r.payload)+length(r.row_manifest),0)),0)
                FROM business_obligation o
                JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
                LEFT JOIN business_result r ON r.obligation_id=o.id
                LEFT JOIN business_publication p ON p.result_id=r.id
                WHERE o.state IN ('PENDING','COMPUTED')
                    AND (p.state IS NULL OR p.state NOT IN ('PUBLISHED','INVALIDATED'))
            """).fetchone()
            if pending >= _TASK_LIMIT or used + _RESERVATION > _PENDING_LIMIT:
                return None
            row = db.execute("""
                SELECT c.* FROM core_work c
                JOIN core_window w ON w.id=c.window_id AND w.generation=c.generation
                LEFT JOIN business_hold h ON h.window_id=c.window_id AND h.generation=c.generation
                WHERE c.state IN ('PENDING','RUNNING') AND h.window_id IS NULL
                ORDER BY CASE c.state WHEN 'RUNNING' THEN 0 ELSE 1 END, c.eligible_at,c.window_id,c.generation
                LIMIT 1
            """).fetchone()
            if row is None:
                row = db.execute("""
                    SELECT c.* FROM business_metadata_dirty m
                    JOIN core_window w ON w.id=m.window_id
                    JOIN core_work c ON c.window_id=w.id AND c.generation=w.generation
                    LEFT JOIN business_hold h ON h.window_id=c.window_id AND h.generation=c.generation
                    WHERE c.state='COMPLETE' AND h.window_id IS NULL
                    ORDER BY c.completed_at,c.window_id LIMIT 1
                """).fetchone()
                if row is None:
                    return None
            db.execute("INSERT INTO business_reservation VALUES (?,?,?,?)",
                       (row["window_id"], row["generation"], self.lease_token, _RESERVATION))
            if row["state"] != "COMPLETE":
                db.execute("UPDATE core_work SET state='RUNNING' WHERE window_id=? AND generation=?",
                           (row["window_id"], row["generation"]))
            return row

    def finish(self, ledger, work, input_hash, member_count, projection, *,
               definition_set, source_semantics, coverage_policy, population_digest, metadata_digest,
               metadata_marker=None, verify_metadata=False, barrier=None):
        if type(member_count) is not int or not 0 <= member_count <= 3_000:
            raise CoreStateFault("Business projection exceeds fixture member budget")
        projection_hash = _verify_canonical(projection, _PROJECTION_LIMIT)
        key = work["window_id"], work["generation"]

        def seal(db, work_id):
            reservation = db.execute("""
                SELECT lease_token FROM business_reservation WHERE window_id=? AND generation=?
            """, key).fetchone()
            if reservation is None or reservation[0] != self.lease_token:
                raise CoreStateFault("Core completion requires a matching Business reservation")
            if verify_metadata:
                current = db.execute("SELECT receipt_digest FROM business_metadata_dirty WHERE window_id=?",
                                     (key[0],)).fetchone()
                if (current[0] if current is not None else None) != metadata_marker:
                    raise CoreStateFault("Business measurement metadata changed while sealing work")
                db.execute("DELETE FROM business_metadata_dirty WHERE window_id=?", (key[0],))
            binding_id = db.execute("SELECT binding_id FROM core_binding WHERE namespace=?",
                                    (self.namespace,)).fetchone()[0]
            scope = canonical_hash(["stage-a-scope-v1", self.namespace, binding_id, key[0],
                                    definition_set, source_semantics, coverage_policy])
            identity = canonical_hash(["stage-a-obligation-v1", self.namespace, binding_id, *key,
                                       work_id, input_hash, definition_set, source_semantics, coverage_policy,
                                       population_digest, metadata_digest, 1])
            db.execute("""
                INSERT INTO business_obligation(
                    id,window_id,generation,work_id,input_hash,binding_id,scope_id,definition_set,
                    source_semantics,coverage_policy,population_digest,metadata_digest,codec,
                    state,projection,projection_hash,remaining_reserve,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'PENDING',?,?,?,?)
            """, (identity, *key, work_id, input_hash, binding_id, scope, definition_set,
                  source_semantics, coverage_policy, population_digest, metadata_digest,
                  projection, projection_hash, _RESULT_LIMIT + _MANIFEST_LIMIT, time.time()))
            db.execute("DELETE FROM business_reservation WHERE window_id=? AND generation=?", key)
            if barrier:
                barrier(db)

        # Invalid lease/generation or any exception in seal rolls back the Core proof too.
        if ledger.store is not self.store or ledger.namespace != self.namespace or ledger.token != self.lease_token:
            raise CoreStateFault("Core ledger must share the Business writer lease")
        if work["state"] == "COMPLETE":
            if not verify_metadata or metadata_marker is None:
                raise CoreStateFault("Business recomputation requires dirty metadata on a completed Core proof")
            with self._tx() as db:
                proof = db.execute("""
                    SELECT c.work_id,c.input_hash,c.input_count FROM core_work c JOIN core_window w
                    ON w.id=c.window_id AND w.generation=c.generation
                    WHERE c.window_id=? AND c.generation=? AND c.state='COMPLETE'
                """, key).fetchone()
                if proof is None:
                    return False
                if (proof["input_hash"], proof["input_count"]) != (input_hash, member_count):
                    raise CoreStateFault("Recomputation differs from the immutable Core proof")
                seal(db, proof["work_id"])
                return True
        return ledger.finish(work, input_hash, member_count, seal=seal)
