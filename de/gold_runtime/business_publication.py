"""Isolated Stage A result persistence and exact-row publication protocol."""

import json
import time
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from de.gold.business import VERIFIED_DEFINITION
from de.gold_runtime.business_state import _digest, _verify_canonical, _RESULT_LIMIT, _MANIFEST_LIMIT
from de.gold_runtime.checkpoint_store import CoreStateFault
from de.silver.incremental_contract import canonical_bytes, canonical_hash

_MAX_ATTEMPTS = 4
_MAX_ROWS = 23


@dataclass(frozen=True, slots=True)
class PhysicalRow:
    grain: str
    key: str
    payload: bytes


class ResultRows(Protocol):
    """An isolated immutable row target, with adapter-enforced I/O deadlines."""

    def read(self, result_id: str, *, limit: int) -> tuple[PhysicalRow, ...]: ...
    def insert(self, result_id: str, rows: tuple[PhysicalRow, ...]) -> None: ...


class DivergentResult(CoreStateFault):
    """Stored rows have an identity or payload that cannot safely be reconciled."""


def result_rows(payload: bytes) -> tuple[PhysicalRow, ...]:
    document = json.loads(payload)
    verified = document.get("definition_set") == VERIFIED_DEFINITION
    rows = document["rows"]
    if not isinstance(rows, list) or not rows or len(rows) > _MAX_ROWS:
        raise CoreStateFault("Invalid result row population")
    keys = set()
    physical = []
    directions = set()
    intersections = set()
    referenced = set()
    networks = 0
    for row in rows:
        grain, node, direction = row["grain"], row["intersection"], row["direction"]
        if grain == "direction" and isinstance(node, str) and node and isinstance(direction, str) and direction:
            directions.add((node, direction))
            referenced.add(node)
        elif grain == "intersection" and isinstance(node, str) and node and direction is None:
            intersections.add(node)
        elif grain == "network" and node is None and direction is None:
            networks += 1
        else:
            raise CoreStateFault("Invalid result grain or entity ID")
        key = canonical_hash(["stage-a-row-v1", grain, node, direction])
        if key in keys:
            raise CoreStateFault("Duplicate result row key")
        keys.add(key)
        if verified:
            row = dict(row, window_start_us=document["start_us"], window_duration_s=document["duration_s"],
                       definition_set=document["definition_set"])
        physical.append(PhysicalRow(grain, key, canonical_bytes(row)))
    if networks != 1 or not directions or referenced != intersections:
        raise CoreStateFault("Expected all three consistent Gold grains")
    return tuple(sorted(physical, key=lambda row: (row.grain, row.key)))


def _manifest(rows):
    return canonical_bytes([[row.grain, row.key, _digest(row.payload)] for row in rows])


def _population_digest(rows):
    population = {}
    for row in rows:
        if row.grain == "direction":
            sample = json.loads(row.payload)
            population.setdefault(sample["intersection"], []).append(sample["direction"])
    return canonical_hash([(node, sorted(directions)) for node, directions in sorted(population.items())])


class BusinessPublisher:
    def __init__(self, state, target: ResultRows, *, clock=time.time):
        self.state = state
        self.target = target
        self.clock = clock

    def effective_pointer(self, scope_id: str) -> tuple[str, int] | None:
        """Read the isolated publication fence against the current Core generation."""
        with self.state._tx() as db:
            row = db.execute("""
                SELECT s.result_id,s.epoch FROM business_scope s
                JOIN business_result r ON r.id=s.result_id AND r.scope_id=s.id
                JOIN business_publication p ON p.result_id=r.id AND p.state='PUBLISHED'
                JOIN business_obligation o ON o.id=r.obligation_id AND o.state='COMPUTED'
                JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
                JOIN core_binding b ON b.binding_id=o.binding_id AND b.namespace=?
                WHERE s.id=?
            """, (self.state.namespace, scope_id)).fetchone()
            return (row["result_id"], row["epoch"]) if row else None

    def save_computed(self, obligation_id: str, projection_hash: str, payload: bytes):
        payload_hash = _verify_canonical(payload, _RESULT_LIMIT)
        rows = result_rows(payload)
        manifest = _manifest(rows)
        manifest_hash = _verify_canonical(manifest, _MANIFEST_LIMIT)
        population_hash = _population_digest(rows)
        with self.state._tx() as db:
            obligation = db.execute("""
                SELECT o.* FROM business_obligation o
                JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
                JOIN core_binding b ON b.binding_id=o.binding_id
                WHERE o.id=? AND b.namespace=?
            """, (obligation_id, self.state.namespace)).fetchone()
            if obligation is None or obligation["projection_hash"] != projection_hash:
                raise CoreStateFault("Computed result does not match current obligation")
            if (obligation["source_semantics"] == "silver_observation_metadata_v1" and
                    obligation["population_digest"] != population_hash):
                raise DivergentResult("Computed result does not cover the sealed approach population")
            current = db.execute("SELECT * FROM business_result WHERE obligation_id=?",
                                 (obligation_id,)).fetchone()
            if current is not None:
                if current["payload_hash"] != payload_hash or current["manifest_hash"] != manifest_hash:
                    raise DivergentResult("Computed retry differs from the durable result")
                return current["id"]
            if obligation["state"] != "PENDING":
                raise CoreStateFault("Obligation is not pending")
            scope = obligation["scope_id"]
            db.execute("INSERT OR IGNORE INTO business_scope(id,next_revision) VALUES (?,1)", (scope,))
            revision = db.execute("SELECT next_revision FROM business_scope WHERE id=?", (scope,)).fetchone()[0]
            result_id = canonical_hash(["stage-a-result-v1", obligation_id, revision, 1])
            db.execute("UPDATE business_scope SET next_revision=next_revision+1 WHERE id=?", (scope,))
            db.execute("INSERT INTO business_result VALUES (?,?,?,?,?,?,?,?,1)",
                       (result_id, obligation_id, scope, revision, payload, payload_hash, manifest, manifest_hash))
            db.execute("INSERT INTO business_publication(result_id,state) VALUES (?,'PENDING')", (result_id,))
            db.execute("UPDATE business_obligation SET state='COMPUTED',remaining_reserve=0 WHERE id=?", (obligation_id,))
            return result_id

    def recover(self):
        """Release claims interrupted before verification. Verified work retains its CAS."""
        with self.state._tx() as db:
            return db.execute("""
                UPDATE business_publication SET state=CASE WHEN attempts>=? THEN 'BLOCKED' ELSE 'PENDING' END,
                    claim_token=NULL WHERE state='WRITING'
                    AND NOT EXISTS (SELECT 1 FROM business_publication_verification v
                                    WHERE v.result_id=business_publication.result_id)
            """, (_MAX_ATTEMPTS,)).rowcount

    def _claim(self):
        with self.state._tx() as db:
            if db.execute("SELECT 1 FROM business_publication WHERE state='WRITING' LIMIT 1").fetchone():
                return None
            row = db.execute("""
                SELECT r.*,p.attempts,s.epoch AS scope_epoch
                FROM business_publication p JOIN business_result r ON r.id=p.result_id
                JOIN business_obligation o ON o.id=r.obligation_id
                JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
                JOIN core_work c ON c.window_id=o.window_id AND c.generation=o.generation
                  AND c.state='COMPLETE' AND c.work_id=o.work_id AND c.input_hash=o.input_hash
                JOIN core_binding b ON b.binding_id=o.binding_id
                JOIN business_scope s ON s.id=r.scope_id
                WHERE b.namespace=? AND o.state='COMPUTED' AND p.state='PENDING' AND p.attempts<? AND p.retry_at<=?
                ORDER BY p.retry_at,o.created_at,p.result_id LIMIT 1
            """, (self.state.namespace, _MAX_ATTEMPTS, self.clock())).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            db.execute("UPDATE business_publication SET state='WRITING',claim_token=?,attempts=attempts+1 WHERE result_id=?",
                       (token, row["id"]))
            return row, token

    def _verify_target(self, result_id, expected):
        observed = self.target.read(result_id, limit=2 * _MAX_ROWS + 1)
        if len(observed) > 2 * _MAX_ROWS:
            raise DivergentResult("Physical result exceeds the duplicate inspection cap")
        expected_by_key = {(row.grain, row.key): row for row in expected}
        seen = set()
        for row in observed:
            key = row.grain, row.key
            if expected_by_key.get(key) != row:
                raise DivergentResult("Unexpected result key or divergent row")
            seen.add(key)
        return tuple(row for key, row in expected_by_key.items() if key not in seen)

    def publish_once(self, *, barrier=None, after_verify=None):
        existing = self.commit_verified()
        if existing != "IDLE":
            return existing
        claim = self._claim()
        if claim is None:
            return "IDLE"
        row, token = claim
        result_id = row["id"]
        state = "VERIFIED"
        try:
            if _verify_canonical(row["payload"], _RESULT_LIMIT) != row["payload_hash"]:
                raise DivergentResult("Durable output hash mismatch")
            expected = result_rows(row["payload"])
            manifest = _manifest(expected)
            if manifest != row["row_manifest"] or _digest(manifest) != row["manifest_hash"]:
                raise DivergentResult("Durable row manifest mismatch")
            missing = self._verify_target(result_id, expected)
            if missing and row["attempts"] + 1 < _MAX_ATTEMPTS:
                self.target.insert(result_id, missing)
                missing = self._verify_target(result_id, expected)
            if missing:
                state = "PENDING"
        except (CoreStateFault, KeyError, TypeError, ValueError):
            state = "BLOCKED"
        except (TimeoutError, ConnectionError):
            state = "PENDING"
        if barrier:
            barrier()
        if state != "VERIFIED":
            return self._finish_unverified(row, token, state)
        if not self._record_verification(row, token):
            return "INVALIDATED"
        if after_verify:
            after_verify()
        return self.commit_verified()

    def _current(self, db, row):
        return db.execute("""
            SELECT 1 FROM business_scope s JOIN business_result r ON r.scope_id=s.id
            JOIN business_obligation o ON o.id=r.obligation_id AND o.state='COMPUTED'
            JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
            JOIN core_work c ON c.window_id=o.window_id AND c.generation=o.generation
              AND c.state='COMPLETE' AND c.work_id=o.work_id AND c.input_hash=o.input_hash
            JOIN core_binding b ON b.binding_id=o.binding_id AND b.namespace=?
            WHERE r.id=? AND s.epoch=?
        """, (self.state.namespace, row["id"], row["scope_epoch"])).fetchone() is not None

    def _record_verification(self, row, token):
        with self.state._tx() as db:
            if not self._current(db, row):
                self._retire_claim(db, row["id"], token, "INVALIDATED", 0)
                return False
            inserted = db.execute("""
                INSERT INTO business_publication_verification
                SELECT p.result_id,p.claim_token,?,?,? FROM business_publication p
                JOIN business_result r ON r.id=p.result_id AND r.manifest_hash=?
                WHERE p.result_id=? AND p.state='WRITING' AND p.claim_token=?
            """, (row["manifest_hash"], row["scope_epoch"], self.clock(), row["manifest_hash"], row["id"], token))
            if inserted.rowcount != 1:
                raise CoreStateFault("Publication verification claim CAS was lost")
            return True

    def commit_verified(self):
        """Publish a durable exact-row verification under the current Gold lease."""
        if self.state.store.conn.execute("""
            SELECT 1 FROM business_publication p JOIN business_publication_verification v ON v.result_id=p.result_id
            WHERE p.state='WRITING' LIMIT 1
        """).fetchone() is None:
            return "IDLE"
        with self.state._tx() as db:
            row = db.execute("""
                SELECT r.id,r.scope_id,r.manifest_hash,v.manifest_hash AS verified_hash,v.scope_epoch,
                       v.claim_token AS verified_token,p.claim_token AS current_token
                FROM business_publication_verification v JOIN business_publication p ON p.result_id=v.result_id
                JOIN business_result r ON r.id=v.result_id WHERE p.state='WRITING' LIMIT 1
            """).fetchone()
            if row is None:
                return "IDLE"
            if not row["current_token"] or row["current_token"] != row["verified_token"]:
                raise CoreStateFault("Durable publication verification no longer matches its claim")
            if row["verified_hash"] != row["manifest_hash"]:
                raise DivergentResult("Durable publication verification differs from the result manifest")
            state = "INVALIDATED"
            if self._current(db, row):
                changed = db.execute("UPDATE business_scope SET result_id=?,epoch=epoch+1 WHERE id=? AND epoch=?",
                                     (row["id"], row["scope_id"], row["scope_epoch"]))
                if changed.rowcount != 1:
                    raise CoreStateFault("Business publication pointer CAS was lost")
                state = "PUBLISHED"
            self._retire_claim(db, row["id"], row["verified_token"], state, 0)
            return state

    def _finish_unverified(self, row, token, state):
        with self.state._tx() as db:
            if not self._current(db, row):
                state = "INVALIDATED"
            attempts = row["attempts"] + 1
            if state == "PENDING" and attempts >= _MAX_ATTEMPTS:
                state = "BLOCKED"
            ready = self.clock() + 0.25 * 2 ** (attempts - 1) if state == "PENDING" else 0
            self._retire_claim(db, row["id"], token, state, ready)
        return state

    @staticmethod
    def _retire_claim(db, result_id, token, state, ready):
        changed = db.execute("""
            UPDATE business_publication SET state=?,claim_token=NULL,retry_at=?
            WHERE result_id=? AND state='WRITING' AND claim_token=?
        """, (state, ready, result_id, token))
        if changed.rowcount != 1:
            raise CoreStateFault("Publication claim CAS was lost")
