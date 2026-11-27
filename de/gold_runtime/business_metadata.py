"""Gold acknowledgement of verified Silver-owned observation metadata."""

import json

from contracts.observation_measurements import digest as is_digest
from contracts.observation_schedule import ObservationSchedule
from contracts.traffic_observation import canonical_json
from de.gold_runtime.checkpoint_store import CoreStateFault
from de.silver.incremental_contract import canonical_bytes, canonical_hash
from de.silver.observation_contract import ObservationMetadataCursor, VerifiedObservationMetadata


_RECEIPT_LIMIT = 64 * 1024
_MAX_ISOLATED_RECEIPTS = 4096
_MAX_ISOLATED_RECEIPT_BYTES = 32 * 1024 * 1024


class BusinessMetadata:
    def __init__(self, state):
        self.state = state

    def cursor(self, run_id, generation, topology_digest, manifest_identity, stream):
        key = canonical_hash(dict(run_id=run_id, run_generation=generation, topology_digest=topology_digest,
                                  manifest_identity=manifest_identity))
        row = self.state.store.conn.execute("SELECT * FROM business_metadata_cursor WHERE run_key=? AND stream=?",
                                            (key, stream)).fetchone()
        return self._cursor(stream, row)

    @staticmethod
    def _cursor(stream, row):
        return ObservationMetadataCursor(stream=stream, **({"sequence": row["sequence"], "digest": row["digest"],
                    "settled_ordinal": row["settled_ordinal"], "pending_digest": row["pending_digest"]}
                    if row is not None else {}))

    def apply(self, receipt, *, barrier=None):
        """Commit the metadata CAS, local receipts, and Business visibility fence together."""
        if type(receipt) is not VerifiedObservationMetadata or receipt.contract_version != 1:
            raise CoreStateFault("Expected Silver observation metadata v1")
        if receipt.status in ("WAITING", "EMPTY"):
            if receipt.after != receipt.before or receipt.payload is not None:
                raise CoreStateFault("Unverified metadata advanced its cursor")
            return receipt.status
        before, after = receipt.before, receipt.after
        stream = before.stream
        if (receipt.status != "VERIFIED" or type(after) is not ObservationMetadataCursor
                or stream != after.stream or stream not in ("SCHEDULE", "MEASUREMENT")
                or not (before.sequence <= after.sequence <= before.sequence + 1
                        and after.sequence <= receipt.captured_through_sequence
                        and before.settled_ordinal <= after.settled_ordinal)
                or before == after
                or (after.sequence and not is_digest(after.digest))
                or (after.sequence == before.sequence and after.digest != before.digest)
                or not is_digest(receipt.schedule_id) or not is_digest(receipt.archive_head_digest)
                or min(receipt.required_change_sequence, receipt.required_invalidation_sequence) < 0):
            raise CoreStateFault("Invalid Silver observation metadata receipt")
        same_segment = before.sequence == after.sequence
        if stream == "MEASUREMENT" and (same_segment or after.settled_ordinal != -1 or after.pending_digest is not None):
            raise CoreStateFault("Invalid Silver measurement cursor")
        if stream == "SCHEDULE" and (
                (same_segment and (not is_digest(after.pending_digest)
                                   or before.pending_digest not in (None, after.pending_digest)))
                or (not same_segment and (after.pending_digest is not None
                                          or before.pending_digest not in (None, after.digest)))):
            raise CoreStateFault("Pending Silver schedule segment changed")
        encoded = canonical_bytes(receipt)
        if len(encoded) > _RECEIPT_LIMIT:
            raise CoreStateFault("Silver observation metadata receipt exceeds byte limit")
        run = {key: getattr(receipt, key) for key in ("run_id", "run_generation", "topology_digest", "manifest_identity")}
        run_key = canonical_hash(run)
        digest = receipt.receipt_digest
        with self.state._tx() as db:
            binding = self.state.store.binding()
            if binding is None or binding["binding_id"] != receipt.source_binding_id:
                raise CoreStateFault("Silver metadata source binding differs from Core")
            cursors = self.state.store.cursors()
            if (cursors["CHANGE"] < receipt.required_change_sequence
                    or cursors["INVALIDATION"] < receipt.required_invalidation_sequence):
                return "WAITING"
            recorded = db.execute("SELECT payload FROM business_metadata_receipt WHERE digest=?", (digest,)).fetchone()
            if recorded is not None:
                if recorded[0] != encoded:
                    raise CoreStateFault("Observation metadata receipt digest collision")
                return "REUSED"
            count, size = db.execute("SELECT receipt_count,receipt_bytes FROM business_metadata_budget "
                                     "WHERE singleton=1").fetchone()
            if count >= _MAX_ISOLATED_RECEIPTS or size + len(encoded) > _MAX_ISOLATED_RECEIPT_BYTES:
                raise CoreStateFault("Isolated metadata history exceeds its resource budget")
            existing = db.execute("SELECT * FROM business_metadata_cursor WHERE run_key=? AND stream=?",
                                  (run_key, stream)).fetchone()
            if ((existing is not None and existing["schedule_id"] != receipt.schedule_id)
                    or self._cursor(stream, existing) != before):
                raise CoreStateFault("Observation metadata cursor or schedule identity changed")
            other_scope = db.execute("SELECT 1 FROM business_metadata_cursor WHERE run_key=? AND schedule_id<>?",
                                     (run_key, receipt.schedule_id)).fetchone()
            if other_scope is not None:
                raise CoreStateFault("Observation metadata streams disagree on run schedule identity")
            db.execute("INSERT INTO business_metadata_receipt VALUES (?,?,?)", (digest, run_key, encoded))
            db.execute("UPDATE business_metadata_budget SET receipt_count=receipt_count+1, "
                       "receipt_bytes=receipt_bytes+? WHERE singleton=1", (len(encoded),))
            if stream == "SCHEDULE":
                self._apply_schedule(db, run_key, receipt)
            else:
                self._apply_measurement(db, run_key, receipt)
            db.execute("""
                INSERT INTO business_metadata_cursor VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(run_key,stream) DO UPDATE SET sequence=excluded.sequence,digest=excluded.digest,
                    settled_ordinal=excluded.settled_ordinal,pending_digest=excluded.pending_digest
            """, (run_key, stream, receipt.schedule_id, after.sequence, after.digest, after.settled_ordinal,
                  after.pending_digest))
            if barrier is not None:
                barrier()
        return "APPLIED"

    @staticmethod
    def _apply_schedule(db, run_key, receipt):
        payload = receipt.payload
        previous = receipt.before.settled_ordinal
        rows = payload["slots"]
        schedule = ObservationSchedule.decode(canonical_json(json.loads(canonical_bytes(payload["schedule"]))).encode(),
                                              receipt.schedule_id)
        if (schedule.run_id, schedule.generation, schedule.topology_digest, schedule.manifest_digest) != (
                receipt.run_id, receipt.run_generation, receipt.topology_digest, receipt.manifest_identity):
            raise CoreStateFault("Schedule document and verified metadata run differ")
        if (len(rows) > 16 or len(rows) != receipt.after.settled_ordinal - previous
                or any(item["ordinal"] != previous + index for index, item in enumerate(rows, 1))):
            raise CoreStateFault("Verified schedule prefix is not contiguous")
        db.execute("""
            INSERT INTO business_observation_schedule VALUES (?,?,?,?,?,?)
            ON CONFLICT(run_key) DO UPDATE SET lifecycle=excluded.lifecycle,cutoff=excluded.cutoff,
                start_tick=coalesce(business_observation_schedule.start_tick,excluded.start_tick)
        """, (run_key, receipt.schedule_id, canonical_bytes(payload["schedule"]), payload["lifecycle"],
              payload["terminalCutoffTick"], payload["lifecycleTick"] if payload["lifecycle"] == "RUNNING" else None))
        db.executemany("INSERT INTO business_observation_slot VALUES (?,?,?,?)",
                       ((run_key, row["ordinal"], receipt.receipt_digest, canonical_bytes(row)) for row in rows))
        db.execute("""
            INSERT INTO business_schedule_recheck VALUES (?,?,?)
            ON CONFLICT(run_key) DO UPDATE SET receipt_digest=excluded.receipt_digest,next_due_tick=excluded.next_due_tick
        """, (run_key, receipt.receipt_digest, schedule.due_tick(receipt.after.settled_ordinal + 1)))

    @staticmethod
    def _apply_measurement(db, run_key, receipt):
        payload = receipt.payload
        anchor = payload["anchor"]
        event_id = anchor["eventId"]
        revision = payload["revision"]
        object_id = payload["objectDigest"]
        if ((anchor["runId"], anchor["generation"], anchor["manifestIdentity"], anchor["topologyDigest"])
                != (receipt.run_id, receipt.run_generation, receipt.manifest_identity, receipt.topology_digest)):
            raise CoreStateFault("Measurement anchor differs from the receipt run")
        coords = run_key, anchor["simulationTimeSeconds"], anchor["intersectionId"], anchor["cycle"]
        suppression = db.execute("SELECT * FROM core_suppression WHERE event_id=?", (event_id,)).fetchone()
        event = db.execute("SELECT * FROM core_event WHERE id=?", (event_id,)).fetchone()
        evidence = suppression if suppression is not None else event
        if evidence is None or tuple(evidence[k] for k in ("run_key", "simulation_time", "intersection_id", "cycle")) != coords:
            raise CoreStateFault("Measurement has no matching applied Core coordinates")
        if event is not None:
            transition = db.execute("SELECT provenance FROM core_transition WHERE kind='CHANGE' AND sequence=?",
                                    (event["change_sequence"],)).fetchone()
            if transition is None or json.loads(transition[0])["source_payload_hash"] != anchor["payloadHash"]:
                raise CoreStateFault("Measurement has a different applied Core payload hash")
        if suppression is not None and payload["measurements"] is not None:
            # A verified older receipt can race a later Core invalidation. Store
            # the immutable lineage and suppress its locally effective values.
            payload = dict(payload, measurements=None, canonicalDisposition="CONFLICTED")
        previous = db.execute("SELECT * FROM business_measurement WHERE event_id=?", (event_id,)).fetchone()
        expected = (previous["revision"] + 1, previous["digest"]) if previous is not None else (1, None)
        if ((revision, payload["previousDigest"]) != expected or not is_digest(object_id) or not is_digest(event_id)
                or (previous is not None and previous["run_key"] != run_key)):
            raise CoreStateFault("Measurement revision forks the durable Silver lineage")
        db.execute("""
            INSERT INTO business_measurement VALUES (?,?,?,?,?)
            ON CONFLICT(event_id) DO UPDATE SET revision=excluded.revision,digest=excluded.digest,payload=excluded.payload
        """, (event_id, run_key, revision, object_id, canonical_bytes(payload)))
        # Each observation has at most two current Core window memberships. A
        # correction must make any previous Business result ineffective before
        # future work can republish it. Core proof generations stay independent.
        for (window_id,) in db.execute("SELECT window_id FROM core_membership WHERE event_id=?", (event_id,)):
            db.execute("""
                INSERT INTO business_metadata_dirty VALUES (?,?)
                ON CONFLICT(window_id) DO UPDATE SET receipt_digest=excluded.receipt_digest
            """, (window_id, receipt.receipt_digest))
            db.execute("""
                UPDATE business_scope SET result_id=NULL,epoch=epoch+1 WHERE result_id IN (
                    SELECT r.id FROM business_result r JOIN business_obligation o ON o.id=r.obligation_id
                    WHERE o.window_id=?)
            """, (window_id,))
            db.execute("UPDATE business_obligation SET state='SUPERSEDED' WHERE window_id=? AND state<>'SUPERSEDED'",
                       (window_id,))
