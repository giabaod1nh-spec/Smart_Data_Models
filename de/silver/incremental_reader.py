"""Silver-owned read-only sequence API. The Phase 3 scan keeps its own boundary."""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from dataclasses import asdict, replace

from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError
from de.feed_read_budget import FeedEvidenceDeadline, FeedEvidenceSize, feed_query_budget
from de.silver.archive_resolver import ArchiveIntegrityError, ArchiveUnavailable
from de.silver.incremental_contract import (
    CONTRACT_VERSION, MAX_SEQUENCE, CapturedBoundary, CanonicalBatchReceipt,
    CanonicalChangeEntry, CanonicalChangePage, CanonicalChangePageRequest,
    CanonicalFeedError, CanonicalInvalidationEntry, CanonicalInvalidationPage,
    CanonicalInvalidationPageRequest, CanonicalInvalidationReceipt, FeedLimits,
    SourceDiscovery, SourceDiscoveryRequest, batch_digest, canonical_bytes,
    canonical_hash, freeze, size_page,
)
from de.silver.repositories import FactBusinessKeyConflictError, RetryableRepositoryError
from de.silver.source_binding import SourceVerificationError, verify_bronze_source

_LOG = logging.getLogger(__name__)
_BINDING_KEYS = ("namespace", "source_path", "file_identity", "schema_fingerprint", "bronze_version")
_TRANSITION_KEYS = ("namespace", "change_sequence", "event_id", "revision", "change_type",
                    "previous_state", "new_state", "content_hash", "invalidation_id")


class _PrefixBlocked(Exception):
    pass


def _check(condition, code, message, **details):
    if not condition:
        raise CanonicalFeedError(code, message, **details)


class IncrementalSilverSource:
    """Additive API on a Silver read session with exclusive use of its RO handles."""

    feed_limits = FeedLimits()

    def _validate_feed_request(self, request, expected_type):
        _check(type(request) is expected_type and request.namespace == "live",
               "INVALID_REQUEST", "Expected a v1 request for the live namespace")
        _check(request.contract_version == CONTRACT_VERSION, "UNSUPPORTED_CONTRACT_VERSION",
               "Only canonical feed contract version 1 is supported")
        binding = request.expected_source_binding_id
        discovery = expected_type is SourceDiscoveryRequest
        _check((discovery and binding is None) or (type(binding) is str and bool(binding)),
               "INVALID_REQUEST", "Expected a nonempty source binding ID")
        if not discovery:
            for name in ("max_entries", "max_encoded_bytes", "max_processing_ms", "max_entry_processing_ms"):
                value = getattr(request, name)
                _check(type(value) is int and 1 <= value <= getattr(self.feed_limits, name + "_cap"),
                       "INVALID_REQUEST", f"Invalid {name}")
            names = ("after_change_sequence",) if expected_type is CanonicalChangePageRequest else (
                "after_invalidation_sequence", "through_invalidation_sequence")
            for name in names:
                sequence = getattr(request, name)
                _check(type(sequence) is int and 0 <= sequence <= MAX_SEQUENCE,
                       "INVALID_REQUEST", f"Invalid {name}")

    @staticmethod
    def _translate_feed_error(exc):
        from de.silver.canonical_reader import (
            CanonicalContentConflict, SnapshotStaleError, ValidityUnknownError,
        )

        if isinstance(exc, SourceVerificationError):
            code = {
                "CURSOR_GAP": "RETENTION_GAP", "SOURCE_SCHEMA_UNSUPPORTED": "SOURCE_BINDING_CHANGED",
                "SOURCE_BINDING_CHANGED": "SOURCE_BINDING_CHANGED",
                "AUTHORITY_UNAVAILABLE": "AUTHORITY_UNAVAILABLE",
            }.get(exc.code, "SOURCE_INTEGRITY_ERROR")
        elif isinstance(exc, SnapshotStaleError):
            code = "BOUNDARY_STALE"
        elif isinstance(exc, (OperationalError, RetryableRepositoryError, ConnectionError)):
            code = "CLICKHOUSE_UNAVAILABLE"
        elif isinstance(exc, (ArchiveUnavailable, OSError, sqlite3.OperationalError)):
            code = "AUTHORITY_UNAVAILABLE"
        elif isinstance(exc, ValidityUnknownError):
            code = "CANONICAL_EVIDENCE_MISSING"
        elif isinstance(exc, (ClickHouseError, FactBusinessKeyConflictError, CanonicalContentConflict, ArchiveIntegrityError,
                              sqlite3.DatabaseError, KeyError, TypeError, ValueError)):
            code = "SOURCE_INTEGRITY_ERROR"
        else:
            raise exc
        return CanonicalFeedError(code, str(exc))

    def _capture_feed(self, namespace, expected_id, *, cold=False):
        # Sample applied cursors first so a concurrent normal append cannot make
        # a newer Silver cursor appear ahead of an older Bronze allocator sample.
        applied = self.silver.execute(
            "SELECT * FROM silver_v1_source_binding WHERE namespace=?", (namespace,)).fetchone()
        _check(applied is not None, "CANONICAL_EVIDENCE_MISSING", "Silver source binding absent")
        saved_binding = json.loads(applied["binding_json"])
        source = verify_bronze_source(self.bronze_path, namespace,
                                      expected_binding=saved_binding, full_history=cold)
        binding = {key: source[key] for key in _BINDING_KEYS}
        binding_id = "sha256:" + canonical_hash({
            "canonical_source_path" if key == "source_path" else key: value
            for key, value in binding.items()
        })
        _check(expected_id is None or expected_id == binding_id,
               "SOURCE_BINDING_CHANGED", "Saved feed source binding differs")
        for path, identity in self._feed_file_identities.items():
            info = path.stat()
            _check((info.st_dev, info.st_ino) == identity,
                   "SOURCE_BINDING_CHANGED", f"Open read session was replaced: {path}")
        hc, hi = source["change_high_water"], source["invalidation_high_water"]
        sc, si = applied["change_cursor"], applied["invalidation_cursor"]
        _check(0 <= sc and 0 <= si, "SOURCE_INTEGRITY_ERROR", "Negative Silver cursor")
        _check(sc <= hc and si <= hi, "SOURCE_REWOUND", "Bronze allocator trails applied Silver")
        return binding_id, binding, CapturedBoundary(
            captured_bronze_change_high_water=hc, captured_bronze_invalidation_high_water=hi,
            captured_silver_change_applied_sequence=sc, captured_silver_invalidation_applied_sequence=si,
        )

    def discover_canonical_source(self, request: SourceDiscoveryRequest) -> SourceDiscovery:
        self._validate_feed_request(request, SourceDiscoveryRequest)
        cold = request.expected_source_binding_id is None
        try:
            binding_id, binding, boundary = self._capture_feed(
                request.namespace, request.expected_source_binding_id, cold=cold)
            hc, hi, sc, si = self._feed_cursors(boundary)
            if cold:
                self._verify_feed_history(request.namespace, hc, sc, si)
                self._check_feed_end(request.namespace, binding_id, boundary, ())
            return SourceDiscovery(
                **asdict(self.feed_limits), source_binding_id=binding_id, namespace=request.namespace,
                source_binding=freeze(binding), bronze_change_high_water=hc,
                bronze_invalidation_high_water=hi, silver_change_applied_sequence=sc,
                silver_invalidation_applied_sequence=si, verified_change_prefix=sc,
                verified_invalidation_prefix=si, bootstrap_required=cold,
            )
        except CanonicalFeedError:
            raise
        except Exception as exc:
            raise self._translate_feed_error(exc) from exc

    @staticmethod
    def _feed_cursors(boundary):
        return (boundary.captured_bronze_change_high_water,
                boundary.captured_bronze_invalidation_high_water,
                boundary.captured_silver_change_applied_sequence,
                boundary.captured_silver_invalidation_applied_sequence)

    def _verify_feed_history(self, ns, hc, sc, si):
        # Allocator ranges and both journal link directions were checked by
        # verify_bronze_source. Walk the captured range without retaining it.
        for sequence in range(1, hc + 1):
            change = self._journal_row(ns, "changes", sequence)
            self._validate_transition(change)
            if sequence <= sc:
                self._validity_obligation(change)
        for sequence in range(1, si + 1):
            invalidation = self._journal_row(ns, "invalidations", sequence)
            self._invalidation_proof(ns, invalidation)

    def _journal_row(self, ns, stream, sequence):
        table, key = ("v1_changes", "change_sequence") if stream == "changes" else (
            "v1_invalidations", "invalidation_sequence")
        row = self.bronze.db.execute(
            f"SELECT * FROM {table} WHERE namespace=? AND {key}=?", (ns, sequence)).fetchone()
        _check(row is not None, "RETENTION_GAP", f"Missing {stream} sequence {sequence}", sequence=sequence)
        return dict(row)

    @staticmethod
    def _validate_transition(change):
        kind = change["change_type"]
        _check(change["new_state"] == kind and (
            (kind == "ACCEPTED" and change["previous_state"] == "PENDING" and
             change["revision"] == 1 and change["invalidation_id"] is None) or
            (kind == "CONFLICTED" and change["previous_state"] == "ACCEPTED" and
             change["revision"] == 2 and bool(change["invalidation_id"]))
        ), "INVALID_TRANSITION", "Invalid Bronze V1 transition", sequence=change["change_sequence"])

    @staticmethod
    def _transition_hash(change):
        return canonical_hash({key: change[key] for key in _TRANSITION_KEYS})

    def _identity(self, ns, event_id):
        row = self.bronze.current(ns, event_id)
        _check(row is not None, "SOURCE_INTEGRITY_ERROR", "Bronze current identity absent", event_id=event_id)
        return dict(row)

    def _obligations(self, ns, stream, sequence, event_id, *, table=None):
        params = (ns, stream, sequence)
        where = "namespace=? AND source_kind=? AND source_sequence=?"
        if table:
            where += " AND target_table=?"
            params += (table,)
        # Check lengths without pulling a corrupt, arbitrarily large JSON value
        # into Python. Stream the rows after this size check per obligation.
        sql = (
            "SELECT event_id,target_table,status,expected_hash,"
            "length(CAST(expected_json AS BLOB)) AS size,"
            "CASE WHEN length(CAST(expected_json AS BLOB))<=? THEN expected_json END AS body "
            "FROM silver_v1_obligation WHERE " + where
        )
        total = 0
        for row in self.silver.execute(sql, (self.feed_limits.max_single_entry_bytes, *params)):
            size = row["size"]
            total += size
            _check(total <= self.feed_limits.max_encoded_bytes_cap, "RECORD_TOO_LARGE",
                   "Frozen evidence exceeds the configured read budget",
                   sequence=sequence, event_id=event_id, observed_bytes_at_least=total)
            _check(row["body"] is not None, "RECORD_TOO_LARGE",
                   "One frozen obligation exceeds the single-entry limit",
                   sequence=sequence, event_id=event_id, observed_bytes=size)
            _check(row["event_id"] == event_id and hashlib.sha256(
                row["body"].encode()).hexdigest() == row["expected_hash"],
                "SOURCE_INTEGRITY_ERROR", "Frozen obligation event or hash differs", sequence=sequence)
            yield row["target_table"], row["status"], json.loads(row["body"]), row["expected_hash"]

    def _validity_obligation(self, change):
        ns, seq, eid = change["namespace"], change["change_sequence"], change["event_id"]
        marker = None
        signature = hashlib.sha256()
        for table, status, row, digest in self._obligations(ns, "changes", seq, eid):
            _check(status == "RECONCILED", "CANONICAL_EVIDENCE_MISSING",
                   "Applied change obligation is not reconciled", sequence=seq)
            signature.update(digest.encode("ascii"))
            if table == "silver_event_validity_v1":
                _check(marker is None, "SOURCE_INTEGRITY_ERROR", "Multiple validity obligations")
                marker = row
        _check(marker is not None, "CANONICAL_EVIDENCE_MISSING", "Missing change validity obligation",
               sequence=seq, event_id=eid)
        expected = dict(namespace=ns, event_id=eid, semantic_revision=change["revision"],
                        bronze_change_sequence=seq, invalidation_id=change["invalidation_id"])
        state = (("VALID", "ACCEPTED"), ("INVALIDATED", "SUPERSEDED_BY_CONFLICT")) if (
            change["change_type"] == "ACCEPTED") else (("INVALIDATED", "BRONZE_CONFLICT"),)
        _check(all(marker.get(key) == value for key, value in expected.items()) and
               (marker.get("validity_state"), marker.get("reason")) in state,
               "SOURCE_INTEGRITY_ERROR", "Frozen validity marker does not match transition", sequence=seq)
        return marker, signature.hexdigest()

    def _local_validity(self, ns, identity, status, invalidation_sequence=None):
        row = self.silver.execute(
            "SELECT state, semantic_revision, change_sequence, invalidation_sequence, "
            "simulation_run_id, topology_digest, simulation_time_seconds "
            "FROM silver_v1_event_validity WHERE namespace=? AND event_id=?",
            (ns, identity["event_id"])).fetchone()
        expected = (status, identity["revision"], identity["change_sequence"], invalidation_sequence,
                    identity["simulation_run_id"], identity["topology_digest"],
                    identity["simulation_time_seconds"])
        _check(row is not None and tuple(row) == expected, "CANONICAL_EVIDENCE_MISSING",
               "Silver current validity does not match applied source")

    def _invalidation_proof(self, ns, invalidation):
        seq, eid, iid = (invalidation[key] for key in ("invalidation_sequence", "event_id", "invalidation_id"))
        identity = self._identity(ns, eid)
        conflict = self._journal_row(ns, "changes", identity["change_sequence"])
        self._validate_transition(conflict)
        accepted = self.bronze.db.execute(
            "SELECT * FROM v1_changes WHERE namespace=? AND event_id=? AND revision=1", (ns, eid)
        ).fetchone()
        _check(accepted is not None, "RETENTION_GAP", "Conflict has no retained ACCEPTED transition")
        self._validate_transition(accepted)
        _check(identity["state"] == conflict["change_type"] == "CONFLICTED" and
               identity["revision"] == conflict["revision"] == invalidation["revision"] and
               conflict["invalidation_id"] == iid and conflict["event_id"] == eid and
               accepted["change_sequence"] < conflict["change_sequence"] and
               accepted["content_hash"] == conflict["content_hash"] == identity["content_hash"] and
               identity["first_raw_id"] == invalidation["first_raw_id"] and
               identity["envelope_hash"] == invalidation["previous_hash"] and
               invalidation["previous_hash"] != invalidation["conflicting_hash"],
               "INVALIDATION_LINK_MISMATCH", "Bronze invalidation and conflict identities differ")
        try:
            provenance = self.bronze.read_invalidation_provenance(ns, iid)
        except RuntimeError as exc:
            raise CanonicalFeedError("CANONICAL_EVIDENCE_MISSING", str(exc)) from exc
        self._local_validity(ns, identity, "INVALIDATED", seq)
        obligations = self._obligations(ns, "invalidations", seq, eid, table="silver_invalidation_v1")
        obligation = next(obligations, None)
        _check(obligation is not None, "CANONICAL_EVIDENCE_MISSING", "Invalidation obligation absent")
        _check(next(obligations, None) is None, "SOURCE_INTEGRITY_ERROR", "Ambiguous invalidation obligation")
        _, mirror_status, row, digest = obligation
        expected = dict(namespace=ns, invalidation_id=iid, invalidation_sequence=seq, event_id=eid,
                        semantic_revision=invalidation["revision"],
                        previous_immutable_envelope_hash=invalidation["previous_hash"],
                        conflicting_immutable_envelope_hash=invalidation["conflicting_hash"],
                        original_raw_ingestion_id=invalidation["first_raw_id"],
                        conflicting_raw_ingestion_id=invalidation["conflicting_raw_id"])
        for key in ("simulation_run_id", "topology_digest", "intersection_id", "cycle_sequence",
                    "simulation_time_seconds"):
            expected[key] = identity[key]
        _check(provenance is not None and all(row.get(k) == v for k, v in expected.items())
               and mirror_status in {"PENDING", "RECONCILED"},
               "INVALIDATION_LINK_MISMATCH", "Silver suppression differs from Bronze provenance")
        # The full lineage was captured by the Silver worker. Delivery attempt
        # timestamps/status can subsequently change, so compare immutable keys.
        lineage = json.loads(row["lineage_json"])
        for name in ("original", "conflicting"):
            keys = ("namespace", "raw_id", "topic", "partition_id", "offset_value", "topic_identity",
                    "event_id", "disposition", "expected_hash", "revision")
            delivery = provenance[name + "_delivery"]
            _check(hashlib.sha256(delivery["expected_json"].encode()).hexdigest() == delivery["expected_hash"],
                   "SOURCE_INTEGRITY_ERROR", "Bronze invalidation delivery obligation hash differs")
            _check(all(lineage[name][key] == delivery[key] for key in keys),
                   "INVALIDATION_LINK_MISMATCH", "Frozen invalidation lineage differs")
        return identity, conflict, row, mirror_status, digest

    def _run_identity(self, identity, generation=None):
        context = self.archive_resolver.resolve(
            identity["simulation_run_id"], identity["topology_digest"], generation)
        _check(context.registry_status is not None and identity["intersection_id"] in context.directions,
               "CANONICAL_EVIDENCE_MISSING", "Missing published archive identity")
        return dict(run_id=context.run_id, run_generation=context.generation,
                    topology_digest=context.topology_digest, manifest_identity=context.manifest_identity)

    def _verified_bundle(self, ns, event_id, sequence, revision, run):
        from de.silver.canonical_reader import CanonicalReadQuery, ObservationBundle

        query = CanonicalReadQuery(ns, run["run_id"], run["topology_digest"], -1e300, 1e300)
        physical = self._bundle(event_id, revision, sequence, query)
        # Read the stable JSON form from the same verified obligations. Driver
        # objects such as DateTime64 cannot be hashed as JSON.
        tables = {
            "silver_intersection_state_fact_v1": (physical.intersection,),
            "silver_traffic_observation_fact_v1": physical.traffic,
            "silver_signal_state_fact_v1": physical.signal,
        }
        facts = []
        for table, actual in tables.items():
            frozen = list(self._obligations(ns, "changes", sequence, event_id, table=table))
            expected = {row.get("source_direction"): row for _, status, row, _ in frozen
                        if status == "RECONCILED"}
            _check(len(expected) == len(actual) == len(frozen), "CANONICAL_EVIDENCE_MISSING",
                   "Missing or duplicate frozen fact")
            ordered = []
            for physical_row in actual:
                row = expected[physical_row.get("source_direction")]
                _check((row["run_generation"], row["manifest_identity"]) ==
                       (run["run_generation"], run["manifest_identity"]),
                       "SOURCE_INTEGRITY_ERROR", "Canonical bundle archive identity differs")
                ordered.append(freeze(row))
            facts.append(tuple(ordered))
        return ObservationBundle(event_id, facts[0][0], facts[1], facts[2])

    def _read_change_entry(self, ns, sequence, boundary, *, materialize=True):
        change = self._journal_row(ns, "changes", sequence)
        self._validate_transition(change)
        eid = change["event_id"]
        identity = self._identity(ns, eid)
        hc, hi, sc, si = self._feed_cursors(boundary)
        current_seq = identity["change_sequence"]
        _check(type(current_seq) is int and current_seq >= sequence and
               identity["content_hash"] == change["content_hash"],
               "SOURCE_INTEGRITY_ERROR", "Current identity differs from its transition")
        _check(current_seq <= hc, "BOUNDARY_STALE", "Included identity advanced above captured Hc")
        bundle = bundle_hash = superseding = inv_seq = suppression_digest = generation = None
        if identity["state"] == "CONFLICTED":
            if current_seq > sc:
                raise _PrefixBlocked("Conflict awaits Silver change application")
            conflict = self._journal_row(ns, "changes", current_seq)
            inv = self.bronze.db.execute(
                "SELECT * FROM v1_invalidations WHERE namespace=? AND invalidation_id=?",
                (ns, conflict["invalidation_id"])).fetchone()
            _check(inv is not None, "INVALIDATION_LINK_MISMATCH", "Conflict invalidation absent")
            inv_seq = inv["invalidation_sequence"]
            if inv_seq > min(hi, si):
                raise _PrefixBlocked("Conflict awaits Silver invalidation application")
            _, _, row, _, suppression_digest = self._invalidation_proof(ns, dict(inv))
            generation = row["run_generation"]
            if change["change_type"] == "ACCEPTED":
                superseding = current_seq
                disposition = "SUPERSEDED_BY_CONFLICT"
            else:
                _check(sequence == current_seq, "INVALID_TRANSITION", "Unexpected historical conflict")
                disposition = "CONFLICTED"
        else:
            _check(identity["state"] == change["change_type"] == "ACCEPTED" and
                   identity["revision"] == change["revision"] and current_seq == sequence,
                   "INVALID_TRANSITION", "Accepted transition is not the current accepted identity")
            try:
                self._local_validity(ns, identity, "VALID")
            except CanonicalFeedError as exc:
                if self._identity(ns, eid) != identity:
                    raise CanonicalFeedError(
                        "BOUNDARY_STALE", "Included identity changed before its Silver validity check"
                    ) from exc
                raise
            disposition = "BUNDLE_VALID"
        marker, signature = self._validity_obligation(change)
        _check(marker["simulation_run_id"] == identity["simulation_run_id"] and
               marker["topology_digest"] == identity["topology_digest"],
               "SOURCE_INTEGRITY_ERROR", "Validity obligation archive identity differs")
        run = self._run_identity(identity, generation)
        if disposition == "BUNDLE_VALID":
            _check(marker["validity_state"] == "VALID", "SOURCE_INTEGRITY_ERROR", "Valid marker expected")
            if materialize:
                bundle = self._verified_bundle(ns, eid, sequence, change["revision"], run)
                bundle_hash = canonical_hash(bundle)
        transition_hash = self._transition_hash(change)
        entry = CanonicalChangeEntry(
            **run, change_sequence=sequence, event_id=eid, semantic_revision=change["revision"],
            change_type=change["change_type"], previous_state=change["previous_state"],
            new_state=change["new_state"], source_transition_hash=transition_hash,
            source_invalidation_id=change["invalidation_id"], effective_disposition=disposition,
            superseding_change_sequence=superseding, invalidation_sequence=inv_seq,
            canonical_bundle=bundle, bundle_hash=bundle_hash,
        )
        return entry, (transition_hash, identity, signature, suppression_digest, run)

    def _read_invalidation_entry(self, ns, sequence, boundary, *, materialize=True):
        invalidation = self._journal_row(ns, "invalidations", sequence)
        identity, conflict, row, status, digest = self._invalidation_proof(ns, invalidation)
        hc, _, _, _ = self._feed_cursors(boundary)
        _check(conflict["change_sequence"] <= hc, "BOUNDARY_STALE", "Conflict exceeds captured Hc")
        run = self._run_identity(identity, row["run_generation"])
        entry = CanonicalInvalidationEntry(
            **run, invalidation_sequence=sequence, invalidation_id=row["invalidation_id"],
            event_id=row["event_id"], semantic_revision=row["semantic_revision"],
            previous_immutable_envelope_hash=row["previous_immutable_envelope_hash"],
            conflicting_immutable_envelope_hash=row["conflicting_immutable_envelope_hash"],
            source_transition_hash=self._transition_hash(conflict), mirror_status=status,
            intersection_id=row["intersection_id"], cycle_sequence=row["cycle_sequence"],
            simulation_time_seconds=row["simulation_time_seconds"],
        )
        return entry, (entry.source_transition_hash, identity, digest, run)

    def _check_feed_end(self, ns, binding, boundary, proofs):
        try:
            _, _, ending = self._capture_feed(ns, binding)
        except (CanonicalFeedError, SourceVerificationError) as exc:
            if exc.code == "SOURCE_BINDING_CHANGED":
                raise CanonicalFeedError("BOUNDARY_STALE", str(exc)) from exc
            raise
        hc, hi, sc, si = self._feed_cursors(boundary)
        end_hc, end_hi, end_sc, end_si = self._feed_cursors(ending)
        _check(end_hc >= hc and end_hi >= hi, "SOURCE_REWOUND", "Bronze allocator regressed")
        _check(end_sc >= sc and end_si >= si, "SILVER_CURSOR_REGRESSION", "Silver applied cursor regressed")
        # One final Bronze snapshot checks every included effective identity
        # together, after the slower external evidence reads have finished.
        self.bronze.db.execute("BEGIN")
        try:
            for event, wanted in proofs:
                _check(self._identity(ns, event) == wanted, "BOUNDARY_STALE", "Included identity changed")
        finally:
            self.bronze.db.execute("ROLLBACK")

    def read_canonical_changes_page(self, request: CanonicalChangePageRequest) -> CanonicalChangePage:
        self._validate_feed_request(request, CanonicalChangePageRequest)
        return self._read_feed_page(request, False)

    def read_canonical_invalidations_page(
        self, request: CanonicalInvalidationPageRequest,
    ) -> CanonicalInvalidationPage:
        self._validate_feed_request(request, CanonicalInvalidationPageRequest)
        return self._read_feed_page(request, True)

    def _read_feed_page(self, request, invalidations):
        started = time.monotonic()
        try:
            return self._build_feed_page(request, invalidations, started)
        except CanonicalFeedError:
            raise
        except Exception as exc:
            raise self._translate_feed_error(exc) from exc

    def _build_feed_page(self, request, invalidations, started):
        ns = request.namespace
        deadline = started + request.max_processing_ms / 1000
        binding, _, boundary = self._capture_feed(ns, request.expected_source_binding_id)
        hc, hi, sc, si = self._feed_cursors(boundary)
        bounds = asdict(boundary)
        boundary_id = "sha256:" + canonical_hash(dict(
            bounds, contract_version=CONTRACT_VERSION, source_binding_id=binding))
        if invalidations:
            after, requested = request.after_invalidation_sequence, request.through_invalidation_sequence
            _check(after <= requested, "INVALID_REQUEST", "Cursor exceeds requested invalidation ceiling")
            high, applied, ceiling = hi, si, min(requested, hi, si)
            read_entry = self._read_invalidation_entry
        else:
            after = request.after_change_sequence
            high, applied, ceiling, requested = hc, sc, sc, hc
            read_entry = self._read_change_entry
        _check(after <= high, "CURSOR_AHEAD", "Cursor exceeds Bronze allocator")
        _check(after <= applied, "SILVER_CURSOR_REGRESSION", "Cursor exceeds Silver applied prefix")
        lag = "INVALIDATION_LAG" if invalidations else "SOURCE_LAG"
        _check(after < ceiling or after == requested, lag, "Silver has not applied more sequences")

        def envelope(through, entries, count):
            receipt = None
            if count:
                header = dict(bounds, namespace=ns, source_binding_id=binding,
                              requested_after_sequence=after, returned_after_sequence=after,
                              returned_through_sequence=through, entry_count=count)
                if invalidations:
                    receipt = CanonicalInvalidationReceipt(**header, receipt_kind="INVALIDATION",
                        requested_through_invalidation_sequence=requested, safe_invalidation_through_sequence=None)
                else:
                    receipt = CanonicalBatchReceipt(**header, receipt_kind="CHANGE",
                                                    safe_invalidation_through_sequence=si)
            common = dict(namespace=ns, source_binding_id=binding, boundary_id=boundary_id,
                          entries=tuple(entries), entry_count=count, has_more=through < ceiling,
                          status="OK" if count else "EMPTY", receipt=receipt)
            if invalidations:
                return CanonicalInvalidationPage(**common, after_invalidation_sequence=after,
                    through_invalidation_sequence=through, requested_through_invalidation_sequence=requested,
                    captured_bronze_invalidation_high_water=hi, captured_silver_invalidation_applied_sequence=si,
                    verified_invalidation_prefix=ceiling)
            return CanonicalChangePage(**common, **bounds, after_change_sequence=after,
                through_change_sequence=through, verified_change_prefix=sc, safe_invalidation_through_sequence=si)

        entries, proofs = [], []
        entry_bytes = 0
        page = envelope(after, (), 0)
        # Reserve time for the final checks. A page-time stop may emit only what
        # was complete before this cutoff, and final validation shares the hard deadline.
        entry_cutoff = deadline - min(0.250, request.max_processing_ms / 10_000)
        for sequence in range(after + 1, min(ceiling, after + request.max_entries) + 1):
            if time.monotonic() >= entry_cutoff:
                _check(entries, "DEADLINE_EXCEEDED", "Page deadline before first entry")
                break
            entry_started = time.monotonic()
            evidence_deadline = entry_started + request.max_entry_processing_ms / 1000
            try:
                with feed_query_budget(min(entry_cutoff, evidence_deadline), self.feed_limits.max_encoded_bytes_cap):
                    entry, proof = read_entry(ns, sequence, boundary)
            except FeedEvidenceDeadline:
                if entry_cutoff < evidence_deadline and time.monotonic() < evidence_deadline:
                    _check(entries, "DEADLINE_EXCEEDED", "Page budget expired on the first entry")
                    break
                source = self._journal_row(ns, "invalidations" if invalidations else "changes", sequence)
                raise CanonicalFeedError("ENTRY_TIMEOUT", "ClickHouse evidence deadline exceeded",
                    sequence=sequence, event_id=source["event_id"],
                    elapsed_ms=(time.monotonic() - entry_started) * 1000,
                    operation="read ClickHouse evidence")
            except FeedEvidenceSize as exc:
                source = self._journal_row(ns, "invalidations" if invalidations else "changes", sequence)
                raise CanonicalFeedError("RECORD_TOO_LARGE", str(exc), sequence=sequence,
                    event_id=source["event_id"], evidence_limit_bytes=self.feed_limits.max_encoded_bytes_cap) from exc
            except _PrefixBlocked as exc:
                _check(entries, "PREFIX_BLOCKED", str(exc), sequence=sequence)
                break
            size = len(canonical_bytes(entry))
            elapsed = (time.monotonic() - entry_started) * 1000
            _check(elapsed <= request.max_entry_processing_ms, "ENTRY_TIMEOUT", "Entry deadline exceeded",
                   sequence=sequence, event_id=entry.event_id, elapsed_ms=elapsed,
                   operation="verify and serialize source entry")
            if time.monotonic() >= entry_cutoff:
                _check(entries, "DEADLINE_EXCEEDED", "Page deadline before first complete entry")
                break
            _check(size <= self.feed_limits.max_single_entry_bytes, "RECORD_TOO_LARGE",
                   "Canonical entry exceeds supported size", sequence=sequence, event_id=entry.event_id,
                   observed_bytes=size, max_single_entry_bytes=self.feed_limits.max_single_entry_bytes)
            candidate = envelope(sequence, (), len(entries) + 1)
            candidate_size = size_page(candidate, entry_bytes + size)
            if candidate_size > request.max_encoded_bytes:
                _check(entries, "INVALID_REQUEST", "Page cannot fit first canonical entry",
                       sequence=sequence, minimum_encoded_bytes=candidate_size)
                break
            entries.append(entry)
            proofs.append((sequence, proof))
            entry_bytes += size
            page = candidate
        for sequence, wanted in proofs:
            _check(time.monotonic() < deadline, "DEADLINE_EXCEEDED", "Final verification deadline")
            try:
                _, actual = read_entry(ns, sequence, boundary, materialize=False)
            except _PrefixBlocked as exc:
                raise CanonicalFeedError("BOUNDARY_STALE", str(exc)) from exc
            _check(actual == wanted, "BOUNDARY_STALE", "Included source evidence changed", sequence=sequence)
        self._check_feed_end(ns, binding, boundary, ((p[1]["event_id"], p[1]) for _, p in proofs))
        page = replace(page, entries=tuple(entries))
        size = size_page(page, entry_bytes)
        _check(size <= request.max_encoded_bytes, "INVALID_REQUEST", "Page envelope exceeds byte budget",
               minimum_encoded_bytes=size)
        if page.receipt:
            digest = batch_digest(page.receipt, page.entries)
            page = replace(page, receipt=replace(page.receipt, encoded_bytes=size,
                                                  batch_digest=digest, receipt_id="sha256:" + digest))
        _check(time.monotonic() <= deadline, "DEADLINE_EXCEEDED", "Page deadline during final validation")
        _LOG.debug("Silver feed %s sequences=%d bytes=%d elapsed_ms=%.2f",
                   "invalidation" if invalidations else "change", len(entries), size,
                   (time.monotonic() - started) * 1000)
        return replace(page, encoded_bytes=size)
