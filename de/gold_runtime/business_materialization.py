"""Isolated Core-to-Business handoff through verified Silver/Gold metadata."""

import json
import time
import zlib
from base64 import b64decode, b64encode
from collections import Counter
from dataclasses import replace

from contracts.observation_schedule import MAX_CANONICAL_INTEGER, ObservationSchedule, TICKS_PER_SECOND, seconds_to_ticks
from contracts.traffic_observation import canonical_json
from de.gold.business import Claim, DirectionSample, ResourceLimit, VERIFIED_DEFINITION, WindowSpec, compute_fixture_window
from de.gold_runtime.business_state import _MEMBER_LIMIT, _PROJECTION_LIMIT, _verify_canonical
from de.gold_runtime.checkpoint_store import CoreStateFault
from de.silver.incremental_contract import canonical_bytes, canonical_hash


_ENCODED_RECEIPT_BUDGET = 64 * 1024 * 1024
_ISOLATED_WORK_SECONDS = 60
_DECODED_PROJECTION_LIMIT = 512 * 1024


def _encode_projection(document):
    document_bytes = canonical_bytes(document)
    if document["version"] == 1:
        return document_bytes
    if len(document_bytes) > _DECODED_PROJECTION_LIMIT:
        raise ResourceLimit("Business projection exceeds its decoded byte budget")
    return canonical_bytes({"version": 2, "deflate": b64encode(zlib.compress(document_bytes)).decode("ascii")})


def _decode_projection(raw, expected_version):
    value = json.loads(raw)
    if value["version"] != expected_version:
        raise CoreStateFault("Unsupported Business projection version")
    if expected_version == 2:
        decoder = zlib.decompressobj()
        try:
            content = decoder.decompress(b64decode(value["deflate"], validate=True), _DECODED_PROJECTION_LIMIT + 1)
            if len(content) > _DECODED_PROJECTION_LIMIT or not decoder.eof or decoder.unused_data:
                raise ValueError("invalid size or trailing data")
            value = json.loads(content)
            if value["version"] != 2 or canonical_bytes(value) != content:
                raise ValueError("projection content is not canonical")
        except (KeyError, TypeError, ValueError, zlib.error) as exc:
            raise CoreStateFault("Invalid compressed Business projection") from exc
    return value


def _ordinals(schedule, start, end):
    def first_at(tick):
        return max(0, (tick - schedule.first_tick + schedule.period_ticks - 1) // schedule.period_ticks)

    return first_at(start), first_at(end)


def applied_window_coverage(state, run_key, start_s, duration_s, *, core_eligible=False):
    """Classify an aligned interval from applied metadata, including absent Core windows."""
    if (type(start_s) is not int or type(duration_s) is not int or duration_s not in (60, 300)
            or start_s < 0 or start_s % duration_s or (start_s + duration_s) * TICKS_PER_SECOND > MAX_CANONICAL_INTEGER):
        raise ValueError("Expected an aligned 60s/300s Business interval")
    db = state.store.conn
    row = db.execute("""
        SELECT s.*,c.settled_ordinal FROM business_observation_schedule s JOIN business_metadata_cursor c USING(run_key)
        WHERE s.run_key=? AND c.stream='SCHEDULE'
    """, (run_key,)).fetchone()
    if row is None:
        return "UNSUPPORTED_SCHEDULE", None
    start = start_s * TICKS_PER_SECOND
    end = start + duration_s * TICKS_PER_SECOND
    started, cutoff = row["start_tick"], row["cutoff"]
    if started is None:
        return ("OUTSIDE_RUN" if cutoff is not None else "WAITING"), None
    if end <= started or (cutoff is not None and start >= cutoff):
        return "OUTSIDE_RUN", None
    if start < started:
        return "PARTIAL_BOUNDARY", None
    if cutoff is not None and end > cutoff:
        return "TERMINAL_PARTIAL", None
    raw = canonical_json(json.loads(row["schedule"])).encode()
    schedule = ObservationSchedule.decode(raw, row["schedule_id"])
    first, stop = _ordinals(schedule, start, end)
    if stop - first < 2 or stop - first > 300:
        raise ResourceLimit("Business expected window exceeds its slot bounds")
    settled = row["settled_ordinal"]
    settled_tick = schedule.due_tick(settled) if settled >= 0 else 0
    observed_tick = max(row["lifecycle_tick"] or 0, settled_tick, cutoff or 0)
    if stop - 1 > settled or (not core_eligible and observed_tick < end):
        return "WAITING", None
    slots = db.execute("""
        SELECT ordinal,outcome FROM business_observation_slot WHERE run_key=? AND ordinal>=? AND ordinal<?
        ORDER BY ordinal LIMIT 301
    """, (run_key, first, stop)).fetchall()
    if tuple(slot["ordinal"] for slot in slots) != tuple(range(first, stop)):
        raise CoreStateFault("Gold applied observation slots contain a gap")
    suppressed = db.execute("""
        SELECT event_id,sequence FROM core_suppression WHERE run_key=? AND simulation_time>=? AND simulation_time<?
        LIMIT ?
    """, (run_key, start_s, start_s + duration_s, _MEMBER_LIMIT + 1)).fetchall()
    if len(suppressed) > _MEMBER_LIMIT:
        raise ResourceLimit("Business invalidation evidence exceeds its member budget")
    suppressions = dict(map(tuple, suppressed))
    due, conflicts, outcomes, linked_suppressions = {}, [], [], []
    for slot in slots:
        ordinal = slot["ordinal"]
        outcome = json.loads(slot["outcome"])
        outcomes.append([ordinal, outcome])
        events = outcome["events"]
        if outcome["outcome"] in ("CAPTURE_FAILED", "MISSED_TICK", "CLOCK_MISMATCH") and not events:
            continue
        expected = dict(zip(schedule.event_ids(ordinal), (node for node, _ in schedule.participants)))
        if (outcome["outcome"] != "DELIVERED" or len(events) != len(expected)
                or {event["eventId"] for event in events} != expected.keys()):
            raise CoreStateFault("Applied schedule result differs from the expected participants")
        tick = schedule.due_tick(ordinal)
        for event in events:
            event_id = event["eventId"]
            if event_id in suppressions:
                linked_suppressions.append((event_id, suppressions[event_id]))
            if event["disposition"] == "BUNDLE_VALID" and event_id not in suppressions:
                due[event_id] = tick
            elif event["disposition"] in ("BUNDLE_VALID", "CONFLICTED", "SUPERSEDED_BY_CONFLICT"):
                conflicts.append((expected[event_id], tick))
            else:
                raise CoreStateFault("Applied schedule result has an unknown canonical disposition")
    if not due:
        return ("CONFLICTED_WINDOW" if conflicts else "MISSING_WINDOW"), None
    spec = WindowSpec(start, duration_s, tuple(schedule.due_tick(i) for i in range(first, stop)),
                      schedule.participants, schedule.due_tick(stop - 1), conflicted_slots=tuple(sorted(conflicts)))
    evidence = [row["schedule_id"], outcomes]
    if conflicts:
        evidence.extend((spec.conflicted_slots, sorted(linked_suppressions)))
    boundary_hash = canonical_hash(evidence)
    return "READY", (spec, due, boundary_hash)


def _window_boundary(state, window_id):
    db = state.store.conn
    window = db.execute("SELECT * FROM core_window WHERE id=?", (window_id,)).fetchone()
    if window is None or not window["eligible"]:
        raise CoreStateFault("Business work is not a verified eligible Core interval")
    status, coverage = applied_window_coverage(state, window["run_key"], window["start"], window["duration"],
                                               core_eligible=True)
    if coverage is None:
        return status, None
    spec, due, boundary_hash = coverage
    marker = db.execute("SELECT receipt_digest FROM business_metadata_dirty WHERE window_id=?", (window_id,)).fetchone()
    return status, (spec, due, marker[0] if marker else None, boundary_hash)


def seal_business_work(state, ledger, work, materialize):
    """Use one normal verified Core traversal to construct a bounded Business projection."""
    progress = iter_seal_business_work(state, ledger, work, materialize,
                                       deadline=None, fixture_deadline=time.monotonic() + _ISOLATED_WORK_SECONDS)
    while True:
        try:
            next(progress)
        except StopIteration as result:
            return result.value


def iter_seal_business_work(state, ledger, work, materialize, *, deadline, measure_occupancy=False,
                            fixture_deadline=None):
    """Yield after each verified member so the coordinator can service other lanes."""
    window_id = work["window_id"]
    try:
        status, boundary = _window_boundary(state, window_id)
    except ResourceLimit:
        state.hold_unsealed(work)
        return "RESOURCE_LIMIT"
    if status != "READY":
        state.hold_unsealed(work, reason=status)
        return status
    spec, due, marker, boundary_hash = boundary
    claims = []
    queue_methods = {}
    occupancy_methods = {}
    context = {}
    measurement_refs = []
    stream = materialize(work)

    def check_time():
        now = time.monotonic()
        if deadline is not None and now >= deadline:
            raise TimeoutError("Stage A drain stopped before sealing the current proof")
        if fixture_deadline is not None and now >= fixture_deadline:
            raise ResourceLimit("Business fixture exceeded its wall budget")

    try:
        for index, bundle in enumerate(stream, 1):
            if index > _MEMBER_LIMIT:
                raise ResourceLimit("Business work exceeds its member budget")
            if stream.bytes > _ENCODED_RECEIPT_BUDGET:
                raise ResourceLimit("Business work exceeds its encoded receipt budget")
            check_time()
            event_id = bundle.event_id
            if event_id not in due:
                yield
                continue
            coord = bundle.intersection
            current = state.store.conn.execute("SELECT digest,payload FROM business_measurement WHERE event_id=?",
                                              (event_id,)).fetchone()
            values = None
            if current is not None:
                record = json.loads(current["payload"])
                measurement_refs.append((event_id, current["digest"]))
                values = record["measurements"]
            if measure_occupancy:
                node = coord["intersection_id"]
                local = context.setdefault(node, {"scenario_samples": Counter(), "control_samples": Counter(),
                    "incident_slots": 0, "spillback_slots": 0, "spillback_detected_slots": 0})
                for key, source in (("scenario_samples", "scenario_id"), ("control_samples", "control_mode")):
                    if coord.get(source):
                        local[key][coord[source]] += 1
                incidents = coord.get("incidents_json")
                if incidents and json.loads(incidents):
                    local["incident_slots"] += 1
                spillback = values.get("spillback") if values else None
                if spillback is not None:
                    local["spillback_slots"] += 1
                    local["spillback_detected_slots"] += int(spillback["value"]["detected"])
            samples = []
            for row in bundle.traffic:
                direction = row["source_direction"]
                measures = values["directions"].get(direction, {}) if values else {}
                queue = measures.get("queue")
                occupancy = measures.get("occupancy") if measure_occupancy else None
                if queue is not None:
                    queue_methods.setdefault((coord["intersection_id"], direction), set()).add(
                        (queue["method"], queue["scopeDigest"]))
                if occupancy is not None:
                    occupancy_methods.setdefault((coord["intersection_id"], direction), set()).add(
                        (occupancy["method"], occupancy["scopeDigest"]))
                samples.append(DirectionSample(direction, row["vehicle_count"], row["waiting_vehicle_count"],
                                               row["average_speed_kmh"], queue["value"] if queue is not None else None,
                                               occupancy["value"] if occupancy is not None else None))
            claims.append(Claim(due[event_id], seconds_to_ticks(coord["simulation_time_seconds"]),
                                coord["intersection_id"], event_id, tuple(samples)))
            yield
        if not claims:
            state.hold_unsealed(work, reason="NO_LINKED_MEMBERS")
            return "NO_LINKED_MEMBERS"
        mixed_queue = {key for key, methods in queue_methods.items() if len(methods) > 1}
        mixed_occupancy = {key for key, methods in occupancy_methods.items() if len(methods) > 1}
        if mixed_queue or mixed_occupancy:
            claims = [replace(claim, directions=tuple(replace(sample,
                queue_m=None if (claim.intersection, sample.direction) in mixed_queue else sample.queue_m,
                occupancy_pct=None if (claim.intersection, sample.direction) in mixed_occupancy else sample.occupancy_pct)
                for sample in claim.directions)) for claim in claims]
        projection = _encode_projection({
            "version": 2 if measure_occupancy else 1, "start": spec.start_us, "duration": spec.duration_s,
            "slots": spec.slots_us, "population": spec.population, "through": spec.settled_through_us,
            **({"context": context} if measure_occupancy else {}),
            **({"measurement_methods": [
                [node, direction, {
                    "queue": sorted(queue_methods.get((node, direction), ())),
                    "occupancy": sorted(occupancy_methods.get((node, direction), ())),
                }] for node, directions in spec.population for direction in directions
            ]} if measure_occupancy else {}),
            **({"conflicted_slots": spec.conflicted_slots} if spec.conflicted_slots else {}),
            "claims": [[claim.due_us, claim.actual_us, claim.intersection, claim.event_id,
                        [[x.direction, x.vehicles, x.stopped, x.moving_speed_kmh, x.queue_m]
                         + ([x.occupancy_pct] if measure_occupancy else []) for x in claim.directions]]
                       for claim in sorted(claims, key=lambda item: (item.due_us, item.intersection, item.event_id))],
        })
        if len(projection) > _PROJECTION_LIMIT:
            raise ResourceLimit("Business projection exceeds its byte budget")
        check_time()
    except ResourceLimit:
        state.hold_unsealed(work)
        return "RESOURCE_LIMIT"
    metadata_digest = canonical_hash([boundary_hash, sorted(measurement_refs)])
    return "SEALED" if state.finish(ledger, work, stream.input_hash, stream.count, projection,
        definition_set=VERIFIED_DEFINITION if measure_occupancy else "stage_a_fixture_v1",
        source_semantics="silver_observation_metadata_v1",
        coverage_policy="isolated_strict_v1", population_digest=canonical_hash(spec.population),
        metadata_digest=metadata_digest, metadata_marker=marker, verify_metadata=True) else "STALE"


def compute_business_once(publisher):
    """Compute the oldest durable supported projection, then persist its exact result bytes."""
    state = publisher.state
    row = state.store.conn.execute("""
        SELECT o.* FROM business_obligation o JOIN core_binding b ON b.binding_id=o.binding_id
        JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
        WHERE b.namespace=? AND o.state='PENDING' AND o.source_semantics='silver_observation_metadata_v1'
        ORDER BY o.created_at,o.id LIMIT 1
    """, (state.namespace,)).fetchone()
    if row is None:
        return None
    if _verify_canonical(row["projection"], _PROJECTION_LIMIT) != row["projection_hash"]:
        raise CoreStateFault("Sealed Business projection hash differs")
    expected_version = 2 if row["definition_set"] == VERIFIED_DEFINITION else 1
    value = _decode_projection(row["projection"], expected_version)
    spec = WindowSpec(value["start"], value["duration"], tuple(value["slots"]),
                      tuple((node, tuple(dirs)) for node, dirs in value["population"]), value["through"],
                      conflicted_slots=tuple(tuple(item) for item in value.get("conflicted_slots", ())))
    claims = (Claim(due, actual, node, event, tuple(DirectionSample(*item) for item in dirs))
              for due, actual, node, event, dirs in value["claims"])
    result = compute_fixture_window(spec, claims, definition_set=row["definition_set"])
    return publisher.save_computed(row["id"], row["projection_hash"], result.encode(
        context=value.get("context"), measurement_methods=value.get("measurement_methods")))
