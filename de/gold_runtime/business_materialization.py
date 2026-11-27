"""Isolated Core-to-Business handoff through verified Silver/Gold metadata."""

import json
import time
from dataclasses import replace

from contracts.observation_schedule import ObservationSchedule, TICKS_PER_SECOND, seconds_to_ticks
from contracts.traffic_observation import canonical_json
from de.gold.business import Claim, DirectionSample, ResourceLimit, WindowSpec, compute_fixture_window
from de.gold_runtime.business_state import _MEMBER_LIMIT, _PROJECTION_LIMIT, _verify_canonical
from de.gold_runtime.checkpoint_store import CoreStateFault
from de.silver.incremental_contract import canonical_bytes, canonical_hash


_ENCODED_RECEIPT_BUDGET = 64 * 1024 * 1024
_ISOLATED_WORK_SECONDS = 60


def _ordinals(schedule, start, end):
    def first_at(tick):
        return max(0, (tick - schedule.first_tick + schedule.period_ticks - 1) // schedule.period_ticks)

    return first_at(start), first_at(end)


def _window_boundary(state, window_id):
    db = state.store.conn
    window = db.execute("SELECT * FROM core_window WHERE id=?", (window_id,)).fetchone()
    if window is None or not window["eligible"]:
        raise CoreStateFault("Business work is not a verified eligible Core interval")
    run = window["run_key"]
    row = db.execute("""
        SELECT s.*,c.settled_ordinal FROM business_observation_schedule s JOIN business_metadata_cursor c USING(run_key)
        WHERE s.run_key=? AND c.stream='SCHEDULE'
    """, (run,)).fetchone()
    if row is None:
        return "UNSUPPORTED_SCHEDULE", None
    start = window["start"] * TICKS_PER_SECOND
    end = window["end"] * TICKS_PER_SECOND
    if row["start_tick"] is None:
        return "WAITING", None
    if row["start_tick"] > start or (row["cutoff"] is not None and row["cutoff"] < end):
        return "PARTIAL_BOUNDARY", None
    raw = canonical_json(json.loads(row["schedule"])).encode()
    schedule = ObservationSchedule.decode(raw, row["schedule_id"])
    first, stop = _ordinals(schedule, start, end)
    if stop - first < 2 or stop - first > 300:
        raise ResourceLimit("Business expected window exceeds its slot bounds")
    if stop - 1 > row["settled_ordinal"]:
        return "WAITING", None
    slots = db.execute("""
        SELECT ordinal,outcome FROM business_observation_slot WHERE run_key=? AND ordinal>=? AND ordinal<?
        ORDER BY ordinal LIMIT 301
    """, (run, first, stop)).fetchall()
    if tuple(slot["ordinal"] for slot in slots) != tuple(range(first, stop)):
        raise CoreStateFault("Gold applied observation slots contain a gap")
    due = {event["eventId"]: schedule.due_tick(slot["ordinal"])
           for slot in slots for event in json.loads(slot["outcome"])["events"]
           if event["disposition"] == "BUNDLE_VALID"}
    if not due:
        return "MISSING_WINDOW", None
    spec = WindowSpec(start, window["duration"], tuple(schedule.due_tick(i) for i in range(first, stop)),
                      schedule.participants, schedule.due_tick(stop - 1))
    marker = db.execute("SELECT receipt_digest FROM business_metadata_dirty WHERE window_id=?", (window_id,)).fetchone()
    boundary_hash = canonical_hash([row["schedule_id"], [[slot["ordinal"], json.loads(slot["outcome"])] for slot in slots]])
    return "READY", (spec, due, marker[0] if marker else None, boundary_hash)


def seal_business_work(state, ledger, work, materialize):
    """Use one normal verified Core traversal to construct a bounded Business projection."""
    window_id = work["window_id"]
    deadline = time.monotonic() + _ISOLATED_WORK_SECONDS
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
    measurement_refs = []
    stream = materialize(work)
    try:
        for index, bundle in enumerate(stream, 1):
            if index > _MEMBER_LIMIT:
                raise ResourceLimit("Business work exceeds its member budget")
            if stream.bytes > _ENCODED_RECEIPT_BUDGET:
                raise ResourceLimit("Business work exceeds its encoded receipt budget")
            if time.monotonic() >= deadline:
                raise ResourceLimit("Business work exceeds its isolated wall budget")
            event_id = bundle.event_id
            if event_id not in due:
                continue
            coord = bundle.intersection
            current = state.store.conn.execute("SELECT digest,payload FROM business_measurement WHERE event_id=?",
                                              (event_id,)).fetchone()
            values = None
            if current is not None:
                record = json.loads(current["payload"])
                measurement_refs.append((event_id, current["digest"]))
                values = record["measurements"]
            samples = []
            for row in bundle.traffic:
                direction = row["source_direction"]
                queue = values["directions"].get(direction, {}).get("queue") if values else None
                if queue is not None:
                    queue_methods.setdefault((coord["intersection_id"], direction), set()).add(
                        (queue["method"], queue["scopeDigest"]))
                samples.append(DirectionSample(direction, row["vehicle_count"], row["waiting_vehicle_count"],
                                               row["average_speed_kmh"], queue["value"] if queue is not None else None))
            claims.append(Claim(due[event_id], seconds_to_ticks(coord["simulation_time_seconds"]),
                                coord["intersection_id"], event_id, tuple(samples)))
        if not claims:
            state.hold_unsealed(work, reason="NO_LINKED_MEMBERS")
            return "NO_LINKED_MEMBERS"
        mixed = {key for key, methods in queue_methods.items() if len(methods) > 1}
        if mixed:
            claims = [replace(claim, directions=tuple(replace(sample, queue_m=None)
                      if (claim.intersection, sample.direction) in mixed else sample for sample in claim.directions))
                      for claim in claims]
        projection = canonical_bytes({
            "version": 1, "start": spec.start_us, "duration": spec.duration_s,
            "slots": spec.slots_us, "population": spec.population, "through": spec.settled_through_us,
            "claims": [[claim.due_us, claim.actual_us, claim.intersection, claim.event_id,
                        [[x.direction, x.vehicles, x.stopped, x.moving_speed_kmh, x.queue_m] for x in claim.directions]]
                       for claim in sorted(claims, key=lambda item: (item.due_us, item.intersection, item.event_id))],
        })
        if len(projection) > _PROJECTION_LIMIT:
            raise ResourceLimit("Business projection exceeds its byte budget")
        if time.monotonic() >= deadline:
            raise ResourceLimit("Business work exceeds its isolated wall budget")
    except ResourceLimit:
        state.hold_unsealed(work)
        return "RESOURCE_LIMIT"
    metadata_digest = canonical_hash([boundary_hash, sorted(measurement_refs)])
    return "SEALED" if state.finish(ledger, work, stream.input_hash, stream.count, projection,
        definition_set="stage_a_fixture_v1", source_semantics="silver_observation_metadata_v1",
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
    value = json.loads(row["projection"])
    if value["version"] != 1:
        raise CoreStateFault("Unsupported Business projection version")
    spec = WindowSpec(value["start"], value["duration"], tuple(value["slots"]),
                      tuple((node, tuple(dirs)) for node, dirs in value["population"]), value["through"])
    claims = (Claim(due, actual, node, event, tuple(DirectionSample(*item) for item in dirs))
              for due, actual, node, event, dirs in value["claims"])
    return publisher.save_computed(row["id"], row["projection_hash"], compute_fixture_window(spec, claims).encode())
