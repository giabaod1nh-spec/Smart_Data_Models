"""Bounded, read-only Stage A sidecar verification within Silver authority."""

import json
from hashlib import sha256

from contracts.observation_measurements import MAX_MEASUREMENT_BYTES, digest, verify_measurements
from contracts.observation_schedule import MAX_CANONICAL_INTEGER, ObservationSchedule
from contracts.traffic_observation import canonical_json
from de.silver.archive_resolver import ArchiveIntegrityError, ArchiveUnavailable


_HEAD_BYTES = 1024
_MAX_SCHEDULE_BYTES = 32 * 1024
_MAX_SEGMENT_BYTES = 64 * 1024
_MAX_SLOTS_PER_SEGMENT = 1024
_SEGMENT_KEYS = ("schemaVersion", "sequence", "previousDigest", "scheduleId", "lifecycle", "lifecycleTick",
                 "settledOrdinal", "defaultSuccessRange", "sparseFailures", "terminalCutoffTick")
_LIFECYCLE = {
    "REGISTERED": {"REGISTERED", "RUNNING", "ABORTED"},
    "RUNNING": {"RUNNING", "PAUSED", "ENDED", "ABORTED"},
    "PAUSED": {"PAUSED", "RUNNING", "ENDED", "ABORTED"},
    "ENDED": set(), "ABORTED": set(),
}


def _read(path, limit):
    try:
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
    except OSError as exc:
        raise ArchiveUnavailable(f"cannot read observation sidecar {path.name}") from exc
    if not raw or len(raw) > limit:
        raise ArchiveIntegrityError("observation sidecar byte limit violated")
    return raw


def _decode(raw, keys):
    try:
        document = json.loads(raw)
        if not isinstance(document, dict) or set(document) != set(keys) or canonical_json(document).encode() != raw:
            raise ValueError("archive object shape or canonical bytes differ")
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ArchiveIntegrityError("invalid observation sidecar object") from exc
    return document


def _integer(value, minimum):
    return type(value) is int and minimum <= value <= MAX_CANONICAL_INTEGER


class VerifiedObservationArchive:
    """Read one pinned archive head for an already verified Silver run context."""

    def __init__(self, root, context):
        self.directory = root / "runs" / context.run_id / "observation_schedule_v1"
        self.head_bytes = _read(self.directory / "head.json", _HEAD_BYTES)
        self.head = _decode(self.head_bytes, ("schemaVersion", "scheduleId", "sequence", "segmentDigest",
                                             "measurementSequence", "measurementDigest"))
        head = self.head
        if (type(head["schemaVersion"]) is not int or head["schemaVersion"] != 1
                or any(not digest(head[key]) for key in ("scheduleId", "segmentDigest"))
                or not _integer(head["sequence"], 1) or not _integer(head["measurementSequence"], 0)
                or not (digest(head["measurementDigest"]) if head["measurementSequence"]
                        else head["measurementDigest"] is None)):
            raise ArchiveIntegrityError("invalid observation archive head")
        raw = _read(self.directory / (head["scheduleId"] + ".json"), _MAX_SCHEDULE_BYTES)
        try:
            self.schedule = ObservationSchedule.decode(raw, head["scheduleId"])
        except (TypeError, KeyError, ValueError) as exc:
            raise ArchiveIntegrityError("invalid immutable observation schedule") from exc
        expected = tuple((node, tuple(sorted(context.directions[node]))) for node in sorted(context.participants))
        if (not context.registry_status or
                (self.schedule.run_id, self.schedule.generation, self.schedule.manifest_digest,
                 self.schedule.config_digest, self.schedule.topology_digest, self.schedule.participants)
                != (context.run_id, context.generation, context.manifest_identity,
                    context.manifest.get("config_hash"), context.topology_digest, expected)):
            raise ArchiveIntegrityError("schedule differs from the verified archived run context")

    def revalidate(self):
        if self.head_bytes != _read(self.directory / "head.json", _HEAD_BYTES):
            raise ArchiveUnavailable("observation archive advanced during verification")

    def measurement(self, sequence, previous_index_digest, *, load_anchor):
        if not _integer(sequence, 1) or sequence > self.head["measurementSequence"]:
            return None
        raw = _read(self.directory / f"measurement-{sequence}.json", _HEAD_BYTES)
        index = _decode(raw, ("sequence", "scheduleId", "previousIndexDigest", "objectDigest"))
        index_hash = sha256(raw).hexdigest()
        if (index["sequence"] != sequence or index["scheduleId"] != self.head["scheduleId"]
                or index["previousIndexDigest"] != previous_index_digest or not digest(index["objectDigest"])):
            raise ArchiveIntegrityError("measurement index chain differs")
        if sequence == self.head["measurementSequence"]:
            expected = self.head["measurementDigest"]
        else:
            following = _read(self.directory / f"measurement-{sequence + 1}.json", _HEAD_BYTES)
            following = _decode(following, ("sequence", "scheduleId", "previousIndexDigest", "objectDigest"))
            if following["sequence"] != sequence + 1 or following["scheduleId"] != self.head["scheduleId"]:
                raise ArchiveIntegrityError("following measurement index differs")
            expected = following["previousIndexDigest"]
        if index_hash != expected:
            raise ArchiveIntegrityError("measurement index conflicts with the committed prefix")
        object_hash = index["objectDigest"]
        content = _read(self.directory / (object_hash + ".json"), MAX_MEASUREMENT_BYTES)
        if sha256(content).hexdigest() != object_hash:
            raise ArchiveIntegrityError("measurement content differs from its index")
        try:
            event_id = json.loads(content)["anchor"]["eventId"]
            if not digest(event_id):
                raise ValueError("invalid observation identity")
            anchor, directions = load_anchor(event_id)
            record = verify_measurements(content, object_hash, anchor, directions)
            if record["revision"] > 1:
                previous_digest = record["previousDigest"]
                previous_bytes = _read(self.directory / (previous_digest + ".json"), MAX_MEASUREMENT_BYTES)
                previous = verify_measurements(previous_bytes, previous_digest, anchor, directions)
                if previous["revision"] != record["revision"] - 1 or previous["measurements"] is None:
                    raise ValueError("correction does not replace an existing measurement revision")
        except (ValueError, KeyError, TypeError) as exc:
            raise ArchiveIntegrityError("measurement evidence differs from its verified original") from exc
        return index_hash, object_hash, record

    def schedule_segment(self, sequence, previous):
        if not _integer(sequence, 1) or sequence > self.head["sequence"]:
            return None
        raw = _read(self.directory / f"schedule-{sequence}.json", _MAX_SEGMENT_BYTES)
        record = _decode(raw, _SEGMENT_KEYS)
        object_hash = sha256(raw).hexdigest()
        if sequence == self.head["sequence"]:
            expected = self.head["segmentDigest"]
        else:
            following = _decode(_read(self.directory / f"schedule-{sequence + 1}.json", _MAX_SEGMENT_BYTES),
                                _SEGMENT_KEYS)
            if following["sequence"] != sequence + 1 or following["scheduleId"] != self.head["scheduleId"]:
                raise ArchiveIntegrityError("following schedule segment differs")
            expected = following["previousDigest"]
        if object_hash != expected:
            raise ArchiveIntegrityError("schedule segment conflicts with the committed prefix")
        prior_hash, before, previous_tick, settled = previous
        state, tick, through = record["lifecycle"], record["lifecycleTick"], record["settledOrdinal"]
        if (type(record["schemaVersion"]) is not int or record["schemaVersion"] != 1
                or record["sequence"] != sequence or record["previousDigest"] != prior_hash
                or record["scheduleId"] != self.head["scheduleId"]
                or not isinstance(state, str) or state not in _LIFECYCLE.get(before, ())
                or not _integer(tick, previous_tick) or tick % self.schedule.step_ticks
                or not _integer(through, settled) or through - settled > _MAX_SLOTS_PER_SEGMENT):
            raise ArchiveIntegrityError("invalid schedule segment continuation")
        if sequence == 1 and (state, tick, through, prior_hash) != ("REGISTERED", 0, -1, None):
            raise ArchiveIntegrityError("schedule has an invalid genesis segment")
        if before == "PAUSED" and (tick != previous_tick or through != settled):
            raise ArchiveIntegrityError("paused schedule advanced")
        terminal = state in ("ENDED", "ABORTED")
        terminal_cutoff = record["terminalCutoffTick"]
        try:
            if terminal and (type(terminal_cutoff) is not int or terminal_cutoff != tick
                    or (through >= 0 and self.schedule.due_tick(through) > tick)
                    or self.schedule.due_tick(through + 1) <= tick):
                raise ArchiveIntegrityError("invalid terminal schedule cutoff")
        except ValueError as exc:
            raise ArchiveIntegrityError("terminal schedule exceeds its clock bounds") from exc
        if not terminal and terminal_cutoff is not None:
            raise ArchiveIntegrityError("nonterminal segment has a terminal cutoff")
        bounds = [settled + 1, through] if through > settled else None
        if record["defaultSuccessRange"] != bounds or not isinstance(record["sparseFailures"], list):
            raise ArchiveIntegrityError("invalid sparse producer outcome range")
        last = settled
        for failure in record["sparseFailures"]:
            if (not isinstance(failure, dict) or set(failure) != {"ordinal", "reason"}
                    or not _integer(failure["ordinal"], last + 1) or failure["ordinal"] > through
                    or failure["reason"] not in ("CAPTURE_FAILED", "MISSED_TICK", "CLOCK_MISMATCH")):
                raise ArchiveIntegrityError("invalid or duplicate producer outcome")
            last = failure["ordinal"]
        return object_hash, record

    def schedule_continuation(self, sequence, expected_digest):
        if sequence == 0:
            return None, "REGISTERED", 0, -1
        raw = _read(self.directory / f"schedule-{sequence}.json", _MAX_SEGMENT_BYTES)
        record = _decode(raw, _SEGMENT_KEYS)
        if (sha256(raw).hexdigest() != expected_digest or record["sequence"] != sequence
                or record["scheduleId"] != self.head["scheduleId"]):
            raise ArchiveIntegrityError("prior verified schedule segment changed")
        return expected_digest, record["lifecycle"], record["lifecycleTick"], record["settledOrdinal"]
