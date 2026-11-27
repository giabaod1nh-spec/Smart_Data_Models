"""The isolated producer's immutable observation schedule sidecar.

The sidecar does not change the V1 observation envelope or active-run selection.
"""

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from re import fullmatch

from contracts.topology.v1_digest import validate_digest_syntax
from contracts.traffic_observation import canonical_json, observation_event_id


TICKS_PER_SECOND = 1_000_000
MAX_CANONICAL_INTEGER = 2**53 - 1


def _integer(value: int, minimum: int) -> int:
    if type(value) is not int or not minimum <= value <= MAX_CANONICAL_INTEGER:
        raise ValueError("schedule integer is outside its canonical range")
    return value


def seconds_to_ticks(seconds: str | float | int) -> int:
    """Accept an exact microsecond decimal value and reject off-grid clocks."""
    try:
        value = Decimal(str(seconds))
    except InvalidOperation as exc:
        raise ValueError("invalid timestamp") from exc
    if not value.is_finite() or not 0 <= value <= Decimal("9007199254.740991"):
        raise ValueError("timestamp is outside the schedule range")
    if value.is_zero():
        return 0
    if value.adjusted() < -6:
        raise ValueError("timestamp is off the microsecond lattice")
    numerator, denominator = value.as_integer_ratio()
    ticks, remainder = divmod(numerator * TICKS_PER_SECOND, denominator)
    if remainder:
        raise ValueError("timestamp is off the microsecond lattice")
    return _integer(ticks, 0)


@dataclass(frozen=True)
class ObservationSchedule:
    run_id: str
    generation: int
    manifest_digest: str
    config_digest: str
    topology_digest: str
    participants: tuple[tuple[str, tuple[str, ...]], ...]
    first_tick: int
    period_ticks: int
    step_ticks: int
    cycle_base: int

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id.strip():
            raise ValueError("run ID is required")
        for digest in (self.manifest_digest, self.config_digest):
            if not isinstance(digest, str) or fullmatch(r"[a-f0-9]{64}", digest) is None:
                raise ValueError("manifest and configuration digests must be lowercase SHA-256")
        validate_digest_syntax(self.topology_digest)
        _integer(self.generation, 1)
        _integer(self.first_tick, 0)
        _integer(self.period_ticks, 1)
        _integer(self.step_ticks, 1)
        _integer(self.cycle_base, 1)
        if self.first_tick % self.step_ticks or self.period_ticks % self.step_ticks:
            raise ValueError("schedule must lie on the fixed simulation-step lattice")
        if not isinstance(self.participants, tuple) or any(
            not isinstance(item, tuple) or len(item) != 2 for item in self.participants
        ):
            raise ValueError("participant population must be immutable")
        if len(self.participants) > 5:
            raise ValueError("schedule exceeds the isolated participant limit")
        nodes = tuple(node for node, _ in self.participants)
        self._validate_ids(nodes)
        for _, directions in self.participants:
            self._validate_ids(directions)
        if sum(len(directions) for _, directions in self.participants) > 17:
            raise ValueError("schedule exceeds the isolated approach limit")

    @staticmethod
    def _validate_ids(ids: tuple[str, ...]) -> None:
        if (not isinstance(ids, tuple) or not ids
                or any(not isinstance(item, str) or not 1 <= len(item) <= 128 or not item.strip() for item in ids)
                or ids != tuple(sorted(set(ids)))):
            raise ValueError("schedule population must be sorted, unique and nonempty")

    def canonical_bytes(self) -> bytes:
        return canonical_json({
            "schemaVersion": 1, "simulationRunId": self.run_id,
            "runGeneration": self.generation, "manifestDigest": self.manifest_digest,
            "configDigest": self.config_digest, "topologyDigest": self.topology_digest,
            "participants": [{"intersectionId": node, "directions": list(directions)}
                             for node, directions in self.participants],
            "firstTick": self.first_tick, "periodTicks": self.period_ticks,
            "tickUnit": "microsecond", "stepTicks": self.step_ticks, "cycleBase": self.cycle_base,
        }).encode("utf-8")

    @property
    def schedule_id(self) -> str:
        return sha256(self.canonical_bytes()).hexdigest()

    def due_tick(self, ordinal: int) -> int:
        return _integer(self.first_tick + _integer(ordinal, 0) * self.period_ticks, 0)

    def cycle(self, ordinal: int) -> int:
        return _integer(self.cycle_base + _integer(ordinal, 0), 1)

    def event_ids(self, ordinal: int) -> tuple[str, ...]:
        cycle = self.cycle(ordinal)
        return tuple(observation_event_id(self.run_id, node, cycle) for node, _ in self.participants)

    def verify_capture(self, ordinal: int, seconds: str | float | int, completed_step: int) -> None:
        due = self.due_tick(ordinal)
        if seconds_to_ticks(seconds) != due or _integer(completed_step, 0) != due // self.step_ticks:
            raise ValueError("actual clock and completed step must both match the scheduled slot")

    @classmethod
    def decode(cls, content: bytes, expected_id: str):
        document = json.loads(content)
        schedule = cls(
            document["simulationRunId"], document["runGeneration"], document["manifestDigest"],
            document["configDigest"], document["topologyDigest"],
            tuple((item["intersectionId"], tuple(item["directions"])) for item in document["participants"]),
            document["firstTick"], document["periodTicks"], document["stepTicks"], document["cycleBase"],
        )
        if schedule.schedule_id != expected_id or schedule.canonical_bytes() != content:
            raise ValueError("schedule archive identity differs")
        return schedule
