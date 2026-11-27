"""Pure, bounded Stage A computation over prequalified observation claims.

The caller supplies the expected schedule and archived population. This module
does not interpret Silver receipts or authorize an official publication.
"""

from dataclasses import asdict, dataclass
from math import fsum, isfinite

from de.silver.incremental_contract import canonical_bytes

_US = 1_000_000
_MAX_SAFE_INT = (1 << 53) - 1
_CONFLICT = object()
VERIFIED_DEFINITION = "stage_a_verified_v2"


class ResourceLimit(ValueError):
    """A fixture or work item exceeded a hard allocation guard."""


@dataclass(frozen=True, slots=True)
class WindowSpec:
    start_us: int
    duration_s: int
    slots_us: tuple[int, ...]
    population: tuple[tuple[str, tuple[str, ...]], ...]
    settled_through_us: int
    tolerance_us: int = 0
    conflicted_slots: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class DirectionSample:
    direction: str
    vehicles: int | None
    stopped: int | None
    moving_speed_kmh: float | None
    queue_m: float | None
    occupancy_pct: float | None = None


@dataclass(frozen=True, slots=True)
class Claim:
    due_us: int
    actual_us: int
    intersection: str
    event_id: str
    directions: tuple[DirectionSample, ...]


@dataclass(frozen=True, slots=True)
class Metric:
    value: float | None
    expected: int
    valid: int
    status: str


@dataclass(frozen=True, slots=True)
class Row:
    grain: str
    intersection: str | None
    direction: str | None
    moving_speed: Metric
    stopped_ratio: Metric
    observed_load: Metric
    queue_mean: Metric | None
    queue_max: Metric | None
    queue_p95: Metric
    worst_queue_direction: tuple[str, str] | None
    weighted_speed: float
    moving_count: int
    stopped_count: int
    ratio_vehicle_count: int
    load_vehicle_count: int
    time_slots: int
    queue_sum: float | None
    occupancy_mean: Metric | None = None
    occupancy_sum: float | None = None


@dataclass(frozen=True, slots=True)
class WindowResult:
    start_us: int
    duration_s: int
    slot_states: tuple[tuple[str, int, int, int, int, int], ...]
    rows: tuple[Row, ...]
    definition_set: str = "stage_a_fixture_v1"

    def encode(self, *, context=None, measurement_methods=None) -> bytes:
        document = asdict(self)
        if self.definition_set != VERIFIED_DEFINITION:
            for row in document["rows"]:
                del row["occupancy_mean"], row["occupancy_sum"]
        else:
            methods = {(node, direction): items for node, direction, items in (measurement_methods or ())}
            for row in document["rows"]:
                if row["grain"] == "intersection" and context is not None:
                    row["traffic_context"] = context.get(row["intersection"], {})
                if row["grain"] == "direction" and measurement_methods is not None:
                    row["measurement_methods"] = methods.get((row["intersection"], row["direction"]), {})
        payload = canonical_bytes(dict(document, codec_version=1))
        if len(payload) > 32 * 1024:
            raise ResourceLimit("computed payload exceeds the fixture limit")
        return payload


def _id(value: str) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256 and not any(
        0xD800 <= ord(ch) <= 0xDFFF for ch in value
    )


def _num(value: float | None) -> float | None:
    # Reject nonfinite or excessively large readings before weight/rollup.
    if type(value) not in (float, int) or not 0 <= value <= _MAX_SAFE_INT:
        return None
    return float(value) if isfinite(value) else None


def _count(value: int | None) -> int | None:
    # 32-bit readings keep even the maximum fixture rollup JSON-safe.
    return value if type(value) is int and 0 <= value < (1 << 32) else None


def _sample(row: DirectionSample) -> DirectionSample:
    occupancy = _num(row.occupancy_pct)
    return DirectionSample(row.direction, _count(row.vehicles), _count(row.stopped),
                           _num(row.moving_speed_kmh), _num(row.queue_m),
                           occupancy if occupancy is not None and occupancy <= 100 else None)


def _validate(spec: WindowSpec) -> dict[str, frozenset[str]]:
    if (type(spec.start_us) is not int or spec.start_us < 0
            or spec.duration_s not in (60, 300) or type(spec.duration_s) is not int
            or spec.start_us % (spec.duration_s * _US)
            or spec.start_us + spec.duration_s * _US > _MAX_SAFE_INT):
        raise ValueError("expected an aligned 60s/300s simulation window")
    if len(spec.slots_us) > 300 or len(spec.population) > 5:
        raise ResourceLimit("scheduled population exceeds the fixture limit")
    if (not spec.slots_us or not spec.population
            or type(spec.settled_through_us) is not int
            or type(spec.tolerance_us) is not int or spec.tolerance_us < 0):
        raise ValueError("invalid expected population or schedule")
    end = spec.start_us + spec.duration_s * _US
    if any(type(slot) is not int or not spec.start_us <= slot < end for slot in spec.slots_us):
        raise ValueError("scheduled slot falls outside the window")
    gaps = [b - a for a, b in zip(spec.slots_us, spec.slots_us[1:])]
    if gaps and (len(set(gaps)) != 1 or gaps[0] <= 2 * spec.tolerance_us):
        raise ValueError("expected slots must use a uniform, unambiguous lattice")
    if not gaps or spec.slots_us[0] - gaps[0] >= spec.start_us or spec.slots_us[-1] + gaps[0] < end:
        raise ValueError("expected schedule must span the entire fixture window")
    population = {}
    count = 0
    for intersection, directions in spec.population:
        count += len(directions)
        if count > 17:
            raise ResourceLimit("direction population exceeds the fixture limit")
        if not _id(intersection) or intersection in population or not directions:
            raise ValueError("invalid or duplicate intersection")
        if not all(_id(direction) for direction in directions) or len(set(directions)) != len(directions):
            raise ValueError("invalid or duplicate direction")
        population[intersection] = frozenset(directions)
    return population


def _reduce(spec, population, members):
    slots = frozenset(spec.slots_us)
    if len(spec.conflicted_slots) > len(slots) * len(population):
        raise ResourceLimit("conflicted slots exceed the fixture population")
    claims = {}
    for node, due in spec.conflicted_slots:
        if node not in population or type(due) is not int or due not in slots or due > spec.settled_through_us:
            raise ValueError("conflicted slot lacks a settled expected identity")
        claims[node, due] = _CONFLICT
    end = spec.start_us + spec.duration_s * _US
    for index, claim in enumerate(members, start=1):
        if index > 3_000:
            raise ResourceLimit("verified member count exceeds the fixture limit")
        if (not _id(claim.event_id) or claim.intersection not in population
                or type(claim.due_us) is not int or claim.due_us not in slots):
            raise ValueError("claim lacks a valid expected slot identity")
        if len(claim.directions) > len(population[claim.intersection]):
            raise ValueError("claim exceeds archived direction population")
        key = claim.intersection, claim.due_us
        columns = {}
        for direction in claim.directions:
            if (direction.direction not in population[claim.intersection]
                    or direction.direction in columns):
                raise ValueError("unknown or duplicate direction in one claim")
            columns[direction.direction] = _sample(direction)
        if claims.get(key) is _CONFLICT:
            continue
        valid = (type(claim.actual_us) is int and spec.start_us <= claim.actual_us < end
                 and abs(claim.actual_us - claim.due_us) <= spec.tolerance_us)
        actual = claim.actual_us if type(claim.actual_us) is int and 0 <= claim.actual_us <= _MAX_SAFE_INT else None
        identity = (claim.event_id, actual, tuple(columns[d] for d in sorted(columns)))
        prior = claims.get(key)
        if prior is None:
            claims[key] = identity, valid
        elif prior[0] != identity:
            claims[key] = _CONFLICT
    return claims


def _check_projection_size(claims, *, limit):
    # Check the final projection, so which conflict claim arrived first
    # cannot determine whether a work item passes this guard.
    size = 2
    for (node, due), entry in claims.items():
        if entry is _CONFLICT:
            record = [node, due, "CONFLICT"]
        else:
            (event, actual, directions), valid = entry
            record = [node, due, event, actual, valid, [
                ([row.direction, row.vehicles, row.stopped, row.moving_speed_kmh, row.queue_m]
                 + ([row.occupancy_pct] if row.occupancy_pct is not None else []))
                for row in directions
            ]]
        size += len(canonical_bytes(record)) + 1
        if size > limit:
            raise ResourceLimit("projected slot bytes exceed the fixture limit")


@dataclass(slots=True)
class _Stats:
    load_valid: int
    ratio_valid: int
    speed_valid: int
    load_vehicles: int
    ratio_vehicles: int
    stopped: int
    moving: int
    weighted: float
    queue_count: int
    queue_sum: float
    queue_max: float | None
    queue_p95: float | None
    occupancy_count: int
    occupancy_sum: float


def _direction_stats(samples):
    load_valid = ratio_valid = speed_valid = vehicles = ratio_vehicles = stopped = moving = 0
    weighted = []
    queues = []
    occupancy = []
    for row in samples:
        if row.queue_m is not None:
            queues.append(row.queue_m)
        if row.occupancy_pct is not None:
            occupancy.append(row.occupancy_pct)
        if row.vehicles is None:
            continue
        load_valid += 1
        vehicles += row.vehicles
        if row.stopped is None or row.stopped > row.vehicles:
            continue
        ratio_valid += 1
        ratio_vehicles += row.vehicles
        stopped += row.stopped
        n_move = row.vehicles - row.stopped
        if n_move and row.moving_speed_kmh is None:
            continue
        speed_valid += 1
        moving += n_move
        if n_move:
            weighted.append(row.moving_speed_kmh * n_move)
    queues.sort()
    count = len(queues)
    return _Stats(load_valid, ratio_valid, speed_valid, vehicles, ratio_vehicles,
                  stopped, moving, fsum(weighted), count, fsum(queues),
                  queues[-1] if count else None,
                  queues[(95 * count + 99) // 100 - 1] if count else None,
                  len(occupancy), fsum(occupancy))


def _metric(numerator, denominator, expected, valid):
    if valid != expected:
        return Metric(None, expected, valid, "UNAVAILABLE")
    if not denominator:
        return Metric(None, expected, valid, "NOT_APPLICABLE")
    return Metric(numerator / denominator, expected, valid, "OK")


def _make_row(grain, intersection, direction, items, time_slots, *, measure_occupancy):
    group = tuple(stat for _, stat in items)
    expected = len(group) * time_slots
    weighted = fsum(stat.weighted for stat in group)
    moving = sum(stat.moving for stat in group)
    stopped = sum(stat.stopped for stat in group)
    ratio_vehicles = sum(stat.ratio_vehicles for stat in group)
    load_vehicles = sum(stat.load_vehicles for stat in group)
    speed = _metric(weighted, moving, expected, sum(stat.speed_valid for stat in group))
    ratio = _metric(100 * stopped, ratio_vehicles, expected, sum(stat.ratio_valid for stat in group))
    load = _metric(load_vehicles, time_slots, expected, sum(stat.load_valid for stat in group))
    queue_valid = sum(stat.queue_count for stat in group)
    worst = q_sum = q_mean = q_max = None
    p95 = Metric(None, expected, queue_valid, "UNAVAILABLE")
    if queue_valid == expected:
        # The higher grains select the maximum direction P95, never a pooled quantile.
        winner = None
        for key, stat in items:
            value = stat.queue_p95
            if winner is None or value > winner:
                winner, worst = value, key
        p95 = Metric(winner, expected, queue_valid, "OK")
        if direction is not None:
            q_sum = group[0].queue_sum
            q_mean = Metric(q_sum / time_slots, time_slots, time_slots, "OK")
            q_max = Metric(group[0].queue_max, time_slots, time_slots, "OK")
    elif direction is not None:
        q_mean = q_max = p95
    occupancy_mean = occupancy_sum = None
    if direction is not None and measure_occupancy:
        stat = group[0]
        occupancy_sum = stat.occupancy_sum
        occupancy_mean = _metric(occupancy_sum, time_slots, time_slots, stat.occupancy_count)
    return Row(grain, intersection, direction, speed, ratio, load, q_mean, q_max, p95,
               worst, weighted, moving, stopped, ratio_vehicles, load_vehicles, time_slots, q_sum,
               occupancy_mean, occupancy_sum)


def compute_fixture_window(spec: WindowSpec, members, *, definition_set="stage_a_fixture_v1") -> WindowResult:
    """Fold a bounded work item in deterministic archive and slot order."""
    measure_occupancy = definition_set == VERIFIED_DEFINITION
    population = _validate(spec)
    claims = _reduce(spec, population, members)
    _check_projection_size(claims, limit=(512 if measure_occupancy else 256) * 1024)
    time_slots = len(spec.slots_us)
    rows = []
    slot_states = []
    network_items = []
    for intersection in sorted(population):
        columns = {direction: [] for direction in sorted(population[intersection])}
        unique = missing = waiting = conflict = invalid = 0
        for slot in spec.slots_us:
            entry = claims.get((intersection, slot))
            if slot > spec.settled_through_us:
                waiting += 1
            elif entry is None:
                missing += 1
            elif entry is _CONFLICT:
                conflict += 1
            elif not entry[1]:
                invalid += 1
            else:
                unique += 1
                for sample in entry[0][2]:
                    columns[sample.direction].append(sample)
        slot_states.append((intersection, unique, missing, waiting, conflict, invalid))
        items = []
        for direction, samples in columns.items():
            item = (intersection, direction), _direction_stats(samples)
            items.append(item)
            rows.append(_make_row("direction", intersection, direction, (item,), time_slots,
                                  measure_occupancy=measure_occupancy))
        rows.append(_make_row("intersection", intersection, None, items, time_slots,
                              measure_occupancy=measure_occupancy))
        network_items.extend(items)
    rows.append(_make_row("network", None, None, network_items, time_slots,
                          measure_occupancy=measure_occupancy))
    return WindowResult(spec.start_us, spec.duration_s, tuple(slot_states), tuple(rows), definition_set)
