"""Independent arithmetic for the A-I01a handoff. No Gold Business imports."""

from fractions import Fraction
from dataclasses import replace
from itertools import repeat

import pytest


@pytest.fixture(params=(60, 300))
def stage_a_window(request):
    """Frozen 1 Hz synthetic population: A has two directions, B has one."""
    duration = request.param
    schedule = tuple(range(duration))
    directions = {"A": ("east", "west"), "B": ("north",)}
    # Each row represents one certified sample: V, H, moving speed (km/h), q (m).
    # At the 95th percentile boundary, max and nearest-rank P95 differ.
    samples = {
        (slot, "A", "east"): (10, 2, 36, 8 if slot < duration * 19 // 20 else 100)
        for slot in schedule
    }
    for slot in schedule:
        samples[slot, "A", "west"] = (6, 4, 18, 12)
        samples[slot, "B", "north"] = (0, 0, None, 0)
    return schedule, directions, samples


def test_frozen_population_and_weighted_oracle(stage_a_window):
    schedule, directions, samples = stage_a_window
    duration = len(schedule)
    expected_keys = {
        (slot, intersection, direction)
        for slot in schedule
        for intersection, approaches in directions.items()
        for direction in approaches
    }
    assert samples.keys() == expected_keys

    # Expand per-vehicle values only in the reference oracle. The production
    # path must use sufficient statistics to avoid this allocation.
    moving_speeds = [
        speed
        for vehicles, stopped, speed, _ in samples.values()
        for _ in range(vehicles - stopped)
    ]
    assert len(moving_speeds) == 10 * duration
    assert Fraction(sum(moving_speeds), len(moving_speeds)) == Fraction(162, 5)
    assert sum(row[0] for row in samples.values()) == 16 * duration
    assert sum(row[1] for row in samples.values()) == 6 * duration
    assert Fraction(sum(row[1] for row in samples.values()), 16 * duration) == Fraction(3, 8)
    assert all(samples[slot, "B", "north"] == (0, 0, None, 0) for slot in schedule)


def test_frozen_queue_reference_keeps_direction_distributions(stage_a_window):
    schedule, _, samples = stage_a_window
    east = [samples[slot, "A", "east"][3] for slot in schedule]
    west = [samples[slot, "A", "west"][3] for slot in schedule]

    # Quantile from empirical CDF, independent of nearest-rank index arithmetic.
    def p95_reference(values):
        return min(q for q in set(values) if
                   Fraction(sum(value <= q for value in values), len(values)) >= Fraction(95, 100))

    assert p95_reference(east) == 8
    assert max(east) == 100
    assert Fraction(sum(east), len(east)) == Fraction(63, 5)
    assert p95_reference(west) == 12
    assert p95_reference([0] * len(schedule)) == 0
    assert max(p95_reference(east), p95_reference(west)) == 12
    assert p95_reference(east + west) == 12


def test_fault_oracle_preserves_missing_and_conflict_identity(stage_a_window):
    schedule, directions, samples = stage_a_window
    expected_slots = {(slot, node) for slot in schedule for node in directions}
    rows = [
        ((slot, node), f"{node}-{slot}", tuple(samples[slot, node, d] for d in approaches))
        for slot in schedule
        for node, approaches in directions.items()
        if (slot, node) != (1, "A")
    ]
    duplicate = next(row for row in rows if row[0] == (0, "A"))
    conflict = ((2, "A"), "different-event", ((20, 2, 36, 8), (6, 4, 18, 12)))
    # Retain all rows in the small oracle. Production should only retain a
    # sticky conflict state per slot.
    claims = rows + [duplicate, conflict]
    distinct = {
        slot: {(event, payload) for key, event, payload in claims if key == slot}
        for slot in expected_slots
    }
    assert {slot for slot, claim in distinct.items() if not claim} == {(1, "A")}
    assert {slot for slot, claim in distinct.items() if len(claim) > 1} == {(2, "A")}
    assert len(distinct[0, "A"]) == 1
    assert sum(len(claim) == 1 for claim in distinct.values()) == 2 * len(schedule) - 2


@pytest.fixture
def projected_window(stage_a_window):
    from de.gold.business import Claim, DirectionSample, WindowSpec

    schedule, population, rows = stage_a_window
    ticks = tuple(slot * 1_000_000 for slot in schedule)
    spec = WindowSpec(0, len(schedule), ticks, tuple(population.items()), ticks[-1])
    claims = [
        Claim(tick, tick, node, f"{node}-{slot}", tuple(
            DirectionSample(direction, *rows[slot, node, direction])
            for direction in directions
        ))
        for slot, tick in zip(schedule, ticks)
        for node, directions in population.items()
    ]
    return spec, claims


def test_fixture_engine_matches_independent_expected_values(projected_window):
    from de.gold.business import compute_fixture_window

    spec, claims = projected_window
    result = compute_fixture_window(spec, claims + [claims[0]])
    assert result.encode() == compute_fixture_window(spec, reversed(claims)).encode()
    assert result.slot_states == tuple((node, spec.duration_s, 0, 0, 0, 0) for node in ("A", "B"))
    east, west, a, north, b, network = result.rows
    assert (east.queue_mean.value, east.queue_max.value, east.queue_p95.value) == (12.6, 100, 8)
    assert (west.stopped_ratio.value, west.moving_speed.value) == (100 * 4 / 6, 18)
    assert a.queue_p95.value == network.queue_p95.value == 12
    assert network.worst_queue_direction == ("A", "west")
    assert (a.moving_speed.value, a.stopped_ratio.value, a.observed_load.value) == (32.4, 37.5, 16)
    assert (network.moving_speed.value, network.stopped_ratio.value, network.observed_load.value) == (32.4, 37.5, 16)
    assert north.moving_speed.status == b.stopped_ratio.status == "NOT_APPLICABLE"
    assert north.observed_load.value == north.queue_p95.value == 0


def test_fixture_engine_conflict_missing_waiting_invalid_and_independent_quality(projected_window):
    from de.gold.business import compute_fixture_window

    spec, claims = projected_window
    boundary = replace(claims[6], actual_us=spec.duration_s * 1_000_000)
    conflict = replace(claims[4], event_id="other")
    claims = [*claims[:2], *claims[3:6], boundary, *claims[7:], conflict]
    spec = replace(spec, settled_through_us=spec.slots_us[-2])
    result = compute_fixture_window(spec, claims)
    assert result.encode() == compute_fixture_window(spec, reversed(claims)).encode()
    assert result.slot_states[0] == ("A", spec.duration_s - 4, 1, 1, 1, 1)
    assert result.rows[-1].observed_load.value is None
    assert result.rows[-1].observed_load.valid == 3 * spec.duration_s - 9
    assert result.rows[-1].queue_p95.value is None


def test_fixture_engine_keeps_independent_metrics_and_rejects_cap_overflow(projected_window):
    from de.gold.business import ResourceLimit, compute_fixture_window

    spec, claims = projected_window
    a = claims[0]
    claims[0] = replace(a, directions=(replace(a.directions[0], moving_speed_kmh=None), a.directions[1]))
    network = compute_fixture_window(spec, claims).rows[-1]
    assert network.moving_speed.status == "UNAVAILABLE"
    assert network.stopped_ratio.value == 37.5
    assert network.observed_load.value == 16
    assert network.queue_p95.value == 12
    with pytest.raises(ResourceLimit, match="verified member count"):
        compute_fixture_window(spec, repeat(claims[0], 3_001))
    with pytest.raises(ValueError, match="entire"):
        compute_fixture_window(replace(spec, slots_us=spec.slots_us[:-1]), ())
