"""Pressure calibration, strict coverage and historical serving requirements."""

import json

from de.gold.business import Claim, DirectionSample, VERIFIED_DEFINITION, WindowSpec, compute_fixture_window
from de.gold.pressure import PressureDefinition
from de.gold_runtime.business_reader import overview
from de.gold_runtime.pressure_calibration import evaluate


def test_calibrated_pressure_keeps_unrounded_severity_and_full_network_population():
    definition = PressureDefinition("fixture", 50, 40, 70, "MEASURED_ZERO", "test fixture", "1" * 64)
    population = (("A", ("East", "West")), ("B", ("East",)), ("C", ("East",)))
    slots = tuple(i * 1_000_000 for i in range(60))
    spec = WindowSpec(0, 60, slots, population, slots[-1])

    def rows(missing=False):
        samples = []
        for tick in slots:
            samples.extend((Claim(tick, tick, "A", f"A{tick}", (
                DirectionSample("East", 200, 160, 40, 1, 59.9),
                DirectionSample("West", 200, 152, 40, 20, 88))),
                Claim(tick, tick, "B", f"B{tick}", (DirectionSample("East", 0, 0, None, 0, 0),)),
                Claim(tick, tick, "C", f"C{tick}", (
                    DirectionSample("East", 25, 19, 40, 4, None if missing and tick == 0 else 88),))))
        result = compute_fixture_window(spec, samples, definition_set=VERIFIED_DEFINITION)
        return json.loads(result.encode(pressure_definition=definition))["rows"]

    physical = rows()
    directions = {(r["intersection"], r["direction"]): r for r in physical if r["grain"] == "direction"}
    assert directions["A", "East"]["pressure_score"]["value"] == 69.95
    assert directions["A", "East"]["pressure_score"]["severity"] == "MODERATE"
    assert directions["B", "East"]["pressure_score"]["value"] == 0
    assert physical[-1]["high_pressure_count"]["high_intersections"] == ["A", "C"]
    assert rows(missing=True)[-1]["high_pressure_count"]["value"] is None
    selection = dict(duration=60, participants=["A", "B", "C"], windows=[
        dict(start=0, state="PUBLISHED", rows=physical),
        dict(start=60, state="MISSING_WINDOW", rows=[]),
        dict(start=120, state="PUBLISHED", rows=physical)])
    view = overview(selection)
    assert view["pressure_ranking"]["rows"][0]["intersection"] == "A"
    assert view["pressure_ranking"]["rows"][0]["worst_pressure_direction"] == ["A", "West"]
    assert len(view["pressure_heatmap"]) == 9
    assert view["pressure_heatmap"][3]["status"] == "MISSING_WINDOW"
    assert view["bottleneck_persistence"]["rows"]["A"]["longest_streak"] == 1
    assert view["bottleneck_persistence"]["rows"]["A"]["high_share"] is None


def test_calibration_does_not_pass_without_independent_positives_or_joint_coverage():
    assert not evaluate([("NORMAL_RED", (0, 0))], 50, 70)["meets_targets"]
    missed = evaluate([("NORMAL_RED", (0, 0)), ("BOTTLENECK", None)], 50, 70)
    assert missed["missed_bottleneck"] == 1 and not missed["meets_targets"]
    assert evaluate([("NORMAL_RED", (0, 0)), ("BOTTLENECK", (90, 90))], 50, 70)["meets_targets"]
