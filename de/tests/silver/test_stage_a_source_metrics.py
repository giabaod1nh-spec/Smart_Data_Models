"""Independent detector vectors and source-to-wire/fact preservation."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from contracts.traffic_observation import build_observation
from contracts.observation_measurements import encode_measurements, evidence_from_isolated_capture, original_anchor
from de.silver.archive_resolver import VerifiedRunContext
from de.silver.fact_builders import build_v1_fact_rows
from de.tests.silver.v1_fixtures import snapshot_v1


@pytest.fixture
def source(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[3] / "Visualize"))
    from observation import snapshot_provider as module

    return module


def _traci(occupancy):
    def getter(_):
        if occupancy is None:
            raise RuntimeError("missing getter result")
        return occupancy

    return SimpleNamespace(
        lanearea=SimpleNamespace(getLastStepOccupancy=getter),
        lane=SimpleNamespace(getLastStepOccupancy=getter, getLastStepVehicleIDs=lambda _: (),
                             getLength=lambda _: 100, getLastStepHaltingNumber=lambda _: 0),
    )


@pytest.mark.parametrize("percent", [0, 0.5, 0.8, 1, 1.1, 50, 100])
@pytest.mark.parametrize("using_e2", [False, True])
def test_all_four_occupancy_paths(source, monkeypatch, percent, using_e2):
    cfg = source.cfg
    tls, direction, edge = "tls", "East", "edge"
    monkeypatch.setattr(cfg, "APPROACH_EDGES", {tls: {direction: edge}})
    monkeypatch.setattr(cfg, "OUTGOING_EDGES", {tls: ["out"]})
    monkeypatch.setattr(cfg, "directions_for_node", lambda _: (direction,))
    monkeypatch.setattr(cfg, "approach_lane_ids", lambda *_: ("in_0",))
    monkeypatch.setattr(cfg, "edge_lane_ids", lambda e: (e + "_0",))
    monkeypatch.setattr(cfg, "movement_for_lane", lambda *_: "mixed")
    provider = source.SumoSnapshotProvider(tls, "fixture", stage_a_measurements=True)
    provider.detectors._checked = True
    provider.detectors.available = using_e2
    traci = _traci(percent)
    incoming_quality, outgoing_quality = {}, {}
    incoming = provider._build_direction(traci, direction, "green", quality=incoming_quality)
    outgoing = provider._spillback_metrics(traci, quality=outgoing_quality)
    assert incoming["occupancy_pct"] == pytest.approx(percent)
    assert outgoing["spillback_pressure"] == pytest.approx(percent / 100)
    assert outgoing["spillback_detected"] == (percent / 100 >= cfg.BOX_OCCUPANCY_THRESHOLD)
    assert incoming["occupancy_method"] == ("e2_percent_v2" if using_e2 else "lane_percent_v2")
    assert outgoing["spillback_occupancy_method"] == ("e2_ratio_v2" if using_e2 else "lane_ratio_v2")
    assert incoming["analytics_measurement_evidence"]["occupancy"]["percent"] == [percent]
    assert outgoing["analytics_spillback_evidence"]["percentGroups"] == [[percent]]
    legacy = source.SumoSnapshotProvider(tls, "fixture")
    legacy.detectors._checked = True
    legacy.detectors.available = using_e2
    old_incoming = legacy._build_direction(traci, direction, "green", quality={})
    old_outgoing = legacy._spillback_metrics(traci, quality={})
    assert old_incoming["occupancy_pct"] == pytest.approx(percent * 100 if percent <= 1 else percent)
    assert old_outgoing["spillback_pressure"] == pytest.approx(percent if percent <= 1 else percent / 100)
    assert "occupancy_method" not in old_incoming
    assert "queue_measurement" not in old_incoming
    assert "spillback_occupancy_method" not in old_outgoing
    assert "analytics_measurement_evidence" not in old_incoming
    assert "analytics_spillback_evidence" not in old_outgoing


@pytest.mark.parametrize("raw", [None, -1, 101, float("nan"), float("inf")])
def test_invalid_occupancy_is_marked_unavailable(source, monkeypatch, raw):
    cfg = source.cfg
    monkeypatch.setattr(cfg, "OUTGOING_EDGES", {"tls": ["out"]})
    monkeypatch.setattr(cfg, "directions_for_node", lambda _: ("East",))
    monkeypatch.setattr(cfg, "approach_lane_ids", lambda *_: ("in_0",))
    monkeypatch.setattr(cfg, "edge_lane_ids", lambda e: (e + "_0",))
    provider = source.SumoSnapshotProvider("tls", "fixture", stage_a_measurements=True)
    provider.detectors._checked = True
    quality = {}
    result = provider._build_direction(_traci(raw), "East", "green", quality=quality)
    assert "occupancy_pct" in quality["unavailable_core_metrics"]
    assert "occupancy_method" not in result
    quality = {}
    result = provider._spillback_metrics(_traci(raw), quality=quality)
    assert quality["spillback_unavailable"]
    assert result["spillback_occupancy_method"] is None


def test_queue_method_is_fixed_and_quality_is_not_measurement_zero(source, monkeypatch):
    cfg = source.cfg
    tls, direction, edge = "tls", "East", "edge"
    monkeypatch.setattr(cfg, "APPROACH_EDGES", {tls: {direction: edge}})
    monkeypatch.setattr(cfg, "directions_for_node", lambda _: (direction,))
    monkeypatch.setattr(cfg, "approach_lane_ids", lambda *_: ("edge_0",))
    monkeypatch.setattr(cfg, "edge_lane_ids", lambda e: (e + "_0",))
    monkeypatch.setattr(cfg, "TURN_BY_OD", {(edge, "out"): "right"})
    monkeypatch.setattr(cfg, "movement_for_lane", lambda *_: "mixed")
    provider = source.SumoSnapshotProvider(tls, "fixture", stage_a_measurements=True)
    provider.detectors._checked = True
    provider.detectors.available = True
    traci = _traci(0.8)
    traci.lanearea.getJamLengthMeters = lambda _: 0
    traci.lane.getLastStepVehicleIDs = lambda _: ("a", "a", "b")
    traci.vehicle = SimpleNamespace(getTypeID=lambda _: "passenger", getSpeed=lambda _: 0,
        getLength=lambda v: 4 if v == "a" else 6, getRoute=lambda _: (edge, "out"),
        getRouteIndex=lambda _: 0)
    quality = {}
    measured = provider._build_direction(traci, direction, "red", quality=quality)
    assert measured["queue_length_m"] == 13  # (4 + 1.5) + (6 + 1.5)
    assert measured["queue_measurement"]["method"] == "route_stopped_length_v1"
    assert measured["analytics_measurement_evidence"]["queue"]["samples"] == [["right", 4, 0], ["right", 6, 0]]
    assert "queue_length_m" not in quality["unavailable_core_metrics"]
    monkeypatch.setattr(cfg, "movement_for_lane", lambda *_: "right")
    measured_e2 = provider._build_direction(traci, direction, "red", quality={})
    assert measured_e2["queue_length_m"] == 0
    assert measured_e2["queue_measurement"]["method"] == "e2_jam_movement_v1"
    assert measured_e2["analytics_measurement_evidence"]["queue"]["samples"] == [["right", 0]]
    assert measured_e2["queue_measurement"]["scope_digest"] != measured["queue_measurement"]["scope_digest"]
    legacy = source.SumoSnapshotProvider(tls, "fixture")
    legacy.detectors._checked = True
    legacy.detectors.available = True
    assert legacy._build_direction(traci, direction, "red", quality={})["queue_length_m"] == 13
    provider.detectors.available = False
    quality = {}
    unavailable = provider._build_direction(traci, direction, "red", quality=quality)
    assert "queue_length_m" in quality["unavailable_core_metrics"]
    assert "queue_measurement" not in unavailable


def test_isolated_queue_rejects_route_on_another_edge_without_affecting_default(source, monkeypatch):
    monkeypatch.setattr(source.cfg, "directions_for_node", lambda _: ("East",))
    monkeypatch.setattr(source.cfg, "TURN_BY_OD", {("edge", "out"): "right"})
    vehicle = SimpleNamespace(getRoute=lambda _: ("earlier", "edge", "out"), getRouteIndex=lambda _: 0)
    traci = SimpleNamespace(vehicle=vehicle)
    legacy, corrected = (source.SumoSnapshotProvider("tls", "fixture", stage_a_measurements=opt_in)
                         for opt_in in (False, True))
    errors = []
    assert legacy._infer_movement(traci, "v", "edge", errors=errors) == "right" and not errors
    assert corrected._infer_movement(traci, "v", "edge", errors=errors) == "straight" and errors == ["v"]
    vehicle.getRouteIndex = lambda _: 1
    errors.clear()
    assert corrected._infer_movement(traci, "v", "edge", errors=errors) == "right" and not errors


def test_raw_evidence_materializes_corrected_values_without_changing_v1_facts():
    snapshot = snapshot_v1()
    direction = next(iter(snapshot["directions"]))
    payload = snapshot["directions"][direction]
    payload["occupancy_pct"] = 80.0
    payload["queue_length_m"] = 13.0
    snapshot["spillback_pressure"] = 0.8
    event = build_observation(snapshot, cycle_sequence=4, simulation_step=1200)
    payload["analytics_measurement_evidence"] = {
        "occupancy": {"method": "e2_percent_v2", "percent": [0.8]},
        "queue": {"method": "e2_jam_movement_v1", "scopeDigest": "0" * 64, "samples": [["right", 0]]},
    }
    snapshot["analytics_spillback_evidence"] = {"method": "lane_ratio_v2", "percentGroups": [[0.8]], "threshold": 0.7}
    snapshot["analytics_capture_seconds"] = 12.0
    assert event == build_observation(snapshot, cycle_sequence=4, simulation_step=1200)
    evidence = evidence_from_isolated_capture(event, snapshot, completed_step=1200)
    with pytest.raises(ValueError, match="original event coordinate"):
        evidence_from_isolated_capture(event, snapshot, completed_step=1199)
    node = snapshot["node_id"]
    context = VerifiedRunContext(snapshot["simulation_run_id"], 1, snapshot["topology_digest"],
        "a" * 64, (node,), {node: tuple(snapshot["directions"])}, {}, {}, "b" * 64, "c" * 64, "RUNNING", "t1")
    provenance = dict(bronze_semantic_revision=1, bronze_canonical_hash="d" * 64,
        source_raw_ingestion_id="raw", source_topic="traffic.observations.v1", source_partition=0,
        source_offset=12, source_physical_record_hash="e" * 64, source_byte_hash="f" * 64)
    facts = build_v1_fact_rows(event, context, provenance)
    traffic = next(f for f in facts["traffic"] if f["source_direction"] == direction)
    assert traffic["occupancy_pct"] == 80.0 and traffic["queue_length_m"] == 13.0
    assert facts["intersection"][0]["spillback_pressure"] == 0.8
    result = json.loads(encode_measurements(original_anchor(event, 1, "a" * 64),
                                            context.directions[node], evidence))
    assert result["anchor"]["payloadHash"] == event["payloadHash"]
    values = result["measurements"]
    assert values["directions"][direction]["occupancy"]["value"] == 0.8
    assert values["directions"][direction]["queue"]["value"] == 0.0
    assert values["spillback"]["value"] == {"pressure": 0.008, "downstreamFull": False, "detected": False}
    with pytest.raises(ValueError, match="no direct source measurement evidence"):
        encode_measurements(result["anchor"], context.directions[node], {"directions": {}, "spillback": None})
