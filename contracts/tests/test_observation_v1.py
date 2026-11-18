"""Focused Phase 1 wire, semantic, builder, and catalog identity checks."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from contracts.traffic_observation import (
    assert_same_observation,
    build_observation,
    canonical_json,
    validate_observation,
)
from contracts.topology.v1_digest import (
    CatalogIntegrityError,
    CatalogUnavailableError,
    ResolvedRunCatalog,
    resolve_v1_catalog,
    topology_digest,
    validate_observation_in_catalog,
)

ROOT = Path(__file__).resolve().parents[2]
CATALOG = json.loads((ROOT / "Visualize/generated/network_topology_catalog.json").read_text(encoding="utf-8"))


def _snapshot() -> dict:
    approaches = CATALOG["approach_edges"][CATALOG["node_to_tls"]["A"]]
    empty_direction = {
        "vehicle_count": 0, "pcu_equivalent": 0.0,
        "left_count": 0, "straight_count": 0, "right_count": 0,
        "average_speed_kmh": 0.0, "waiting_vehicle_count": 0,
        "queue_length_m": 0.0, "occupancy_pct": 0.0,
        "vehicle_class_composition": {},
    }
    return {
        "node_id": "A", "simulation_run_id": "run-contract", "simulation_time_sec": 12.0,
        "scenario": "normal", "control_mode": "FIXED", "phase": "NS_GREEN",
        "captured_at": "2026-01-01T00:00:12Z", "topology_digest": topology_digest(CATALOG),
        "incidents": [], "colors": {name: "green" for name in approaches},
        "directions": {name: copy.deepcopy(empty_direction) for name in approaches},
    }


def _event() -> dict:
    return build_observation(_snapshot(), cycle_sequence=4, simulation_step=1200)


def test_builder_selects_declared_fields_and_keeps_capture_time() -> None:
    source = _snapshot()
    source["internal_cache_token"] = "secret"
    source["directions"]["East"]["internal_detector_id"] = "not wire data"
    one = build_observation(source, cycle_sequence=4, simulation_step=1200)
    two = build_observation(source, cycle_sequence=4, simulation_step=1200)
    assert "internal_cache_token" not in one["payload"]
    assert "internal_detector_id" not in one["payload"]["directions"]["East"]
    assert one["capturedAt"] == two["capturedAt"] == source["captured_at"]
    assert one["eventId"] == two["eventId"]
    assert one["payloadHash"] == two["payloadHash"]
    assert one["scenarioId"] == "normal"
    assert canonical_json(one["payload"]) == canonical_json(two["payload"])
    validate_observation(one)
    assert_same_observation(one, two)


@pytest.mark.parametrize("change", [
    lambda e: e.pop("scenarioId"),
    lambda e: e.__setitem__("capturedAt", "not-a-date"),
    lambda e: e.__setitem__("cycleSequence", True),
    lambda e: e.__setitem__("topologyDigest", "v2:sha256:" + "0" * 64),
    lambda e: e["payload"].__setitem__("internal_field", 1),
    lambda e: e["payload"]["directions"]["East"].__setitem__("internal_detector_id", "lane-1"),
    lambda e: e["payload"]["directions"]["East"].__setitem__("vehicle_count", 1.5),
    lambda e: e["payload"]["directions"]["East"].__setitem__("occupancy_pct", 101.0),
])
def test_structural_rejections(change) -> None:
    event = _event()
    change(event)
    with pytest.raises(ValueError):
        validate_observation(event)


@pytest.mark.parametrize("change", [
    lambda e: e.__setitem__("eventId", "0" * 64),
    lambda e: e.__setitem__("payloadHash", "0" * 64),
    lambda e: e.__setitem__("scenarioId", "rain"),
    lambda e: e.__setitem__("simulationTimeSeconds", 13.0),
    lambda e: e["payload"]["colors"].__setitem__("North", "red"),
    lambda e: e["payload"]["directions"]["East"].__setitem__("average_speed_kmh", float("nan")),
])
def test_semantic_rejections(change) -> None:
    event = _event()
    change(event)
    with pytest.raises(ValueError):
        validate_observation(event)


def test_null_is_explicitly_unavailable_and_not_zero() -> None:
    source = _snapshot()
    source["directions"]["East"]["occupancy_pct"] = None
    source["directions"]["East"]["metric_availability"] = {"occupancy_pct": "UNAVAILABLE"}
    event = build_observation(source, cycle_sequence=4, simulation_step=1200)
    assert event["payload"]["directions"]["East"]["occupancy_pct"] is None
    bad = copy.deepcopy(event)
    del bad["payload"]["directions"]["East"]["metric_availability"]
    with pytest.raises(ValueError, match="metric_availability"):
        validate_observation(bad)

    source["directions"]["East"]["metric_availability"] = {"occupancy_pct": "NOT_APPLICABLE"}
    validate_observation(build_observation(source, cycle_sequence=5, simulation_step=1300))


def test_builder_requires_source_capture_metadata() -> None:
    source = _snapshot()
    del source["captured_at"]
    with pytest.raises(ValueError):
        build_observation(source, cycle_sequence=4, simulation_step=1200)
    source = _snapshot()
    del source["topology_digest"]
    with pytest.raises(ValueError):
        build_observation(source, cycle_sequence=4, simulation_step=1200)


def test_retry_requires_identical_frozen_event() -> None:
    event = _event()
    changed_time = copy.deepcopy(event)
    changed_time["capturedAt"] = "2026-01-01T00:00:13Z"
    with pytest.raises(ValueError, match="entire frozen observation"):
        assert_same_observation(event, changed_time)
    changed_payload = copy.deepcopy(event)
    changed_payload["payload"]["directions"]["East"]["vehicle_count"] = 1
    from hashlib import sha256
    changed_payload["payloadHash"] = sha256(canonical_json(changed_payload["payload"]).encode()).hexdigest()
    with pytest.raises(ValueError, match="conflicting payloadHash"):
        assert_same_observation(event, changed_payload)


def test_digest_covers_projection_fields_without_changing_legacy_hash() -> None:
    baseline = topology_digest(CATALOG)
    assert baseline == topology_digest(json.loads(json.dumps(CATALOG, sort_keys=True)))
    for key, mutation in (
        ("node_to_tls_groups", lambda c: c["node_to_tls_groups"]["A"].append("tls-extra")),
        ("node_locations", lambda c: c["node_locations"]["A"].__setitem__("lat", 1.234)),
        ("approach_edges", lambda c: c["approach_edges"][c["node_to_tls"]["A"]].__setitem__("East", "other-edge")),
    ):
        changed = copy.deepcopy(CATALOG)
        mutation(changed)
        assert topology_digest(changed) != baseline, key
        assert changed["topology_hash"] == CATALOG["topology_hash"]
    path_only = copy.deepcopy(CATALOG)
    path_only["source_net"] = "other-host/intersection.net.xml"
    assert topology_digest(path_only) == baseline


def test_run_catalog_resolution_distinguishes_availability_and_integrity() -> None:
    digest = topology_digest(CATALOG)

    class Resolver:
        def __init__(self, run: str, catalog: dict):
            self.run, self.catalog = run, catalog

        def resolve(self, _requested: str) -> ResolvedRunCatalog:
            return ResolvedRunCatalog(self.run, self.catalog)

    assert resolve_v1_catalog("run-contract", digest, Resolver("run-contract", CATALOG)) is CATALOG
    assert validate_observation_in_catalog(_event(), Resolver("run-contract", CATALOG)) is CATALOG
    incomplete = _event()
    del incomplete["payload"]["directions"]["South"]
    del incomplete["payload"]["colors"]["South"]
    from hashlib import sha256
    incomplete["payloadHash"] = sha256(canonical_json(incomplete["payload"]).encode()).hexdigest()
    with pytest.raises(CatalogIntegrityError, match="approaches differ"):
        validate_observation_in_catalog(incomplete, Resolver("run-contract", CATALOG))
    with pytest.raises(CatalogIntegrityError):
        resolve_v1_catalog("run-contract", digest, Resolver("different-run", CATALOG))
    changed = copy.deepcopy(CATALOG)
    changed["node_locations"]["A"]["lat"] = 1.234
    with pytest.raises(CatalogIntegrityError):
        resolve_v1_catalog("run-contract", digest, Resolver("run-contract", changed))

    class Missing:
        def resolve(self, _requested: str) -> ResolvedRunCatalog:
            raise FileNotFoundError("archive not yet visible")

    with pytest.raises(CatalogUnavailableError):
        resolve_v1_catalog("run-contract", digest, Missing())


def test_checked_in_fixtures_have_documented_validity() -> None:
    fixture_dir = ROOT / "contracts/observation/fixtures"
    for filename in ("valid_code_derived.json", "unavailable_synthetic.json"):
        validate_observation(json.loads((fixture_dir / filename).read_text(encoding="utf-8")))
    invalid = json.loads((fixture_dir / "invalid_unknown_field_synthetic.json").read_text(encoding="utf-8"))
    with pytest.raises(ValueError):
        validate_observation(invalid)
