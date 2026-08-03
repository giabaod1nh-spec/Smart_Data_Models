"""Runtime dimension hash helpers must match Plan 2 dimension_builders._sha256 canonicalization
(Plan 3 §19)."""
from __future__ import annotations

from de.silver import dimension_builders, dimension_state
from de.tests.silver.conftest import make_run


def test_run_hash_parity_with_seed():
    row = {
        "simulation_run_id": "run-1",
        "scenario_id": "normal",
        "producer_id": "producer-a",
        "contract_version": "2.0.0",
        "seed": "session-1",
    }
    expected = dimension_builders._sha256(
        row["simulation_run_id"], row["scenario_id"], row["producer_id"],
        row["contract_version"], row["seed"],
    )
    assert dimension_state.recompute_run_hash(row) == expected


def test_run_hash_parity_none_seed_maps_to_empty_string():
    row = {
        "simulation_run_id": "run-2", "scenario_id": "wet",
        "producer_id": "producer-b", "contract_version": "2.0.0", "seed": None,
    }
    expected = dimension_builders._sha256(
        row["simulation_run_id"], row["scenario_id"], row["producer_id"],
        row["contract_version"], "",
    )
    assert dimension_state.recompute_run_hash(row) == expected


def test_approach_hash_parity():
    expected = dimension_builders._sha256("INT-1", "N")
    assert dimension_state.recompute_approach_hash("INT-1", "N") == expected


def test_scenario_hash_parity():
    expected = dimension_builders._sha256("normal")
    assert dimension_state.recompute_scenario_hash("normal") == expected


def test_end_to_end_run_dimension_candidate_hash_matches_recomputed_hash():
    record = make_run()
    candidate = dimension_builders.build_run_dimension(record)
    stored_row = {
        "simulation_run_id": candidate.row.simulation_run_id,
        "scenario_id": candidate.row.scenario_id,
        "producer_id": candidate.row.producer_id,
        "contract_version": candidate.row.contract_version,
        "seed": candidate.row.seed,
    }
    assert dimension_state.recompute_run_hash(stored_row) == candidate.source_hash


def test_end_to_end_scenario_dimension_candidate_hash_matches():
    record = make_run(scenario_id="rainy")
    candidate = dimension_builders.build_scenario_dimension(record)
    assert dimension_state.recompute_scenario_hash(candidate.row.scenario_id) == candidate.source_hash


def test_intersection_hash_uses_stored_value_directly_not_recomputed():
    stored_row = {"source_hash": "some-persisted-hash"}
    assert (
        dimension_state.resolve_current_hash("silver_dim_intersection", ("I1",), stored_row)
        == "some-persisted-hash"
    )


def test_resolve_current_hash_none_when_no_stored_row():
    assert dimension_state.resolve_current_hash("silver_dim_run", ("r1",), None) is None


def test_decide_persisted_candidates_retains_every_distinct_transition():
    c1 = dimension_builders.DimensionCandidate("silver_dim_scenario", ("s1",), "hashA", row=None)
    c2 = dimension_builders.DimensionCandidate("silver_dim_scenario", ("s1",), "hashA", row=None)  # dup
    c3 = dimension_builders.DimensionCandidate("silver_dim_scenario", ("s1",), "hashB", row=None)  # changed
    c4 = dimension_builders.DimensionCandidate("silver_dim_scenario", ("s1",), "hashB", row=None)  # dup
    c5 = dimension_builders.DimensionCandidate("silver_dim_scenario", ("s1",), "hashC", row=None)  # changed
    accepted = dimension_state.decide_persisted_candidates([c1, c2, c3, c4, c5], current_hash_by_key={})
    assert [c.source_hash for c in accepted] == ["hashA", "hashB", "hashC"]


def test_decide_persisted_candidates_suppresses_unchanged_from_current_state():
    c1 = dimension_builders.DimensionCandidate("silver_dim_scenario", ("s1",), "hashA", row=None)
    accepted = dimension_state.decide_persisted_candidates(
        [c1], current_hash_by_key={("silver_dim_scenario", ("s1",)): "hashA"}
    )
    assert accepted == ()


def test_decide_persisted_candidates_first_seen_when_no_current_state():
    c1 = dimension_builders.DimensionCandidate("silver_dim_run", ("r1",), "hashA", row=None)
    accepted = dimension_state.decide_persisted_candidates([c1], current_hash_by_key={})
    assert accepted == (c1,)


def test_decide_persisted_candidates_is_independent_per_key():
    a1 = dimension_builders.DimensionCandidate("silver_dim_approach", ("I1", "N"), "hA", row=None)
    b1 = dimension_builders.DimensionCandidate("silver_dim_approach", ("I1", "S"), "hB", row=None)
    accepted = dimension_state.decide_persisted_candidates([a1, b1], current_hash_by_key={})
    assert accepted == (a1, b1)
