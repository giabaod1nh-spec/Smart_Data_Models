"""Replay suppression of Approach/Scenario dimension candidates — no replay mirror exists
in DDL 004 (Plan 3 §17.3/§32.10)."""
from __future__ import annotations

from de.silver import dimension_state
from de.silver.dimension_builders import DimensionCandidate


def _candidate(target: str, key: tuple, hash_: str) -> DimensionCandidate:
    return DimensionCandidate(target, key, hash_, row=None)


def test_live_mode_keeps_all_candidates():
    candidates = [
        _candidate("silver_dim_run", ("r1",), "h1"),
        _candidate("silver_dim_approach", ("i1", "N"), "h2"),
        _candidate("silver_dim_scenario", ("s1",), "h3"),
    ]
    kept, suppressed = dimension_state.filter_for_replay(candidates, is_replay=False)
    assert kept == tuple(candidates)
    assert suppressed == 0


def test_replay_mode_suppresses_approach_and_scenario_only():
    candidates = [
        _candidate("silver_dim_run", ("r1",), "h1"),
        _candidate("silver_dim_intersection", ("i1",), "h2"),
        _candidate("silver_dim_approach", ("i1", "N"), "h3"),
        _candidate("silver_dim_scenario", ("s1",), "h4"),
    ]
    kept, suppressed = dimension_state.filter_for_replay(candidates, is_replay=True)
    kept_targets = {c.target_table for c in kept}
    assert kept_targets == {"silver_dim_run", "silver_dim_intersection"}
    assert suppressed == 2


def test_replay_suppression_counts_all_suppressed_even_when_none_kept():
    candidates = [
        _candidate("silver_dim_approach", ("i1", "N"), "h1"),
        _candidate("silver_dim_scenario", ("s1",), "h2"),
    ]
    kept, suppressed = dimension_state.filter_for_replay(candidates, is_replay=True)
    assert kept == ()
    assert suppressed == 2


def test_replay_suppression_empty_candidates_is_zero():
    kept, suppressed = dimension_state.filter_for_replay([], is_replay=True)
    assert kept == ()
    assert suppressed == 0


def test_replay_run_and_intersection_use_their_mirrors_not_suppressed():
    candidates = [
        _candidate("silver_dim_run", ("r1",), "h1"),
        _candidate("silver_dim_intersection", ("i1",), "h2"),
    ]
    kept, suppressed = dimension_state.filter_for_replay(candidates, is_replay=True)
    assert kept == tuple(candidates)
    assert suppressed == 0


def test_suppressed_targets_constant_matches_ddl_limitation():
    # DDL 004 has replay mirrors only for silver_dim_run / silver_dim_intersection.
    assert dimension_state.SUPPRESSED_REPLAY_TARGETS == frozenset(
        {"silver_dim_approach", "silver_dim_scenario"}
    )
