"""Silver Plan 3 — dimension current/exact-state wrappers and Plan 2 hash parity helpers.

Implements Plan 3 §19, clarified by §17.3/§32.10/§32.11:

- ``silver_dim_intersection`` stores ``source_hash`` physically; comparison is direct.
- ``silver_dim_run`` / ``silver_dim_approach`` / ``silver_dim_scenario`` do not store a
  hash column, so the runtime must recompute the identical Plan 2 canonicalization from
  stored row values (``_sha256`` here is intentionally a byte-identical copy of
  ``de.silver.dimension_builders._sha256`` — parity is proven by test, not by import).
- Replay mode has no mirror for Approach/Scenario, so those candidates are suppressed
  (counted, not persisted, and never looked up) rather than routed to a main-table fallback.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from de.silver.dimension_builders import DimensionCandidate

SUPPRESSED_REPLAY_TARGETS = frozenset({"silver_dim_approach", "silver_dim_scenario"})

DimensionKey = Tuple[str, Tuple[str, ...]]


def _sha256(*parts: str) -> str:
    """Must remain byte-identical to de.silver.dimension_builders._sha256."""
    material = "|".join(parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def recompute_run_hash(row: Mapping[str, Any]) -> str:
    seed = row.get("seed") or ""
    return _sha256(
        str(row["simulation_run_id"]),
        str(row["scenario_id"]),
        str(row["producer_id"]),
        str(row["contract_version"]),
        str(seed),
    )


def recompute_approach_hash(intersection_id: str, direction: str) -> str:
    return _sha256(str(intersection_id), str(direction))


def recompute_scenario_hash(scenario_id: str) -> str:
    return _sha256(str(scenario_id))


def resolve_current_hash(
    target_table: str,
    business_key: Tuple[str, ...],
    stored_row: Optional[Mapping[str, Any]],
) -> Optional[str]:
    """Table-specific current-hash resolution per the §19 lookup table."""
    if stored_row is None:
        return None
    if target_table == "silver_dim_intersection":
        value = stored_row.get("source_hash")
        return None if value is None else str(value)
    if target_table == "silver_dim_run":
        return recompute_run_hash(stored_row)
    if target_table == "silver_dim_approach":
        return recompute_approach_hash(business_key[0], business_key[1])
    if target_table == "silver_dim_scenario":
        return recompute_scenario_hash(business_key[0])
    raise ValueError(f"Unsupported dimension target: {target_table!r}")


def filter_for_replay(
    candidates: Sequence[DimensionCandidate], *, is_replay: bool
) -> Tuple[Tuple[DimensionCandidate, ...], int]:
    """§17.3/§32.10 — suppress Approach/Scenario candidates in replay mode; count them."""
    if not is_replay:
        return tuple(candidates), 0
    kept = tuple(c for c in candidates if c.target_table not in SUPPRESSED_REPLAY_TARGETS)
    suppressed = len(candidates) - len(kept)
    return kept, suppressed


def decide_persisted_candidates(
    candidates: Sequence[DimensionCandidate],
    current_hash_by_key: Mapping[DimensionKey, Optional[str]],
) -> Tuple[DimensionCandidate, ...]:
    """Retain first/changed hash transitions in source-offset order (Plan 3 §19).

    A candidate is suppressed only when its hash equals the immediately prior accepted
    hash for the same ``(target_table, business_key)`` — starting from the persisted
    current-state hash. Every distinct transition within the batch is retained.
    """
    last_hash: Dict[DimensionKey, Optional[str]] = dict(current_hash_by_key)
    accepted: list[DimensionCandidate] = []
    for candidate in candidates:
        key: DimensionKey = (candidate.target_table, candidate.business_key)
        prior = last_hash.get(key)
        if prior == candidate.source_hash:
            continue
        accepted.append(candidate)
        last_hash[key] = candidate.source_hash
    return tuple(accepted)


def fetch_current_hashes(
    repository: Any,
    candidates: Sequence[DimensionCandidate],
    *,
    replay_run_id: Optional[str] = None,
) -> Dict[DimensionKey, Optional[str]]:
    """Wrap ``repository.fetch_current_dimension_states`` with Plan 2 hash resolution."""
    states = repository.fetch_current_dimension_states(candidates, replay_run_id=replay_run_id)
    return {key: resolve_current_hash(key[0], key[1], row) for key, row in states.items()}


def fetch_exact_versions(
    repository: Any,
    candidates: Sequence[DimensionCandidate],
    *,
    replay_run_id: Optional[str] = None,
) -> Dict[Tuple[str, Tuple[str, ...], str], bool]:
    """Wrap ``repository.find_exact_dimension_versions`` for uncertain-write recovery."""
    return repository.find_exact_dimension_versions(candidates, replay_run_id=replay_run_id)
