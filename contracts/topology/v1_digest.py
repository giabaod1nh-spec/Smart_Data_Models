"""Versioned V1 observation topology identity and contextual resolution.

This module deliberately does not import the active SUMO configuration or alter
the legacy ``topology_hash`` used by run manifests and DE.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

DIGEST_VERSION = "v1"
DIGEST_PATTERN = re.compile(r"^v1:sha256:[0-9a-f]{64}$")

# Every catalog member that can affect V1 interpretation or future projection.
# source_net is a machine-relative path, topology_hash has older semantics, and
# lane_movement_convention_note is documentation rather than topology data.
PROJECTION_FIELDS = (
    "schema_version",
    "signal_program_hash",
    "signal_programs",
    "phase_name_to_index",
    "junction_types",
    "node_to_tls",
    "node_to_tls_groups",
    "tls_to_node",
    "grouped_tls_axes",
    "node_locations",
    "approach_edges",
    "sensing_edges",
    "outgoing_edges",
    "inter_node_links",
    "edges",
    "connections",
    "movements_by_approach",
)


def canonical_catalog_bytes(catalog: Mapping[str, Any]) -> bytes:
    missing = [name for name in PROJECTION_FIELDS if name not in catalog]
    if missing:
        raise ValueError(f"catalog lacks V1 digest fields: {missing}")
    selected = {name: catalog[name] for name in PROJECTION_FIELDS}
    return json.dumps(
        selected, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def topology_digest(catalog: Mapping[str, Any]) -> str:
    return f"{DIGEST_VERSION}:sha256:{hashlib.sha256(canonical_catalog_bytes(catalog)).hexdigest()}"


def validate_digest_syntax(value: str) -> None:
    if not isinstance(value, str) or DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError("topologyDigest must be v1:sha256:<64 lowercase hex characters>")


@dataclass(frozen=True)
class ResolvedRunCatalog:
    simulation_run_id: str
    catalog: Mapping[str, Any]


class RunCatalogResolver(Protocol):
    """Phase 4 supplies an archive lookup keyed strictly by simulation run."""

    def resolve(self, simulation_run_id: str) -> ResolvedRunCatalog:
        """Raise CatalogUnavailableError for missing or temporarily inaccessible archives."""


class CatalogUnavailableError(Exception):
    """The run archive cannot currently be consulted; retry may succeed."""


class CatalogIntegrityError(ValueError):
    """A resolved archive is inconsistent with the event identity or digest."""


def resolve_v1_catalog(
    simulation_run_id: str, expected_digest: str, resolver: RunCatalogResolver,
) -> Mapping[str, Any]:
    """Contextual check, intentionally separate from observation validation."""
    validate_digest_syntax(expected_digest)
    try:
        resolved = resolver.resolve(simulation_run_id)
    except (FileNotFoundError, ConnectionError, TimeoutError) as exc:
        raise CatalogUnavailableError(str(exc)) from exc
    if resolved.simulation_run_id != simulation_run_id:
        raise CatalogIntegrityError("resolved catalog belongs to a different simulation run")
    try:
        actual_digest = topology_digest(resolved.catalog)
    except (TypeError, ValueError, KeyError) as exc:
        raise CatalogIntegrityError("resolved catalog cannot produce a V1 digest") from exc
    if actual_digest != expected_digest:
        raise CatalogIntegrityError(
            f"topology digest mismatch: expected={expected_digest} actual={actual_digest}"
        )
    return resolved.catalog


def validate_observation_in_catalog(
    event: Mapping[str, Any], resolver: RunCatalogResolver,
) -> Mapping[str, Any]:
    """Check an otherwise valid V1 event against its run's archived approaches."""
    from contracts.traffic_observation import validate_observation

    validate_observation(event)
    catalog = resolve_v1_catalog(
        event["simulationRunId"], event["topologyDigest"], resolver,
    )
    node_id = event["intersectionId"]
    primary_tls = catalog["node_to_tls"].get(node_id)
    if primary_tls is None:
        raise CatalogIntegrityError(f"intersection {node_id} is absent from archived catalog")
    expected = set(catalog["approach_edges"].get(primary_tls, {}))
    actual = set(event["payload"]["directions"])
    if not expected or actual != expected:
        raise CatalogIntegrityError(
            f"intersection {node_id} approaches differ from archived catalog: "
            f"expected={sorted(expected)} actual={sorted(actual)}"
        )
    return catalog
