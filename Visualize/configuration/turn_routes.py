"""Turn-route helpers for boundary-source demand (first-TLS turn split + full O-D)."""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

MOVEMENTS = ("left", "straight", "right")

# First-hop only destinations (pre full-O-D fix) — must not appear as flow `to=`.
HOP_ONLY_DESTINATIONS = frozenset(
    {
        "J3J1",
        "J3J4",
        "J3W2",
        "J1J3",
        "J1W1",
        "J1J2",
        "J4J2",
        "J4E2",
        "J4J3",
        "J2J4",
        "J2J1",
        "J2E1",
        "J1S1",
        "J2S2",
        "J3N1",
        "J4N2",
    }
)


def route_key(source_id: str, movement: str) -> str:
    return f"{source_id}_{movement}"


def route_distribution_from_destinations(source_id: str, destinations: List[Mapping[str, Any]]) -> Dict[str, float]:
    return {route_key(source_id, str(d["movement"])): float(d["weight"]) for d in destinations}


def _destination_for_route_key(source_id: str, rk: str, src: Mapping[str, Any]) -> Mapping[str, Any]:
    for d in src.get("destinations") or []:
        mov = str(d["movement"])
        if rk == route_key(source_id, mov):
            return d
    raise KeyError(f"Unknown route_key {rk!r} for source {source_id!r}")


def turn_edge_for_route_key(source_id: str, rk: str, src: Mapping[str, Any]) -> str:
    d = _destination_for_route_key(source_id, rk, src)
    return str(d["turn_edge"])


def to_edge_for_route_key(source_id: str, rk: str, src: Mapping[str, Any]) -> str:
    d = _destination_for_route_key(source_id, rk, src)
    return str(d["to_edge"])


def weighted_pick(items: List[Tuple[str, float]], r: float) -> str:
    cum = 0.0
    last = items[-1][0]
    for key, weight in items:
        cum += float(weight)
        if r <= cum:
            return key
    return last


def validate_source_turn_routes(source_id: str, src: Mapping[str, Any]) -> None:
    from configuration.network_topology import OUTGOING_EDGES, TURN_BY_OD

    if "source_edge" not in src:
        raise ValueError(f"boundary_sources.{source_id}: missing source_edge")
    if "turn_at_tls" not in src:
        raise ValueError(f"boundary_sources.{source_id}: missing turn_at_tls")
    tls = str(src["turn_at_tls"])
    if tls not in OUTGOING_EDGES:
        raise ValueError(f"boundary_sources.{source_id}: unknown turn_at_tls {tls!r}")

    destinations = src.get("destinations") or []
    if len(destinations) != 3:
        raise ValueError(f"boundary_sources.{source_id}: expected 3 destinations, got {len(destinations)}")

    movements = []
    weight_sum = 0.0
    from_edge = str(src["source_edge"])
    allowed = set(OUTGOING_EDGES[tls])

    for d in destinations:
        mov = str(d.get("movement", ""))
        if mov not in MOVEMENTS:
            raise ValueError(f"boundary_sources.{source_id}: invalid movement {mov!r}")
        if mov in movements:
            raise ValueError(f"boundary_sources.{source_id}: duplicate movement {mov!r}")
        movements.append(mov)

        if "turn_edge" not in d:
            raise ValueError(f"boundary_sources.{source_id}: destination {mov!r} missing turn_edge")
        if "to_edge" not in d:
            raise ValueError(f"boundary_sources.{source_id}: destination {mov!r} missing to_edge")

        turn_edge = str(d["turn_edge"])
        to_edge = str(d["to_edge"])

        if turn_edge not in allowed:
            raise ValueError(
                f"boundary_sources.{source_id}: turn_edge {turn_edge!r} not in OUTGOING_EDGES[{tls}]"
            )
        expected = TURN_BY_OD.get((from_edge, turn_edge))
        if expected != mov:
            raise ValueError(
                f"boundary_sources.{source_id}: {from_edge}->{turn_edge} is {expected!r} in TURN_BY_OD, "
                f"destinations says {mov!r}"
            )
        if to_edge == turn_edge and to_edge in HOP_ONLY_DESTINATIONS:
            raise ValueError(
                f"boundary_sources.{source_id}: to_edge {to_edge!r} is hop-only; use full O-D destination"
            )
        if to_edge == turn_edge:
            raise ValueError(
                f"boundary_sources.{source_id}: to_edge must differ from turn_edge for {mov!r} "
                f"(got {to_edge!r})"
            )

        weight_sum += float(d.get("weight", 0))

    if abs(weight_sum - 1.0) > 1e-6:
        raise ValueError(f"boundary_sources.{source_id}: destination weights must sum to 1, got {weight_sum}")

    rd = src.get("route_distribution") or {}
    expected_rd = route_distribution_from_destinations(source_id, destinations)
    for key, weight in expected_rd.items():
        if key not in rd:
            raise ValueError(f"boundary_sources.{source_id}: route_distribution missing {key!r}")
        if abs(float(rd[key]) - weight) > 1e-6:
            raise ValueError(
                f"boundary_sources.{source_id}: route_distribution[{key!r}]={rd[key]!r} "
                f"!= destination weight {weight!r}"
            )
    if abs(sum(float(v) for v in rd.values()) - 1.0) > 1e-6:
        raise ValueError(f"boundary_sources.{source_id}: route_distribution must sum to 1")

    straight = next(d for d in destinations if d["movement"] == "straight")
    if str(src.get("to_edge", "")) != str(straight["to_edge"]):
        raise ValueError(
            f"boundary_sources.{source_id}: to_edge must match straight full O-D "
            f"({straight['to_edge']!r})"
        )
