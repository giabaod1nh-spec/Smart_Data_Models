"""Per-intersection scenario apply/clear — physical SUMO effects before metadata commit."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

import configuration.config as cfg
from configuration.model_params import get_registry

log = logging.getLogger(__name__)


def clear_node_scenario_effects(runtime, traci_module, node_id: str) -> List[str]:
    """Remove overlays, incident vehicles, and restore lanes for one intersection."""
    removed: List[str] = []
    removed.extend(runtime.capacity.remove_overlays_for_intersection(traci_module, node_id))
    runtime.incident.clear(traci_module, node_id)
    nstate = runtime.state.nodes.get(node_id)
    if nstate:
        nstate.operational_state["incident_active"] = False
        nstate.active_overlay_ids = [
            oid for oid in nstate.active_overlay_ids if oid not in removed
        ]
    return removed


def _node_demand_delta(runtime, node_id: str) -> Dict[str, float]:
    tls = cfg.NODE_TO_TLS[node_id]
    out: Dict[str, float] = {}
    for sid, bucket in runtime.demand._buckets.items():
        src = get_registry().boundary_sources().get(sid, {})
        if str(src.get("turn_at_tls") or "") == tls:
            out[sid] = float(bucket.target_delta_vph)
    return out


def _verify_rain_lanes(traci_module, node_id: str) -> None:
    tls = cfg.NODE_TO_TLS[node_id]
    reduced = 0
    for direction in cfg.DIRECTIONS:
        for lid in cfg.approach_lane_ids(tls, direction):
            try:
                speed = float(traci_module.lane.getMaxSpeed(lid))
                if speed < 12.0:
                    reduced += 1
            except Exception:
                pass
    if reduced == 0:
        raise RuntimeError(f"rain overlay did not reduce lane speeds at {node_id}")


def apply_node_scenario(
    runtime,
    traci_module,
    node_id: str,
    scenario: str,
    sim_t: float,
) -> Dict[str, Any]:
    """
    Apply canonical scenario at node_id only. Clears prior physical effects at that node first.
    Raises RuntimeError if physical apply fails.
    """
    scenario = cfg.normalize_scenario_id(scenario)
    if scenario not in cfg.CANONICAL_SCENARIO_IDS:
        raise ValueError(f"Unknown scenario '{scenario}'")

    clear_node_scenario_effects(runtime, traci_module, node_id)

    result: Dict[str, Any] = {
        "scenarioId": scenario,
        "affectedIntersections": [node_id],
        "demandProfileChanged": False,
        "overlayIds": [],
        "incidentVehicles": [],
        "failures": [],
    }

    try:
        if scenario == "normal":
            runtime.set_demand_profile("normal", target_intersection=node_id)
            result["demandProfileChanged"] = True

        elif scenario in ("peak", "oversaturated"):
            runtime.set_demand_profile(scenario, target_intersection=node_id)
            result["demandProfileChanged"] = True
            delta = _node_demand_delta(runtime, node_id)
            result["demandDeltaVphBySource"] = delta
            if scenario == "oversaturated" and delta:
                min_delta = min(delta.values())
                if min_delta < 60000:
                    raise RuntimeError(
                        f"oversaturated delta too low at {node_id}: min={min_delta}"
                    )

        elif scenario == "rain":
            ov = runtime.add_overlay(
                traci_module,
                overlay_type="heavy_rain",
                intersection_id=node_id,
                sim_t=sim_t,
            )
            result["overlayIds"].append(ov.get("overlay_id"))
            _verify_rain_lanes(traci_module, node_id)

        elif scenario == "incident":
            direction = cfg.incident_approach_direction(node_id)
            # Physical blockers only — do NOT setDisallowed the whole crash lane
            # so upstream traffic can still use the middle lane until the red zone.
            vids = runtime.incident.stage(traci_module, node_id, direction, sim_t)
            result["incidentVehicles"] = vids
            runtime.set_demand_profile("peak", target_intersection=node_id)
            result["demandProfileChanged"] = True
            nstate = runtime.state.nodes.get(node_id)
            if nstate:
                nstate.operational_state["incident_active"] = True

        else:
            result["failures"].append(f"NOT_SUPPORTED:{scenario}")

    except Exception as e:
        result["failures"].append(str(e))
        log.exception("scenario apply failed node=%s scenario=%s", node_id, scenario)
        raise RuntimeError(f"SUMO scenario apply failed: {e}") from e

    if result["failures"]:
        raise RuntimeError("; ".join(result["failures"]))

    runtime.state.per_node_scenarios[node_id] = scenario
    return result
