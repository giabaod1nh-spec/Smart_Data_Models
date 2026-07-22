"""NetworkContextCoordinator — local multi-dimensional context + causal orchestrator."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from configuration.model_params import get_registry
from runtime.layer_contracts import DerivedPhenomena, DirectionLocalContext
from runtime.state import (
    SEVERITY,
    NetworkRuntimeState,
    NodeRuntimeState,
)

from .cause_inference import CauseInferenceEngine
from .propagation_analyzer import PropagationAnalyzer
from .topology_resolver import TopologyResolver


class NetworkContextCoordinator:
    """Orchestrator — local derivation + network causal."""

    def __init__(self, catalog: Optional[Dict[str, Any]] = None):
        self.topology = TopologyResolver(catalog)
        self.propagation = PropagationAnalyzer()
        self.causes = CauseInferenceEngine(self.topology, self.propagation)
        # (node, dir) -> (traffic_state, since_s)
        self._dir_hold: Dict[Tuple[str, str], Tuple[str, float]] = {}
        # (node, dir) -> time when candidate recovery started
        self._recovery_since: Dict[Tuple[str, str], float] = {}

    def update_from_snapshots(
        self,
        state: NetworkRuntimeState,
        snapshots: Dict[str, dict],
        overlays: List[Dict[str, Any]],
        sim_t: float,
        preemption_by_node: Optional[Dict[str, bool]] = None,
    ) -> None:
        ctx = get_registry().context_derivation()
        busy = float(ctx.get("direction_pcu_busy", 15))
        cong = float(ctx.get("direction_pcu_congested", 35))
        oversat_pcu = float(ctx.get("direction_pcu_oversaturated", 55))
        hyst = float(ctx.get("hysteresis_s", 8))
        recovery_s = float(ctx.get("recovery_s", 12))
        order = ctx.get("traffic_state_order") or ctx.get("severity_order") or SEVERITY
        # Filter to traffic-load axis only
        order = [s for s in order if s in SEVERITY] or list(SEVERITY)
        agg_rule = ctx.get("aggregate_rule") or {}
        agg_type = agg_rule.get("type", "worst_state")

        preemption_by_node = preemption_by_node or {}
        incident_nodes = {
            o["intersection_id"]
            for o in overlays
            if o.get("type") in ("accident", "blocked_intersection") and o.get("state") == "ACTIVE"
        }
        restrict_nodes = {
            o["intersection_id"]
            for o in overlays
            if o.get("type") == "downstream_restriction" and o.get("state") == "ACTIVE"
        }

        for node_id, snap in snapshots.items():
            nstate = state.nodes.setdefault(node_id, NodeRuntimeState(node_id=node_id))
            pressure = float(snap.get("spillback_pressure") or 0)
            box_blocked = bool(
                snap.get("intersection_box_blocked") or snap.get("downstream_edge_full")
            )
            spillback_active = bool(snap.get("spillback_detected"))
            spillback_risk = (not spillback_active) and pressure >= 0.7

            nstate.derived_phenomena = DerivedPhenomena(
                spillback_risk=spillback_risk,
                spillback_active=spillback_active,
                box_blocked=box_blocked,
            )

            dirs = snap.get("directions") or {}
            for dname, d in dirs.items():
                pcu = float(d.get("pcu_equivalent") or 0)
                q = float(d.get("queue_length_m") or 0)
                raw = self._classify_traffic_state(pcu, q, busy, cong, oversat_pcu)

                key = (node_id, dname)
                prev = self._dir_hold.get(key)
                held = raw
                if prev:
                    prev_state, since = prev
                    if prev_state != raw:
                        # worsening: apply hysteresis before accepting worse? Plan: hold previous on change
                        if (sim_t - since) < hyst:
                            held = prev_state
                        else:
                            held = raw
                            self._dir_hold[key] = (raw, sim_t)
                            self._recovery_since.pop(key, None)
                    else:
                        # same raw — recovery path when improving from held worse
                        held = raw
                        self._dir_hold[key] = (raw, sim_t)
                else:
                    self._dir_hold[key] = (raw, sim_t)

                # Recovery: require recovery_s of improved raw before leaving congested band
                if prev and prev[0] in ("CONGESTED", "OVERSATURATED", "BUSY") and raw in (
                    "FREE_FLOW",
                    "STABLE",
                ):
                    rs = self._recovery_since.get(key)
                    if rs is None:
                        self._recovery_since[key] = sim_t
                        held = prev[0]
                    elif (sim_t - rs) < recovery_s:
                        held = prev[0]
                    else:
                        held = raw
                        self._dir_hold[key] = (raw, sim_t)
                        self._recovery_since.pop(key, None)
                elif prev and prev[0] != raw and raw in ("CONGESTED", "OVERSATURATED", "BUSY"):
                    self._recovery_since.pop(key, None)

                nstate.directions[dname] = DirectionLocalContext(
                    traffic_state=held,
                    queue_length_m=q,
                    pcu=pcu,
                    occupancy_pct=float(d.get("occupancy_pct") or 0),
                    mean_speed_kmh=float(d.get("average_speed_kmh") or 0),
                )
                self.propagation.record(
                    node_id,
                    dname,
                    sim_t,
                    q,
                    occupancy=float(d.get("occupancy_pct") or 0) / 100.0,
                )

            states = [x.traffic_state for x in nstate.directions.values()]
            if agg_type == "weighted":
                nstate.aggregate_traffic_state = self._weighted(states, agg_rule, order)
            elif agg_type == "worst_state":
                nstate.aggregate_traffic_state = self._worst(states, order)
            else:
                raise ValueError(f"Unsupported aggregate_rule.type={agg_type}")
            nstate.sync_aliases()

            nstate.operational_state["incident_active"] = node_id in incident_nodes
            nstate.operational_state["downstream_restriction_active"] = node_id in restrict_nodes
            nstate.operational_state["emergency_preemption_active"] = bool(
                preemption_by_node.get(node_id) or snap.get("preemption_active")
            )
            nstate.active_overlay_ids = [
                o["overlay_id"]
                for o in overlays
                if o.get("intersection_id") == node_id and o.get("state") == "ACTIVE"
            ]

        self.causes.infer(sim_t=sim_t, overlays=overlays, nodes=state.nodes, snapshots=snapshots)
        state.link_states = self._build_link_states(state, snapshots, sim_t)
        state.network_summary = {
            "nodes_congested": sum(
                1
                for n in state.nodes.values()
                if n.aggregate_traffic_state in ("CONGESTED", "OVERSATURATED")
            ),
            "nodes_spillback": sum(
                1 for n in state.nodes.values() if n.derived_phenomena.spillback_active
            ),
            "active_overlays": len(overlays),
            "probable_cause_count": sum(len(n.probable_causes) for n in state.nodes.values()),
        }
        state.last_context_sim_t = sim_t

    def _build_link_states(
        self, state: NetworkRuntimeState, snapshots: Dict[str, dict], sim_t: float
    ) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for up, dn in (("A", "B"), ("B", "A"), ("A", "C"), ("C", "A"), ("B", "D"), ("D", "B"), ("C", "D"), ("D", "C")):
            link = self.topology.link_between(up, dn)
            if not link:
                continue
            lid = link.get("id") or f"{up}_to_{dn}"
            up_dir = link.get("upstream_dir") or "East"
            slope = self.propagation.queue_slope(up, up_dir, 45.0, sim_t)
            snap = snapshots.get(up) or {}
            d = (snap.get("directions") or {}).get(up_dir) or {}
            out[lid] = {
                "link_id": lid,
                "edge": link.get("edge"),
                "upstream_node": up,
                "downstream_node": dn,
                "upstream_dir": up_dir,
                "queue_slope_veh_per_s": slope,
                "occupancy": float(d.get("occupancy_pct") or 0) / 100.0,
                "queue_length_m": float(d.get("queue_length_m") or 0),
                "sim_t": sim_t,
            }
        return out

    @staticmethod
    def _classify_traffic_state(
        pcu: float, q: float, busy: float, cong: float, oversat: float
    ) -> str:
        if pcu >= oversat or q > 70:
            return "OVERSATURATED"
        if pcu >= cong or q > 40:
            return "CONGESTED"
        if pcu >= busy or q > 15:
            return "BUSY"
        if pcu >= 5:
            return "STABLE"
        return "FREE_FLOW"

    @staticmethod
    def _worst(contexts: List[str], order: List[str]) -> str:
        if not contexts:
            return "FREE_FLOW"
        rank = {c: i for i, c in enumerate(order)}
        return max(contexts, key=lambda c: rank.get(c, 0))

    @staticmethod
    def _weighted(contexts: List[str], agg_rule: Dict[str, Any], order: List[str]) -> str:
        weights = (agg_rule.get("weighted") or {}).get("weights_by_context") or {}
        if not weights or not contexts:
            return NetworkContextCoordinator._worst(contexts, order)
        # Score by weight of each state's rank; pick state with max weighted presence
        scores: Dict[str, float] = {}
        for c in contexts:
            scores[c] = scores.get(c, 0.0) + float(weights.get(c, 1.0))
        rank = {c: i for i, c in enumerate(order)}
        return max(scores.keys(), key=lambda c: (scores[c], rank.get(c, 0)))
