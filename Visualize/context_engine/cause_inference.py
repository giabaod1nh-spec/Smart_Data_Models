"""CauseInferenceEngine — probable_causes with evidence (interpretive only)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from configuration.model_params import get_registry
from runtime.state import NodeRuntimeState

from .propagation_analyzer import PropagationAnalyzer
from .topology_resolver import TopologyResolver


class CauseInferenceEngine:
    def __init__(self, topology: TopologyResolver, propagation: PropagationAnalyzer):
        self.topology = topology
        self.propagation = propagation

    def infer(
        self,
        *,
        sim_t: float,
        overlays: List[Dict[str, Any]],
        nodes: Dict[str, NodeRuntimeState],
        snapshots: Optional[Dict[str, dict]] = None,
    ) -> None:
        cfg_c = get_registry().causal_inference()
        window = float(cfg_c.get("causal_observation_window_s", 45))
        min_delay = float(cfg_c.get("propagation_min_delay_s", 5))
        max_delay = float(cfg_c.get("propagation_max_delay_s", 120))
        conf = float(cfg_c.get("default_cause_confidence", 1.0))
        snapshots = snapshots or {}

        for node in nodes.values():
            node.probable_causes = []

        pairs = (
            ("A", "B"),
            ("B", "A"),
            ("A", "C"),
            ("C", "A"),
            ("B", "D"),
            ("D", "B"),
            ("C", "D"),
            ("D", "C"),
        )
        for ov in overlays:
            ov_type = ov.get("type")
            if ov_type not in ("accident", "blocked_intersection", "downstream_restriction"):
                continue
            src = ov.get("intersection_id")
            created = float(ov.get("created_at_s") or 0)
            if sim_t - created < min_delay or sim_t - created > max_delay:
                continue
            cause_type = (
                "DOWNSTREAM_RESTRICTION"
                if ov_type == "downstream_restriction"
                else "DOWNSTREAM_BLOCKAGE"
            )
            for up, dn in pairs:
                if dn != src:
                    continue
                link = self.topology.link_between(up, dn)
                if not link:
                    continue
                up_dir = (
                    link.get("upstream_dir")
                    or (link.get("upstream") or {}).get("direction")
                    or "East"
                )
                up_node = nodes.get(up)
                if not up_node:
                    continue
                slope = self.propagation.queue_slope(up, up_dir, window, sim_t)
                occ = self.propagation.mean_occupancy(up, up_dir, window, sim_t)
                drop = self.propagation.discharge_proxy_drop(up, up_dir, window, sim_t)
                dstate = up_node.directions.get(up_dir)
                congested = dstate and dstate.traffic_state in ("CONGESTED", "OVERSATURATED")
                phenomena = up_node.derived_phenomena
                if slope > 0.05 or congested or (phenomena and phenomena.spillback_active):
                    evidence = {
                        "queue_slope_veh_per_s": round(slope, 4),
                        "occupancy": round(occ, 4),
                        "observation_window_s": window,
                    }
                    if drop is not None:
                        evidence["discharge_drop_ratio"] = round(drop, 4)
                    up_snap = snapshots.get(up) or {}
                    up_d = (up_snap.get("directions") or {}).get(up_dir) or {}
                    if up_d.get("queue_length_m") is not None:
                        evidence["queue_length_m"] = float(up_d["queue_length_m"])
                    up_node.probable_causes.append(
                        {
                            "type": cause_type,
                            "source_node": src,
                            "affected_node": up,
                            "target_node": up,  # compat
                            "link_id": link.get("id") or link.get("edge"),
                            "link": link.get("id") or link.get("edge"),
                            "confidence": conf,
                            "inference_method": "RULE_BASED",
                            "window_s": window,
                            "observed_from_s": created,
                            "observed_to_s": sim_t,
                            "evidence": evidence,
                        }
                    )
