"""Runtime network state holders — Phase 2 multi-dimensional context."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from runtime.layer_contracts import (
    CONTEXT_SCHEMA_VERSION,
    TRAFFIC_LOAD_STATES,
    DerivedPhenomena,
    DirectionLocalContext,
    LocalContextState,
    NetworkContextState,
)

# Traffic-load severity only (no spillback/box in this axis)
SEVERITY = list(TRAFFIC_LOAD_STATES)

# Compat alias for older imports
DirectionRuntimeState = DirectionLocalContext


@dataclass
class NodeRuntimeState:
    """Per-node runtime view (bridges LocalContextState + causes)."""

    node_id: str
    directions: Dict[str, DirectionLocalContext] = field(default_factory=dict)
    aggregate_traffic_state: str = "FREE_FLOW"
    aggregate_context: str = "FREE_FLOW"  # compat alias
    derived_phenomena: DerivedPhenomena = field(default_factory=DerivedPhenomena)
    operational_state: Dict[str, bool] = field(
        default_factory=lambda: {
            "incident_active": False,
            "emergency_preemption_active": False,
            "downstream_restriction_active": False,
        }
    )
    active_overlay_ids: List[str] = field(default_factory=list)
    probable_causes: List[Dict[str, Any]] = field(default_factory=list)

    def sync_aliases(self) -> None:
        self.aggregate_context = self.aggregate_traffic_state

    def to_local_context(self) -> LocalContextState:
        self.sync_aliases()
        return LocalContextState(
            intersection_id=self.node_id,
            direction_states=dict(self.directions),
            aggregate_traffic_state=self.aggregate_traffic_state,
            derived_phenomena=DerivedPhenomena(**self.derived_phenomena.to_dict()),
            operational_state=dict(self.operational_state),
            active_overlay_ids=list(self.active_overlay_ids),
        )

    def to_dict(self) -> Dict[str, Any]:
        self.sync_aliases()
        return {
            "intersection_id": self.node_id,
            "directions": {
                k: {
                    "context": v.traffic_state,
                    "traffic_state": v.traffic_state,
                    "queue_length_m": v.queue_length_m,
                    "pcu": v.pcu,
                }
                for k, v in self.directions.items()
            },
            "aggregate_traffic_state": self.aggregate_traffic_state,
            "aggregate_context": self.aggregate_context,
            "derived_phenomena": self.derived_phenomena.to_dict(),
            "operational_state": dict(self.operational_state),
            "active_overlay_ids": list(self.active_overlay_ids),
            "probable_causes": list(self.probable_causes),
            "context_schema_version": CONTEXT_SCHEMA_VERSION,
        }


@dataclass
class NetworkRuntimeState:
    demand_profile_id: str = "normal"
    control_mode: str = "FIXED"
    nodes: Dict[str, NodeRuntimeState] = field(default_factory=dict)
    overlays: List[Dict[str, Any]] = field(default_factory=list)
    source_stats: Dict[str, Any] = field(default_factory=dict)
    insertion_stats: Dict[str, Any] = field(default_factory=dict)
    link_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    network_summary: Dict[str, Any] = field(default_factory=dict)
    last_tick_phases: List[str] = field(default_factory=list)
    last_context_sim_t: float = -1.0

    def ensure_nodes(self, ids: List[str]) -> None:
        for n in ids:
            if n not in self.nodes:
                self.nodes[n] = NodeRuntimeState(node_id=n)

    def to_network_context(self) -> NetworkContextState:
        causes: List[Dict[str, Any]] = []
        for n in self.nodes.values():
            causes.extend(n.probable_causes)
        return NetworkContextState(
            node_contexts={k: v.to_local_context() for k, v in self.nodes.items()},
            link_states=dict(self.link_states),
            probable_causes=causes,
            network_summary=dict(self.network_summary),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "demand_profile_id": self.demand_profile_id,
            "control_mode": self.control_mode,
            "overlays": list(self.overlays),
            "nodes": {k: v.to_dict() for k, v in self.nodes.items()},
            "source_stats": dict(self.source_stats),
            "insertion_stats": dict(self.insertion_stats),
            "link_states": dict(self.link_states),
            "network_summary": dict(self.network_summary),
            "last_tick_phases": list(self.last_tick_phases),
            "last_context_sim_t": self.last_context_sim_t,
            "context_schema_version": CONTEXT_SCHEMA_VERSION,
        }
