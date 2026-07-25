"""
layer_contracts.py — Phase 2 DTO contracts and field ownership.

Three field kinds:
  RAW_OBSERVATION — TraCI / detector readings
  PHYSICAL_AGGREGATE — deterministic physical totals (PCU, queue_m, …)
  SEMANTIC_CONTEXT — traffic_state / phenomena / causes (not PhysicalSnapshot)

Schema versions travel in EntityMappingInput.provenance.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

PHYSICAL_SNAPSHOT_SCHEMA_VERSION = "2.0.0"
CONTEXT_SCHEMA_VERSION = "2.0.0"

TRAFFIC_LOAD_STATES = (
    "FREE_FLOW",
    "STABLE",
    "BUSY",
    "CONGESTED",
    "OVERSATURATED",
)

# PhysicalSnapshot: allowed semantic-free keys (documentation / tests)
PHYSICAL_RAW_KEYS = frozenset(
    {
        "vehicle_count",
        "halting_count",
        "left_count",
        "straight_count",
        "right_count",
        "occupancy_pct",
        "phase",
        "colors",
        "simulation_time_sec",
    }
)
PHYSICAL_AGGREGATE_KEYS = frozenset(
    {
        "pcu_equivalent",
        "queue_length_m",
        "queue_length_vehicles",
        "queue_by_movement",
        "average_speed_kmh",
        "estimated_density_veh_per_km",
        "theoretical_speed_kmh",
        "arrival_rate_pcu_per_sec",
        "spillback_pressure",
        "downstream_edge_full",
        "vehicles_blocked_at_entry",
        "yellow_commitment_count",
        "moto_front_pct",
        "vehicle_class_composition",
        "approach_length_m",
        "full_link_halting_count",
        "full_link_jam_length_m",
        "storage_utilization_lane",
        "storage_utilization_pcu",
        "nominal_storage_pcu_geometric",
    }
)
# Strategy C dual-write may still expose these on the flat snapshot dict for NGSI;
# ownership is SEMANTIC — derived by context layer, not PhysicalSnapshot contract.
SEMANTIC_LEGACY_KEYS = frozenset(
    {
        "density",  # load-class label (LIGHT/MODERATE/…)
        "waiting_reason_counts",
        "dominant_waiting_reason",
        "derived_traffic_state",
        "derived_phenomena",
        "operational_state",
        "probable_causes",
        "direction_contexts",
        "derived_aggregate_context",
        "aggregate_traffic_state",
    }
)


@dataclass
class DirectionPhysicalMetrics:
    vehicle_count: int = 0
    pcu_equivalent: float = 0.0
    average_speed_kmh: float = 0.0
    queue_length_m: float = 0.0
    queue_length_vehicles: float = 0.0
    occupancy_pct: float = 0.0
    waiting_vehicle_count: int = 0
    left_count: int = 0
    straight_count: int = 0
    right_count: int = 0
    arrival_rate_pcu_per_sec: float = 0.0
    estimated_density_veh_per_km: float = 0.0
    theoretical_speed_kmh: float = 0.0
    approach_length_m: float = 0.0
    full_link_halting_count: int = 0
    full_link_jam_length_m: float = 0.0
    storage_utilization_lane: float = 0.0
    storage_utilization_pcu: float = 0.0
    nominal_storage_pcu_geometric: float = 0.0
    # Strategy C dual-write (semantic ownership; kept for entity_generator)
    density: Optional[str] = None
    waiting_reason_counts: Optional[Dict[str, int]] = None
    dominant_waiting_reason: Optional[str] = None
    moto_front_pct: float = 0.0
    vehicle_class_composition: Optional[Dict[str, float]] = None
    queue_by_movement: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


@dataclass
class PhysicalSnapshot:
    """Raw observations + physical aggregates for one intersection at one time."""

    intersection_id: str
    timestamp_s: float
    directions: Dict[str, DirectionPhysicalMetrics] = field(default_factory=dict)
    signal_state: Dict[str, Any] = field(default_factory=dict)
    link_metrics: Dict[str, Any] = field(default_factory=dict)
    detector_metrics: Dict[str, Any] = field(default_factory=dict)
    spillback_pressure: float = 0.0
    downstream_edge_full: bool = False
    vehicles_blocked_at_entry: int = 0
    yellow_commitment_count: int = 0
    preemption_active: bool = False
    scenario: str = "normal"
    blocked_direction: Optional[str] = None
    incidents: List[dict] = field(default_factory=list)
    simulation_run_id: Optional[str] = None
    schema_version: str = PHYSICAL_SNAPSHOT_SCHEMA_VERSION
    # Provenance re-exports (compat)
    pcu_profile_id: Optional[str] = None
    config_hash: Optional[str] = None
    model_schema_version: Optional[str] = None

    def to_legacy_dict(self) -> Dict[str, Any]:
        """Flat snapshot dict compatible with existing API / entity_generator."""
        dirs = {k: v.to_dict() for k, v in self.directions.items()}
        out: Dict[str, Any] = {
            "node_id": self.intersection_id,
            "directions": dirs,
            "simulation_time_sec": round(self.timestamp_s, 2),
            "spillback_pressure": self.spillback_pressure,
            "downstream_edge_full": self.downstream_edge_full,
            "spillback_detected": self.downstream_edge_full
            or self.spillback_pressure >= 0.7,
            "intersection_box_blocked": self.downstream_edge_full
            or self.spillback_pressure >= 0.7,
            "vehicles_blocked_at_entry": self.vehicles_blocked_at_entry,
            "yellow_commitment_count": self.yellow_commitment_count,
            "preemption_active": self.preemption_active,
            "scenario": self.scenario,
            "blocked_direction": self.blocked_direction,
            "incidents": list(self.incidents),
            "simulation_run_id": self.simulation_run_id,
            "physical_snapshot_schema_version": self.schema_version,
            "schema_version": self.model_schema_version,
            "pcu_profile_id": self.pcu_profile_id,
            "config_hash": self.config_hash,
            "link_metrics": dict(self.link_metrics),
            "detector_metrics": dict(self.detector_metrics),
        }
        out.update(self.signal_state)
        return out


@dataclass
class DerivedPhenomena:
    spillback_risk: bool = False
    spillback_active: bool = False
    box_blocked: bool = False

    def to_dict(self) -> Dict[str, bool]:
        return asdict(self)


@dataclass
class DirectionLocalContext:
    traffic_state: str = "FREE_FLOW"
    queue_length_m: float = 0.0
    pcu: float = 0.0
    occupancy_pct: float = 0.0
    mean_speed_kmh: float = 0.0

    @property
    def context(self) -> str:
        """Compat alias for traffic_state."""
        return self.traffic_state

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LocalContextState:
    intersection_id: str
    direction_states: Dict[str, DirectionLocalContext] = field(default_factory=dict)
    aggregate_traffic_state: str = "FREE_FLOW"
    derived_phenomena: DerivedPhenomena = field(default_factory=DerivedPhenomena)
    operational_state: Dict[str, bool] = field(
        default_factory=lambda: {
            "incident_active": False,
            "emergency_preemption_active": False,
            "downstream_restriction_active": False,
        }
    )
    active_overlay_ids: List[str] = field(default_factory=list)
    schema_version: str = CONTEXT_SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intersection_id": self.intersection_id,
            "direction_states": {k: v.to_dict() for k, v in self.direction_states.items()},
            "aggregate_traffic_state": self.aggregate_traffic_state,
            # Compat aliases
            "aggregate_context": self.aggregate_traffic_state,
            "derived_phenomena": self.derived_phenomena.to_dict(),
            "operational_state": dict(self.operational_state),
            "active_overlay_ids": list(self.active_overlay_ids),
            "directions": {
                k: {
                    "context": v.traffic_state,
                    "traffic_state": v.traffic_state,
                    "queue_length_m": v.queue_length_m,
                    "pcu": v.pcu,
                }
                for k, v in self.direction_states.items()
            },
            "context_schema_version": self.schema_version,
        }


@dataclass
class NetworkContextState:
    node_contexts: Dict[str, LocalContextState] = field(default_factory=dict)
    link_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    probable_causes: List[Dict[str, Any]] = field(default_factory=list)
    network_summary: Dict[str, Any] = field(default_factory=dict)
    schema_version: str = CONTEXT_SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_contexts": {k: v.to_dict() for k, v in self.node_contexts.items()},
            "link_states": dict(self.link_states),
            "probable_causes": list(self.probable_causes),
            "network_summary": dict(self.network_summary),
            "context_schema_version": self.schema_version,
        }


@dataclass
class EntityMappingInput:
    physical_snapshot: PhysicalSnapshot
    local_context: Optional[LocalContextState] = None
    network_context: Optional[NetworkContextState] = None
    provenance: Dict[str, Any] = field(default_factory=dict)

    def to_legacy_snapshot(self) -> Dict[str, Any]:
        """Merge physical + local/network context for Strategy C entity mapping."""
        snap = self.physical_snapshot.to_legacy_dict()
        if self.local_context:
            lc = self.local_context
            snap["derived_traffic_state"] = lc.aggregate_traffic_state
            snap["aggregate_traffic_state"] = lc.aggregate_traffic_state
            snap["derived_aggregate_context"] = lc.aggregate_traffic_state
            snap["derived_phenomena"] = lc.derived_phenomena.to_dict()
            snap["operational_state"] = dict(lc.operational_state)
            snap["direction_contexts"] = {
                k: v.traffic_state for k, v in lc.direction_states.items()
            }
            snap["direction_states"] = {
                k: v.to_dict() for k, v in lc.direction_states.items()
            }
        if self.network_context:
            # Node-scoped causes
            nid = self.physical_snapshot.intersection_id
            node_causes = [
                c
                for c in self.network_context.probable_causes
                if c.get("affected_node") == nid or c.get("target_node") == nid
            ]
            snap["probable_causes"] = node_causes
            snap["network_summary"] = dict(self.network_context.network_summary)
        snap["provenance"] = dict(self.provenance)
        snap["context_schema_version"] = CONTEXT_SCHEMA_VERSION
        return snap
