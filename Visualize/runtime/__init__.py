"""runtime — contracts, state, command queue, network controller, run manifest."""
from runtime.layer_contracts import (
    CONTEXT_SCHEMA_VERSION,
    PHYSICAL_AGGREGATE_KEYS,
    PHYSICAL_RAW_KEYS,
    PHYSICAL_SNAPSHOT_SCHEMA_VERSION,
    SEMANTIC_LEGACY_KEYS,
    TRAFFIC_LOAD_STATES,
    DerivedPhenomena,
    DirectionLocalContext,
    DirectionPhysicalMetrics,
    EntityMappingInput,
    LocalContextState,
    NetworkContextState,
    PhysicalSnapshot,
)
from runtime.state import (
    SEVERITY,
    DirectionRuntimeState,
    NetworkRuntimeState,
    NodeRuntimeState,
)
from runtime.command_queue import Command, CommandQueue

# network_controller / run_manifest imported lazily to avoid cycles with context_engine

__all__ = [
    "PHYSICAL_SNAPSHOT_SCHEMA_VERSION",
    "CONTEXT_SCHEMA_VERSION",
    "TRAFFIC_LOAD_STATES",
    "PHYSICAL_RAW_KEYS",
    "PHYSICAL_AGGREGATE_KEYS",
    "SEMANTIC_LEGACY_KEYS",
    "DirectionPhysicalMetrics",
    "PhysicalSnapshot",
    "DerivedPhenomena",
    "DirectionLocalContext",
    "LocalContextState",
    "NetworkContextState",
    "EntityMappingInput",
    "SEVERITY",
    "DirectionRuntimeState",
    "NodeRuntimeState",
    "NetworkRuntimeState",
    "Command",
    "CommandQueue",
    "NetworkRuntimeController",
    "cfg_dt_fallback",
    "atomic_write_json",
    "build_and_write_manifest",
]


def __getattr__(name: str):
    if name in ("NetworkRuntimeController", "cfg_dt_fallback"):
        from runtime.network_controller import NetworkRuntimeController, cfg_dt_fallback

        return {
            "NetworkRuntimeController": NetworkRuntimeController,
            "cfg_dt_fallback": cfg_dt_fallback,
        }[name]
    if name in ("atomic_write_json", "build_and_write_manifest"):
        from runtime.run_manifest import atomic_write_json, build_and_write_manifest

        return {
            "atomic_write_json": atomic_write_json,
            "build_and_write_manifest": build_and_write_manifest,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
