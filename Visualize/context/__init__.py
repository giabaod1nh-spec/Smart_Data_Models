"""Compatibility package — re-exports context_engine (monorepo-safe name preferred)."""
from context_engine import (
    CauseInferenceEngine,
    NetworkContextCoordinator,
    PropagationAnalyzer,
    TopologyResolver,
)

__all__ = [
    "TopologyResolver",
    "PropagationAnalyzer",
    "CauseInferenceEngine",
    "NetworkContextCoordinator",
]
