"""
model_params.py — Thin facade over ParameterRegistry.

Application code should prefer get_registry(). config.py may re-export
read-only views for backward compatibility.
"""
from __future__ import annotations

from types import MappingProxyType
from typing import Dict, Mapping, Optional

from configuration.parameter_registry import ParameterRegistry, get_registry, reset_registry_for_tests

__all__ = [
    "get_registry",
    "reset_registry_for_tests",
    "ParameterRegistry",
    "pcu_factors",
    "get_pcu",
    "profile_id",
    "config_hash",
    "schema_version",
]


def pcu_factors() -> Mapping[str, float]:
    return get_registry().pcu_factors()


def get_pcu(vtype: str) -> float:
    return get_registry().get_pcu(vtype)


def profile_id() -> str:
    return get_registry().profile_id


def config_hash() -> str:
    return get_registry().config_hash


def schema_version() -> str:
    return get_registry().schema_version


def as_pcu_dict() -> Dict[str, float]:
    """Defensive plain copy for callers that need a dict (not for mutation)."""
    return dict(get_registry().pcu_factors())
