"""configuration — ParameterRegistry, model_params, demand profiles, config facade."""

__all__ = [
    "get_registry",
    "reset_registry_for_tests",
    "ParameterRegistry",
]


def __getattr__(name: str):
    if name in ("get_registry", "reset_registry_for_tests", "ParameterRegistry"):
        from configuration.parameter_registry import (
            ParameterRegistry,
            get_registry,
            reset_registry_for_tests,
        )

        return {
            "get_registry": get_registry,
            "reset_registry_for_tests": reset_registry_for_tests,
            "ParameterRegistry": ParameterRegistry,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
