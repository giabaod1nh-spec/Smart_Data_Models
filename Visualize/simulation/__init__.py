"""simulation — SUMO backend, signal controller, scenario manager."""

__all__ = ["SumoBackend", "SumoSignalController", "SumoScenarioManager"]


def __getattr__(name: str):
    if name == "SumoBackend":
        from simulation.backend import SumoBackend

        return SumoBackend
    if name == "SumoSignalController":
        from simulation.signal_controller import SumoSignalController

        return SumoSignalController
    if name == "SumoScenarioManager":
        from simulation.scenario_manager import SumoScenarioManager

        return SumoScenarioManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
