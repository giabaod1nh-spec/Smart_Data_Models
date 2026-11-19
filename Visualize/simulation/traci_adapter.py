"""Connection owner for the SUMO TraCI runtime.

Domain access is forwarded for the existing signal, demand and observation
components while those interfaces are narrowed. Only this module starts,
steps and closes the underlying TraCI connection.
"""
from __future__ import annotations

from typing import Any, Optional, Sequence


class TraCIAdapter:
    def __init__(self, module: Optional[Any] = None) -> None:
        if module is None:
            import traci  # type: ignore

            module = traci
        self._module = module
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self, command: Sequence[str]) -> None:
        if self._connected:
            raise RuntimeError("TraCI connection is already active")
        try:
            self._module.start(list(command))
        except Exception:
            # TraCI may have opened a socket before start() reports failure.
            try:
                self._module.close()
            except Exception:
                pass
            raise
        self._connected = True

    def simulation_step(self) -> None:
        if not self._connected:
            raise RuntimeError("TraCI connection is not active")
        self._module.simulationStep()

    def close(self) -> None:
        if not self._connected:
            return
        try:
            self._module.close()
        finally:
            self._connected = False

    def __getattr__(self, name: str) -> Any:
        # Compatibility port: existing components receive this adapter, never
        # the module owning the socket. New code should use explicit methods.
        if name in {"start", "simulationStep"}:
            raise AttributeError(f"Use TraCIAdapter lifecycle method for {name}")
        return getattr(self._module, name)
