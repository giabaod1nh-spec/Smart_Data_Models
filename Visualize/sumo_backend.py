"""
SumoBackend — TraCI facade with engine-like interface for NGSI publish path.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, Optional

import config as cfg
from sumo_scenario_manager import SumoScenarioManager
from sumo_signal_controller import SumoSignalController
from sumo_snapshot_provider import SumoSnapshotProvider

log = logging.getLogger(__name__)


class SumoBackend:
    """
    Minimal interface aligned with CityNetworkEngine usage in main/control_api:

      start / step / get_snapshot / force_phase / set_green_duration /
      set_scenario / get_stats / stop
    """

    def __init__(
        self,
        sumo_config: Optional[os.PathLike] = None,
        use_gui: Optional[bool] = None,
        publish_node: Optional[str] = None,
    ):
        self.sumo_config = cfg.SUMO_CONFIG if sumo_config is None else sumo_config
        self.use_gui = cfg.SUMO_GUI if use_gui is None else use_gui
        self.publish_node = publish_node or cfg.PUBLISH_NODE
        if self.publish_node not in cfg.NODE_TO_TLS:
            raise ValueError(
                f"Unknown publish node '{self.publish_node}'. "
                f"Known: {list(cfg.NODE_TO_TLS)}"
            )
        self.tls_id = cfg.NODE_TO_TLS[self.publish_node]

        self._traci = None
        self._started = False
        self.signal = SumoSignalController(self.tls_id)
        self.scenario = SumoScenarioManager(self.tls_id)
        self.snapshot_provider = SumoSnapshotProvider(self.tls_id, self.publish_node)

        # Compatibility attributes for control_api-style callers
        self.current_scenario = "normal"
        self.per_node_scenario: Dict[str, str] = {}
        self.trip_records: list = []
        self.last_spawn_count = 0
        self.simulation_time_sec = 0.0

    # ── lifecycle ───────────────────────────────────────────────────

    def start(self) -> None:
        cfg.ensure_sumo_tools_on_path()
        if not os.environ.get("SUMO_HOME", "").strip():
            log.warning(
                "SUMO_HOME is not set. TraCI import may fail. "
                "Example: $env:SUMO_HOME='C:\\Program Files (x86)\\Eclipse\\Sumo'"
            )
        try:
            import traci
        except ImportError as e:
            raise RuntimeError(
                "Cannot import traci. Set SUMO_HOME to your SUMO install root "
                "(folder that contains bin\\ and tools\\), then add "
                "%SUMO_HOME%\\bin to PATH. Detail: " + str(e)
            ) from e

        binary = cfg.resolve_sumo_binary(self.use_gui)
        config_path = str(self.sumo_config)
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"SUMO config not found: {config_path}")

        sumo_cmd = [
            binary,
            "-c", config_path,
            "--start",
        ]
        # Headless: quit when sim ends. GUI: keep window until user closes it.
        if not self.use_gui:
            sumo_cmd.append("--quit-on-end")
        # Optional step-length override (keeps sumocfg default if not set specially)
        if os.getenv("SUMO_STEP_LENGTH"):
            sumo_cmd.extend(["--step-length", str(cfg.SUMO_STEP_LENGTH)])

        log.info("Starting SUMO: %s", " ".join(sumo_cmd))
        traci.start(sumo_cmd)
        self._traci = traci
        self._started = True
        self.simulation_time_sec = float(traci.simulation.getTime())
        log.info(
            "SUMO started. publish_node=%s tls=%s sim_t=%.2f",
            self.publish_node, self.tls_id, self.simulation_time_sec,
        )

    def step(self) -> bool:
        """
        Advance one simulation step.
        Returns False when simulation has ended (no more expected vehicles / end time).
        """
        self._require_started()
        traci = self._traci
        traci.simulationStep()
        self.signal.tick_pending(traci)
        self.simulation_time_sec = float(traci.simulation.getTime())
        # End conditions: sumocfg end time reached or no more departures & empty
        try:
            if traci.simulation.getMinExpectedNumber() <= 0:
                # Allow a short drain; if time past typical end, stop
                if self.simulation_time_sec >= 3600.0:
                    return False
        except Exception:
            pass
        return True

    def stop(self) -> None:
        if self._traci is not None:
            try:
                self._traci.close()
                log.info("TraCI closed.")
            except Exception as e:
                log.warning("traci.close() error: %s", e)
            finally:
                self._traci = None
                self._started = False

    # ── observation / control ───────────────────────────────────────

    def get_snapshot(self, node_id: str = "A") -> dict:
        self._require_started()
        if node_id != self.publish_node:
            # v1 only supports the configured publish node
            if node_id not in cfg.NODE_TO_TLS:
                raise KeyError(f"Unknown node_id '{node_id}'")
            if cfg.NODE_TO_TLS[node_id] != self.tls_id:
                raise NotImplementedError(
                    f"v1 only publishes {self.publish_node} ({self.tls_id}); "
                    f"requested {node_id}"
                )
        snap = self.snapshot_provider.build_snapshot(
            self._traci, self.signal, self.scenario,
        )
        snap["node_id"] = self.publish_node
        return snap

    def force_phase(self, node_id: str, phase: str) -> None:
        self._require_started()
        self._assert_node(node_id)
        self.signal.force_phase(self._traci, phase)

    def set_green_duration(self, node_id: str, seconds: int) -> None:
        self._require_started()
        self._assert_node(node_id)
        self.signal.set_green_duration(self._traci, seconds)

    def set_scenario(
        self,
        scenario: str,
        target_intersection: Optional[str] = None,
        target_direction: Optional[str] = None,
    ) -> None:
        """Signature compatible with CityNetworkEngine.set_scenario."""
        self._require_started()
        node = target_intersection or self.publish_node
        self._assert_node(node)
        self.scenario.set_scenario(self._traci, scenario, target_direction)
        self.current_scenario = scenario

    def get_stats(self) -> dict:
        self._require_started()
        traci = self._traci
        try:
            active = len(traci.vehicle.getIDList())
        except Exception:
            active = 0
        try:
            arrived = int(traci.simulation.getArrivedNumber())
        except Exception:
            arrived = 0
        return {
            "total_active_vehicles": active,
            "total_exited_network": arrived,  # step-local; see TODO
            "last_spawn_count": self.last_spawn_count,
            "simulation_time_sec": self.simulation_time_sec,
            "per_node_scenario": dict(self.per_node_scenario),
            "scenario": self.current_scenario,
            "publish_node": self.publish_node,
            "tls_id": self.tls_id,
        }

    # Compatibility helpers used by control_api patterns
    def count_total_vehicles(self) -> int:
        if not self._started:
            return 0
        return len(self._traci.vehicle.getIDList())

    def count_exited_network(self) -> int:
        # TODO(v1): accumulate arrived over time for true total
        if not self._started:
            return 0
        try:
            return int(self._traci.simulation.getArrivedNumber())
        except Exception:
            return 0

    @property
    def intersections(self) -> dict:
        """Shim so control_api-style phase_controller access can be adapted later."""
        backend = self

        class _PhaseProxy:
            def force_phase(self, phase: str) -> None:
                backend.force_phase(backend.publish_node, phase)

            def set_green_duration(self, seconds: int) -> None:
                backend.set_green_duration(backend.publish_node, seconds)

        class _Ix:
            phase_controller = _PhaseProxy()

        return {self.publish_node: _Ix()}

    # ── internals ───────────────────────────────────────────────────

    def _require_started(self) -> None:
        if not self._started or self._traci is None:
            raise RuntimeError("SumoBackend is not started. Call start() first.")

    def _assert_node(self, node_id: str) -> None:
        if node_id != self.publish_node:
            raise NotImplementedError(
                f"v1 only controls publish node {self.publish_node}, got {node_id}"
            )
