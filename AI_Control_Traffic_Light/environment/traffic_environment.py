"""MDP wrapper around SumoBackend for RL training."""
from __future__ import annotations

import logging
from typing import List, Optional

from AI_Control_Traffic_Light.agents.agent_manager import AgentManager
from AI_Control_Traffic_Light.config.settings import RLConfig
from AI_Control_Traffic_Light.environment.action_executor import ActionExecutor
from AI_Control_Traffic_Light.utils.path_setup import ensure_paths

log = logging.getLogger(__name__)

ALL_NODES = ["A", "B", "C", "D"]


class TrafficEnvironment:
    """Runs decision cycles on existing SumoBackend without duplicate SUMO."""

    def __init__(
        self,
        config: RLConfig,
        *,
        mode: str = "cooperative",
        use_gui: bool = False,
    ) -> None:
        ensure_paths()
        self.config = config
        self.mode = mode
        cooperative = mode == "cooperative"
        self.manager = AgentManager(
            config,
            ALL_NODES,
            cooperative=cooperative,
            trainable=mode != "fixed",
        )
        self.action_exec = ActionExecutor()
        self.backend = None
        self.use_gui = use_gui
        self.steps_per_decision = 1

    def reset(self, scenario: str = "normal") -> None:
        ensure_paths()
        import configuration.config as cfg
        from simulation.backend import SumoBackend

        if self.backend is not None:
            try:
                self.backend.stop()
            except Exception:
                pass

        self.backend = SumoBackend(
            use_gui=self.use_gui,
            publish_nodes=ALL_NODES,
        )
        self.backend.start()
        self.steps_per_decision = max(
            1, int(self.config.decision_interval_sec / cfg.SUMO_STEP_LENGTH)
        )
        self._apply_scenario(scenario)
        if self.mode != "fixed":
            self.action_exec.enter_adaptive_mode(self.backend.signals, self.backend._traci)
        self.manager.reset_episode()

    def _apply_scenario(self, scenario: str) -> None:
        """Network-wide demand profiles apply globally; per-node scenarios
        (rain/incident) apply at every intersection — backend.set_scenario
        requires an explicit target_intersection."""
        import configuration.config as cfg

        profile = cfg.normalize_demand_profile_id(cfg.normalize_scenario_id(scenario))
        if profile in ("normal", "peak", "oversaturated"):
            self.backend.set_demand_profile(profile)
            return
        for nid in ALL_NODES:
            self.backend.set_scenario(scenario, target_intersection=nid)

    def step_once(self, *, train: bool = False) -> bool:
        """One decision cycle: decide+act, then advance SUMO. Returns False when episode ends."""
        import configuration.config as cfg

        backend = self.backend
        assert backend is not None

        if self.mode != "fixed":
            snaps, global_r, local_rs, tp = self.manager.decision_step(
                backend, train=train, greedy=not train
            )
            self._last_step = (snaps, global_r, local_rs, tp)
        else:
            self._last_step = ({}, 0.0, {}, 0.0)

        for _ in range(self.steps_per_decision):
            if not backend.step():
                return False
            if backend.simulation_time_sec >= cfg.SIM_END_SEC:
                return False

        return backend.simulation_time_sec < cfg.SIM_END_SEC

    def run_episode(self, episode: int, scenario: str, *, train: bool = True) -> "EpisodeMetrics":
        from AI_Control_Traffic_Light.utils.metrics import MetricsCollector, save_episode_metrics

        self.reset(scenario)
        collector = MetricsCollector()
        self._last_step = ({}, 0.0, {}, 0.0)

        while self.step_once(train=train and self.mode != "fixed"):
            snaps, global_r, local_rs, tp = self._last_step
            if snaps:
                collector.record_step(snaps, global_r, local_rs, tp)

        metrics = collector.finalize(episode)
        save_episode_metrics(metrics, self.config.models_dir / "logs")
        return metrics

    def close(self) -> None:
        if self.backend is not None:
            try:
                if self.mode != "fixed":
                    self.action_exec.exit_adaptive_mode(self.backend.signals, self.backend._traci)
                self.backend.stop()
            except Exception as e:
                log.debug("env close: %s", e)
            self.backend = None
