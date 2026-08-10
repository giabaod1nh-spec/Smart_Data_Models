"""Runtime inference controller for ADAPTIVE mode in live simulation."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from AI_Control_Traffic_Light.agents.agent_manager import AgentManager
from AI_Control_Traffic_Light.config.settings import RLConfig, load_rl_config
from AI_Control_Traffic_Light.environment.action_executor import ActionExecutor

log = logging.getLogger(__name__)

_controller: Optional["MultiAgentController"] = None


class MultiAgentController:
    """Lazy-loaded singleton for SumoBackend ADAPTIVE mode."""

    def __init__(self, config: Optional[RLConfig] = None) -> None:
        self.config = config or load_rl_config()
        self.manager = AgentManager(self.config, cooperative=True, trainable=False)
        self.action_exec = ActionExecutor()
        self._last_decision_t = -1e9
        self._enabled = False
        self._models_loaded = False

    def enable(self, backend) -> None:
        if not self._models_loaded:
            self.manager.load_models()
            self._models_loaded = True
        self.action_exec.enter_adaptive_mode(backend.signals, backend._traci)
        self._enabled = True
        self._last_decision_t = backend.simulation_time_sec
        log.info("MultiAgentController ADAPTIVE enabled")

    def disable(self, backend) -> None:
        self.action_exec.exit_adaptive_mode(backend.signals, backend._traci)
        self._enabled = False
        log.info("MultiAgentController ADAPTIVE disabled")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def maybe_tick(self, backend) -> None:
        if not self._enabled or backend is None:
            return
        dt = backend.simulation_time_sec - self._last_decision_t
        if dt < self.config.decision_interval_sec:
            return
        self._last_decision_t = backend.simulation_time_sec
        try:
            self.manager.decision_step(backend, train=False, greedy=True)
        except Exception as e:
            log.warning("RL decision tick failed: %s", e)

    def get_agent_status(self) -> List[dict]:
        return self.manager.get_agent_status()

    def get_global_metrics(self) -> dict:
        return self.manager.get_global_metrics()


def get_multi_agent_controller() -> MultiAgentController:
    global _controller
    if _controller is None:
        _controller = MultiAgentController()
    return _controller
