"""Synchronized multi-agent decision loop."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from AI_Control_Traffic_Light.agents.dqn_agent import DQNAgent
from AI_Control_Traffic_Light.communication.communication_bus import CommunicationBus
from AI_Control_Traffic_Light.config.settings import RLConfig
from AI_Control_Traffic_Light.environment.action_executor import ActionExecutor
from AI_Control_Traffic_Light.environment.reward_calculator import RewardCalculator
from AI_Control_Traffic_Light.environment.state_builder import StateBuilder
from AI_Control_Traffic_Light.environment.topology import ACTION_NAMES, neighbors_of

log = logging.getLogger(__name__)


class AgentManager:
    """Orchestrates 4 DQN agents with cooperative communication."""

    def __init__(
        self,
        config: RLConfig,
        node_ids: Optional[List[str]] = None,
        *,
        cooperative: bool = True,
        trainable: bool = True,
    ) -> None:
        self.config = config
        self.node_ids = list(node_ids or config.agent_nodes)
        self.cooperative = cooperative
        self.bus = CommunicationBus()
        self.state_builder = StateBuilder(config)
        self.reward_calc = RewardCalculator(config)
        self.action_exec = ActionExecutor()
        self.agents: Dict[str, DQNAgent] = {
            nid: DQNAgent(nid, config, trainable=trainable) for nid in self.node_ids
        }
        self._prev_obs: Dict[str, np.ndarray] = {}
        self._prev_actions: Dict[str, int] = {nid: 0 for nid in self.node_ids}
        self._last_status: Dict[str, dict] = {}
        self._global_metrics: dict = {}
        self._last_global_reward = 0.0
        self._last_local_rewards: Dict[str, float] = {}

    def reset_episode(self) -> None:
        self.bus.clear_messages()
        self.reward_calc.reset()
        self._prev_obs.clear()
        self._prev_actions = {nid: 0 for nid in self.node_ids}
        self._last_status.clear()
        self._global_metrics = {}

    def load_models(self, models_dir: Optional[Any] = None) -> None:
        from pathlib import Path

        base = Path(models_dir) if models_dir else self.config.models_dir
        for nid, agent in self.agents.items():
            path = base / f"agent_{nid}.keras"
            if path.is_file():
                agent.load(path)
                log.info("Loaded model for agent %s from %s", nid, path)

    def save_models(self, models_dir: Optional[Any] = None) -> None:
        from pathlib import Path

        base = Path(models_dir) if models_dir else self.config.models_dir
        for nid, agent in self.agents.items():
            agent.save(base / f"agent_{nid}.keras")

    def update_targets(self) -> None:
        for agent in self.agents.values():
            agent.update_target()

    def decay_epsilon_all(self) -> None:
        for agent in self.agents.values():
            agent.decay_epsilon()

    def _collect_snapshots(self, backend) -> Dict[str, dict]:
        snaps: Dict[str, dict] = {}
        for nid in self.node_ids:
            try:
                snaps[nid] = backend.get_snapshot_fresh(nid)
            except Exception as e:
                log.debug("snapshot %s: %s", nid, e)
        return snaps

    def _build_observations(
        self, snapshots: Dict[str, dict]
    ) -> Dict[str, np.ndarray]:
        observations: Dict[str, np.ndarray] = {}
        for nid in self.node_ids:
            snap = snapshots.get(nid) or {}
            msgs = self.bus.receive(nid) if self.cooperative else []
            observations[nid] = self.state_builder.full_observation(
                nid, snap, msgs, cooperative=self.cooperative
            )
        return observations

    def _publish_messages(self, snapshots: Dict[str, dict]) -> None:
        for nid in self.node_ids:
            snap = snapshots.get(nid)
            if not snap:
                continue
            msg = self.state_builder.message_from_snapshot(
                nid, snap, snapshots, self._prev_actions.get(nid, 0)
            )
            for nb in neighbors_of(nid):
                self.bus.send(nid, nb, msg)

    def decision_step(
        self,
        backend,
        *,
        train: bool = False,
        greedy: bool = False,
    ) -> Tuple[Dict[str, dict], float, Dict[str, float], float]:
        """
        observe → (learn from prev transition) → communicate → decide → act
        Simulation advance happens outside (TrafficEnvironment).
        """
        traci = backend._traci
        snapshots = self._collect_snapshots(backend)
        throughput_delta = self.reward_calc.network_throughput_delta(backend, self.node_ids)

        local_rs: Dict[str, float] = {}
        for nid in self.node_ids:
            snap = snapshots.get(nid) or {}
            local_rs[nid] = self.reward_calc.local_reward(
                nid, snap, throughput_delta / max(len(self.node_ids), 1)
            )

        global_r, spill_pen = self.reward_calc.global_reward(snapshots, throughput_delta)
        self._last_global_reward = global_r
        self._last_local_rewards = dict(local_rs)

        if self.cooperative:
            self._publish_messages(snapshots)

        observations = self._build_observations(snapshots)

        # Learn from transition (s_{t-1}, a_{t-1}) -> s_t
        if train:
            import configuration.config as cfg

            done = backend.simulation_time_sec >= float(cfg.SIM_END_SEC)
            for nid in self.node_ids:
                prev = self._prev_obs.get(nid)
                if prev is not None:
                    combined = self.reward_calc.combined_reward(
                        local_rs[nid], global_r, cooperative=self.cooperative
                    )
                    self.agents[nid].remember(
                        prev,
                        self._prev_actions[nid],
                        combined,
                        observations[nid],
                        done,
                    )
                    self.agents[nid].replay()

        actions: Dict[str, int] = {}
        for nid in self.node_ids:
            actions[nid] = self.agents[nid].act(observations[nid], greedy=greedy or not train)

        for nid in self.node_ids:
            applied = self.action_exec.apply(
                nid,
                actions[nid],
                backend.signals[nid],
                traci,
                adaptive_mode=True,
            )
            self._prev_actions[nid] = applied

        for nid in self.node_ids:
            self._prev_obs[nid] = observations[nid]

        self.bus.clear_messages()
        self._update_status(snapshots, self._prev_actions, local_rs, global_r, spill_pen)
        return snapshots, global_r, local_rs, throughput_delta

    def _update_status(
        self,
        snapshots: Dict[str, dict],
        actions: Dict[str, int],
        local_rs: Dict[str, float],
        global_r: float,
        spill_pen: float,
    ) -> None:
        for nid in self.node_ids:
            snap = snapshots.get(nid) or {}
            dirs = snap.get("directions") or {}
            total_q = sum(float((dirs.get(d) or {}).get("queue_length_vehicles") or 0) for d in dirs)
            total_w = sum(float((dirs.get(d) or {}).get("waiting_vehicle_count") or 0) for d in dirs)
            act = actions.get(nid, 0)
            self._last_status[nid] = {
                "id": nid,
                "phase": snap.get("phase"),
                "action": ACTION_NAMES[act] if 0 <= act < len(ACTION_NAMES) else "UNKNOWN",
                "action_id": act,
                "queue": round(total_q, 1),
                "waiting": round(total_w, 1),
                "reward": round(local_rs.get(nid, 0.0), 3),
                "neighbors": neighbors_of(nid),
                "spillback_detected": bool(snap.get("spillback_detected")),
            }
        self._global_metrics = {
            "globalReward": round(global_r, 3),
            "spillbackPenalty": round(spill_pen, 3),
            "averageQueue": round(
                sum(s.get("queue", 0) for s in self._last_status.values()) / max(len(self._last_status), 1),
                2,
            ),
            "averageWaitingTime": round(
                sum(s.get("waiting", 0) for s in self._last_status.values()) / max(len(self._last_status), 1),
                2,
            ),
        }

    def get_agent_status(self) -> List[dict]:
        return [self._last_status[nid] for nid in self.node_ids if nid in self._last_status]

    def get_global_metrics(self) -> dict:
        return dict(self._global_metrics)
