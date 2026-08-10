"""Load RL configuration from rl_parameters.yaml."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "rl_parameters.yaml"
_MODELS_DIR = Path(__file__).resolve().parents[1] / "models"


@dataclass
class RLConfig:
    num_agents: int = 4
    agent_nodes: List[str] = field(default_factory=lambda: ["A", "B", "C", "D"])
    gamma: float = 0.95
    epsilon_start: float = 1.0
    epsilon_min: float = 0.05
    epsilon_decay: float = 0.995
    learning_rate: float = 0.001
    batch_size: int = 64
    memory_size: int = 50000
    target_update_frequency: int = 10
    decision_interval_sec: float = 5.0
    local_reward_weight: float = 0.5
    global_reward_weight: float = 0.5
    w_queue: float = 0.1
    w_waiting: float = 0.15
    w_halted: float = 0.05
    w_throughput: float = 0.2
    W_queue: float = 0.05
    W_waiting: float = 0.08
    W_throughput: float = 0.15
    spillback_fixed_penalty: float = 5.0
    spillback_occ_threshold: float = 0.7
    num_episodes: int = 500
    seed: int = 42
    training_mode: str = "cooperative"
    max_queue_veh: float = 50.0
    max_waiting: float = 30.0
    hidden_layers: List[int] = field(default_factory=lambda: [128, 64, 32])
    action_size: int = 4
    max_neighbors: int = 2
    local_state_dim: int = 17
    neighbor_block_dim: int = 12

    @property
    def state_dim(self) -> int:
        return self.local_state_dim + self.max_neighbors * self.neighbor_block_dim

    @property
    def models_dir(self) -> Path:
        return _MODELS_DIR


def load_rl_config(path: Path | None = None) -> RLConfig:
    cfg_path = path or _CONFIG_PATH
    if not cfg_path.is_file():
        return RLConfig()
    with cfg_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return RLConfig(**{k: v for k, v in raw.items() if k in RLConfig.__dataclass_fields__})
