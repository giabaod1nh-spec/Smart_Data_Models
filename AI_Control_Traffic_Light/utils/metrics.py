"""Episode metrics logging for training and evaluation."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class EpisodeMetrics:
    episode: int = 0
    global_reward: float = 0.0
    local_rewards: Dict[str, float] = field(default_factory=dict)
    avg_waiting: float = 0.0
    avg_queue: float = 0.0
    throughput: float = 0.0
    spillback_events: int = 0
    steps: int = 0

    def to_dict(self) -> dict:
        return {
            "episode": self.episode,
            "global_reward": self.global_reward,
            "local_rewards": dict(self.local_rewards),
            "avg_waiting": self.avg_waiting,
            "avg_queue": self.avg_queue,
            "throughput": self.throughput,
            "spillback_events": self.spillback_events,
            "steps": self.steps,
        }


class MetricsCollector:
    def __init__(self) -> None:
        self.waiting_samples: List[float] = []
        self.queue_samples: List[float] = []
        self.spillback_events = 0
        self.throughput = 0.0
        self.global_reward = 0.0
        self.local_rewards: Dict[str, float] = {}
        self.steps = 0

    def reset(self) -> None:
        self.waiting_samples.clear()
        self.queue_samples.clear()
        self.spillback_events = 0
        self.throughput = 0.0
        self.global_reward = 0.0
        self.local_rewards = {}
        self.steps = 0

    def record_step(
        self,
        snapshots: Dict[str, dict],
        global_r: float,
        local_rs: Dict[str, float],
        throughput_delta: float,
    ) -> None:
        self.steps += 1
        self.global_reward += global_r
        for nid, lr in local_rs.items():
            self.local_rewards[nid] = self.local_rewards.get(nid, 0.0) + lr
        self.throughput += throughput_delta
        for snap in snapshots.values():
            dirs = snap.get("directions") or {}
            for d in dirs.values():
                self.waiting_samples.append(float(d.get("waiting_vehicle_count") or 0))
                self.queue_samples.append(float(d.get("queue_length_vehicles") or 0))
            if snap.get("spillback_detected"):
                self.spillback_events += 1

    def finalize(self, episode: int) -> EpisodeMetrics:
        n_w = len(self.waiting_samples) or 1
        n_q = len(self.queue_samples) or 1
        return EpisodeMetrics(
            episode=episode,
            global_reward=self.global_reward,
            local_rewards=dict(self.local_rewards),
            avg_waiting=sum(self.waiting_samples) / n_w,
            avg_queue=sum(self.queue_samples) / n_q,
            throughput=self.throughput,
            spillback_events=self.spillback_events,
            steps=self.steps,
        )


def save_episode_metrics(metrics: EpisodeMetrics, log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"episode_{metrics.episode:04d}.json"
    path.write_text(json.dumps(metrics.to_dict(), indent=2), encoding="utf-8")
    return path
