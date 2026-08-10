"""Local, global, and spillback rewards from intersection snapshots."""
from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np

from AI_Control_Traffic_Light.config.settings import RLConfig
from AI_Control_Traffic_Light.environment.topology import DOWNSTREAM_CORRIDORS
from AI_Control_Traffic_Light.utils.state_normalizer import norm_occupancy


class RewardCalculator:
    def __init__(self, config: RLConfig) -> None:
        self.config = config
        self._prev_arrived: Dict[str, int] = {}

    def reset(self) -> None:
        self._prev_arrived.clear()

    def _direction_totals(self, snapshot: dict) -> Tuple[float, float, float]:
        dirs = snapshot.get("directions") or {}
        q = w = h = 0.0
        for d in ("North", "South", "East", "West"):
            dd = dirs.get(d) or {}
            q += float(dd.get("queue_length_vehicles") or 0)
            w += float(dd.get("waiting_vehicle_count") or 0)
            h += float(dd.get("full_link_halting_count") or 0)
        return q, w, h

    def local_reward(self, node_id: str, snapshot: dict, throughput_delta: float) -> float:
        q, w, h = self._direction_totals(snapshot)
        r = (
            -self.config.w_queue * q
            - self.config.w_waiting * w
            - self.config.w_halted * h
            + self.config.w_throughput * throughput_delta
        )
        return float(r)

    def spillback_penalty(self, snapshots: Dict[str, dict]) -> float:
        penalty = 0.0
        thr = self.config.spillback_occ_threshold
        for node_id, corridors in DOWNSTREAM_CORRIDORS.items():
            snap = snapshots.get(node_id) or {}
            for _dir, neighbor, approach in corridors:
                nd = (snap.get("directions") or {}).get(_dir) or {}
                nb_snap = snapshots.get(neighbor) or {}
                nb_dd = (nb_snap.get("directions") or {}).get(approach) or {}
                occ = norm_occupancy(float(nb_dd.get("occupancy_pct") or 0))
                if occ > thr:
                    penalty += occ ** 2
                if nb_snap.get("spillback_detected"):
                    penalty += self.config.spillback_fixed_penalty
        return float(penalty)

    def global_reward(
        self,
        snapshots: Dict[str, dict],
        network_throughput_delta: float,
    ) -> Tuple[float, float]:
        total_q = total_w = 0.0
        spill_events = 0
        for snap in snapshots.values():
            q, w, _ = self._direction_totals(snap)
            total_q += q
            total_w += w
            if snap.get("spillback_detected"):
                spill_events += 1
        spill_pen = self.spillback_penalty(snapshots)
        g = (
            -self.config.W_queue * total_q
            - self.config.W_waiting * total_w
            + self.config.W_throughput * network_throughput_delta
            - spill_pen
        )
        return float(g), float(spill_pen)

    def combined_reward(
        self,
        local: float,
        global_r: float,
        *,
        cooperative: bool = True,
    ) -> float:
        if not cooperative:
            return local
        a = self.config.local_reward_weight
        b = self.config.global_reward_weight
        return float(a * local + b * global_r)

    def network_throughput_delta(self, backend, node_ids: List[str]) -> float:
        arrived = 0
        try:
            arrived = int(backend._traci.simulation.getArrivedNumber())
        except Exception:
            pass
        prev = self._prev_arrived.get("_net", 0)
        self._prev_arrived["_net"] = arrived
        return float(max(0, arrived - prev))
