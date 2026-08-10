"""Build fixed-size observation vectors from SUMO snapshots."""
from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np

from AI_Control_Traffic_Light.communication.message import AgentMessage
from AI_Control_Traffic_Light.config.settings import RLConfig
from AI_Control_Traffic_Light.environment.topology import PHASE_NAMES, neighbors_of
from AI_Control_Traffic_Light.utils.state_normalizer import norm_occupancy, norm_queue, norm_waiting


class StateBuilder:
    def __init__(self, config: RLConfig) -> None:
        self.config = config

    def phase_one_hot(self, phase: str) -> np.ndarray:
        vec = np.zeros(len(PHASE_NAMES), dtype=np.float32)
        if phase in PHASE_NAMES:
            vec[PHASE_NAMES.index(phase)] = 1.0
        return vec

    def action_one_hot(self, action: int) -> np.ndarray:
        vec = np.zeros(self.config.action_size, dtype=np.float32)
        if 0 <= action < self.config.action_size:
            vec[action] = 1.0
        return vec

    def local_vector(self, snapshot: dict) -> np.ndarray:
        dirs = snapshot.get("directions") or {}
        parts: List[float] = []
        for d in ("North", "South", "East", "West"):
            dd = dirs.get(d) or {}
            parts.append(norm_queue(float(dd.get("queue_length_vehicles") or 0), self.config.max_queue_veh))
            parts.append(norm_waiting(float(dd.get("waiting_vehicle_count") or 0), self.config.max_waiting))
            parts.append(norm_occupancy(float(dd.get("occupancy_pct") or 0)))
        phase = str(snapshot.get("phase") or "NS_GREEN")
        parts.extend(self.phase_one_hot(phase).tolist())
        green_dur = float(snapshot.get("green_duration") or 42)
        rem = float(snapshot.get("phase_remaining") or 0)
        parts.append(float(np.clip(rem / max(green_dur, 1.0), 0.0, 1.0)))
        return np.asarray(parts, dtype=np.float32)

    def downstream_capacity(self, node_id: str, snapshots: Dict[str, dict]) -> float:
        """Min available capacity toward downstream neighbors."""
        from AI_Control_Traffic_Light.environment.topology import DOWNSTREAM_CORRIDORS

        corridors = DOWNSTREAM_CORRIDORS.get(node_id, [])
        if not corridors:
            return 1.0
        caps: List[float] = []
        for _dir, neighbor, approach in corridors:
            snap = snapshots.get(neighbor) or {}
            nd = (snap.get("directions") or {}).get(approach) or {}
            occ = norm_occupancy(float(nd.get("occupancy_pct") or 0))
            caps.append(max(0.0, 1.0 - occ))
        return float(min(caps)) if caps else 1.0

    def message_from_snapshot(
        self,
        node_id: str,
        snapshot: dict,
        snapshots: Dict[str, dict],
        intended_action: int,
    ) -> AgentMessage:
        dirs = snapshot.get("directions") or {}
        queue_by_dir = {
            d: float((dirs.get(d) or {}).get("queue_length_vehicles") or 0)
            for d in ("North", "South", "East", "West")
        }
        total_waiting = sum(float((dirs.get(d) or {}).get("waiting_vehicle_count") or 0) for d in queue_by_dir)
        occs = [norm_occupancy(float((dirs.get(d) or {}).get("occupancy_pct") or 0)) for d in queue_by_dir]
        return AgentMessage(
            sender_id=node_id,
            timestamp=float(snapshot.get("simulation_time_sec") or 0),
            queue_by_direction=queue_by_dir,
            total_waiting=total_waiting,
            mean_occupancy=float(np.mean(occs)) if occs else 0.0,
            current_phase=str(snapshot.get("phase") or "NS_GREEN"),
            phase_remaining=float(snapshot.get("phase_remaining") or 0),
            downstream_capacity=self.downstream_capacity(node_id, snapshots),
            spillback_pressure=float(snapshot.get("spillback_pressure") or 0),
            intended_action=intended_action,
        )

    def neighbor_block(self, msg: Optional[AgentMessage]) -> np.ndarray:
        dim = self.config.neighbor_block_dim
        if msg is None:
            return np.zeros(dim, dtype=np.float32)
        total_q = sum(msg.queue_by_direction.values())
        parts: List[float] = [
            norm_queue(total_q, self.config.max_queue_veh * 4),
            norm_waiting(msg.total_waiting, self.config.max_waiting * 4),
            float(np.clip(msg.mean_occupancy, 0.0, 1.0)),
        ]
        parts.extend(self.phase_one_hot(msg.current_phase).tolist())
        parts.append(float(np.clip(msg.downstream_capacity, 0.0, 1.0)))
        parts.append(float(np.clip(msg.spillback_pressure, 0.0, 1.0)))
        parts.extend(self.action_one_hot(msg.intended_action).tolist())
        vec = np.asarray(parts, dtype=np.float32)
        if vec.shape[0] < dim:
            vec = np.pad(vec, (0, dim - vec.shape[0]))
        return vec[:dim]

    def full_observation(
        self,
        node_id: str,
        snapshot: dict,
        neighbor_messages: List[AgentMessage],
        *,
        cooperative: bool = True,
    ) -> np.ndarray:
        local = self.local_vector(snapshot)
        if not cooperative:
            return np.pad(
                local,
                (0, self.config.max_neighbors * self.config.neighbor_block_dim),
            )
        nb_ids = neighbors_of(node_id)
        msg_by_sender = {m.sender_id: m for m in neighbor_messages}
        blocks: List[np.ndarray] = []
        for nb in nb_ids[: self.config.max_neighbors]:
            blocks.append(self.neighbor_block(msg_by_sender.get(nb)))
        while len(blocks) < self.config.max_neighbors:
            blocks.append(self.neighbor_block(None))
        return np.concatenate([local] + blocks).astype(np.float32)
