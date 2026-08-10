"""Explicit agent-to-agent message (no learned encoder in v1)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class AgentMessage:
    sender_id: str
    timestamp: float
    queue_by_direction: Dict[str, float] = field(default_factory=dict)
    total_waiting: float = 0.0
    mean_occupancy: float = 0.0
    current_phase: str = "NS_GREEN"
    phase_remaining: float = 0.0
    downstream_capacity: float = 1.0
    spillback_pressure: float = 0.0
    intended_action: int = 0

    def to_dict(self) -> dict:
        return {
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "queue_by_direction": dict(self.queue_by_direction),
            "total_waiting": self.total_waiting,
            "mean_occupancy": self.mean_occupancy,
            "current_phase": self.current_phase,
            "phase_remaining": self.phase_remaining,
            "downstream_capacity": self.downstream_capacity,
            "spillback_pressure": self.spillback_pressure,
            "intended_action": self.intended_action,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AgentMessage":
        return cls(
            sender_id=str(data.get("sender_id", "")),
            timestamp=float(data.get("timestamp", 0.0)),
            queue_by_direction=dict(data.get("queue_by_direction") or {}),
            total_waiting=float(data.get("total_waiting", 0.0)),
            mean_occupancy=float(data.get("mean_occupancy", 0.0)),
            current_phase=str(data.get("current_phase", "NS_GREEN")),
            phase_remaining=float(data.get("phase_remaining", 0.0)),
            downstream_capacity=float(data.get("downstream_capacity", 1.0)),
            spillback_pressure=float(data.get("spillback_pressure", 0.0)),
            intended_action=int(data.get("intended_action", 0)),
        )
