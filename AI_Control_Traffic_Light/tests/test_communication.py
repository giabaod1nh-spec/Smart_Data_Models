"""Unit tests for AI_Control_Traffic_Light communication and state building."""
from __future__ import annotations

import numpy as np

from AI_Control_Traffic_Light.communication.communication_bus import CommunicationBus
from AI_Control_Traffic_Light.communication.message import AgentMessage
from AI_Control_Traffic_Light.config.settings import load_rl_config
from AI_Control_Traffic_Light.environment.state_builder import StateBuilder
from AI_Control_Traffic_Light.environment.topology import NEIGHBOR_GRAPH


def test_neighbor_graph_four_nodes():
    assert set(NEIGHBOR_GRAPH.keys()) == {"A", "B", "C", "D"}
    assert "B" in NEIGHBOR_GRAPH["A"]
    assert "A" in NEIGHBOR_GRAPH["B"]


def test_communication_bus_deliver():
    bus = CommunicationBus()
    msg = AgentMessage(sender_id="A", timestamp=1.0, total_waiting=10.0)
    bus.send("A", "B", msg)
    received = bus.receive("B")
    assert len(received) == 1
    assert received[0].sender_id == "A"
    assert received[0].total_waiting == 10.0
    bus.clear_messages()
    assert bus.receive("B") == []


def test_state_builder_dimensions():
    cfg = load_rl_config()
    sb = StateBuilder(cfg)
    snap = {
        "phase": "NS_GREEN",
        "phase_remaining": 20,
        "green_duration": 42,
        "directions": {
            d: {
                "queue_length_vehicles": 5,
                "waiting_vehicle_count": 2,
                "occupancy_pct": 30,
            }
            for d in ("North", "South", "East", "West")
        },
    }
    local = sb.local_vector(snap)
    assert local.shape == (17,)
    full = sb.full_observation("B", snap, [], cooperative=True)
    assert full.shape == (cfg.state_dim,)
    assert np.all(full[-24:] == 0)  # padded neighbors when no messages
