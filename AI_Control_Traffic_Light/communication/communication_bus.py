"""In-memory message bus for cooperative agents (not WebSocket)."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional

from AI_Control_Traffic_Light.communication.message import AgentMessage


class CommunicationBus:
    """Point-to-point mailbox cleared each decision cycle."""

    def __init__(self) -> None:
        self._inbox: Dict[str, List[AgentMessage]] = defaultdict(list)

    def send(self, sender_id: str, receiver_id: str, message: AgentMessage) -> None:
        message.sender_id = sender_id
        self._inbox[receiver_id].append(message)

    def receive(self, agent_id: str) -> List[AgentMessage]:
        return list(self._inbox.get(agent_id, []))

    def broadcast(self, sender_id: str, message: AgentMessage, receivers: List[str]) -> None:
        for rid in receivers:
            self.send(sender_id, rid, message)

    def clear_messages(self) -> None:
        self._inbox.clear()

    def peek_all(self) -> Dict[str, List[AgentMessage]]:
        return {k: list(v) for k, v in self._inbox.items()}
