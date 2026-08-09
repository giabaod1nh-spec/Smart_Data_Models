"""Node micro-batch buffers + cycle completeness tracking (K-3)."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class BufferedEvent:
    event: dict
    topic: str
    partition: int
    offset: int
    received_at: float = field(default_factory=time.monotonic)
    broker_timestamp_epoch: Optional[float] = None
    consumer_received_epoch: float = field(default_factory=time.time)


@dataclass
class NodeBuffer:
    simulation_run_id: str
    cycle_sequence: int
    node_id: str
    node_entity_count: int
    events: Dict[str, BufferedEvent] = field(default_factory=dict)  # entity_id →
    first_seen_at: float = field(default_factory=time.monotonic)

    @property
    def key(self) -> Tuple[str, int, str]:
        return (self.simulation_run_id, self.cycle_sequence, self.node_id)

    def add(self, be: BufferedEvent) -> Optional[str]:
        """Add event; return error reason if invariant broken."""
        ev = be.event
        nec = ev.get("nodeEntityCount")
        if nec is None:
            return "missing nodeEntityCount"
        if int(nec) != self.node_entity_count:
            return "nodeEntityCount drift"
        if str(ev.get("nodeId")) != self.node_id:
            return "nodeId drift"
        eid = str(ev["entity"]["id"])
        if eid in self.events:
            return "duplicate entityId in node buffer"
        if len(self.events) >= self.node_entity_count:
            return "exceeds nodeEntityCount"
        self.events[eid] = be
        return None

    @property
    def is_complete(self) -> bool:
        return len(self.events) >= self.node_entity_count

    def age_ms(self) -> float:
        return (time.monotonic() - self.first_seen_at) * 1000.0


class NodeBufferManager:
    def __init__(
        self,
        *,
        timeout_ms: float = 100.0,
        max_buffered_events: int = 2000,
        max_buffered_cycles: int = 32,
    ) -> None:
        self.timeout_ms = float(timeout_ms)
        self.max_buffered_events = int(max_buffered_events)
        self.max_buffered_cycles = int(max_buffered_cycles)
        self._nodes: Dict[Tuple[str, int, str], NodeBuffer] = {}
        self._cycle_entity_ids: Dict[Tuple[str, int], set] = {}
        self.paused = False
        self.node_partial_count = 0
        self.quarantine_count = 0

    @property
    def buffered_event_count(self) -> int:
        return sum(len(n.events) for n in self._nodes.values())

    @property
    def buffered_cycle_count(self) -> int:
        return len({(k[0], k[1]) for k in self._nodes})

    def should_pause(self) -> bool:
        return (
            self.buffered_event_count >= self.max_buffered_events
            or self.buffered_cycle_count >= self.max_buffered_cycles
        )

    def ingest(self, be: BufferedEvent) -> Tuple[str, Optional[NodeBuffer]]:
        """
        Returns (action, buffer):
          action in ready | buffered | quarantine | stale_skip_handled_elsewhere
        """
        ev = be.event
        run = str(ev["simulationRunId"])
        cyc = int(ev["cycleSequence"])
        node = str(ev["nodeId"])
        nec = ev.get("nodeEntityCount")
        if nec is None:
            self.quarantine_count += 1
            return "quarantine", None
        key = (run, cyc, node)
        buf = self._nodes.get(key)
        if buf is None:
            buf = NodeBuffer(
                simulation_run_id=run,
                cycle_sequence=cyc,
                node_id=node,
                node_entity_count=int(nec),
            )
            self._nodes[key] = buf
        err = buf.add(be)
        if err:
            self.quarantine_count += 1
            return "quarantine", None
        self.paused = self.should_pause()
        if buf.is_complete:
            return "ready", buf
        return "buffered", buf

    def pop_ready(self, key: Tuple[str, int, str]) -> Optional[NodeBuffer]:
        return self._nodes.pop(key, None)

    def take_complete(self) -> List[NodeBuffer]:
        """Remove and return complete buffers after the consumer queue drains."""
        out: List[NodeBuffer] = []
        for key, buf in list(self._nodes.items()):
            if buf.is_complete:
                out.append(self._nodes.pop(key))
        self.paused = self.should_pause()
        return out

    def take_complete_cycles(self) -> List[NodeBuffer]:
        """Remove complete node buffers only when their whole cycle is present."""
        by_cycle: Dict[Tuple[str, int], List[NodeBuffer]] = {}
        for buf in self._nodes.values():
            by_cycle.setdefault(
                (buf.simulation_run_id, buf.cycle_sequence), []
            ).append(buf)

        out: List[NodeBuffer] = []
        for cycle_buffers in by_cycle.values():
            if not cycle_buffers or not all(buf.is_complete for buf in cycle_buffers):
                continue
            first_events = [
                next(iter(buf.events.values())).event
                for buf in cycle_buffers
                if buf.events
            ]
            if not first_events:
                continue
            expected_values = {
                int(event.get("cycleEntityCount") or 0) for event in first_events
            }
            actual = sum(len(buf.events) for buf in cycle_buffers)
            if len(expected_values) != 1 or actual != next(iter(expected_values)):
                continue
            for buf in cycle_buffers:
                popped = self._nodes.pop(buf.key, None)
                if popped is not None:
                    out.append(popped)
        self.paused = self.should_pause()
        return out

    def pause(self) -> None:
        self.paused = True

    def flush_timed_out(self) -> List[NodeBuffer]:
        """Return incomplete buffers past timeout (caller applies partial)."""
        out: List[NodeBuffer] = []
        for key, buf in list(self._nodes.items()):
            if not buf.is_complete and buf.age_ms() >= self.timeout_ms and buf.events:
                out.append(self._nodes.pop(key))
                self.node_partial_count += 1
        self.paused = self.should_pause()
        return out

    def discard_all(self) -> None:
        self._nodes.clear()
        self.paused = False
