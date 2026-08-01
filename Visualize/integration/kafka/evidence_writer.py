"""Async JSONL evidence writer — disk I/O never on TraCI / poll callback threads."""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

_SENTINEL = object()


@dataclass(frozen=True)
class AckEvidence:
    kind: str = "acked"
    eventId: str = ""
    simulationRunId: str = ""
    cycleSequence: int = 0
    entityId: str = ""
    topic: str = ""
    partition: int = -1
    offset: int = -1
    ackLatencyMs: float = 0.0
    ts: float = field(default_factory=time.time)


@dataclass(frozen=True)
class FailedEvidence:
    kind: str = "failed"
    eventId: str = ""
    simulationRunId: str = ""
    cycleSequence: int = 0
    entityId: str = ""
    reason: str = ""
    permanent: bool = False
    ts: float = field(default_factory=time.time)


class EvidenceWriter:
    """Single thread-safe writer; TraCI/callbacks only try_enqueue evidence objects."""

    def __init__(
        self,
        *,
        root: Path,
        simulation_run_id: str,
        queue_size: int = 10_000,
        flush_every: int = 50,
    ) -> None:
        self.simulation_run_id = simulation_run_id
        self.run_dir = Path(root) / simulation_run_id
        self.queue_size = int(queue_size)
        self.flush_every = int(flush_every)
        self._q: queue.Queue = queue.Queue(maxsize=self.queue_size)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._accepting = False
        self._rejected = 0
        self._written = 0

    def start(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "simulationRunId": self.simulation_run_id,
            "startedAt": time.time(),
        }
        (self.run_dir / "run_metadata.json").write_text(
            json.dumps(meta, indent=2) + "\n", encoding="utf-8"
        )
        self._accepting = True
        self._thread = threading.Thread(
            target=self._run, name="kafka-evidence-writer", daemon=True
        )
        self._thread.start()

    def try_enqueue(self, evidence: Any) -> bool:
        if not self._accepting:
            self._rejected += 1
            return False
        try:
            self._q.put_nowait(evidence)
            return True
        except queue.Full:
            self._rejected += 1
            log.warning("evidence writer queue full — dropping evidence kind=%s", getattr(evidence, "kind", type(evidence)))
            return False

    def stop(self, timeout: float = 5.0) -> None:
        self._accepting = False
        try:
            self._q.put(_SENTINEL, timeout=1.0)
        except queue.Full:
            pass
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    @property
    def rejected_count(self) -> int:
        return self._rejected

    @property
    def written_count(self) -> int:
        return self._written

    def _run(self) -> None:
        acked_path = self.run_dir / "acked.jsonl"
        failed_path = self.run_dir / "failed.jsonl"
        since_flush = 0
        try:
            with acked_path.open("a", encoding="utf-8") as af, failed_path.open(
                "a", encoding="utf-8"
            ) as ff:
                while True:
                    try:
                        item = self._q.get(timeout=0.25)
                    except queue.Empty:
                        if self._stop.is_set() and self._q.empty():
                            break
                        continue
                    if item is _SENTINEL:
                        af.flush()
                        ff.flush()
                        break
                    line = json.dumps(asdict(item) if hasattr(item, "__dataclass_fields__") else item, separators=(",", ":"), ensure_ascii=True) + "\n"
                    kind = getattr(item, "kind", None)
                    if kind == "acked":
                        af.write(line)
                    else:
                        ff.write(line)
                    self._written += 1
                    since_flush += 1
                    if since_flush >= self.flush_every:
                        af.flush()
                        ff.flush()
                        since_flush = 0
                af.flush()
                ff.flush()
        except Exception:
            log.exception("evidence writer crashed")
