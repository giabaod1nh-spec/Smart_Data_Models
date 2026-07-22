"""
command_queue.py — Thread-safe command queue for Control API → TraCI thread (ADR-005).
"""
from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Command:
    name: str
    kwargs: Dict[str, Any] = field(default_factory=dict)
    result_event: Optional[threading.Event] = None
    error: Optional[BaseException] = None
    result: Any = None


class CommandQueue:
    def __init__(self):
        self._q: queue.Queue[Command] = queue.Queue()

    def enqueue(self, name: str, wait: bool = False, timeout: float = 5.0, **kwargs) -> Any:
        cmd = Command(name=name, kwargs=kwargs)
        if wait:
            cmd.result_event = threading.Event()
        self._q.put(cmd)
        if wait and cmd.result_event is not None:
            ok = cmd.result_event.wait(timeout=timeout)
            if not ok:
                raise TimeoutError(f"Command '{name}' timed out")
            if cmd.error:
                raise cmd.error
            return cmd.result
        return None

    def drain(self, handlers: Dict[str, Callable[..., Any]], max_n: int = 50) -> int:
        processed = 0
        while processed < max_n:
            try:
                cmd = self._q.get_nowait()
            except queue.Empty:
                break
            handler = handlers.get(cmd.name)
            try:
                if handler is None:
                    raise KeyError(f"Unknown command '{cmd.name}'")
                cmd.result = handler(**cmd.kwargs)
            except BaseException as e:
                cmd.error = e
            finally:
                if cmd.result_event is not None:
                    cmd.result_event.set()
            processed += 1
        return processed

    def pending(self) -> int:
        return self._q.qsize()
