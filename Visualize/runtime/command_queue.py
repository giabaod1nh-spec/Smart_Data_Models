"""
command_queue.py — Thread-safe command queue for Control API → TraCI thread (ADR-005).
"""
from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

DEFAULT_CAPACITY = 256


class QueueFullError(Exception):
    """Raised when bounded queue cannot accept another command."""


@dataclass
class Command:
    name: str
    kwargs: Dict[str, Any] = field(default_factory=dict)
    command_id: Optional[str] = None
    result_event: Optional[threading.Event] = None
    error: Optional[BaseException] = None
    result: Any = None


class CommandQueue:
    def __init__(self, capacity: int = DEFAULT_CAPACITY):
        self._capacity = capacity
        self._q: queue.Queue[Command] = queue.Queue(maxsize=capacity)

    @property
    def capacity(self) -> int:
        return self._capacity

    def enqueue(
        self,
        name: str,
        wait: bool = False,
        timeout: float = 5.0,
        *,
        command_id: Optional[str] = None,
        **kwargs,
    ) -> Any:
        cmd = Command(name=name, kwargs=kwargs, command_id=command_id)
        if wait:
            cmd.result_event = threading.Event()
        try:
            self._q.put_nowait(cmd)
        except queue.Full as e:
            raise QueueFullError(f"Command queue full (capacity={self._capacity})") from e
        if wait and cmd.result_event is not None:
            ok = cmd.result_event.wait(timeout=timeout)
            if not ok:
                raise TimeoutError(f"Command '{name}' timed out")
            if cmd.error:
                raise cmd.error
            return cmd.result
        return None

    def drain(
        self,
        handlers: Dict[str, Callable[..., Any]],
        max_n: int = 50,
        *,
        on_start: Optional[Callable[[Command], None]] = None,
        on_success: Optional[Callable[[Command, Any], None]] = None,
        on_error: Optional[Callable[[Command, BaseException], None]] = None,
    ) -> int:
        processed = 0
        while processed < max_n:
            try:
                cmd = self._q.get_nowait()
            except queue.Empty:
                break
            handler = handlers.get(cmd.name)
            if on_start is not None:
                on_start(cmd)
            try:
                if handler is None:
                    raise KeyError(f"Unknown command '{cmd.name}'")
                cmd.result = handler(**cmd.kwargs)
                if on_success is not None:
                    on_success(cmd, cmd.result)
            except BaseException as e:
                cmd.error = e
                if on_error is not None:
                    on_error(cmd, e)
            finally:
                if cmd.result_event is not None:
                    cmd.result_event.set()
            processed += 1
        return processed

    def pending(self) -> int:
        return self._q.qsize()
