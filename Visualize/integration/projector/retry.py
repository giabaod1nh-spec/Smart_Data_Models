"""Orion batch retry state machine (RT-D / §58)."""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Sequence, Set


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


RETRY_DELAYS_MS = (0, 250, 500, 1000, 2000)
MAX_ATTEMPTS = 5
CIRCUIT_OPEN_SEC = 30.0
JITTER_FRAC = 0.20


def _jitter_ms(base_ms: float) -> float:
    if base_ms <= 0:
        return 0.0
    delta = base_ms * JITTER_FRAC
    return max(0.0, base_ms + random.uniform(-delta, delta))


@dataclass
class RetryContext:
    entity_ids: tuple[str, ...]
    attempt: int = 0
    next_retry_at: float = 0.0
    circuit_state: CircuitState = CircuitState.CLOSED
    circuit_open_until: float = 0.0
    last_error: str = ""


class OrionRetryManager:
    """Single retry authority for projector batch upsert."""

    def __init__(self) -> None:
        self._pending: Optional[RetryContext] = None
        self.metrics_retry_total: dict[str, int] = {"transient": 0, "permanent": 0}

    @property
    def has_pending(self) -> bool:
        return self._pending is not None

    @property
    def degraded(self) -> bool:
        if self._pending is None:
            return False
        return self._pending.circuit_state in (CircuitState.OPEN, CircuitState.HALF_OPEN)

    def clear(self) -> None:
        self._pending = None

    def start_or_continue(
        self,
        entity_ids: Sequence[str],
        *,
        now: Optional[float] = None,
    ) -> RetryContext:
        now = now or time.monotonic()
        if self._pending is None:
            self._pending = RetryContext(entity_ids=tuple(entity_ids))
        return self._pending

    def record_failure(
        self,
        *,
        retryable: bool,
        error: str = "",
        now: Optional[float] = None,
    ) -> tuple[bool, float]:
        """Returns (should_retry, sleep_seconds)."""
        now = now or time.monotonic()
        if self._pending is None:
            return False, 0.0
        ctx = self._pending
        ctx.last_error = error
        if not retryable:
            self.metrics_retry_total["permanent"] += 1
            return False, 0.0
        ctx.attempt += 1
        self.metrics_retry_total["transient"] += 1
        if ctx.attempt >= MAX_ATTEMPTS:
            ctx.circuit_state = CircuitState.OPEN
            ctx.circuit_open_until = now + CIRCUIT_OPEN_SEC
            return False, 0.0
        delay_ms = RETRY_DELAYS_MS[min(ctx.attempt, len(RETRY_DELAYS_MS) - 1)]
        sleep_sec = _jitter_ms(float(delay_ms)) / 1000.0
        ctx.next_retry_at = now + sleep_sec
        return True, sleep_sec

    def circuit_allows_attempt(self, *, now: Optional[float] = None) -> bool:
        now = now or time.monotonic()
        if self._pending is None:
            return True
        ctx = self._pending
        if ctx.circuit_state == CircuitState.CLOSED:
            return True
        if ctx.circuit_state == CircuitState.OPEN:
            if now >= ctx.circuit_open_until:
                ctx.circuit_state = CircuitState.HALF_OPEN
                return True
            return False
        return True  # HALF_OPEN probe

    def record_success(self) -> None:
        self.clear()

    def half_open_failed(self, *, now: Optional[float] = None) -> None:
        now = now or time.monotonic()
        if self._pending is None:
            return
        self._pending.circuit_state = CircuitState.OPEN
        self._pending.circuit_open_until = now + CIRCUIT_OPEN_SEC


def classify_batch_result(result: Any) -> tuple[Set[str], Set[str], Set[str], bool]:
    """Return success_ids, retryable_ids, permanent_ids, is_transient_transport."""
    success = set(getattr(result, "success_ids", ()) or ())
    retryable = set(getattr(result, "retryable_error_ids", ()) or ())
    ambiguous = set(getattr(result, "ambiguous_ids", ()) or ())
    permanent = {e.entity_id for e in (getattr(result, "permanent_errors", ()) or ())}
    http_status = getattr(result, "http_status", None)
    if not success and http_status in (201, 204):
        return success, retryable, permanent, False
    transport_transient = http_status is None and bool(ambiguous)
    retryable |= ambiguous
    return success, retryable, permanent, transport_transient
