"""Thread-safe runtime gate for direct Orion publish (K-4.5 cutover rehearsal).

Env ORION_PUBLISH_ENABLED sets the *initial* value only. Mid-run changes must go
through set_orion_publish_enabled() / Control API POST /control/orion-publish.
Kafka / durable outbox paths are independent of this flag.
"""
from __future__ import annotations

import os
import threading
from typing import Optional

_lock = threading.RLock()
_enabled: Optional[bool] = None


def _parse_env_bool(raw: Optional[str], default: bool = True) -> bool:
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def init_orion_publish_enabled_from_env() -> bool:
    """Initialize once from env; subsequent calls return current value."""
    global _enabled
    with _lock:
        if _enabled is None:
            profile = str(os.getenv("ARCHITECTURE_PROFILE", "") or "").strip().lower()
            default = False if profile == "k5-cutover" else True
            _enabled = _parse_env_bool(os.getenv("ORION_PUBLISH_ENABLED"), default)
        return _enabled


def is_orion_publish_enabled() -> bool:
    with _lock:
        if _enabled is None:
            return init_orion_publish_enabled_from_env()
        return _enabled


def set_orion_publish_enabled(enabled: bool) -> bool:
    """Set runtime flag. Returns the new value."""
    global _enabled
    with _lock:
        _enabled = bool(enabled)
        return _enabled


def reset_orion_publish_gate_for_tests() -> None:
    """Test helper: clear so next read re-inits from env."""
    global _enabled
    with _lock:
        _enabled = None
