"""Backward-compatible shim — prefer integration.receiver.app.TemporaryNotificationReceiver. """
from __future__ import annotations

from pathlib import Path
import importlib.util

_path = Path(__file__).resolve().parent / "receiver" / "app.py"
_spec = importlib.util.spec_from_file_location("integration_receiver_app", _path)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

TemporaryNotificationReceiver = _mod.TemporaryNotificationReceiver
ThinWebhook = TemporaryNotificationReceiver  # legacy alias

__all__ = ["TemporaryNotificationReceiver", "ThinWebhook"]
