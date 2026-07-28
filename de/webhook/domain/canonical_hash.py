"""Canonical SHA-256 hash for parsed notification payloads."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_hash(parsed_payload: dict[str, Any]) -> str:
    """Compute SHA-256 of canonical JSON (sort object keys only; arrays preserved)."""
    canonical = json.dumps(
        parsed_payload,
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
