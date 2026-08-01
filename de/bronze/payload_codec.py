"""Decode Raw payload_stored + payload_encoding."""
from __future__ import annotations

import base64
import json
from typing import Any, Dict, Tuple

from de.bronze.models import RawRow


def decode_payload(raw: RawRow) -> Tuple[Dict[str, Any], str]:
    """Return (parsed_event_dict, raw_text)."""
    enc = (raw.payload_encoding or "utf8").lower()
    if enc == "base64":
        text = base64.b64decode(raw.payload_stored).decode("utf-8", errors="replace")
    else:
        text = raw.payload_stored
    body = json.loads(text)
    if not isinstance(body, dict):
        raise ValueError("JSON root must be object")
    return body, text
