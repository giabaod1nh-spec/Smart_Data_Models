"""Internal bearer authentication for Control API (G2 / RC2-T4A)."""
from __future__ import annotations

import hmac
import os
from typing import Optional

from fastapi import Header, HTTPException


def _expected_token() -> Optional[str]:
    token = os.getenv("CONTROL_API_INTERNAL_TOKEN", "").strip()
    return token or None


def require_internal_token(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> None:
    expected = _expected_token()
    if not expected:
        return  # dev mode: auth disabled when token unset
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail={"code": "UPSTREAM_UNAVAILABLE", "message": "missing token"})
    provided = authorization[7:].strip()
    if not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail={"code": "UPSTREAM_UNAVAILABLE", "message": "invalid token"})
