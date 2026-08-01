"""Canonical SHA-256 hash for parsed notification payloads.

Re-exports the shared Contract algorithm so Producer/DE/DVT stay aligned.
"""
from __future__ import annotations

from contracts.canonical_json import canonical_hash, canonical_json

__all__ = ["canonical_hash", "canonical_json"]
