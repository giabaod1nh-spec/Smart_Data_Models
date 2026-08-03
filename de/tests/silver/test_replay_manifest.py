"""Replay manifest validation tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from de.silver.config import SilverConfigError
from de.silver.replay import canonical_manifest_hash, validate_manifest


def test_manifest_validation(tmp_path: Path):
    doc = {
        "manifest_version": "silver-replay-v1",
        "replay_run_id": "r1",
        "source_database": "smart_traffic",
        "stream_windows": [
            {
                "source_table": "bronze_entity_events",
                "topic": "t",
                "partition": 0,
                "start_offset": 0,
                "end_offset": 10,
            }
        ],
    }
    h = validate_manifest(doc, "r1")
    assert h == canonical_manifest_hash(doc)
    with pytest.raises(SilverConfigError):
        validate_manifest(doc, "other")
