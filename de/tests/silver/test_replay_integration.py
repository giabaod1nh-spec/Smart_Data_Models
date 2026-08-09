"""S4-E replay isolation evidence."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

EVIDENCE = Path(__file__).resolve().parents[3] / "de" / "artifacts" / "silver" / "evidence" / "s4e" / "replay_isolation.json"


def test_replay_isolation_evidence():
    if not EVIDENCE.is_file():
        pytest.skip("S4-E replay evidence not collected")
    doc = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    assert doc["main_before"] == doc["main_after"]
    assert doc["live_checkpoint_unchanged"] is True
    assert str(doc["namespace"]).startswith("replay:")
