"""S4-E oracle package — asserts collected smoke evidence covers core oracle predicates."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

SMOKE = Path(__file__).resolve().parents[3] / "de" / "artifacts" / "silver" / "evidence" / "s4c" / "smoke_evidence.json"
REPLAY = Path(__file__).resolve().parents[3] / "de" / "artifacts" / "silver" / "evidence" / "s4e" / "replay_isolation.json"


def test_oracle_core_from_smoke_evidence():
    if not SMOKE.is_file():
        pytest.skip("smoke evidence missing")
    doc = json.loads(SMOKE.read_text(encoding="utf-8"))
    # C duplicate: uniq == count for fact tables sampled
    for name, payload in doc["counts"].items():
        if name.startswith("silver_fact_"):
            assert payload["count"] == payload["uniq_source"], name
    # H ledger: no multi-disposition
    assert doc["multi_disposition"] == []
    # D lineage empty check on traffic facts
    assert doc["traffic_fact_lineage_empty"] == 0


def test_oracle_i_namespace_isolation_evidence():
    if not REPLAY.is_file():
        pytest.skip("replay evidence missing")
    doc = json.loads(REPLAY.read_text(encoding="utf-8"))
    assert doc["main_before"] == doc["main_after"]
    assert doc["live_checkpoint_unchanged"] is True
