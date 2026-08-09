"""S4-C shared-stack smoke evidence presence."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

EVIDENCE = Path(__file__).resolve().parents[3] / "de" / "artifacts" / "silver" / "evidence" / "s4c" / "smoke_evidence.json"


def test_shared_stack_smoke_evidence_file():
    if not EVIDENCE.is_file():
        pytest.skip("S4-C smoke evidence not collected")
    doc = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    assert doc["silver_table_count"] >= 19
    assert doc["traffic_fact_lineage_empty"] == 0
    assert doc["multi_disposition"] == []
    assert doc["counts"]["silver_processing_ledger"]["uniq_source"] > 0
