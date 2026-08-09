"""S4-D restart contract: checkpoint offsets must not regress across restart evidence files."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

EVIDENCE = Path(__file__).resolve().parents[3] / "de" / "artifacts" / "silver" / "evidence" / "s4d"


def test_restart_evidence_has_no_checkpoint_regression():
    before_p = EVIDENCE / "checkpoint_before_restart.json"
    after_p = EVIDENCE / "checkpoint_after_restart.json"
    if not before_p.is_file() or not after_p.is_file():
        pytest.skip("S4-D restart evidence not collected yet")
    before = {tuple(r[:-1]): r[-1] for r in json.loads(before_p.read_text(encoding="utf-8"))}
    after = {tuple(r[:-1]): r[-1] for r in json.loads(after_p.read_text(encoding="utf-8"))}
    regressions = [
        (k, before[k], after.get(k))
        for k in before
        if k not in after or after[k] < before[k]
    ]
    assert regressions == [], regressions
