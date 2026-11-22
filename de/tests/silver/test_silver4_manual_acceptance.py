"""S4-F manual acceptance / runbook completeness."""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def test_runbook_exists_and_covers_required_topics():
    text = (REPO / "docs" / "shared" / "SILVER_RUNBOOK.md").read_text(encoding="utf-8")
    for needle in [
        "docker compose",
        "/health",
        "/ready",
        "checkpoint",
        "replay",
        "FAULTED",
        "Forbidden",
    ]:
        assert needle in text, needle


def test_plan4_completion_or_review_docs_exist():
    phase3 = REPO / 'docs' / 'DEv1' / 'phase-3'
    for name in ('plan.md', 'decisions.md', 'acceptance.md', 'handover.md'):
        assert (phase3 / name).is_file()
