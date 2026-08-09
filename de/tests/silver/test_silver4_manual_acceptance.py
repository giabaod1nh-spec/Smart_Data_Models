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
    assert (REPO / "docs" / "siliver" / "SILVER_4_INTEGRATION_VALIDATION_PLAN.md").is_file()
    assert (REPO / "docs" / "siliver" / "SILVER_4_PLAN_REVIEW.md").is_file()
