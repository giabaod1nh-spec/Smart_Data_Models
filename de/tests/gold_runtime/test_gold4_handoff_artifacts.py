"""Gold4 consumer-handoff artifacts exist and enforce live namespace filters."""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
TEMPLATES = REPO / "docs" / "gold" / "gold4_query_templates"
CONTRACT = REPO / "docs" / "shared" / "GOLD_ANALYTICS_CONSUMER_CONTRACT.md"
RUNBOOK = REPO / "docs" / "gold" / "GOLD_4_RUNBOOK.md"


def test_gold4_handoff_files_exist():
    assert CONTRACT.is_file()
    assert RUNBOOK.is_file()
    assert TEMPLATES.is_dir()
    assert len(list(TEMPLATES.glob("*.sql"))) >= 8


def test_query_templates_force_live_namespace():
    for path in TEMPLATES.glob("*.sql"):
        text = path.read_text(encoding="utf-8")
        if path.name.startswith("09_"):
            assert "namespace = 'live'" in text
            assert "replay:%" in text or "replay:" in text
            continue
        assert "namespace = 'live'" in text
        assert "LIMIT" in text
        assert "ORDER BY" in text
