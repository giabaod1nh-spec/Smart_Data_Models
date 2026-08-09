"""S4-D dependency-failure procedure is documented; live CH outage is operator-gated."""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def test_runbook_documents_clickhouse_outage_recovery_path():
    text = (REPO / "docs" / "shared" / "SILVER_RUNBOOK.md").read_text(encoding="utf-8")
    plan = (REPO / "docs" / "siliver" / "SILVER_4_INTEGRATION_VALIDATION_PLAN.md").read_text(
        encoding="utf-8"
    )
    assert "ClickHouse" in text or "clickhouse" in text.lower()
    assert "ClickHouse unavailable" in plan
