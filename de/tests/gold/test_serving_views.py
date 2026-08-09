"""Serving-view current-row and version-ordering contracts."""
from __future__ import annotations

from pathlib import Path

from de.gold.contracts import (
    CURRENT_ROW_ORDER,
    MART_BUSINESS_IDENTITY,
    MART_SOURCE_FACT,
    MART_VIEWS,
    STRUCTURAL_MARTS_PENDING_GOLD2,
    parse_definition_version,
    select_current_row,
)

REPO = Path(__file__).resolve().parents[3]
DDL = (REPO / "de" / "migrations" / "005_create_gold_m1.sql").read_text(encoding="utf-8")


def test_mart_business_identities_and_sources_are_complete():
    assert set(MART_BUSINESS_IDENTITY) == set(MART_VIEWS)
    assert set(MART_SOURCE_FACT) == set(MART_VIEWS)
    assert STRUCTURAL_MARTS_PENDING_GOLD2 == {"gold_mart_network_window_overview"}
    assert "STRUCTURAL SERVING VIEW — POPULATED AFTER GOLD 2" in DDL


def test_views_encode_locked_current_row_order():
    assert CURRENT_ROW_ORDER == (
        "definition_major", "definition_minor", "revision_seq", "computed_at", "source_set_hash",
    )
    for mart in MART_VIEWS:
        if mart in STRUCTURAL_MARTS_PENDING_GOLD2:
            continue
        assert f"smart_traffic.{mart}" in DDL
    assert "definition_major DESC, definition_minor DESC, revision_seq DESC, computed_at DESC, source_set_hash DESC" in DDL
    assert "row_number() OVER" in DDL
    # No unbounded FINAL and no lex order on version strings in ORDER BY of views.
    assert " FINAL" not in DDL
    assert "ORDER BY definition_version" not in DDL


def test_semantic_version_v10_is_newer_than_v2():
    assert parse_definition_version("v10.0") > parse_definition_version("v2.0")
    assert parse_definition_version("v2.0") > parse_definition_version("v1.0")
    # Lexicographic string order would wrongly rank v10 below v2.
    assert "v10.0" < "v2.0"


def test_late_revision_and_tie_break_are_deterministic():
    older = {
        "definition_major": 1, "definition_minor": 0, "revision_seq": 1,
        "computed_at": 100, "source_set_hash": "a" * 64, "value": "old",
    }
    newer_rev = {
        "definition_major": 1, "definition_minor": 0, "revision_seq": 2,
        "computed_at": 50, "source_set_hash": "b" * 64, "value": "new_rev",
    }
    assert select_current_row([older, newer_rev])["value"] == "new_rev"

    left = {
        "definition_major": 1, "definition_minor": 0, "revision_seq": 2,
        "computed_at": 100, "source_set_hash": "a" * 64, "value": "a",
    }
    right = {
        "definition_major": 1, "definition_minor": 0, "revision_seq": 2,
        "computed_at": 100, "source_set_hash": "b" * 64, "value": "b",
    }
    assert select_current_row([left, right])["value"] == "b"


def test_higher_definition_major_wins_over_revision():
    v2 = {
        "definition_major": 2, "definition_minor": 0, "revision_seq": 1,
        "computed_at": 1, "source_set_hash": "0" * 64, "value": "v2",
    }
    v1_high_rev = {
        "definition_major": 1, "definition_minor": 9, "revision_seq": 99,
        "computed_at": 999, "source_set_hash": "f" * 64, "value": "v1",
    }
    assert select_current_row([v2, v1_high_rev])["value"] == "v2"


def test_consumer_need_not_deduplicate_contract():
    # Each non-structural mart SQL filters _gold_rn = 1.
    assert DDL.count("WHERE _gold_rn = 1") == 5
