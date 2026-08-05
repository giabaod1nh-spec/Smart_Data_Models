"""Dimension lineage and revision decision surface."""
from __future__ import annotations

from de.gold_runtime.config import LateClass, WindowState
from de.gold_runtime.checkpoint_store import WindowStateRow
from de.gold_runtime.dimensions import lineage_hash
from de.gold_runtime.revisions import RevisionAction, decide_revision
from de.tests.gold_runtime.conftest import DIM_ROWS, SOURCE_TABLE_DIM_APPROACH, SOURCE_TABLE_DIM_RUN


def test_approach_lineage_excludes_absent_fields():
    row = dict(DIM_ROWS[SOURCE_TABLE_DIM_APPROACH][0])
    digest = lineage_hash(SOURCE_TABLE_DIM_APPROACH, row)
    assert len(digest) == 64
    # Fields outside the Contract v1 hash list do not participate.
    row2 = dict(row)
    row2["non_contract_noise"] = "ignored"
    assert lineage_hash(SOURCE_TABLE_DIM_APPROACH, row2) == digest
    # Absent optional hash inputs are omitted; no VALID/placeholder substitution.
    row3 = dict(row)
    del row3["updated_at"]
    assert lineage_hash(SOURCE_TABLE_DIM_APPROACH, row3) != digest


def test_run_lineage_changes_with_seed():
    row = dict(DIM_ROWS[SOURCE_TABLE_DIM_RUN][0])
    base = lineage_hash(SOURCE_TABLE_DIM_RUN, row)
    row["seed"] = "changed-seed"
    assert lineage_hash(SOURCE_TABLE_DIM_RUN, row) != base


def test_decide_revision_caps_at_one():
    closed = WindowStateRow(
        namespace="live",
        window_id="wid",
        revision_seq=0,
        state=WindowState.CLOSED.value,
        watermark=120.0,
        batch_id="b1",
        source_set_hash="a" * 64,
        output_digest="",
        attempt_count=1,
        updated_at="2026-01-01T00:00:00+00:00",
    )
    first = decide_revision(
        window_state=closed,
        observed_source_set_hash="b" * 64,
        late_class=LateClass.LATE_AFTER_CLOSE,
    )
    assert first.action is RevisionAction.REVISE
    revised = WindowStateRow(
        namespace="live",
        window_id="wid",
        revision_seq=1,
        state=WindowState.REVISED.value,
        watermark=120.0,
        batch_id="b2",
        source_set_hash="b" * 64,
        output_digest="",
        attempt_count=1,
        updated_at="2026-01-01T00:00:00+00:00",
    )
    second = decide_revision(
        window_state=revised,
        observed_source_set_hash="c" * 64,
        late_class=LateClass.LATE_AFTER_CLOSE,
    )
    assert second.action is RevisionAction.QUARANTINE
