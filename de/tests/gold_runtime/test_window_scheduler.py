"""Watermark, window eligibility, late-data and queue order."""
from __future__ import annotations

import pytest

from de.gold_runtime.checkpoint_store import WindowStateRow
from de.gold_runtime.config import LateClass, WindowState
from de.gold_runtime.revisions import RevisionAction, decide_revision, next_revision_seq
from de.gold_runtime.window_scheduler import (
    OutputFamily,
    StreamMaxima,
    WindowStateError,
    assert_transition,
    candidate_windows,
    classify_late_row,
    is_eligible,
    make_window_identity,
    previous_window,
    queue_order,
    runtime_watermark,
    watermark,
)


def test_family_watermark_formulas_match_contract_v1():
    maxima = StreamMaxima(traffic=120.0, intersection=110.0, signal=130.0, camera=140.0)
    assert watermark(OutputFamily.TRAFFIC, maxima) == 120.0
    assert watermark(OutputFamily.INTERSECTION, maxima) == 110.0
    assert watermark(OutputFamily.SIGNAL, maxima) == 130.0
    assert watermark(OutputFamily.NETWORK, maxima) == 110.0
    assert runtime_watermark(maxima) == 110.0
    missing = StreamMaxima(traffic=120.0, intersection=None, signal=130.0)
    assert watermark(OutputFamily.INTERSECTION, missing) is None
    assert not is_eligible(60.0, None)


def test_eligibility_requires_watermark_strictly_after_window_end():
    assert is_eligible(60.0, 60.0) is False
    assert is_eligible(60.0, 60.0001) is True


def test_queue_order_oldest_end_then_60_before_300():
    w60 = make_window_identity("live", "run", "sc", 60, 0.0)       # end=60
    w60_later = make_window_identity("live", "run", "sc", 60, 60.0) # end=120
    w300 = make_window_identity("live", "run", "sc", 300, 0.0)     # end=300
    # Same end: 60s before 300s
    same_end_60 = make_window_identity("live", "run", "sc", 60, 240.0)   # end=300
    same_end_300 = make_window_identity("live", "run", "sc", 300, 0.0)   # end=300
    ordered = queue_order([w300, w60_later, w60])
    assert ordered == (w60, w60_later, w300)
    tied = queue_order([same_end_300, same_end_60])
    assert tied[0].window_size_sec == 60
    assert tied[1].window_size_sec == 300


def test_previous_window_same_size():
    current = make_window_identity("live", "run", "sc", 60, 60.0)
    prev = previous_window(current)
    assert prev.window_size_sec == 60
    assert prev.window_start_sim_sec == 0.0
    assert prev.window_end_sim_sec == 60.0


def test_late_classification_and_revision_cap():
    assert classify_late_row(
        simulation_time_sec=50.0,
        window_end_sim_sec=60.0,
        window_state=WindowState.OPEN,
        source_set_hash_changed=False,
    ) == LateClass.LATE_BEFORE_CLOSE
    # Post-close late row still inside the window bounds with a new source-set hash.
    assert classify_late_row(
        simulation_time_sec=55.0,
        window_end_sim_sec=60.0,
        window_state=WindowState.CLOSED,
        source_set_hash_changed=True,
    ) == LateClass.LATE_AFTER_CLOSE
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
    revise = decide_revision(
        window_state=closed,
        observed_source_set_hash="b" * 64,
        late_class=LateClass.LATE_AFTER_CLOSE,
    )
    assert revise.action == RevisionAction.REVISE
    assert next_revision_seq(0) == 1
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
    capped = decide_revision(
        window_state=revised,
        observed_source_set_hash="c" * 64,
        late_class=LateClass.LATE_AFTER_CLOSE,
    )
    assert capped.action == RevisionAction.QUARANTINE


def test_state_transitions_are_guarded():
    assert assert_transition(WindowState.OPEN, WindowState.ELIGIBLE) == WindowState.ELIGIBLE
    assert assert_transition(WindowState.CLOSED, WindowState.REVISED) == WindowState.REVISED
    with pytest.raises(WindowStateError):
        assert_transition(WindowState.CLOSED, WindowState.OPEN)


def test_candidate_windows_require_network_watermark_past_end():
    maxima = StreamMaxima(traffic=130.0, intersection=125.0, signal=140.0)
    windows = candidate_windows(
        namespace="live",
        simulation_run_id="run",
        scenario_id="sc",
        maxima=maxima,
        window_sizes_sec=(60, 300),
        limit=3,
    )
    assert windows
    assert all(is_eligible(w.window_end_sim_sec, runtime_watermark(maxima)) for w in windows)
