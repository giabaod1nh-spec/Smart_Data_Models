"""Cursor codec, Appendix R predicate, ordering, dedup and conflict detection."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from de.gold_runtime.config import SOURCE_TABLE_TRAFFIC
from de.gold_runtime.cursor import (
    FACT_CURSOR_COLUMNS,
    ZERO_FACT_CURSOR,
    CursorError,
    DimensionCursor,
    FactCursor,
    build_receipt,
    cursor_parameters,
    deduplicate_rows,
    fact_cursor_order_by,
    fact_cursor_predicate,
    normalize_hash,
    row_identity,
    rows_source_set_hash,
)
from de.tests.gold_runtime.conftest import traffic_row

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def cursor(**overrides) -> FactCursor:
    values = {
        "processed_at": BASE,
        "source_topic": "t",
        "source_partition": 0,
        "source_offset": 10,
        "source_payload_hash": "aa",
    }
    values.update(overrides)
    return FactCursor(**values)


def test_cursor_tuple_is_the_locked_five_component_shape():
    assert FACT_CURSOR_COLUMNS == (
        "processed_at", "source_topic", "source_partition", "source_offset",
        "source_payload_hash",
    )


@pytest.mark.parametrize(
    "later",
    [
        {"processed_at": BASE + timedelta(milliseconds=1)},
        {"source_topic": "u"},
        {"source_partition": 1},
        {"source_offset": 11},
        {"source_payload_hash": "ab"},
    ],
)
def test_lexicographic_ordering_by_each_component(later):
    assert cursor(**later).is_after(cursor())
    assert not cursor().is_after(cursor(**later))


def test_equal_tuple_is_not_after_itself():
    assert not cursor().is_after(cursor())


def test_zero_cursor_precedes_every_real_row():
    assert cursor(source_partition=0, source_offset=0).is_after(ZERO_FACT_CURSOR)


def test_cursor_json_roundtrip():
    original = cursor()
    assert FactCursor.from_json(original.to_json()) == original


def test_cursor_rejects_null_topic():
    row = traffic_row()
    row["source_topic"] = ""
    with pytest.raises(CursorError):
        FactCursor.from_row(row)


def test_predicate_is_the_appendix_r_shape():
    predicate = fact_cursor_predicate()
    assert "processed_at > {p_at:DateTime64(3)}" in predicate
    assert "source_payload_hash) > {p_hash:String}" in predicate
    assert predicate.count("OR") == 8
    assert "OFFSET" not in predicate
    assert fact_cursor_order_by() == (
        "ORDER BY processed_at, source_topic, source_partition, source_offset, "
        "source_payload_hash"
    )
    from de.gold_runtime.cursor import fact_cursor_order_by_desc

    assert fact_cursor_order_by_desc() == (
        "ORDER BY processed_at DESC, source_topic DESC, source_partition DESC, "
        "source_offset DESC, source_payload_hash DESC"
    )


def test_cursor_parameters_are_bound_by_name():
    params = cursor_parameters(cursor(), "p")
    assert set(params) == {"p_at", "p_topic", "p_partition", "p_offset", "p_hash"}
    assert params["p_at"].tzinfo is not None
    assert params["p_at"].utcoffset().total_seconds() == 0


def test_identical_identity_and_hash_is_idempotent():
    rows = [traffic_row(offset=1), traffic_row(offset=1)]
    result = deduplicate_rows(SOURCE_TABLE_TRAFFIC, rows)
    assert result.duplicates == 1
    assert result.conflicts == ()
    assert len(result.rows) == 1


def test_same_identity_with_different_hash_is_a_conflict():
    rows = [
        traffic_row(offset=1, payload_hash="hash-a"),
        traffic_row(offset=2, payload_hash="hash-b"),
    ]
    result = deduplicate_rows(SOURCE_TABLE_TRAFFIC, rows)
    assert result.conflicts
    assert result.conflicts[0] == row_identity(SOURCE_TABLE_TRAFFIC, rows[0])


def test_receipt_reports_counts_and_source_set_hash():
    rows = sorted(
        [traffic_row(offset=1), traffic_row(offset=2, direction="S")],
        key=lambda row: FactCursor.from_row(row).key(),
    )
    receipt = build_receipt(SOURCE_TABLE_TRAFFIC, rows)
    assert receipt.physical_count == 2
    assert receipt.logical_count == 2
    assert receipt.duplicate_count == 0
    assert receipt.first_cursor.source_offset == 1
    assert receipt.last_cursor.source_offset == 2
    assert receipt.source_set_hash == rows_source_set_hash(SOURCE_TABLE_TRAFFIC, rows)


def test_source_set_hash_is_order_independent():
    rows = [traffic_row(offset=1), traffic_row(offset=2, direction="S")]
    assert rows_source_set_hash(SOURCE_TABLE_TRAFFIC, rows) == rows_source_set_hash(
        SOURCE_TABLE_TRAFFIC, list(reversed(rows))
    )


def test_unordered_batch_is_rejected():
    rows = [traffic_row(offset=2), traffic_row(offset=1)]
    with pytest.raises(CursorError):
        build_receipt(SOURCE_TABLE_TRAFFIC, rows)


def test_fixed_string_hash_is_normalized():
    assert normalize_hash(b"abc\x00\x00") == "abc"
    assert normalize_hash(memoryview(b"abc")) == "abc"


def test_dimension_cursor_orders_by_effective_time_then_stable_id():
    first = DimensionCursor(BASE, "h1", "a")
    second = DimensionCursor(BASE, "h1", "b")
    third = DimensionCursor(BASE + timedelta(seconds=1), "h0", "a")
    assert second.is_after(first)
    assert third.is_after(second)
    assert DimensionCursor.from_json(first.to_json()) == first
