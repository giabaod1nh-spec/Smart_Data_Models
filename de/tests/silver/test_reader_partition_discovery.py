"""Partition discovery: distinct topic/partition, sort, run-before-entity priority (Plan 3 §7)."""
from __future__ import annotations

import pytest

from de.silver.config import SilverSettings, SourceStream
from de.silver.readers import BronzeReader
from de.silver.repositories import SchemaMismatchError
from de.tests.silver.conftest import FakeClient, FakeQueryResult


def test_discover_streams_sorted_topic_partition_priority():
    # First query issued is bronze_run_events, second is bronze_entity_events.
    run_rows = FakeQueryResult([("t1", 0), ("t2", 1)], ["topic", "partition"])
    entity_rows = FakeQueryResult([("t1", 0), ("t1", 1)], ["topic", "partition"])
    client = FakeClient(responses=[run_rows, entity_rows])
    reader = BronzeReader(SilverSettings(), client=client)

    streams = reader.discover_streams(("t1", "t2"))

    assert streams == (
        SourceStream("bronze_run_events", "t1", 0),
        SourceStream("bronze_entity_events", "t1", 0),
        SourceStream("bronze_entity_events", "t1", 1),
        SourceStream("bronze_run_events", "t2", 1),
    )


def test_discover_streams_rejects_negative_partition():
    run_rows = FakeQueryResult([("t1", -1)], ["topic", "partition"])
    client = FakeClient(responses=[run_rows])
    reader = BronzeReader(SilverSettings(), client=client)

    with pytest.raises(SchemaMismatchError):
        reader.discover_streams(("t1",))


def test_discover_streams_rejects_topic_outside_allowlist():
    # Defensive check even though the WHERE predicate should already filter this out.
    run_rows = FakeQueryResult([("unexpected-topic", 0)], ["topic", "partition"])
    client = FakeClient(responses=[run_rows])
    reader = BronzeReader(SilverSettings(), client=client)

    with pytest.raises(SchemaMismatchError):
        reader.discover_streams(("t1",))


def test_discover_streams_empty_allowlist_rejected():
    client = FakeClient(responses=[])
    reader = BronzeReader(SilverSettings(), client=client)

    with pytest.raises(SchemaMismatchError):
        reader.discover_streams(())


def test_discover_streams_merges_without_dropping_existing_relative_order():
    run_rows = FakeQueryResult([], ["topic", "partition"])
    entity_rows = FakeQueryResult([("t1", 2), ("t1", 0), ("t1", 1)], ["topic", "partition"])
    client = FakeClient(responses=[run_rows, entity_rows])
    reader = BronzeReader(SilverSettings(), client=client)

    streams = reader.discover_streams(("t1",))

    assert [s.partition for s in streams] == [0, 1, 2]
