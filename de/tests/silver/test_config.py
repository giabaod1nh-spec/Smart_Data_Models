"""Unit tests for Silver Plan 3 config / namespace guards."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from de.silver.config import (
    SOURCE_TABLE_ENTITY,
    SOURCE_TABLES,
    SilverConfigError,
    SilverSettings,
    SourceStream,
    assert_live_namespace,
    assert_replay_namespace,
    live_namespace,
    replay_namespace,
    make_test_namespace,
    validate_namespace_id,
)


def test_defaults_and_batch_range():
    s = SilverSettings(checkpoint_path=":memory:")
    assert s.batch_size == 500
    assert s.health_port == 8095
    assert s.namespace == "live"
    assert s.poll_interval_sec == 2.0
    with pytest.raises(ValidationError):
        SilverSettings(batch_size=0)
    with pytest.raises(ValidationError):
        SilverSettings(batch_size=501)
    with pytest.raises(ValidationError):
        SilverSettings(worker_count=2)


def test_namespace_helpers():
    assert live_namespace() == "live"
    assert replay_namespace("run-1") == "replay:run-1"
    assert make_test_namespace("t1") == "test:t1"
    with pytest.raises(SilverConfigError):
        validate_namespace_id("../evil")
    with pytest.raises(SilverConfigError):
        assert_live_namespace("replay:x")
    assert_replay_namespace("replay:abc", "abc")


def test_mode_guards_live_and_replay():
    live = SilverSettings(namespace="live", destination_mode="main", replay_run_id="")
    live.validate_mode_guards()
    bad = SilverSettings(namespace="live", destination_mode="main", replay_run_id="x")
    with pytest.raises(SilverConfigError):
        bad.validate_mode_guards()
    rep = SilverSettings(
        namespace="replay:r1",
        destination_mode="replay",
        replay_run_id="r1",
    )
    rep.validate_mode_guards()


def test_password_redaction():
    s = SilverSettings(clickhouse_password="secret")
    assert s.redacted_dict()["clickhouse_password"] == "***"


def test_source_stream_allowlist():
    SourceStream(SOURCE_TABLE_ENTITY, "t", 0)
    with pytest.raises(ValueError):
        SourceStream("bronze_other", "t", 0)
    assert SOURCE_TABLES == frozenset({"bronze_entity_events", "bronze_run_events"})
