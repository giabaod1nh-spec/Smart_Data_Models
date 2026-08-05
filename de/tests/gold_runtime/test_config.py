"""Configuration contract: no implicit default closes a P0-locked value."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from de.gold.contracts import GOLD_SCHEMA_VERSION, WINDOW_SIZES_SEC
from de.gold_runtime.config import (
    ALLOWED_SOURCE_TABLES,
    BACKOFF_SCHEDULE_SEC,
    DEFAULT_HEALTH_PORT,
    GOLD_DATABASE,
    GoldConfigError,
    GoldSettings,
    is_replay_namespace,
    replay_namespace,
    validate_namespace,
)
from de.tests.gold_runtime.conftest import make_settings


def test_required_cadences_have_no_implicit_default(monkeypatch):
    for name in (
        "GOLD_TRAFFIC_EXPECTED_CADENCE_SEC",
        "GOLD_INTERSECTION_EXPECTED_CADENCE_SEC",
        "GOLD_SIGNAL_EXPECTED_CADENCE_SEC",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError):
        GoldSettings()


def test_fixed_contract_values(settings):
    assert settings.clickhouse_database == GOLD_DATABASE
    assert settings.definition_version == "v1.0"
    assert settings.gold_schema_version == GOLD_SCHEMA_VERSION
    assert settings.allowed_lateness_sec == 0
    assert settings.watermark_delay_sec == 0
    assert settings.health_port == DEFAULT_HEALTH_PORT
    assert settings.window_size_list() == tuple(WINDOW_SIZES_SEC)
    assert settings.retry_max_attempts == len(BACKOFF_SCHEDULE_SEC)
    assert settings.silver_fetch_batch_size == 500
    assert settings.processor_version == "gold-runtime-v1"
    assert settings.run_scope == "all" and settings.processes_all_runs()


def test_source_allowlist_is_the_plan_51_list(settings):
    assert settings.source_table_list() == ALLOWED_SOURCE_TABLES


@pytest.mark.parametrize(
    "overrides",
    [
        {"allowed_lateness_sec": 5.0},
        {"watermark_delay_sec": 1.0},
        {"silver_fetch_batch_size": 501},
        {"clickhouse_database": "other_db"},
        {"gold_schema_version": "k10-gold-m1-v2"},
        {"definition_version": "v2.0"},
        {"health_port": 0},
        {"traffic_expected_cadence_sec": 0.0},
        {"retry_max_attempts": 8},
    ],
)
def test_field_validators_reject_contract_violations(tmp_path, overrides):
    with pytest.raises(ValidationError):
        make_settings(tmp_path, **overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_tables": ""},
        {"source_tables": "bronze_entity_events"},
        {"source_tables": "silver_quarantine"},
        {"source_tables": "silver_dim_run,silver_dim_run"},
        {"window_sizes_sec": "60"},
        {"window_sizes_sec": "60,300,900"},
        {"window_sizes_sec": "60,60"},
        {"namespace": "prod"},
        {"replay_id": "abc"},
        {"dry_run": True},
        {"backfill_start": "2026-01-01T00:00:00Z"},
    ],
)
def test_cross_field_validation_rejects_bad_live_config(tmp_path, overrides):
    with pytest.raises(GoldConfigError):
        make_settings(tmp_path, **overrides)


def test_cadence_alias_only_when_all_three_match(tmp_path):
    ok = make_settings(tmp_path, expected_observation_cadence_sec=10.0)
    assert ok.expected_observation_cadence_sec == 10.0
    with pytest.raises(GoldConfigError):
        make_settings(tmp_path, expected_observation_cadence_sec=7.0)
    with pytest.raises(GoldConfigError):
        make_settings(
            tmp_path,
            signal_expected_cadence_sec=5.0,
            expected_observation_cadence_sec=10.0,
        )


def test_checkpoint_path_is_not_shared_with_silver(tmp_path):
    with pytest.raises(GoldConfigError):
        make_settings(tmp_path, checkpoint_path=str(tmp_path / "silver" / "checkpoint.sqlite3"))


def test_replay_namespace_grammar_and_guards(tmp_path):
    assert validate_namespace("live") == "live"
    assert is_replay_namespace(replay_namespace("r1"))
    with pytest.raises(GoldConfigError):
        validate_namespace("replay:")
    replay = make_settings(
        tmp_path,
        destination_mode="replay",
        replay_id="r1",
        namespace="replay:r1",
        checkpoint_path=str(tmp_path / "replay.sqlite3"),
        instance_lock_path=str(tmp_path / "replay.lock"),
    )
    assert replay.is_replay()
    with pytest.raises(GoldConfigError):
        make_settings(tmp_path, destination_mode="replay", replay_id="r1", namespace="live")


def test_run_scope_can_restrict_runs(tmp_path):
    scoped = make_settings(tmp_path, run_scope="run-1,run-2")
    assert scoped.run_scope_list() == ("run-1", "run-2")
    assert not scoped.processes_all_runs()


def test_retry_delay_follows_the_silver_schedule(settings):
    assert [settings.retry_delay(index) for index in range(9)] == [
        0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0
    ]


def test_password_is_redacted(tmp_path):
    redacted = make_settings(tmp_path, clickhouse_password="secret").redacted_dict()
    assert redacted["clickhouse_password"] == "***"
