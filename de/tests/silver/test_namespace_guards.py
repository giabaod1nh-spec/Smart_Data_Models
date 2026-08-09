"""Namespace guard focused tests (Plan 3)."""
from __future__ import annotations

import pytest

from de.silver.config import SilverConfigError, SilverSettings, replay_namespace


def test_live_rejects_replay_namespace():
    s = SilverSettings(namespace="replay:x", destination_mode="main")
    with pytest.raises(SilverConfigError):
        s.validate_mode_guards()


def test_replay_rejects_live_namespace():
    s = SilverSettings(namespace="live", destination_mode="replay", replay_run_id="r1")
    with pytest.raises(SilverConfigError):
        s.validate_mode_guards()


def test_replay_namespace_derivation_locked():
    assert replay_namespace("abc_01") == "replay:abc_01"
    with pytest.raises(SilverConfigError):
        replay_namespace("bad/id")
