"""S4-B container runtime contract checks (Compose stack must be up)."""
from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from de.silver.config import SilverSettings

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='requires running V1 Compose stack')

REPO = Path(__file__).resolve().parents[3]
CP = Path(SilverSettings().checkpoint_path)
HEALTH = "http://127.0.0.1:8095/health"
READY = "http://127.0.0.1:8095/ready"


def _get(url: str, timeout: float = 3.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {"raw": body}
        return exc.code, payload
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"Silver health endpoint unavailable during opted-in runtime acceptance: {exc}")


def test_health_endpoint_returns_live_main_namespace():
    status, payload = _get(HEALTH)
    assert status == 200
    assert payload.get("namespace") == "live"
    assert payload.get("mode") == "main"
    assert "metrics" in payload


def test_ready_eventually_200_when_processor_progressing():
    # Poll for a current READY snapshot under the configured freshness budget.
    last = None
    for _ in range(40):
        status, payload = _get(READY, timeout=2.0)
        last = (status, payload)
        if status == 200 and payload.get("ready") is True:
            return
    pytest.fail(f"/ready did not return 200; last={last}")


def test_checkpoint_sqlite_exists_on_mounted_volume():
    assert CP.is_file(), f"missing checkpoint at {CP}"
    con = sqlite3.connect(CP.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        rows = con.execute(
            "SELECT namespace, COUNT(*) FROM silver_v1_source_binding GROUP BY 1"
        ).fetchall()
    finally:
        con.close()
    assert any(ns == "live" and n > 0 for ns, n in rows), rows
