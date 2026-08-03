"""S4-B container runtime contract checks (Compose stack must be up)."""
from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
CP = REPO / "de" / "artifacts" / "silver" / "checkpoint.sqlite3"
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
        pytest.skip(f"Silver health endpoint unavailable: {exc}")


def test_health_endpoint_returns_live_main_namespace():
    status, payload = _get(HEALTH)
    assert status == 200
    assert payload.get("namespace") == "live"
    assert payload.get("mode") == "main"
    assert "metrics" in payload


def test_ready_eventually_200_when_processor_progressing():
    # Snapshot max-age is 5s; poll briefly for a fresh READY snapshot.
    last = None
    for _ in range(40):
        status, payload = _get(READY, timeout=2.0)
        last = (status, payload)
        if status == 200 and payload.get("ready") is True:
            return
    pytest.fail(f"/ready did not return 200; last={last}")


def test_checkpoint_sqlite_exists_on_mounted_volume():
    assert CP.is_file(), f"missing checkpoint at {CP}"
    con = sqlite3.connect(str(CP))
    try:
        rows = con.execute(
            "SELECT checkpoint_namespace, COUNT(*) FROM silver_checkpoint GROUP BY 1"
        ).fetchall()
    finally:
        con.close()
    assert any(ns == "live" and n > 0 for ns, n in rows), rows
