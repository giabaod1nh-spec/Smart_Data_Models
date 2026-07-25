"""
Live E2E: mapper payloads → Orion-LD (pinned) → Subscription → thin webhook.

Requires Orion reachable at ORION_URL (default http://localhost:1026).
Uses production entity_mapper with a synthetic snapshot (Contract v1 fields).
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
VIS = ROOT / "Visualize"
INTEGRATION_DIR = Path(__file__).resolve().parent

ORION = os.getenv("ORION_URL", "http://localhost:1026").rstrip("/")
HEADERS = {"Content-Type": "application/ld+json"}
CONTRACTS = ROOT / "contracts"
CAPTURED = INTEGRATION_DIR / "captured" / "notification.captured.example.json"


def _load_thin_webhook():
    path = INTEGRATION_DIR / "thin_webhook.py"
    spec = importlib.util.spec_from_file_location("rt_de_thin_webhook", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.ThinWebhook


def _orion_ready() -> bool:
    try:
        r = requests.get(f"{ORION}/version", timeout=3)
        return r.status_code == 200
    except requests.RequestException:
        return False


@pytest.fixture(scope="module")
def require_orion():
    if not _orion_ready():
        pytest.fail(
            f"Orion not ready at {ORION}. Start compose (fiware/orion-ld:1.7.1) "
            "before freeze-gate E2E."
        )


def _synthetic_snapshot() -> dict:
    return {
        "phase": "NS_GREEN",
        "next_phase": "NS_YELLOW",
        "phase_remaining": 20.0,
        "phase_duration": 42,
        "green_duration": 42,
        "yellow_duration": 3,
        "red_duration": 42,
        "colors": {
            "North": "green",
            "South": "green",
            "East": "red",
            "West": "red",
        },
        "simulation_time_sec": 42.0,
        "simulation_run_id": str(uuid.uuid4()),
        "scenario": "normal",
        "incidents": [],
        "directions": {
            d: {
                "vehicle_count": 2,
                "pcu_equivalent": 1.5,
                "left_count": 0,
                "straight_count": 2,
                "right_count": 0,
                "average_speed_kmh": 20.0,
                "waiting_vehicle_count": 1,
                "queue_length_m": 8.0,
                "queue_by_movement": {"straight": 8.0, "left": 0.0, "right": 0.0},
                "occupancy_pct": 10.0,
                "density": "LOW",
                "arrival_rate_pcu_per_sec": 0.1,
                "waiting_reason_counts": {"RED_PHASE": 1, "CONGESTION": 0},
                "dominant_waiting_reason": "RED_PHASE",
                "theoretical_speed_kmh": 40.0,
            }
            for d in ["North", "South", "East", "West"]
        },
        "derived_traffic_state": "FREE_FLOW",
        "derived_phenomena": {
            "spillback_active": False,
            "box_blocked": False,
            "spillback_risk": False,
        },
        "operational_state": {
            "incident_active": False,
            "emergency_preemption_active": False,
            "downstream_restriction_active": False,
        },
        "probable_causes": [],
        "direction_contexts": {},
        "direction_states": {},
    }


def test_live_subscription_notification(require_orion):
    jsonschema = pytest.importorskip("jsonschema")
    sys.path.insert(0, str(VIS))
    from integration.orion.client import reset_created_cache, upsert_entity
    from integration.orion.entity_mapper import build_all_entities

    ThinWebhook = _load_thin_webhook()
    reset_created_cache()
    wh = ThinWebhook()
    wh.start()
    sub_id = f"urn:ngsi-ld:Subscription:e2e-{uuid.uuid4()}"
    try:
        notify_host = os.getenv("ORION_NOTIFY_HOST", "host.docker.internal")
        notify_url = wh.url.replace("127.0.0.1", notify_host).replace(
            "localhost", notify_host
        )
        if os.getenv("ORION_NOTIFY_URL"):
            notify_url = os.environ["ORION_NOTIFY_URL"]

        sub = {
            "id": sub_id,
            "type": "Subscription",
            "entities": [
                {"type": "Intersection"},
                {"type": "TrafficLight"},
                {"type": "VehicleSensor"},
                {"type": "Camera"},
            ],
            "notification": {
                "format": "normalized",
                "endpoint": {"uri": notify_url, "accept": "application/json"},
            },
            "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
        }
        r = requests.post(
            f"{ORION}/ngsi-ld/v1/subscriptions",
            json=sub,
            headers=HEADERS,
            timeout=10,
        )
        assert r.status_code in (201, 204, 200), (
            f"subscribe failed: {r.status_code} {r.text[:300]}"
        )

        time.sleep(1.0)
        snap = _synthetic_snapshot()
        for ent in build_all_entities("A", snap):
            upsert_entity(ent)

        body = wh.wait_for_body(timeout=45.0)
        CAPTURED.parent.mkdir(parents=True, exist_ok=True)
        CAPTURED.write_text(json.dumps(body, indent=2), encoding="utf-8")

        schema = json.loads(
            (CONTRACTS / "delivery" / "notification.schema.json").read_text(
                encoding="utf-8"
            )
        )
        if "data" not in body and isinstance(body.get("body"), dict):
            body = body["body"]

        assert "data" in body, f"unexpected notify shape keys={list(body.keys())}"
        found_run = False
        for ent in body["data"]:
            sr = ent.get("simulationRunId") or {}
            if isinstance(sr, dict) and sr.get("value"):
                found_run = True
            if ent.get("type") in ("TrafficLight", "Intersection"):
                assert "currentPhase" in ent
        assert found_run, "notification data missing simulationRunId"

        if body.get("type") == "Notification":
            jsonschema.validate(instance=body, schema=schema)
    finally:
        try:
            requests.delete(
                f"{ORION}/ngsi-ld/v1/subscriptions/{quote(sub_id, safe='')}",
                headers=HEADERS,
                timeout=5,
            )
        except Exception:
            pass
        wh.stop()
