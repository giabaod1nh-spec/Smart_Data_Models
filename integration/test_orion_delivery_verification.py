"""
Orion Subscription Delivery Verification harness (pre-DE gateway).

Anti-false-pass:
- subscription create == HTTP 201 only + GET 200
- unique subscription id per run
- delivery success = receiver recorded body AND Orion notification.status != failed
- SUMO multi-tick publish + Orion GET attribute delta
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
VIS = ROOT / "Visualize"
INTEGRATION_DIR = Path(__file__).resolve().parent
CONTRACTS = ROOT / "contracts"
SCHEMA_PATH = CONTRACTS / "delivery" / "notification.schema.json"
CAPTURED = INTEGRATION_DIR / "captured" / "notification.captured.example.json"
SCRIPTS = INTEGRATION_DIR / "scripts"

ORION = os.getenv("ORION_URL", "http://localhost:1026").rstrip("/")
HEADERS = {"Content-Type": "application/ld+json"}
PUBLISH_CYCLES = int(os.getenv("VERIFY_PUBLISH_CYCLES", "3"))
MAX_SIM_SEC = float(os.getenv("VERIFY_MAX_SIM_SEC", "30"))
ENTITY_ID = os.getenv(
    "VERIFY_ENTITY_ID", "urn:ngsi-ld:Intersection:A"
)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


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
            f"Orion not ready at {ORION}. "
            "Start: docker compose up -d mongo-db orion (fiware/orion-ld:1.7.1)."
        )


def _prop_value(entity: Dict[str, Any], name: str) -> Any:
    node = entity.get(name)
    if not isinstance(node, dict):
        return None
    return node.get("value")


def _get_entity(entity_id: str) -> Dict[str, Any]:
    url = f"{ORION}/ngsi-ld/v1/entities/{quote(entity_id, safe='')}"
    r = requests.get(url, headers={"Accept": "application/ld+json"}, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"GET entity {entity_id} -> {r.status_code}: {r.text[:200]}")
    return r.json()


def _notification_status(sub_body: Dict[str, Any]) -> Optional[str]:
    notif = sub_body.get("notification") or {}
    if isinstance(notif, dict):
        st = notif.get("status")
        if st:
            return str(st).lower()
    st2 = sub_body.get("status")
    return str(st2).lower() if st2 else None


def _notify_url(local_url: str) -> str:
    if os.getenv("ORION_NOTIFY_URL"):
        return os.environ["ORION_NOTIFY_URL"]
    host = os.getenv("ORION_NOTIFY_HOST", "host.docker.internal")
    return local_url.replace("127.0.0.1", host).replace("localhost", host)


def test_orion_subscription_delivery_gate(require_orion):
    jsonschema = pytest.importorskip("jsonschema")

    sys.path.insert(0, str(VIS))
    from integration.orion.client import reset_created_cache, upsert_entity, wait_orion_ready
    from integration.orion.entity_mapper import build_all_entities
    from simulation.backend import SumoBackend
    import configuration.config as cfg
    from app.traci_runner import publish_once

    reg = _load_module(
        "register_subscription", SCRIPTS / "register_subscription.py"
    )
    rx_mod = _load_module(
        "temp_receiver_app", INTEGRATION_DIR / "receiver" / "app.py"
    )

    wait_orion_ready(retries=15, delay=2.0)
    reset_created_cache()

    rx = rx_mod.TemporaryNotificationReceiver(
        host="127.0.0.1", port=0, capture_path=CAPTURED, save_every=True
    )
    rx.start()
    sub_id = f"urn:ngsi-ld:Subscription:verify-{uuid.uuid4()}"
    backend = None
    try:
        notify_uri = _notify_url(rx.url)
        info = reg.create_subscription(notify_uri, sub_id=sub_id, delete_first=True)
        assert info["create_status"] == 201
        assert info["get_status"] == 200
        assert info["id"] == sub_id

        # Short SUMO run with multi-tick Orion publish
        os.environ.setdefault("SUMO_GUI", "false")
        backend = SumoBackend(
            sumo_config=cfg.SUMO_CONFIG,
            use_gui=False,
            publish_nodes=cfg.PUBLISH_NODES,
        )
        backend.start()

        publish_interval = float(os.getenv("PUBLISH_INTERVAL", str(cfg.PUBLISH_INTERVAL)))
        last_pub = -1e9
        cycles = 0
        t0_val = None
        t1_val = None

        deadline = time.time() + max(60.0, MAX_SIM_SEC * 3)
        while cycles < PUBLISH_CYCLES and time.time() < deadline:
            cont = backend.step()
            sim_t = backend.simulation_time_sec
            if sim_t - last_pub >= publish_interval:
                n = publish_once(backend, upsert_entity, build_all_entities)
                assert n > 0, "publish_once returned 0 entities"
                cycles += 1
                last_pub = sim_t
                ent = _get_entity(ENTITY_ID)
                val = _prop_value(ent, "simulationTime")
                if t0_val is None:
                    t0_val = val
                else:
                    t1_val = val
            if not cont:
                break
            if sim_t >= MAX_SIM_SEC and cycles >= PUBLISH_CYCLES:
                break

        assert cycles >= PUBLISH_CYCLES, (
            f"Need ≥{PUBLISH_CYCLES} publish cycles, got {cycles}"
        )
        assert t0_val is not None, "No simulationTime observed on Orion entity"
        # Require change across ticks when we have two samples
        if t1_val is not None:
            assert t1_val != t0_val, (
                f"Orion entity simulationTime did not change ({t0_val})"
            )
        else:
            # Force one more publish after waiting a step
            while backend.simulation_time_sec - last_pub < publish_interval:
                if not backend.step():
                    break
            publish_once(backend, upsert_entity, build_all_entities)
            t1_val = _prop_value(_get_entity(ENTITY_ID), "simulationTime")
            assert t1_val != t0_val, (
                f"Orion entity simulationTime did not change ({t0_val})"
            )

        body = rx.wait_for_body(timeout=60.0)
        assert rx.request_count >= 1
        assert body, "Empty notification body"

        # Delivery success: Orion must not report failed notification delivery
        code, sub_body = reg.get_subscription(sub_id)
        assert code == 200
        notif = sub_body.get("notification") or {}
        nst = ""
        if isinstance(notif, dict) and notif.get("status") is not None:
            nst = str(notif.get("status")).lower()
        assert nst != "failed", f"Orion notification.status=failed body={sub_body}"
        # When Orion exposes notification.status, require ok
        if nst:
            assert nst == "ok", f"Expected notification.status=ok, got {nst}"

        CAPTURED.parent.mkdir(parents=True, exist_ok=True)
        CAPTURED.write_text(json.dumps(body, indent=2), encoding="utf-8")

        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        # Normalize: some brokers wrap; require data[]
        assert "data" in body, f"captured missing data[] keys={list(body.keys())}"
        if body.get("type") == "Notification":
            jsonschema.validate(instance=body, schema=schema)
        else:
            # Still require Contract sim fields on entities
            wrapped = {
                "id": body.get("id") or "urn:ngsi-ld:Notification:captured",
                "type": "Notification",
                "data": body["data"],
            }
            jsonschema.validate(instance=wrapped, schema=schema)

        found_run = False
        for ent in body["data"]:
            sr = ent.get("simulationRunId") or {}
            if isinstance(sr, dict) and sr.get("value"):
                found_run = True
        assert found_run, "captured data[] missing simulationRunId Property"

    finally:
        try:
            reg.delete_subscription(sub_id)
        except Exception:
            pass
        if backend is not None:
            try:
                backend.stop()
            except Exception:
                pass
        rx.stop()
