"""
client.py — Orion NGSI-LD REST API client

Logic POST/PATCH: POST 1 lan tao entity, cac lan sau PATCH chi update attrs.
"""
import os
import time
import logging
import requests

log = logging.getLogger(__name__)

ORION_URL    = os.getenv("ORION_URL", "http://localhost:1026")
ENTITIES_URL = f"{ORION_URL}/ngsi-ld/v1/entities"
HEADERS      = {"Content-Type": "application/json"}

_created: set = set()


def wait_orion_ready(retries: int = 30, delay: float = 3.0) -> bool:
    for i in range(retries):
        try:
            r = requests.get(f"{ORION_URL}/version", timeout=3)
            if r.status_code == 200:
                log.info("Orion is ready.")
                return True
        except requests.exceptions.RequestException:
            pass
        log.info(f"Waiting for Orion... ({i + 1}/{retries})")
        time.sleep(delay)
    raise RuntimeError("Orion did not become ready in time")


def upsert_entity(entity: dict):
    eid = entity["id"]
    if eid in _created:
        _patch(entity)
    else:
        _post_then_fallback(entity)


def _post_then_fallback(entity: dict):
    eid = entity["id"]
    try:
        r = requests.post(ENTITIES_URL, json=entity, headers=HEADERS, timeout=5)
        if r.status_code == 201:
            _created.add(eid)
        elif r.status_code == 409:
            _created.add(eid)
            _patch(entity)
        else:
            log.warning(f"POST {eid} -> {r.status_code}: {r.text[:150]}")
    except requests.exceptions.RequestException as e:
        log.error(f"POST failed for {eid}: {e}")


def _patch(entity: dict):
    eid = entity["id"]
    attrs = {k: v for k, v in entity.items() if k not in ("id", "type", "@context")}
    try:
        r = requests.patch(f"{ENTITIES_URL}/{eid}/attrs", json=attrs, headers=HEADERS, timeout=5)
        if r.status_code not in (200, 204):
            log.warning(f"PATCH {eid} -> {r.status_code}: {r.text[:150]}")
    except requests.exceptions.RequestException as e:
        log.error(f"PATCH failed for {eid}: {e}")
