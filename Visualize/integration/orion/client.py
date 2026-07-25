"""
orion_client.py — NGSI-LD Context Broker REST client for Visualize.

POST once to create an entity; subsequent updates use PATCH attrs.
Soft-fails on network errors so SUMO keeps running when the broker is down.
"""
from __future__ import annotations

import logging
import time

import requests

import configuration.config as cfg

log = logging.getLogger(__name__)

HEADERS = {"Content-Type": "application/ld+json"}

_created: set[str] = set()


def _entities_url() -> str:
    return f"{cfg.ORION_URL.rstrip('/')}/ngsi-ld/v1/entities"


def wait_orion_ready(retries: int = 30, delay: float = 3.0) -> bool:
    version_url = f"{cfg.ORION_URL.rstrip('/')}/version"
    for i in range(retries):
        try:
            r = requests.get(version_url, timeout=3)
            if r.status_code == 200:
                log.info("Context Broker is ready at %s", cfg.ORION_URL)
                return True
        except requests.exceptions.RequestException:
            pass
        log.info("Waiting for Context Broker... (%d/%d)", i + 1, retries)
        time.sleep(delay)
    raise RuntimeError(
        f"Context Broker at {cfg.ORION_URL} did not become ready in time"
    )


def upsert_entity(entity: dict) -> None:
    eid = entity["id"]
    if eid in _created:
        _patch(entity)
    else:
        _post_then_fallback(entity)


def _post_then_fallback(entity: dict) -> None:
    eid = entity["id"]
    try:
        r = requests.post(_entities_url(), json=entity, headers=HEADERS, timeout=5)
        if r.status_code == 201:
            _created.add(eid)
        elif r.status_code == 409:
            _created.add(eid)
            _patch(entity)
        else:
            log.warning("POST %s -> %s: %s", eid, r.status_code, r.text[:150])
    except requests.exceptions.RequestException as e:
        log.error("POST failed for %s: %s", eid, e)


def _patch(entity: dict) -> None:
    eid = entity["id"]
    # Orion-LD requires @context when Content-Type is application/ld+json
    attrs = {k: v for k, v in entity.items() if k not in ("id", "type")}
    url = f"{_entities_url()}/{eid}/attrs"
    try:
        r = requests.patch(url, json=attrs, headers=HEADERS, timeout=5)
        # 207 Multi-Status = UpdateResult (attrs in "updated" / "notUpdated")
        if r.status_code in (200, 204, 207):
            if r.status_code == 207:
                try:
                    body = r.json() or {}
                    not_updated = body.get("notUpdated") or []
                    missing = []
                    for item in not_updated:
                        reason = str(item.get("reason") or "").lower()
                        name = item.get("attributeName")
                        if name and "doesn't exist" in reason:
                            missing.append(name)
                    if missing:
                        append_payload = {
                            k: attrs[k] for k in missing if k in attrs
                        }
                        # Keep @context for ld+json
                        if "@context" in attrs:
                            append_payload["@context"] = attrs["@context"]
                        ar = requests.post(
                            url, json=append_payload, headers=HEADERS, timeout=5
                        )
                        if ar.status_code not in (201, 204, 207, 200):
                            log.warning(
                                "POST append attrs %s missing=%s -> %s: %s",
                                eid,
                                missing,
                                ar.status_code,
                                ar.text[:150],
                            )
                        else:
                            # Re-PATCH so values are current after append
                            requests.patch(
                                url, json=attrs, headers=HEADERS, timeout=5
                            )
                    elif not_updated:
                        log.warning(
                            "PATCH %s partial notUpdated=%s", eid, not_updated
                        )
                except Exception as e:
                    log.warning("PATCH %s follow-up failed: %s", eid, e)
            return
        log.warning("PATCH %s -> %s: %s", eid, r.status_code, r.text[:150])
    except requests.exceptions.RequestException as e:
        log.error("PATCH failed for %s: %s", eid, e)


def reset_created_cache() -> None:
    """Clear in-memory create cache (for tests)."""
    _created.clear()
