"""Controlled cleanup of Smart Traffic entities before an OSM topology cutover.

Dry-run is the default. Deletion requires both --execute and the confirmation
token so this tool cannot accidentally wipe unrelated Orion-LD data.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import configuration.config as cfg  # noqa: E402

ENTITY_TYPES = ("Intersection", "Camera", "TrafficLight", "VehicleSensor")
CONFIRMATION_TOKEN = "DELETE-SMART-TRAFFIC"


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/ld+json"}
    tenant = os.getenv("ORION_TENANT", "").strip()
    fiware_service = os.getenv("ORION_FIWARE_SERVICE", "").strip()
    service_path = os.getenv("ORION_SERVICE_PATH", "").strip()
    if tenant:
        headers["NGSILD-Tenant"] = tenant
    if fiware_service:
        headers["Fiware-Service"] = fiware_service
    if service_path:
        headers["Fiware-ServicePath"] = service_path
    return headers


def _is_project_entity(entity_id: str, entity_type: str) -> bool:
    return entity_id.startswith(f"urn:ngsi-ld:{entity_type}:")


def discover_entities(session: requests.Session, base_url: str) -> list[tuple[str, str]]:
    entities_url = f"{base_url.rstrip('/')}/ngsi-ld/v1/entities"
    found: list[tuple[str, str]] = []
    for entity_type in ENTITY_TYPES:
        offset = 0
        while True:
            response = session.get(
                entities_url,
                params={"type": entity_type, "limit": 1000, "offset": offset},
                headers=_headers(),
                timeout=10,
            )
            response.raise_for_status()
            page = response.json()
            if not isinstance(page, list):
                raise RuntimeError(f"Unexpected Orion response for type {entity_type}")
            for entity in page:
                entity_id = str(entity.get("id") or "")
                if _is_project_entity(entity_id, entity_type):
                    found.append((entity_type, entity_id))
            if len(page) < 1000:
                break
            offset += len(page)
    return found


def delete_entities(
    session: requests.Session,
    base_url: str,
    entities: Iterable[tuple[str, str]],
) -> int:
    entities_url = f"{base_url.rstrip('/')}/ngsi-ld/v1/entities"
    deleted = 0
    for _entity_type, entity_id in entities:
        response = session.delete(
            f"{entities_url}/{quote(entity_id, safe='')}",
            headers=_headers(),
            timeout=10,
        )
        if response.status_code not in (204, 404):
            response.raise_for_status()
        if response.status_code == 204:
            deleted += 1
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orion-url", default=cfg.ORION_URL)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()

    try:
        with requests.Session() as session:
            entities = discover_entities(session, args.orion_url)
            for entity_type, entity_id in entities:
                print(f"{entity_type}\t{entity_id}")
            print(f"matched={len(entities)}")

            if not args.execute:
                print("dry-run only; no entities deleted")
                return
            if args.confirm != CONFIRMATION_TOKEN:
                raise SystemExit(
                    f"Refusing deletion: pass --confirm {CONFIRMATION_TOKEN} after reviewing dry-run output"
                )
            deleted = delete_entities(session, args.orion_url, entities)
            print(f"deleted={deleted}")
    except requests.RequestException as exc:
        raise SystemExit(f"Orion cleanup failed for {args.orion_url}: {exc}") from exc


if __name__ == "__main__":
    main()
