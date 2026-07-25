"""
Register an NGSI-LD subscription for Orion delivery verification.

Anti-false-pass rules:
- unique subscription id per run (default) OR delete fixed id first
- POST must return HTTP 201 Created only
- verify Location / id then GET subscription → 200
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_TEMPLATE = ROOT / "contracts" / "delivery" / "subscription_template.json"
HEADERS = {"Content-Type": "application/ld+json"}


def orion_base() -> str:
    return os.getenv("ORION_URL", "http://localhost:1026").rstrip("/")


def delete_subscription(sub_id: str, timeout: float = 10.0) -> None:
    url = f"{orion_base()}/ngsi-ld/v1/subscriptions/{quote(sub_id, safe='')}"
    try:
        requests.delete(url, headers=HEADERS, timeout=timeout)
    except requests.RequestException:
        pass


def get_subscription(sub_id: str, timeout: float = 10.0) -> Tuple[int, Dict[str, Any]]:
    url = f"{orion_base()}/ngsi-ld/v1/subscriptions/{quote(sub_id, safe='')}"
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    body: Dict[str, Any] = {}
    try:
        body = r.json() if r.content else {}
    except Exception:
        body = {}
    return r.status_code, body


def create_subscription(
    notify_uri: str,
    *,
    sub_id: Optional[str] = None,
    delete_first: bool = False,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """
    Create subscription. Returns dict with id, location, get_body.
    Raises AssertionError / RuntimeError on non-201 or failed GET.
    """
    template = json.loads(CONTRACT_TEMPLATE.read_text(encoding="utf-8"))
    sid = sub_id or f"urn:ngsi-ld:Subscription:verify-{uuid.uuid4()}"
    if delete_first:
        delete_subscription(sid, timeout=timeout)

    payload = dict(template)
    payload["id"] = sid
    payload["description"] = (
        f"Orion delivery verification (temporary) id={sid}"
    )
    payload["notification"] = dict(template.get("notification") or {})
    payload["notification"]["endpoint"] = {
        "uri": notify_uri,
        "accept": "application/json",
    }
    if "@context" not in payload:
        payload["@context"] = (
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        )

    url = f"{orion_base()}/ngsi-ld/v1/subscriptions/"
    r = requests.post(url, json=payload, headers=HEADERS, timeout=timeout)
    if r.status_code != 201:
        raise RuntimeError(
            f"Subscription create must be HTTP 201 Created; got {r.status_code}: "
            f"{r.text[:400]}"
        )

    location = r.headers.get("Location") or r.headers.get("location") or ""
    # Prefer explicit id we sent
    code, got = get_subscription(sid, timeout=timeout)
    if code != 200:
        raise RuntimeError(
            f"GET subscription after create failed: HTTP {code} body={got!r}"
        )
    return {
        "id": sid,
        "location": location,
        "create_status": r.status_code,
        "get_status": code,
        "get_body": got,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--notify-uri", required=True, help="Receiver webhook URL")
    p.add_argument("--id", default=None, help="Fixed subscription id (optional)")
    p.add_argument(
        "--delete-first",
        action="store_true",
        help="DELETE existing subscription with --id before create",
    )
    args = p.parse_args()
    if args.id and not args.delete_first:
        # fixed id without cleanup is unsafe for re-runs
        print(
            "WARN: fixed --id without --delete-first may 409 on re-run",
            file=sys.stderr,
        )
    info = create_subscription(
        args.notify_uri, sub_id=args.id, delete_first=args.delete_first
    )
    print(json.dumps(info, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
