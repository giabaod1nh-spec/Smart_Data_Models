"""Scoped Orion-LD REST operations used only by the V1 projector."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Sequence
from urllib.parse import quote

import requests

from integration.projector.v1_projection import CONTEXT, ENTITY_TYPES


class RetryableOrionError(RuntimeError):
    pass


class PermanentOrionError(RuntimeError):
    pass


@dataclass(frozen=True)
class BatchOutcome:
    succeeded: frozenset[str]
    retryable: frozenset[str]
    permanent: frozenset[str]


class V1Orion:
    def __init__(self, base_url: str, *, tenant: str = "", fiware_service: str = "", service_path: str = "", timeout: float = 10.0, session: requests.Session | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.headers = {"Accept": "application/ld+json", "Content-Type": "application/ld+json"}
        if tenant:
            self.headers["NGSILD-Tenant"] = tenant
        if fiware_service:
            self.headers["Fiware-Service"] = fiware_service
        if service_path:
            self.headers["Fiware-ServicePath"] = service_path

    @classmethod
    def from_env(cls) -> "V1Orion":
        return cls(os.getenv("ORION_URL", "http://localhost:1026"), tenant=os.getenv("ORION_TENANT", ""), fiware_service=os.getenv("ORION_FIWARE_SERVICE", ""), service_path=os.getenv("ORION_SERVICE_PATH", ""))

    @property
    def entities_url(self) -> str:
        return f"{self.base_url}/ngsi-ld/v1/entities"

    def _request(self, method: str, url: str, **kwargs):
        try:
            return self.session.request(method, url, headers=self.headers, timeout=self.timeout, **kwargs)
        except requests.RequestException as exc:
            raise RetryableOrionError(f"{method} {url}: {exc}") from exc

    @staticmethod
    def _check(response, *, allow: tuple[int, ...]) -> None:
        if response.status_code in allow:
            return
        error = f"Orion HTTP {response.status_code}: {response.text[:300]}"
        if response.status_code in (400, 401, 403, 422):
            raise PermanentOrionError(error)
        raise RetryableOrionError(error)

    def ready(self) -> bool:
        response = self._request("GET", f"{self.base_url}/version")
        return response.status_code == 200

    def get(self, entity_id: str) -> dict | None:
        response = self._request("GET", f"{self.entities_url}/{quote(entity_id, safe='')}")
        if response.status_code == 404:
            return None
        self._check(response, allow=(200,))
        body = response.json()
        if not isinstance(body, dict) or body.get("id") != entity_id:
            raise RetryableOrionError(f"malformed Orion entity response for {entity_id}")
        return body

    def inventory(self) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for kind in ENTITY_TYPES:
            offset = 0
            while True:
                response = self._request("GET", self.entities_url, params={"type": kind, "limit": 1000, "offset": offset})
                self._check(response, allow=(200,))
                page = response.json()
                if not isinstance(page, list):
                    raise RetryableOrionError("Orion inventory response is not a list")
                for entity in page:
                    eid = entity.get("id") if isinstance(entity, dict) else None
                    if isinstance(eid, str) and eid.startswith(f"urn:ngsi-ld:{kind}:") and entity.get("type") == kind:
                        result[eid] = entity
                if len(page) < 1000:
                    break
                offset += len(page)
        return result

    def upsert(self, entities: Sequence[dict]) -> BatchOutcome:
        if not entities:
            return BatchOutcome(frozenset(), frozenset(), frozenset())
        ids = [entity["id"] for entity in entities]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate entity ID in batch")
        response = self._request("POST", f"{self.base_url}/ngsi-ld/v1/entityOperations/upsert?options=update", json=list(entities))
        if response.status_code in (201, 204):
            return BatchOutcome(frozenset(ids), frozenset(), frozenset())
        if response.status_code != 207:
            self._check(response, allow=(201, 204, 207))
        try:
            body: Any = response.json()
            success = body["success"]
            errors = body["errors"]
            if not isinstance(success, list) or not isinstance(errors, list):
                raise ValueError("invalid 207 lists")
            if len(success) != len(set(success)):
                raise ValueError("duplicate 207 success ID")
            succeeded = set(success)
            retryable, permanent = set(), set()
            for entry in errors:
                eid = entry["entityId"]
                if eid in succeeded or eid in retryable or eid in permanent:
                    raise ValueError("duplicate 207 result ID")
                nested = entry.get("error", {})
                status = int(nested.get("status", entry.get("status", 0)))
                (permanent if status in (400, 401, 403, 422) else retryable).add(eid)
            accounted = succeeded | retryable | permanent
            if (accounted - set(ids)) or len(accounted) != len(succeeded) + len(retryable) + len(permanent):
                raise ValueError("207 has unknown or duplicate entity IDs")
            retryable.update(set(ids) - accounted)
            return BatchOutcome(frozenset(succeeded), frozenset(retryable), frozenset(permanent))
        except (KeyError, TypeError, ValueError) as exc:
            raise RetryableOrionError(f"ambiguous Orion 207 response: {exc}") from exc

    def delete_attribute(self, entity_id: str, attribute: str) -> None:
        # Orion's entity and attribute path segments must be URL encoded.
        url = f"{self.entities_url}/{quote(entity_id, safe='')}/attrs/{quote(attribute, safe='')}"
        response = self._request("DELETE", url)
        if response.status_code == 404 and self.get(entity_id) is None:
            raise RetryableOrionError(f"entity disappeared while deleting {attribute}: {entity_id}")
        self._check(response, allow=(204, 404))

    def delete_entity(self, entity_id: str) -> None:
        response = self._request("DELETE", f"{self.entities_url}/{quote(entity_id, safe='')}")
        self._check(response, allow=(204, 404))


def local_attribute_name(name: str) -> str:
    if name == "https://uri.etsi.org/ngsi-ld/name":
        return "name"
    return name


def entity_is_exclusively_owned(entity: dict, static: frozenset[str], dynamic: frozenset[str]) -> bool:
    attrs = {local_attribute_name(key) for key in entity if key not in ("id", "type", "@context")}
    return attrs <= (static | dynamic)
