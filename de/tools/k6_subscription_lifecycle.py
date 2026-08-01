"""Strict, evidence-first Orion-LD subscription lifecycle for K-6b.

This module never runs implicitly. Mutating commands require an expected state,
an immutable subscription snapshot hash, a run id and an evidence directory.
Unexpected HTTP statuses and unverifiable state are hard failures.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote

import requests


class LifecycleError(RuntimeError):
    """A fail-closed subscription lifecycle error."""


class SubscriptionState(str, Enum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"
    ABSENT = "ABSENT"


SERVER_MANAGED_FIELDS = frozenset(
    {
        "createdAt",
        "modifiedAt",
        "lastNotification",
        "lastFailure",
        "lastSuccess",
        "timesSent",
        "timesFailed",
    }
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def write_json_fsync(path: Path, value: Any) -> None:
    """Atomically publish a durable JSON artifact on the same filesystem."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(value, fh, indent=2, sort_keys=True, ensure_ascii=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(temporary, path)


def restore_payload(subscription: Mapping[str, Any]) -> dict[str, Any]:
    """Remove broker-generated fields while preserving subscription semantics."""
    return {
        key: value
        for key, value in subscription.items()
        if key not in SERVER_MANAGED_FIELDS
    }


def semantic_subscription(subscription: Mapping[str, Any]) -> dict[str, Any]:
    value = restore_payload(subscription)
    # Active state is lifecycle state, not subscription identity/semantics.
    value.pop("isActive", None)
    value.pop("status", None)
    return value


@dataclass(frozen=True)
class LifecycleContract:
    update_method: str = "PATCH"
    inactive_field: str = "isActive"
    inactive_value: Any = False
    active_value: Any = True
    get_status: int = 200
    update_status: int = 204
    create_status: int = 201
    delete_status: int = 204
    absent_status: int = 404
    content_type: str = "application/ld+json"

    @classmethod
    def load(cls, path: Path | None) -> "LifecycleContract":
        if path is None:
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        allowed = set(cls.__dataclass_fields__)
        unknown = set(data) - allowed
        if unknown:
            raise LifecycleError(f"unknown lifecycle contract fields: {sorted(unknown)}")
        contract = cls(**data)
        if contract.update_method.upper() not in {"PATCH", "PUT"}:
            raise LifecycleError("update_method must be PATCH or PUT")
        return contract

    @property
    def sha256(self) -> str:
        return canonical_sha256(asdict(self))


class EvidenceJournal:
    def __init__(self, evidence_dir: Path, run_id: str) -> None:
        if not run_id.strip():
            raise LifecycleError("run_id is required")
        self.evidence_dir = evidence_dir.resolve()
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.evidence_dir / "state_journal.jsonl"
        self.run_id = run_id

    def append(self, action: str, phase: str, **fields: Any) -> None:
        record = {
            "ts": _utc_now(),
            "run_id": self.run_id,
            "action": action,
            "phase": phase,
            **fields,
        }
        line = json.dumps(record, sort_keys=True, ensure_ascii=True, default=str)
        with self.path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())


class SubscriptionLifecycleClient:
    def __init__(
        self,
        *,
        orion_url: str,
        subscription_id: str,
        contract: LifecycleContract,
        timeout: float = 10.0,
        session: requests.Session | None = None,
        journal: EvidenceJournal | None = None,
    ) -> None:
        if not subscription_id.strip():
            raise LifecycleError("subscription_id is required")
        self.orion_url = orion_url.rstrip("/")
        self.subscription_id = subscription_id
        self.contract = contract
        self.timeout = timeout
        self.session = session or requests.Session()
        self.journal = journal

    @property
    def item_url(self) -> str:
        sid = quote(self.subscription_id, safe="")
        return f"{self.orion_url}/ngsi-ld/v1/subscriptions/{sid}"

    @property
    def collection_url(self) -> str:
        return f"{self.orion_url}/ngsi-ld/v1/subscriptions/"

    @property
    def headers(self) -> dict[str, str]:
        return {"Content-Type": self.contract.content_type}

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        try:
            return self.session.request(
                method,
                url,
                headers=self.headers,
                timeout=self.timeout,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise LifecycleError(f"{method} {url} failed: {exc}") from exc

    @staticmethod
    def _json_body(response: requests.Response) -> dict[str, Any]:
        if not response.content:
            return {}
        try:
            body = response.json()
        except ValueError as exc:
            raise LifecycleError(
                f"expected JSON response, got HTTP {response.status_code}"
            ) from exc
        if not isinstance(body, dict):
            raise LifecycleError("subscription response must be a JSON object")
        return body

    def get(self) -> tuple[SubscriptionState, dict[str, Any]]:
        response = self._request("GET", self.item_url)
        if response.status_code == self.contract.absent_status:
            return SubscriptionState.ABSENT, {}
        if response.status_code != self.contract.get_status:
            raise LifecycleError(
                f"GET subscription expected HTTP {self.contract.get_status} or "
                f"{self.contract.absent_status}, got {response.status_code}"
            )
        body = self._json_body(response)
        if str(body.get("id", self.subscription_id)) != self.subscription_id:
            raise LifecycleError("GET returned a different subscription id")
        inactive = body.get(self.contract.inactive_field)
        status = str(body.get("status", "")).strip().lower()
        if inactive == self.contract.inactive_value or status == "inactive":
            return SubscriptionState.DISABLED, body
        return SubscriptionState.ACTIVE, body

    def require_state(self, expected: SubscriptionState) -> dict[str, Any]:
        actual, body = self.get()
        if actual is not expected:
            raise LifecycleError(f"expected state {expected.value}, got {actual.value}")
        return body

    def snapshot(self, output: Path) -> dict[str, Any]:
        state, body = self.get()
        if state is SubscriptionState.ABSENT:
            raise LifecycleError("cannot snapshot an absent subscription")
        payload = restore_payload(body)
        artifact = {
            "captured_at": _utc_now(),
            "subscription_id": self.subscription_id,
            "state": state.value,
            "subscription": body,
            "restore_payload": payload,
            "subscription_sha256": canonical_sha256(body),
            "restore_payload_sha256": canonical_sha256(payload),
            "contract_sha256": self.contract.sha256,
        }
        write_json_fsync(output, artifact)
        return artifact

    @staticmethod
    def load_snapshot(path: Path, expected_sha256: str) -> dict[str, Any]:
        artifact = json.loads(path.read_text(encoding="utf-8"))
        actual = str(artifact.get("subscription_sha256", ""))
        body = artifact.get("subscription")
        if not isinstance(body, dict) or canonical_sha256(body) != actual:
            raise LifecycleError("snapshot body/hash is internally inconsistent")
        if actual != expected_sha256:
            raise LifecycleError(
                f"snapshot hash mismatch: expected {expected_sha256}, got {actual}"
            )
        return artifact

    def _journal(self, action: str, phase: str, **fields: Any) -> None:
        if self.journal is None:
            raise LifecycleError("mutating lifecycle action requires an evidence journal")
        self.journal.append(action, phase, contract_sha256=self.contract.sha256, **fields)

    def disable(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        before = self.require_state(SubscriptionState.ACTIVE)
        self._assert_snapshot_identity(before, snapshot)
        self._journal("disable", "before", state=SubscriptionState.ACTIVE.value)
        payload = {self.contract.inactive_field: self.contract.inactive_value}
        response = self._request(
            self.contract.update_method.upper(), self.item_url, json=payload
        )
        if response.status_code != self.contract.update_status:
            self._journal("disable", "failed", http_status=response.status_code)
            raise LifecycleError(
                f"disable expected HTTP {self.contract.update_status}, got {response.status_code}"
            )
        after = self.require_state(SubscriptionState.DISABLED)
        self._assert_snapshot_identity(after, snapshot)
        self._journal("disable", "after", state=SubscriptionState.DISABLED.value)
        return after

    def restore(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        state, before = self.get()
        self._journal("restore", "before", state=state.value)
        if state is SubscriptionState.ACTIVE:
            self._assert_snapshot_identity(before, snapshot)
            raise LifecycleError("subscription is already ACTIVE; restore refused")
        if state is SubscriptionState.DISABLED:
            self._assert_snapshot_identity(before, snapshot)
            payload = {self.contract.inactive_field: self.contract.active_value}
            response = self._request(
                self.contract.update_method.upper(), self.item_url, json=payload
            )
            expected = self.contract.update_status
        else:
            payload = snapshot.get("restore_payload")
            if not isinstance(payload, dict):
                raise LifecycleError("snapshot restore_payload is missing")
            response = self._request("POST", self.collection_url, json=payload)
            expected = self.contract.create_status
        if response.status_code != expected:
            self._journal("restore", "failed", http_status=response.status_code)
            raise LifecycleError(
                f"restore expected HTTP {expected}, got {response.status_code}"
            )
        after = self.require_state(SubscriptionState.ACTIVE)
        self._assert_snapshot_identity(after, snapshot)
        self._journal("restore", "after", state=SubscriptionState.ACTIVE.value)
        return after

    def unregister(self, snapshot: Mapping[str, Any]) -> None:
        before = self.require_state(SubscriptionState.DISABLED)
        self._assert_snapshot_identity(before, snapshot)
        self._journal("unregister", "before", state=SubscriptionState.DISABLED.value)
        response = self._request("DELETE", self.item_url)
        if response.status_code != self.contract.delete_status:
            self._journal("unregister", "failed", http_status=response.status_code)
            raise LifecycleError(
                f"unregister expected HTTP {self.contract.delete_status}, got {response.status_code}"
            )
        state, _ = self.get()
        if state is not SubscriptionState.ABSENT:
            raise LifecycleError("subscription still exists after unregister")
        self._journal("unregister", "after", state=SubscriptionState.ABSENT.value)

    def verify_absent(self, delays: Sequence[float] = (0.0, 10.0, 20.0)) -> None:
        for delay in delays:
            if delay > 0:
                time.sleep(delay)
            self.require_state(SubscriptionState.ABSENT)

    @staticmethod
    def _assert_snapshot_identity(
        actual: Mapping[str, Any], snapshot: Mapping[str, Any]
    ) -> None:
        expected = snapshot.get("subscription")
        if not isinstance(expected, dict):
            raise LifecycleError("snapshot subscription body is missing")
        if semantic_subscription(actual) != semantic_subscription(expected):
            raise LifecycleError("live subscription differs from immutable snapshot")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=[
        "get", "status", "snapshot", "disable", "verify-disabled",
        "restore", "unregister", "verify-absent",
    ])
    parser.add_argument("--orion-url", default="http://localhost:1026")
    parser.add_argument("--id", required=True, dest="subscription_id")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--contract", type=Path, default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--evidence-dir", type=Path, default=None)
    parser.add_argument("--snapshot", type=Path, default=None)
    parser.add_argument("--snapshot-sha256", default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--expected-state", choices=[s.value for s in SubscriptionState])
    return parser


def _require_mutation_args(args: argparse.Namespace) -> None:
    missing = []
    for name in ("run_id", "evidence_dir", "snapshot", "snapshot_sha256", "expected_state"):
        if getattr(args, name) in (None, ""):
            missing.append(f"--{name.replace('_', '-')}")
    if missing:
        raise LifecycleError(f"mutating command requires: {', '.join(missing)}")


def main() -> int:
    args = _parser().parse_args()
    contract = LifecycleContract.load(args.contract)
    mutating = args.command in {"disable", "restore", "unregister"}
    if mutating:
        _require_mutation_args(args)
    journal = (
        EvidenceJournal(args.evidence_dir, args.run_id)
        if mutating
        else None
    )
    client = SubscriptionLifecycleClient(
        orion_url=args.orion_url,
        subscription_id=args.subscription_id,
        contract=contract,
        timeout=args.timeout,
        journal=journal,
    )

    if args.command in {"get", "status"}:
        state, body = client.get()
        print(json.dumps({"state": state.value, "subscription": body}, indent=2))
        return 0
    if args.command == "snapshot":
        if args.output is None:
            raise LifecycleError("snapshot command requires --output")
        print(json.dumps(client.snapshot(args.output), indent=2))
        return 0
    if args.command == "verify-disabled":
        body = client.require_state(SubscriptionState.DISABLED)
        print(json.dumps({"state": "DISABLED", "subscription": body}, indent=2))
        return 0
    if args.command == "verify-absent":
        client.verify_absent()
        print(json.dumps({"state": "ABSENT", "verified": [0, 10, 30]}, indent=2))
        return 0

    expected_state = SubscriptionState(args.expected_state)
    client.require_state(expected_state)
    snapshot = client.load_snapshot(args.snapshot, args.snapshot_sha256)
    if args.command == "disable":
        result = client.disable(snapshot)
    elif args.command == "restore":
        result = client.restore(snapshot)
    else:
        client.unregister(snapshot)
        result = {"state": "ABSENT"}
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LifecycleError as exc:
        print(json.dumps({"pass": False, "error": str(exc)}))
        raise SystemExit(2) from exc
