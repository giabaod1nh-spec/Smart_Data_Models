from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from de.tools.k6_subscription_lifecycle import (
    EvidenceJournal,
    LifecycleContract,
    LifecycleError,
    SubscriptionLifecycleClient,
    SubscriptionState,
    canonical_sha256,
    restore_payload,
)


class FakeResponse:
    def __init__(self, status_code: int, body=None) -> None:
        self.status_code = status_code
        self._body = body
        self.content = b"" if body is None else json.dumps(body).encode()
        self.text = self.content.decode()

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError("unexpected HTTP call")
        response = self.responses.pop(0)
        if isinstance(response, requests.RequestException):
            raise response
        return response


SUBSCRIPTION = {
    "id": "urn:ngsi-ld:Subscription:rt-de-v1",
    "type": "Subscription",
    "entities": [{"type": "Intersection"}],
    "notification": {"endpoint": {"uri": "http://de-webhook:8080/webhook/ngsi"}},
    "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    "isActive": True,
}


def snapshot_artifact(body=SUBSCRIPTION):
    return {
        "subscription": body,
        "restore_payload": restore_payload(body),
        "subscription_sha256": canonical_sha256(body),
    }


def client(tmp_path: Path, responses) -> SubscriptionLifecycleClient:
    return SubscriptionLifecycleClient(
        orion_url="http://orion:1026",
        subscription_id=SUBSCRIPTION["id"],
        contract=LifecycleContract(),
        session=FakeSession(responses),
        journal=EvidenceJournal(tmp_path, "k6b-unit"),
    )


def test_disable_is_expected_state_and_get_verified(tmp_path):
    disabled = {**SUBSCRIPTION, "isActive": False}
    c = client(
        tmp_path,
        [FakeResponse(200, SUBSCRIPTION), FakeResponse(204), FakeResponse(200, disabled)],
    )
    result = c.disable(snapshot_artifact())
    assert result["isActive"] is False
    assert [call[0] for call in c.session.calls] == ["GET", "PATCH", "GET"]
    journal = (tmp_path / "state_journal.jsonl").read_text(encoding="utf-8")
    assert '"action": "disable"' in journal
    assert '"phase": "before"' in journal
    assert '"phase": "after"' in journal


def test_unregister_requires_disabled_and_verifies_absent(tmp_path):
    disabled = {**SUBSCRIPTION, "isActive": False}
    c = client(
        tmp_path,
        [FakeResponse(200, disabled), FakeResponse(204), FakeResponse(404)],
    )
    c.unregister(snapshot_artifact())
    assert [call[0] for call in c.session.calls] == ["GET", "DELETE", "GET"]


def test_restore_absent_uses_same_snapshot_and_verifies_active(tmp_path):
    c = client(
        tmp_path,
        [FakeResponse(404), FakeResponse(201), FakeResponse(200, SUBSCRIPTION)],
    )
    result = c.restore(snapshot_artifact())
    assert result["id"] == SUBSCRIPTION["id"]
    assert [call[0] for call in c.session.calls] == ["GET", "POST", "GET"]
    assert c.session.calls[1][2]["json"]["id"] == SUBSCRIPTION["id"]


def test_unexpected_http_status_is_hard_failure(tmp_path):
    c = client(tmp_path, [FakeResponse(500, {"error": "boom"})])
    with pytest.raises(LifecycleError, match="expected HTTP"):
        c.get()


def test_snapshot_hash_mismatch_fails_closed(tmp_path):
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot_artifact()), encoding="utf-8")
    with pytest.raises(LifecycleError, match="hash mismatch"):
        SubscriptionLifecycleClient.load_snapshot(path, "0" * 64)


def test_verify_absent_does_not_accept_connection_failure(tmp_path):
    c = client(tmp_path, [requests.ConnectionError("down")])
    with pytest.raises(LifecycleError, match="failed"):
        c.verify_absent(delays=(0,))


def test_get_state_detects_disabled(tmp_path):
    c = client(tmp_path, [FakeResponse(200, {**SUBSCRIPTION, "isActive": False})])
    state, _ = c.get()
    assert state is SubscriptionState.DISABLED
