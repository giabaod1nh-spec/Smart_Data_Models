import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from de.silver.incremental_contract import (
    CanonicalBatchReceipt, CanonicalChangePage, CanonicalChangePageRequest,
    batch_digest, canonical_bytes, canonical_hash, size_page,
)


def _vectors():
    doc = Path(__file__).resolve().parents[3] / "docs/DEv1/phase-4/pre/contract-design.md"
    for line in doc.read_text(encoding="utf-8").splitlines():
        if line.startswith(("| Composed Unicode", "| Decomposed Unicode",
                            "| Ordered entries", "| Numeric formatting")):
            name, raw, size, hashes, envelope = (item.strip() for item in line.split("|")[1:-1])
            digest, receipt_id = (item.strip("` ") for item in hashes.split(" / "))
            yield name, raw.strip("`").encode(), int(size), digest, receipt_id, int(envelope)


@pytest.mark.parametrize("name,raw,size,digest,receipt_id,envelope", list(_vectors()))
def test_frozen_jcs_vectors(name, raw, size, digest, receipt_id, envelope):
    values = json.loads(raw)
    if name.startswith("Numeric"):
        values["entries"][0]["numeric_probe"] = dict(a=1.0, b=1e30, c=2e-3, d=-0.0)
    assert canonical_bytes(values) == raw
    assert len(raw) == size
    assert hashlib.sha256(raw).hexdigest() == digest
    entries = tuple(values.pop("entries"))
    receipt = CanonicalBatchReceipt(**values)
    assert batch_digest(receipt, entries) == digest
    boundary = {key: value for key, value in values.items() if key.startswith("captured_")}
    header = dict(contract_version=receipt.contract_version, source_binding_id=receipt.source_binding_id)
    page = CanonicalChangePage(
        **header, **boundary, namespace=receipt.namespace, after_change_sequence=receipt.returned_after_sequence,
        through_change_sequence=receipt.returned_through_sequence, entries=entries, entry_count=len(entries),
        verified_change_prefix=receipt.captured_silver_change_applied_sequence,
        safe_invalidation_through_sequence=receipt.safe_invalidation_through_sequence,
        boundary_id="sha256:" + canonical_hash(dict(header, **boundary)),
        has_more=False, status="OK", receipt=receipt,
    )
    assert size_page(page, sum(len(canonical_bytes(entry)) for entry in entries)) == envelope
    page = replace(page, encoded_bytes=envelope, receipt=replace(
        receipt, encoded_bytes=envelope, receipt_id=receipt_id, batch_digest=digest))
    assert len(canonical_bytes(page)) == envelope


def test_sequence_projection_and_request_immutability():
    request = CanonicalChangePageRequest(
        namespace="live", after_change_sequence=(1 << 63) - 1, expected_source_binding_id="b")
    assert json.loads(canonical_bytes(request))["after_change_sequence"] == "9223372036854775807"
    with pytest.raises(FrozenInstanceError):
        request.namespace = "other"
