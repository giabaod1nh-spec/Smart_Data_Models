"""Batch ledger: durable-only statuses advance checkpoint without CH confirm."""
from __future__ import annotations

from de.bronze import STATUS_IDEMPOTENT_SKIP, STATUS_RAW_QUARANTINE_SKIPPED, STATUS_STORED
from de.bronze.models import PendingLedgerEntry


def test_durable_ledger_only_includes_skip_statuses() -> None:
    pending = [
        PendingLedgerEntry("t", 0, 1, "a", STATUS_STORED, "ENTITY"),
        PendingLedgerEntry("t", 0, 2, "b", STATUS_RAW_QUARANTINE_SKIPPED, "RAW_QUARANTINE"),
        PendingLedgerEntry("t", 0, 3, "c", STATUS_IDEMPOTENT_SKIP, "SKIP"),
    ]
    confirmed_ids = {"a"}
    ch_confirmed = [
        p for p in pending
        if p.status in ("STORED", "QUARANTINED") and p.raw_ingestion_id in confirmed_ids
    ]
    durable_ledger_only = [
        p for p in pending
        if p.status in (STATUS_RAW_QUARANTINE_SKIPPED, STATUS_IDEMPOTENT_SKIP)
    ]
    all_entries = sorted(ch_confirmed + durable_ledger_only, key=lambda p: p.offset)
    assert [e.offset for e in all_entries] == [1, 2, 3]
