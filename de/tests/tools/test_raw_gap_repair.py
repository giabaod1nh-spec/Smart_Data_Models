from __future__ import annotations

from de.kafka_raw.ledger_store import LedgerStore, STATUS_STORED
from de.tools.raw_gap_repair import _ranges


def test_ranges_compacts_missing_offsets() -> None:
    assert _ranges([5, 2, 3, 4, 9, 11, 10]) == [(2, 5), (9, 11)]


def test_batch_ledger_write_is_durable(tmp_path) -> None:
    store = LedgerStore(tmp_path / "ledger.sqlite3")
    store.mark_complete_batch(
        [
            {
                "topic": "traffic.entity-events.v2",
                "partition": 1,
                "offset": 42,
                "raw_ingestion_id": "rid-42",
                "destination": "RAW",
                "status": STATUS_STORED,
                "event_id": "event-42",
                "payload_bytes_hash": "hash-42",
            }
        ]
    )
    assert store.is_complete("traffic.entity-events.v2", 1, 42)
    store.close()
