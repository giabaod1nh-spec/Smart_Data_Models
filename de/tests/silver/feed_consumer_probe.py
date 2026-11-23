"""Test-only durable cursor probe. It never writes Gold state."""
import hashlib
import json
import os
import sqlite3
import sys
from contextlib import closing

from de.silver.incremental_contract import CONTRACT_VERSION


def initialize(path, binding):
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS cursor_state(
                kind TEXT PRIMARY KEY, binding TEXT NOT NULL, sequence INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS ack(receipt_id TEXT PRIMARY KEY, canonical_page BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS transitions(
                kind TEXT NOT NULL, sequence INTEGER NOT NULL, transition_hash TEXT NOT NULL,
                PRIMARY KEY(kind, sequence));
        """)
        db.executemany("INSERT OR IGNORE INTO cursor_state VALUES (?, ?, 0)",
                       [(kind, binding) for kind in ("CHANGE", "INVALIDATION")])
        if db.execute("SELECT DISTINCT binding FROM cursor_state").fetchall() != [(binding,)]:
            raise ValueError("test consumer binding changed")


def acknowledge(path, canonical_page, crash=None):
    page = json.loads(canonical_page)
    receipt = page["receipt"]
    if page["status"] != "OK" or not receipt or page["contract_version"] != CONTRACT_VERSION:
        raise ValueError("non-progress page")
    entries = page["entries"]
    kind = receipt["receipt_kind"]
    header = {key: value for key, value in receipt.items()
              if key not in {"encoded_bytes", "batch_digest", "receipt_id"}}
    from de.silver.incremental_contract import canonical_bytes
    digest = hashlib.sha256(canonical_bytes(dict(header, entries=entries))).hexdigest()
    rid = "sha256:" + digest
    if (receipt["receipt_id"] != rid or receipt["batch_digest"] != digest or
            receipt["encoded_bytes"] != page["encoded_bytes"] or
            len(canonical_page) != page["encoded_bytes"] or canonical_page != canonical_bytes(page)):
        raise ValueError("receipt mismatch")
    after = int(receipt["returned_after_sequence"])
    through = int(receipt["returned_through_sequence"])
    key = "change_sequence" if kind == "CHANGE" else "invalidation_sequence"
    if (not entries or page["entry_count"] != len(entries) or receipt["entry_count"] != len(entries) or
            int(page["after_" + key]) != after or int(page["through_" + key]) != through or
            page["source_binding_id"] != receipt["source_binding_id"] or
            int(receipt["requested_after_sequence"]) != after or
            [int(entry[key]) for entry in entries] != list(range(after + 1, through + 1))):
        raise ValueError("incomplete sequence coverage")
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        binding, cursor = db.execute("SELECT binding,sequence FROM cursor_state WHERE kind=?", (kind,)).fetchone()
        if binding != receipt["source_binding_id"]:
            raise ValueError("binding mismatch")
        seen = db.execute("SELECT canonical_page FROM ack WHERE receipt_id=?", (rid,)).fetchone()
        if seen:
            if seen[0] != canonical_page:
                raise ValueError("receipt identity collision")
            return
        if cursor != after:
            for entry in entries:
                prior = db.execute("SELECT transition_hash FROM transitions WHERE kind=? AND sequence=?",
                                   (kind, int(entry[key]))).fetchone()
                if prior and prior[0] != entry["source_transition_hash"]:
                    raise ValueError("immutable transition differs")
            raise ValueError("cursor mismatch")
        if kind == "CHANGE":
            invalidated = db.execute("SELECT sequence FROM cursor_state WHERE kind='INVALIDATION'").fetchone()[0]
            if invalidated < int(receipt["safe_invalidation_through_sequence"]):
                raise ValueError("invalidation watermark unacknowledged")
        db.executemany("INSERT INTO transitions VALUES (?,?,?)",
                       [(kind, int(entry[key]), entry["source_transition_hash"]) for entry in entries])
        db.execute("INSERT INTO ack VALUES (?,?)", (rid, canonical_page))
        db.execute("UPDATE cursor_state SET sequence=? WHERE kind=?", (through, kind))
        if crash == "before":
            os._exit(73)
    if crash == "after":
        os._exit(74)


if __name__ == "__main__":
    acknowledge(sys.argv[1], sys.stdin.buffer.read(), sys.argv[2])
