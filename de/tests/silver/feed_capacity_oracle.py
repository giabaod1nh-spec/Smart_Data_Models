"""Post-run verification of the focused feed evidence from independent stores."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import rfc8785


def read_only(path):
    db = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    return closing(db)


def verify(fixture):
    """Compare every acknowledged transition and frozen fact to the authorities."""
    transitions = ("namespace", "change_sequence", "event_id", "revision", "change_type",
                   "previous_state", "new_state", "content_hash", "invalidation_id")
    seq = bundles = pages = bytes_verified = 0
    with (
        read_only(fixture / "capacity-probe.db") as probe,
        read_only(fixture / "bronze_v1/state.sqlite3") as bronze,
        read_only(fixture / "silver_v1/state.sqlite3") as silver,
    ):
        journal = iter(bronze.execute("SELECT * FROM v1_changes WHERE namespace='live' ORDER BY change_sequence"))
        for (raw,) in probe.execute("SELECT canonical_page FROM ack ORDER BY rowid"):
            page = json.loads(raw)
            receipt = page["receipt"]
            assert receipt["receipt_kind"] == "CHANGE"  # Approved load capture has no semantic conflicts.
            header = {key: value for key, value in receipt.items()
                      if key not in {"batch_digest", "encoded_bytes", "receipt_id"}}
            digest = hashlib.sha256(rfc8785.dumps(dict(header, entries=page["entries"]))).hexdigest()
            assert digest == receipt["batch_digest"]
            assert receipt["receipt_id"] == "sha256:" + digest
            assert page["encoded_bytes"] == receipt["encoded_bytes"] == len(raw)
            assert page["entry_count"] == receipt["entry_count"] == len(page["entries"])
            assert page["entries"] and int(receipt["returned_after_sequence"]) == seq
            for entry in page["entries"]:
                seq += 1
                row = next(journal)
                assert row["change_sequence"] == seq == int(entry["change_sequence"])
                assert row["event_id"] == entry["event_id"]
                assert row["revision"] == entry["semantic_revision"]
                assert row["change_type"] == entry["change_type"] == "ACCEPTED"
                assert entry["effective_disposition"] == "BUNDLE_VALID"
                source = {key: row[key] for key in transitions}
                source["change_sequence"] = str(source["change_sequence"])
                assert hashlib.sha256(rfc8785.dumps(source)).hexdigest() == entry["source_transition_hash"]
                assert probe.execute(
                    "SELECT transition_hash FROM transitions WHERE kind='CHANGE' AND sequence=?", (seq,)
                ).fetchone()[0] == entry["source_transition_hash"]
                obligation_rows = silver.execute(
                    "SELECT expected_json FROM silver_v1_obligation WHERE namespace='live' "
                    "AND source_kind='changes' AND source_sequence=? AND status='RECONCILED' "
                    "AND target_table LIKE '%fact_v1'", (seq,))
                oracle_facts = [json.loads(item[0]) for item in obligation_rows]
                for fact in oracle_facts:
                    fact["cycle_sequence"] = str(fact["cycle_sequence"])
                bundle = entry["canonical_bundle"]
                assert bundle["event_id"] == row["event_id"]
                assert sorted(map(rfc8785.dumps, oracle_facts)) == sorted(map(rfc8785.dumps, [
                    bundle["intersection"], *bundle["signal"], *bundle["traffic"],
                ]))
                bundles += 1
            assert int(receipt["returned_through_sequence"]) == seq
            pages += 1
            bytes_verified += len(raw)
        assert pages and next(journal, None) is None
        assert dict(probe.execute("SELECT kind,sequence FROM cursor_state")) == {"CHANGE": seq, "INVALIDATION": 0}
    return dict(exact_oracle_match=True, verified_sequences=seq, verified_bundles=bundles,
                verified_receipts=pages, verified_bytes=bytes_verified)


def assess(fixture, evidence):
    """Recalculate the approved gates from a completed run without replaying it."""
    report = json.loads((evidence / "capacity-result.json").read_text(encoding="utf-8"))
    oracle = verify(fixture)
    start, steady, burst, end = json.loads((evidence / "schedule.json").read_text(encoding="utf-8"))
    arrivals = [json.loads(line) for line in (evidence / "applied.jsonl").read_text().splitlines()]
    logs = [json.loads(line) for line in (evidence / "consumer.jsonl").read_text().splitlines()]
    assert logs[-1]["kind"] == "end" and logs[-1]["time"] >= end
    assert (steady - start, burst - steady, end - burst) == (600, 120, 300)
    progress = [line for line in logs if line["kind"] in {"page", "sample", "end"}]

    def through(rows, field, cutoff):
        return max((row[field] for row in rows if row["time"] < cutoff), default=0)

    cadence = report["wall_cycle_interval_sec"]
    per_cycle = report["intersections_per_cycle"]
    expected_steady = 600 // cadence * per_cycle
    expected_burst = 120 // cadence * per_cycle
    assert through(arrivals, "change", steady) == through(progress, "cursor", steady) == expected_steady
    assert through(arrivals, "change", burst) - through(progress, "cursor", burst) == expected_burst
    assert logs[-1]["cursor"] == arrivals[-1]["change"] == oracle["verified_sequences"] == report["offered_events"]
    assert logs[-1]["encoded_bytes"] == oracle["verified_bytes"]
    assert logs[-1]["pages"] == oracle["verified_receipts"]
    samples = [line for line in logs if line["kind"] == "sample"]
    lags = [max(0, through(arrivals, "change", row["time"]) - row["cursor"])
            for row in samples if start + 30 <= row["time"] < steady]
    assert max(lags[-60:]) <= max(lags[:60]) + per_cycle
    recovered_at = next(row["time"] for row in progress if burst <= row["time"] < end and
                        row["cursor"] >= expected_steady + expected_burst and
                        row["cursor"] >= through(arrivals, "change", row["time"]))
    peak = max(row["peak_rss"] for row in samples)
    assert peak <= 512 * 2**20
    return dict(
        **oracle, steady_event_count=expected_steady, steady_event_rate=expected_steady / 600,
        burst_backlog=expected_burst, recovery_seconds=recovered_at - burst, peak_rss_bytes=peak,
        early_steady_backlog_max=max(lags[:60]), late_steady_backlog_max=max(lags[-60:]),
        verdict="SUSTAINABLE AT TESTED WORKLOAD", burst_verdict="BURST RECOVERY VERIFIED",
    )


if __name__ == "__main__":
    result = assess(Path(sys.argv[1]), Path(sys.argv[2])) if len(sys.argv) == 3 else verify(Path(sys.argv[1]))
    print(json.dumps(result, indent=2))
