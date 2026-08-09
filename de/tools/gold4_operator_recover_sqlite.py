"""Operator recovery for stuck Gold SQLite window/work-unit rows (runbook).

Does not change Gold3 code. Marks non-terminal PROCESSING windows and work units
so a restarted live writer can resume without CAS conflict.
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        default=str(Path("de/artifacts/gold/checkpoint.sqlite3")),
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    path = Path(args.path)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    stuck_windows = list(
        con.execute(
            "SELECT window_id, revision_seq, state, batch_id "
            "FROM gold_runtime_window_state "
            "WHERE state NOT IN ('CLOSED','REVISED')"
        )
    )
    stuck_units = list(
        con.execute(
            "SELECT batch_id, window_id, state "
            "FROM gold_runtime_work_unit "
            "WHERE state NOT IN ('CHECKPOINTED','REPLAYED','QUARANTINED','CONFLICTED')"
        )
    )
    print("stuck_windows", [dict(r) for r in stuck_windows])
    print("stuck_units", [dict(r) for r in stuck_units])
    if not args.apply:
        print("dry-run; pass --apply to quarantine stuck rows")
        return 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    con.execute("BEGIN IMMEDIATE")
    # Drop non-terminal window rows so the scheduler can reopen the same identity.
    con.execute(
        "DELETE FROM gold_runtime_window_state "
        "WHERE state NOT IN ('CLOSED','REVISED')"
    )
    # Terminal quarantine for incomplete work units so they are not re-entered.
    con.execute(
        "UPDATE gold_runtime_work_unit "
        "SET state='QUARANTINED', last_error='operator_recover_stuck_processing', updated_at=? "
        "WHERE state NOT IN ('CHECKPOINTED','REPLAYED','QUARANTINED','CONFLICTED')",
        (now,),
    )
    con.commit()
    print("applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
