"""Record stopped fixture counters and preserve SQLite backups without changing their rows."""
from contextlib import ExitStack, closing
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3

from de.tools.gold_core_capacity import verify_pipeline


outbox = Path("/app/recovery-evidence/outbox.sqlite3")
raw = Path("/app/de/artifacts/kafka_raw_v1/ledger.sqlite3")
bronze, silver, gold = (Path(os.environ[key]) for key in
                       ("SILVER_V1_BRONZE_PATH", "SILVER_CHECKPOINT_PATH", "GOLD_CHECKPOINT_PATH"))
folder = Path("/app/recovery-evidence")
states = {}
with ExitStack() as stack:
    for label, path in dict(outbox=outbox, raw=raw, bronze=bronze, silver=silver, gold=gold).items():
        mode = "ro" if label == "outbox" else "rw"
        db = stack.enter_context(closing(sqlite3.connect(path.as_uri() + "?mode=" + mode, uri=True)))
        db.execute("PRAGMA query_only=ON")
        states[label] = db
        backup_path = folder / f"{label}.sqlite3"
        if label == "outbox":
            assert db.execute("PRAGMA quick_check").fetchone() == ("ok",)
            continue
        if backup_path.exists():
            raise RuntimeError(f"Refusing to overwrite {backup_path.name}")
        with closing(sqlite3.connect(backup_path)) as dest:
            db.backup(dest)
            assert dest.execute("PRAGMA quick_check").fetchone() == ("ok",)
    o, r, b, s, g = (states[key] for key in ("outbox", "raw", "bronze", "silver", "gold"))
    values = dict(
        outbox_statuses=dict(o.execute("SELECT status,count(*) FROM kafka_outbox GROUP BY status")),
        outbox_rows_digest=sha256(json.dumps(o.execute(
            "SELECT event_id,payload_json FROM kafka_outbox ORDER BY event_id").fetchall(),
            ensure_ascii=False, separators=(",", ":")).encode()).hexdigest(),
        raw_states={f"{dest}/{state}": n for dest, state, n in r.execute(
            "SELECT destination,state,count(*) FROM deliveries GROUP BY destination,state")},
        raw_committed_offsets=r.execute(
            "SELECT partition_id,committed_next_offset,safe_next_offset FROM partition_progress ORDER BY partition_id"
        ).fetchall(),
        bronze_states=dict(b.execute(
            "SELECT processing_status,count(*) FROM v1_intents WHERE namespace='live' GROUP BY processing_status")),
        bronze_allocators=b.execute(
            "SELECT next_change-1,next_invalidation-1 FROM v1_sequence WHERE namespace='live'").fetchone(),
        silver_cursors=s.execute(
            "SELECT change_cursor,invalidation_cursor FROM silver_v1_source_binding WHERE namespace='live'").fetchone(),
        silver_obligation_statuses=dict(s.execute(
            "SELECT status,count(*) FROM silver_v1_obligation WHERE namespace='live' GROUP BY status")),
        gold_current_work=g.execute("""SELECT w.duration,w.start,w.end,c.state,c.input_count
            FROM core_window w JOIN core_work c ON w.id=c.window_id AND w.generation=c.generation
            ORDER BY w.duration,w.start""").fetchall(),
        prefix=verify_pipeline(outbox, bronze, gold),
    )
    print(json.dumps(values, sort_keys=True), flush=True)
