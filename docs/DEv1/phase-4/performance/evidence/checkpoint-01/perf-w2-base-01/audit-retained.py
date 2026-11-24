from contextlib import ExitStack, closing, redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory

import clickhouse_connect

from de.tools.gold_capacity_evidence import audit_pipeline
from de.tools.gold_core_capacity import main as capacity_main


outbox = Path("/app/Visualize/artifacts/kafka_outbox/v1-outbox.sqlite3")
raw = Path("/app/de/artifacts/kafka_raw_v1/ledger.sqlite3")
bronze, silver, gold = [Path(os.environ[name]) for name in (
    "SILVER_V1_BRONZE_PATH", "SILVER_CHECKPOINT_PATH", "GOLD_CHECKPOINT_PATH")]
with ExitStack() as resources:
    handles = {}
    for path in (raw, bronze, silver, gold):
        db = resources.enter_context(closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True)))
        db.execute("PRAGMA query_only=ON")
        db.execute("SELECT name FROM sqlite_schema LIMIT 1").fetchone()
        handles[path] = db
    client = resources.enter_context(closing(clickhouse_connect.get_client(
        host=os.environ["SILVER_CLICKHOUSE_HOST"],
        username=os.environ["SILVER_CLICKHOUSE_USER"], database="smart_traffic")))
    evidence = audit_pipeline(outbox, raw, bronze, silver, gold, client)
    with TemporaryDirectory(prefix="negative-proof-") as temp:
        copy = Path(temp) / "gold.sqlite3"
        with closing(sqlite3.connect(copy)) as db:
            handles[gold].backup(db)
            db.execute("UPDATE core_work SET input_hash=? WHERE state='COMPLETE'", ("0" * 64,))
            db.commit()
        altered = audit_pipeline(outbox, raw, bronze, silver, copy, client)
        evidence["negative_copy_detected"] = not altered["passed"] and altered["mismatched_completed_work"] > 0
    sys.argv = ["oracle", "oracle", str(outbox.parent.parent), "--require-window", "60"]
    with redirect_stdout(StringIO()):
        evidence["oracle_60_exit"] = capacity_main()
    sys.argv += ["--require-window", "300"]
    output = StringIO()
    with redirect_stdout(output):
        evidence["oracle_60_and_300_exit"] = capacity_main()
    evidence["oracle_missing_required_durations"] = json.loads(
        output.getvalue())["missing_required_durations"]
    print(json.dumps(evidence, sort_keys=True), flush=True)
    if (not evidence["passed"] or not evidence["negative_copy_detected"] or evidence["oracle_60_exit"] != 0
            or evidence["oracle_60_and_300_exit"] != 2 or evidence["oracle_missing_required_durations"] != [300]):
        raise SystemExit(2)
