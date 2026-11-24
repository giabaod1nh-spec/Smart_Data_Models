"""Read isolated durable evidence after the scheduler diagnostic stopped."""
from contextlib import ExitStack, closing
import json
import os
from pathlib import Path
import sqlite3

from de.tools.gold_core_capacity import verify_pipeline

bronze = Path(os.environ["SILVER_V1_BRONZE_PATH"])
gold = Path(os.environ["GOLD_CHECKPOINT_PATH"])
with ExitStack() as stack:
    for path in (bronze, gold):
        connection = stack.enter_context(closing(
            sqlite3.connect(path.resolve().as_uri() + "?mode=rw", uri=True)))
        connection.execute("PRAGMA query_only=ON")
        connection.execute("SELECT name FROM sqlite_schema LIMIT 1").fetchone()
    print(json.dumps(verify_pipeline(
        Path("/app/Visualize/artifacts/kafka_outbox/v1-outbox.sqlite3"), bronze, gold),
        sort_keys=True))
