import sqlite3
from pathlib import Path

cp = Path(__file__).resolve().parents[2] / "de" / "artifacts" / "bronze" / "checkpoint.sqlite3"
c = sqlite3.connect(str(cp))
rows = c.execute(
    "select checkpoint_namespace,partition_id,last_completed_offset from bronze_checkpoint order by 1,2"
).fetchall()
print("checkpoint", rows)
print("ledger", c.execute("select count() from bronze_processing_ledger").fetchone())
