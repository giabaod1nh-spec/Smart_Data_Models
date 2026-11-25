"""Read-only physical checks against the isolated ClickHouse service."""
from contextlib import closing
import json
from pathlib import Path
import sys

import clickhouse_connect


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[2]))

from de.tools.gold_capacity_evidence import audit_pipeline


def main():
    client = clickhouse_connect.get_client(host="127.0.0.1", port=18124,
        username="gold_acceptance_reader", database="smart_traffic", connect_timeout=2,
        send_receive_timeout=8, query_retries=0, settings={"readonly": 1})
    with closing(client):
        paths = [ROOT / (name + ".sqlite3") for name in ("outbox", "raw", "bronze", "silver", "gold")]
        evidence = audit_pipeline(*paths, client)
    print(json.dumps(evidence, sort_keys=True), flush=True)
    return 0 if evidence["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
