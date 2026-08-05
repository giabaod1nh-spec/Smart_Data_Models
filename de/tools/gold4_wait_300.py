import json
import time
import urllib.request

import clickhouse_connect


def main() -> int:
    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="default", password=""
    )
    for index in range(40):
        health = json.loads(
            urllib.request.urlopen("http://localhost:8096/health", timeout=5).read()
        )
        row = client.query(
            "SELECT countIf(window_size_sec = 60), countIf(window_size_sec = 300) "
            "FROM smart_traffic.gold_fact_traffic_window WHERE namespace = 'live'"
        ).result_rows[0]
        print(
            index,
            health["status"],
            health.get("watermark"),
            health["metrics"]["windows_processed_total"],
            "facts60/300",
            row,
        )
        if int(row[1]) > 0:
            return 0
        time.sleep(15)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
