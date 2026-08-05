from __future__ import annotations

import json
from pathlib import Path

import clickhouse_connect

OUT = Path("docs/gold/gold4_samples")


def main() -> None:
    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="default", password=""
    )
    rows = client.query(
        """
        SELECT simulation_run_id,
               countIf(window_size_sec=60) AS n60,
               countIf(window_size_sec=300) AS n300
        FROM smart_traffic.gold_fact_traffic_window
        WHERE namespace='live'
        GROUP BY simulation_run_id
        ORDER BY n300 DESC, n60 DESC
        LIMIT 10
        """
    ).result_rows
    print("runs", rows)
    OUT.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    run_id = rows[0][0]
    for name, sql in (
        (
            "sample_kpi_result.json",
            "SELECT * FROM smart_traffic.gold_fact_kpi_result "
            "WHERE namespace={n:String} AND simulation_run_id={r:String} LIMIT 1",
        ),
        (
            "sample_traffic_fact.json",
            "SELECT * FROM smart_traffic.gold_fact_traffic_window "
            "WHERE namespace={n:String} AND simulation_run_id={r:String} "
            "AND window_size_sec=60 LIMIT 1",
        ),
        (
            "sample_direction_summary.json",
            "SELECT * FROM smart_traffic.gold_mart_direction_window_summary "
            "WHERE namespace={n:String} AND simulation_run_id={r:String} LIMIT 1",
        ),
    ):
        result = client.query(sql, parameters={"n": "live", "r": run_id})
        if not result.result_rows:
            continue
        payload = dict(zip(result.column_names, result.result_rows[0]))
        (OUT / name).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print("wrote", name)


if __name__ == "__main__":
    main()
