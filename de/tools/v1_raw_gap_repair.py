"""Read-only gap report; repair delegates exclusively to isolated replay."""
import argparse
import json
import sqlite3
from pathlib import Path
from de.kafka_raw.v1_replay import main as replay_main

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--topic",required=True)
    p.add_argument("--partition",required=True,type=int)
    p.add_argument("--from-offset",required=True,type=int)
    p.add_argument("--to-offset",required=True,type=int)
    mode=p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--report-only",action="store_true")
    mode.add_argument("--repair",action="store_true")
    p.add_argument("--replay-run-id")
    p.add_argument("--ledger",default="de/artifacts/kafka_raw_v1/ledger.sqlite3")
    a=p.parse_args()
    if a.from_offset<0 or a.to_offset<a.from_offset: p.error("invalid range")
    uri=Path(a.ledger).resolve().as_uri()+"?mode=ro"
    with sqlite3.connect(uri,uri=True) as db:
        rows=db.execute("""SELECT offset_value,state FROM deliveries
WHERE topic=? AND partition_id=? AND offset_value>=? AND offset_value<? ORDER BY offset_value""",
          (a.topic,a.partition,a.from_offset,a.to_offset)).fetchall()
    print(json.dumps({"delivered_records":rows,"note":"integer gaps alone do not prove missing Kafka records"}))
    if a.repair:
        if not a.replay_run_id: p.error("repair requires isolated replay ID")
        replay_main(["--topic",a.topic,"--partition",str(a.partition),
            "--start-offset",str(a.from_offset),"--end-offset",str(a.to_offset),
            "--replay-run-id",a.replay_run_id])

if __name__=="__main__":
    main()

