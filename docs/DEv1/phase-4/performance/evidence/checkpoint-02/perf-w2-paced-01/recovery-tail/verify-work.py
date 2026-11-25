"""Independently rebuild any completed proof using the verified SQLite prefix."""
from collections import defaultdict
from contextlib import closing, ExitStack
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[2]))

from de.tools.gold_capacity_evidence import wire


def snapshot(name):
    return closing(sqlite3.connect((ROOT / f"{name}.sqlite3").as_uri() + "?immutable=1", uri=True))


def digest(value):
    return sha256(wire(value)).hexdigest()


def run():
    with ExitStack() as stack:
        outbox, bronze, silver, gold = [stack.enter_context(snapshot(name))
                                       for name in ("outbox", "bronze", "silver", "gold")]
        source = defaultdict(list)
        for event, body in outbox.execute("SELECT event_id,payload_json FROM kafka_outbox"):
            obs = json.loads(body)
            for duration in (60, 300):
                lower = int(obs["simulationTimeSeconds"]) // duration * duration
                source[(obs["simulationRunId"], duration, lower)].append(event)
        bronze.row_factory = sqlite3.Row
        transitions = {row["event_id"]: dict(row) for row in bronze.execute(
            """SELECT namespace,change_sequence,event_id,revision,change_type,previous_state,
                      new_state,content_hash,invalidation_id
               FROM v1_changes WHERE namespace='live'"""
        )}
        families = {"silver_traffic_observation_fact_v1": "traffic",
                    "silver_signal_state_fact_v1": "signal", "silver_intersection_state_fact_v1": "intersection"}
        facts = defaultdict(lambda: defaultdict(list))
        for event, table, body in silver.execute(
            """SELECT event_id,target_table,expected_json FROM silver_v1_obligation
               WHERE namespace='live' AND status='RECONCILED' AND target_table IN (?,?,?)""", tuple(families)
        ):
            facts[event][families[table]].append(json.loads(body))
        results = []
        for identity, width, start, upper, count, expected_hash in gold.execute(
            """SELECT r.identity,w.duration,w.start,w.end,g.input_count,g.input_hash
               FROM core_run r JOIN core_window w ON w.run_key=r.id
               JOIN core_work g ON g.window_id=w.id AND g.generation=w.generation
               WHERE g.state='COMPLETE'"""
        ):
            run_id = json.loads(identity)["simulation_run_id"]
            members = sorted(source[(run_id, width, start)])
            combined = sha256(b"gold-core-input-set-v1\0")
            for member in members:
                transition = transitions[member]
                data = facts[member]
                intersection, = data["intersection"]
                bundle = dict(event_id=member, intersection=intersection)
                for family in ("signal", "traffic"):
                    bundle[family] = sorted(data[family], key=lambda row: row["source_direction"])
                body = wire(dict(event_id=member, change_sequence=transition["change_sequence"],
                                 semantic_revision=transition["revision"], scenario_id=intersection["scenario_id"],
                                 transition_hash=digest(transition), bundle_hash=digest(bundle)))
                combined.update(len(body).to_bytes(8, "big") + body)
            results.append(dict(run=run_id, duration=width, start=start, end=upper,
                                members=len(members), digest=combined.hexdigest(),
                                matches=count == len(members) and combined.hexdigest() == expected_hash))
    return results


if __name__ == "__main__":
    results = run()
    print(json.dumps(results, sort_keys=True))
    raise SystemExit(0 if results and all(result["matches"] for result in results) else 2)
