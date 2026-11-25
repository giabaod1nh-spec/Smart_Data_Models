"""Account for every source-crossed Gold window in the stopped fixture."""
from collections import defaultdict
from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import runpy
import sqlite3


ROOT = Path(__file__).resolve().parent
proof_check = runpy.run_path(str(ROOT / "verify-work.py"))


def read_copy(name):
    path = ROOT / (name + ".sqlite3")
    return closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True))


def work_id(db, window, generation, input_hash):
    namespace, contract, schema, processor = db.execute(
        "SELECT namespace,contract_version,schema_version,processor_version FROM core_binding"
    ).fetchone()
    return sha256(proof_check["wire"](
        [namespace, window, generation, input_hash, contract, schema, processor]
    )).hexdigest()


def audit_work(db, proofs, expected, horizons):
    retained = {}
    for identity, window, duration, lo, hi, generation, eligible, state, proof, count, identifier, receipt in db.execute(
            """SELECT r.identity,w.id,w.duration,w.start,w.end,w.generation,w.eligible,
                      g.state,g.input_hash,g.input_count,g.work_id,rc.id
               FROM core_run r JOIN core_window w ON w.run_key=r.id
               LEFT JOIN core_work g ON g.window_id=w.id AND g.generation=w.generation
               LEFT JOIN core_receipt rc ON rc.id=g.source_receipt"""):
        run = json.loads(identity)["simulation_run_id"]
        key = run, duration, lo, hi
        if key in retained or not state or not receipt:
            raise ValueError(f"Missing or ambiguous current work and receipt for {key}")
        present = {r[0] for r in db.execute("SELECT event_id FROM core_membership WHERE window_id=?", (window,))}
        if not present <= expected[key]:
            raise ValueError(f"Unexpected retained window members for {key}")
        if state == "COMPLETE":
            if proofs.get(key) != proof or count != len(expected[key]) or present != expected[key] or not eligible:
                raise ValueError(f"Completed input evidence differs for {key}")
            if identifier != work_id(db, window, generation, proof):
                raise ValueError(f"Completed work ID differs for {key}")
        retained[key] = dict(state=state, retained_members=len(present))
    return [
        dict(run=run, duration=duration, start=lo, end=hi, expected_members=len(members),
             **retained.get((run, duration, lo, hi), dict(state="ABSENT", retained_members=0)))
        for (run, duration, lo, hi), members in sorted(expected.items())
        if hi <= horizons[run]
    ]


def main():
    expected = defaultdict(set)
    horizons = defaultdict(float)
    with read_copy("outbox") as source:
        for event, raw in source.execute("SELECT event_id,payload_json FROM kafka_outbox WHERE status='ACKED'"):
            item = json.loads(raw)
            run, clock = item["simulationRunId"], item["simulationTimeSeconds"]
            horizons[run] = max(horizons[run], clock)
            for duration in (60, 300):
                lo = int(clock) // duration * duration
                expected[run, duration, lo, lo + duration].add(event)
    proofs = {(proof["run"], proof["duration"], proof["start"], proof["end"]): proof["digest"]
              for proof in proof_check["run"]() if proof["matches"]}
    if not proofs:
        raise RuntimeError("The completed-proof check returned no verified work")
    with read_copy("gold") as db:
        crossed = audit_work(db, proofs, expected, horizons)
        with closing(sqlite3.connect(":memory:")) as copy:
            db.backup(copy)
            (target,) = copy.execute(
                "SELECT work_id FROM core_work WHERE state='COMPLETE' ORDER BY completed_at LIMIT 1"
            ).fetchone()
            copy.execute("UPDATE core_work SET work_id=? WHERE work_id=?", ("0" * 64, target))
            try:
                audit_work(copy, proofs, expected, horizons)
            except ValueError as exc:
                if "Completed work ID differs" not in str(exc):
                    raise
                negative = str(exc)
            else:
                raise RuntimeError("Changed work ID passed independent validation")
    remaining = sum(row["expected_members"] for row in crossed if row["state"] != "COMPLETE")
    print(json.dumps(dict(crossed=crossed, completed_remaining_members=remaining,
                          work_id_negative_control=negative), sort_keys=True))


if __name__ == "__main__":
    main()
