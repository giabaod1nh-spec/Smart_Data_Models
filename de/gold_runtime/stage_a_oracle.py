"""Independent correctness checks over retained SUMO observations and raw measurements."""

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from hashlib import sha256
from pathlib import Path

from contracts.observation_schedule import ObservationSchedule, TICKS_PER_SECOND, seconds_to_ticks


def _raw_values(evidence):
    result = {}
    if (occupancy := evidence.get("occupancy")) is not None:
        samples = occupancy["percent"]
        result["occupancy"] = (round(math.fsum(samples) / len(samples), 1), occupancy["method"], None)
    if (queue := evidence.get("queue")) is not None:
        route = queue["method"] == "route_stopped_length_v1"
        groups = defaultdict(list)
        for turn, length, *remainder in queue["samples"]:
            if route and remainder[0] >= queue["haltingThreshold"]:
                raise ValueError("Independent oracle encountered moving vehicle in stopped queue evidence")
            groups[turn].append(length + 1.5 if route else length)
        value = round(max((math.fsum(group) for group in groups.values()), default=0), 1)
        result["queue"] = (value, queue["method"], queue["scopeDigest"])
    return result


def _same(left, right):
    if type(left) in (float, int) and type(right) in (float, int):
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)
    return left == right


def read_source(outbox):
    """Read the original disposable Outbox and verify sidecar arithmetic before any Gold reads."""
    path = Path(outbox).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        head = db.execute("""SELECT schedule_id,schedule_bytes,lifecycle,lifecycle_tick,settled_ordinal
            FROM observation_schedule_head""").fetchone()
        identity, document, lifecycle, cutoff, settled = head
        if lifecycle != "ENDED" or not 0 <= settled < 600:
            raise ValueError("Oracle requires a retained, terminal Stage A run of at most 600 slots")
        schedule = ObservationSchedule.decode(document, identity)
        if schedule.due_tick(settled) >= cutoff or schedule.due_tick(settled + 1) < cutoff:
            raise ValueError("Producer terminal cutoff disagrees with the retained schedule")
        samples, counts = {}, Counter()
        rows = db.execute("""
            SELECT k.payload_json,k.payload_hash,k.event_id,r.content,r.digest
            FROM kafka_outbox k LEFT JOIN observation_measurement_revision r
                ON r.event_id=k.event_id AND r.revision=1
            ORDER BY k.outbox_sequence LIMIT 3001
        """).fetchall()
        if len(rows) > 3000:
            raise ValueError("Oracle source exceeds disposable observation cap")
        for raw, payload_hash, event_id, sidecar, digest in rows:
            event = json.loads(raw)
            ordinal = event["cycleSequence"] - schedule.cycle_base
            node = event["intersectionId"]
            approaches = dict(schedule.participants).get(node)
            clock = seconds_to_ticks(event["simulationTimeSeconds"])
            position = (node, ordinal)
            if (position in samples or not 0 <= ordinal <= settled or approaches is None
                    or (event["simulationRunId"], event["topologyDigest"], event["payloadHash"], event["eventId"])
                    != (schedule.run_id, schedule.topology_digest, payload_hash, event_id)
                    or event_id not in schedule.event_ids(ordinal) or clock != schedule.due_tick(ordinal)
                    or event["simulationStep"] * schedule.step_ticks != clock
                    or set(event["payload"]["directions"]) != set(approaches)):
                raise ValueError("Frozen V1 event disagrees with its scheduled observation")
            values = {}
            if sidecar is not None:
                record = json.loads(sidecar)
                anchor = record["anchor"]
                if (sha256(sidecar).hexdigest() != digest or (anchor["eventId"], anchor["payloadHash"],
                        anchor["runId"], anchor["generation"], anchor["manifestIdentity"],
                        anchor["topologyDigest"], anchor["intersectionId"], anchor["cycle"],
                        anchor["simulationStep"], anchor["simulationTimeSeconds"])
                        != (event_id, payload_hash, schedule.run_id, schedule.generation, schedule.manifest_digest,
                            schedule.topology_digest, node, event["cycleSequence"], event["simulationStep"],
                            event["simulationTimeSeconds"])):
                    raise ValueError("Raw measurement object does not match its original event")
                for direction, collected in record["evidence"]["directions"].items():
                    values[direction] = _raw_values(collected)
                    for metric, (value, method, scope) in values[direction].items():
                        saved = record["measurements"]["directions"][direction][metric]
                        if (method, scope) != (saved["method"], saved["scopeDigest"]) or not _same(value, saved["value"]):
                            raise ValueError(f"Independent {metric} differs from saved source measurement")
                        counts[metric + "_samples"] += 1
                        if value == 0:
                            counts[metric + "_zeros"] += 1
                spillback = record["evidence"]["spillback"]
                if spillback is not None:
                    groups = spillback["percentGroups"]
                    if spillback["method"] == "e2_ratio_v2":
                        raw_samples = [item for group in groups for item in group]
                        ratio = math.fsum(raw_samples) / (100 * len(raw_samples))
                        maximum = max(raw_samples) / 100
                    else:
                        ratio = maximum = max(math.fsum(group) / (100 * len(group)) for group in groups)
                    pressure = round(ratio, 3)
                    full = maximum >= spillback["threshold"]
                    if record["measurements"]["spillback"]["value"] != {
                            "pressure": pressure, "downstreamFull": full,
                            "detected": full or pressure >= spillback["threshold"]}:
                        raise ValueError("Independent spillback differs from saved source measurement")
                    counts["spillback_samples"] += 1
                counts["measurement_objects"] += 1
            samples[position] = (event["payload"]["directions"], values)
            counts["events"] += 1
        failures = {row[0] for row in db.execute("SELECT ordinal FROM observation_schedule_failure")}
        for ordinal in range(settled + 1):
            observed = sum((node, ordinal) in samples for node, _ in schedule.participants)
            if observed != (0 if ordinal in failures else len(schedule.participants)):
                raise ValueError("Archived source has an unrecorded observation gap")
    return schedule, cutoff, samples, dict(counts, slots=settled + 1, failed_slots=len(failures))


def expected_windows(schedule, cutoff, samples, duration):
    """Derive each P0 physical metric directly from measured populations and raw samples."""
    duration_ticks = duration * TICKS_PER_SECOND
    for start in range(0, cutoff // duration_ticks * duration_ticks, duration_ticks):
        ordinals = [i for i in range((cutoff - schedule.first_tick) // schedule.period_ticks + 1)
                    if start <= schedule.due_tick(i) < start + duration_ticks]
        n = len(ordinals)
        statistics = {}
        for node, directions in schedule.participants:
            for direction in directions:
                physical = [samples[node, ordinal][0][direction] for ordinal in ordinals if (node, ordinal) in samples]
                measured = [samples[node, ordinal][1].get(direction, {}) for ordinal in ordinals
                            if (node, ordinal) in samples]
                vehicle = [r["vehicle_count"] for r in physical if r.get("vehicle_count") is not None]
                pairs = [(r["vehicle_count"], r["waiting_vehicle_count"], r.get("average_speed_kmh"))
                         for r in physical if r.get("vehicle_count") is not None
                         and r.get("waiting_vehicle_count") is not None
                         and r["waiting_vehicle_count"] <= r["vehicle_count"]]
                speed = [(v - h, s) for v, h, s in pairs if v == h or s is not None]

                def qualified(metric):
                    series = [row[metric] for row in measured if metric in row]
                    return [item[0] for item in series] if len({item[1:] for item in series}) == 1 else []

                queue = sorted(qualified("queue"))
                occupancy = qualified("occupancy")
                statistics[node, direction] = dict(vehicle=vehicle, pairs=pairs, speed=speed, queue=queue,
                    occupancy=occupancy, p95=queue[math.ceil(0.95 * n) - 1] if len(queue) == n else None)

        result = {}
        if not any((node, i) in samples for node, _ in schedule.participants for i in ordinals):
            yield start // TICKS_PER_SECOND, result
            continue
        groups = {("network", None, None): sorted(statistics)}
        for node, directions in schedule.participants:
            groups["intersection", node, None] = [(node, direction) for direction in directions]
            groups.update({("direction", node, direction): [(node, direction)] for direction in directions})

        for key, members in groups.items():
            size = n * len(members)
            vehicles = [value for item in members for value in statistics[item]["vehicle"]]
            pairs = [value for item in members for value in statistics[item]["pairs"]]
            speeds = [value for item in members for value in statistics[item]["speed"]]
            queue_count = sum(len(statistics[item]["queue"]) for item in members)
            movable = sum(count for count, _ in speeds)
            ratio_population = sum(v for v, _, _ in pairs)

            def metric(values, valid, expected=size):
                return dict(value=values if valid == expected else None, valid=valid, expected=expected,
                            status="UNAVAILABLE" if valid != expected else ("OK" if values is not None else "NOT_APPLICABLE"))

            worst = min(members, key=lambda item: (-statistics[item]["p95"], item)) if queue_count == size else None
            metrics = dict(moving_speed=metric(math.fsum(count * (s or 0) for count, s in speeds) / movable
                                               if movable else None, len(speeds)),
                stopped_ratio=metric(100 * sum(h for _, h, _ in pairs) / ratio_population
                                     if ratio_population else None, len(pairs)),
                observed_load=metric(sum(vehicles) / n, len(vehicles)),
                queue_p95=metric(statistics[worst]["p95"] if worst else None, queue_count),
                worst_queue_direction=list(worst) if worst else None)
            if key[0] == "direction":
                stat = statistics[members[0]]
                metrics.update(queue_mean=metric(math.fsum(stat["queue"]) / n, queue_count),
                    queue_max=metric(max(stat["queue"], default=None), queue_count),
                    occupancy_mean=metric(math.fsum(stat["occupancy"]) / n, len(stat["occupancy"])))
            result[key] = metrics
        yield start // TICKS_PER_SECOND, result


def verify_selection(expected, selection, duration):
    expected = dict(expected)
    windows = {window["start"]: window for window in selection["windows"]}
    if expected.keys() != windows.keys():
        raise ValueError(f"Gold reader did not return the expected {duration}s window set")
    for start, by_scope in expected.items():
        window = windows[start]
        if not by_scope and window["state"] == "MISSING_WINDOW":
            continue
        actual = {(r["grain"], r["intersection"], r["direction"]): r for r in window["rows"]}
        if window["state"] != "PUBLISHED" or actual.keys() != by_scope.keys():
            raise ValueError(f"Gold {duration}s window at {start} has unexpected state or population")
        for scope, metrics in by_scope.items():
            for name, value in metrics.items():
                observed = actual[scope][name]
                if isinstance(value, dict):
                    equal = observed is not None and all(_same(v, observed[k]) for k, v in value.items())
                else:
                    equal = _same(value, observed)
                if not equal:
                    raise ValueError(f"Gold {duration}s window at {start} differs for {scope} {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("outbox", type=Path)
    parser.add_argument("--checkpoint", type=Path)
    args = parser.parse_args()
    schedule, cutoff, samples, counts = read_source(args.outbox)
    comparisons = {}
    for duration in (60, 300):
        windows = list(expected_windows(schedule, cutoff, samples, duration))
        directions = [metrics for _, rows in windows for (grain, _, _), metrics in rows.items()
                      if grain == "direction"]
        pressure_pairs = [(row["occupancy_mean"]["value"], row["stopped_ratio"]["value"]) for row in directions
                          if row["occupancy_mean"]["status"] == row["stopped_ratio"]["status"] == "OK"]
        comparisons[duration] = dict(windows=len(windows), rows=sum(len(rows) for _, rows in windows),
            calibration=dict(status="CALIBRATION_REQUIRED", joint_direction_windows=len(pressure_pairs),
                occupancy_extent=[min(p[0] for p in pressure_pairs), max(p[0] for p in pressure_pairs)]
                    if pressure_pairs else None,
                stopped_ratio_extent=[min(p[1] for p in pressure_pairs), max(p[1] for p in pressure_pairs)]
                    if pressure_pairs else None))
        if args.checkpoint and windows:
            from de.gold_runtime.business_reader import GoldReader
            from de.gold_runtime.processing_ledger import canonical_run_identity
            from de.gold_runtime.stage_a import check_guards, open_clickhouse_target
            from de.silver.incremental_contract import canonical_hash

            check_guards(args.checkpoint.resolve(strict=True).parent)
            run_key = canonical_hash(canonical_run_identity(schedule.run_id, schedule.generation,
                                                           schedule.topology_digest, schedule.manifest_digest))
            target = open_clickhouse_target()
            try:
                reader = GoldReader(args.checkpoint, target)
                verify_selection(windows, reader.windows(run_key, duration, 0, windows[-1][0] + duration), duration)
            finally:
                target.client.close()
    print(json.dumps(dict(run_id=schedule.run_id, source=counts, windows=comparisons,
                          physical_verified=args.checkpoint is not None), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
