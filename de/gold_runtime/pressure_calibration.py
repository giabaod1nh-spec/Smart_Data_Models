"""Evaluate frozen pressure candidates against independent SUMO phase/trajectory references."""

import argparse
import json
from collections import Counter
from hashlib import sha256
from pathlib import Path

from contracts.observation_schedule import TICKS_PER_SECOND
from de.gold_runtime.stage_a_oracle import expected_windows, read_source

WEIGHTS = (25, 50, 75)
HIGH_THRESHOLDS = (50, 60, 70, 80)
FALSE_HIGH_LIMIT = 0.10
MISSED_LIMIT = 0.20


def evidence(outbox, reference, *, validate_identity=True):
    raw = reference.read_bytes()
    if len(raw) > 1024 * 1024:
        raise ValueError("Calibration reference exceeds its safety cap")
    document = json.loads(raw)
    schedule, cutoff, samples, _ = read_source(outbox)
    if (document["run_id"], document["manifest"]) != (schedule.run_id, schedule.manifest_digest):
        raise ValueError("Calibration reference belongs to another retained run")
    if validate_identity:
        from de.silver.archive_resolver import ArchiveResolver

        context = ArchiveResolver(outbox.parent).resolve(schedule.run_id, schedule.topology_digest, schedule.generation)
        if (context.manifest_identity != schedule.manifest_digest or not context.manifest.get("stage_a_calibration_reference")
                or context.manifest.get("sumo_seed") != document["seed"]
                or context.manifest.get("demand_scale") != document["demand_scale"]
                or not context.manifest.get("stage_a_detector_profile")):
            raise ValueError("Calibration reference lacks the pinned source definition")
    lookup = {(item["intersection"], item["direction"]): item["observations"] for item in document["measurements"]}
    rows = {}
    for duration in (60, 300):
        output = []
        for start, metrics in expected_windows(schedule, cutoff, samples, duration):
            lo, hi = start * TICKS_PER_SECOND, (start + duration) * TICKS_PER_SECOND
            for (grain, node, direction), window in metrics.items():
                if grain != "direction":
                    continue
                truth = [item["label"] for item in lookup.get((node, direction), ())
                         if lo <= item["started_tick"] < item["ended_tick"] <= hi]
                if "BOTTLENECK" in truth:
                    label = "BOTTLENECK"
                elif truth and set(truth) == {"NORMAL_RED"}:
                    label = "NORMAL_RED"
                else:
                    continue
                occ, ratio = window["occupancy_mean"], window["stopped_ratio"]
                values = ((occ["value"], ratio["value"]) if occ["status"] == ratio["status"] == "OK" else None)
                output.append((label, values))
        rows[duration] = output
    return document["seed"], document["demand_scale"], sha256(raw).hexdigest(), rows


def evaluate(rows, weight, high_threshold):
    counts = Counter()
    for label, values in rows:
        counts[label] += 1
        if values is None:
            counts["unavailable_" + label] += 1
            continue
        occupancy, ratio = values
        high = weight * occupancy + (100 - weight) * ratio >= 100 * high_threshold
        counts[("tp" if high else "fn") if label == "BOTTLENECK" else ("fp" if high else "tn")] += 1
    normal, bottleneck = counts["NORMAL_RED"], counts["BOTTLENECK"]
    false_high = counts["fp"] / normal if normal else None
    missed = (counts["fn"] + counts["unavailable_BOTTLENECK"]) / bottleneck if bottleneck else None
    # Both labels and fully scored reference windows are necessary for qualification.
    qualified = (false_high is not None and missed is not None and false_high <= FALSE_HIGH_LIMIT
                 and missed <= MISSED_LIMIT and not counts["unavailable_NORMAL_RED"]
                 and not counts["unavailable_BOTTLENECK"])
    return dict(counts=dict(counts), false_high=false_high, missed_bottleneck=missed, meets_targets=qualified)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration", nargs=2, action="append", required=True, metavar=("OUTBOX", "REFERENCE"))
    parser.add_argument("--holdout", nargs=2, action="append", required=True, metavar=("OUTBOX", "REFERENCE"))
    args = parser.parse_args()
    calibration, holdout = [[evidence(*(Path(p).resolve(strict=True) for p in pair)) for pair in group]
                            for group in (args.calibration, args.holdout)]
    if {item[0] for item in calibration} & {item[0] for item in holdout}:
        raise ValueError("Holdout must use an independent seed")
    result = []
    for weight in WEIGHTS:
        for threshold in HIGH_THRESHOLDS:
            training = {duration: evaluate(
                [row for _, _, _, windows in calibration for row in windows[duration]], weight, threshold)
                for duration in (60, 300)}
            measurements = {duration: evaluate(
                [row for _, _, _, windows in holdout for row in windows[duration]], weight, threshold)
                for duration in (60, 300)}
            result.append(dict(occupancy_weight=weight, stopped_ratio_weight=100-weight, high_threshold=threshold,
                               calibration=training, holdout=measurements))
    print(json.dumps(dict(reference_inputs=[(seed, scale, digest) for seed, scale, digest, _ in calibration + holdout],
                          false_high_limit=FALSE_HIGH_LIMIT, missed_limit=MISSED_LIMIT, candidates=result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
