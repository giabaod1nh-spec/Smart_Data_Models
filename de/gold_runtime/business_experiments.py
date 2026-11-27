"""Bounded Fixed-Time versus Adaptive comparisons over pinned Gold revisions."""

import json
from math import fsum

from contracts.observation_measurements import digest
from de.gold_runtime.business_reader import StaleGoldSelection
from de.silver.archive_resolver import manifest_identity

_COMPARABLE = ("topologyDigest", "participatingIntersectionIds", "network_hash", "route_hash",
               "sumocfg_hash", "config_hash", "demand_profile_id", "demand_seed", "sumo_seed",
               "detector_profile_id", "detector_geometry_version")
_CONTEXT = ("offered_demand_digest", "initial_conditions_digest", "scenario_schedule_digest",
            "measurement_scope_digest", "warmup_seconds", "step_ticks", "period_ticks")


def validate_pair(baseline, adaptive):
    """Validate both pinned V1 manifests and the externally qualified pair evidence."""
    checks = {}
    for key in _COMPARABLE + _CONTEXT:
        left, right = baseline.get(key), adaptive.get(key)
        checks[key] = "UNKNOWN" if left in (None, "", []) or right in (None, "", []) else (
            "PASS" if left == right else "FAIL")
    modes = baseline.get("control_mode"), adaptive.get("control_mode")
    checks["treatment"] = "PASS" if modes == ("FIXED", "ADAPTIVE") else (
        "UNKNOWN" if None in modes else "FAIL")
    for name in ("controller_build", "controller_config_digest"):
        checks[name] = "PASS" if adaptive.get(name) else "UNKNOWN"
    for manifest, role in ((baseline, "fixed"), (adaptive, "adaptive")):
        for name in _CONTEXT:
            value = manifest.get(name)
            valid = (digest(value) if name.endswith("_digest") else
                     type(value) is int and value >= (0 if name == "warmup_seconds" else 1))
            if value is not None and not valid:
                checks[role + "_" + name] = "FAIL"
    if adaptive.get("controller_config_digest") is not None and not digest(adaptive["controller_config_digest"]):
        checks["controller_config_digest"] = "FAIL"
    values = set(checks.values())
    return dict(status="INCOMPATIBLE" if "FAIL" in values else ("UNKNOWN" if "UNKNOWN" in values else "PASS"),
                checks=checks)


def _qualified_range(rows):
    def ratio(metric, numerator, denominator, scale=1):
        if any(row[metric]["status"] == "UNAVAILABLE" for row in rows):
            return None
        total = sum(row[denominator] for row in rows)
        return scale * fsum(row[numerator] for row in rows) / total if total else None

    return dict(moving_speed=ratio("moving_speed", "weighted_speed", "moving_count"),
                stopped_ratio=ratio("stopped_ratio", "stopped_count", "ratio_vehicle_count", 100),
                observed_load=ratio("observed_load", "load_vehicle_count", "time_slots"),
                worst_window_queue_p95=max(row["queue_p95"]["value"] for row in rows)
                    if all(row["queue_p95"]["status"] == "OK" for row in rows) else None)


def _deltas(left, right):
    return {name: dict(fixed=base, adaptive=compared,
                       status="OK" if base is not None and compared is not None else "UNAVAILABLE",
                       absolute=(compared - base) if base is not None and compared is not None else None,
                       relative_pct=(100 * (compared - base) / base) if base and compared is not None else None)
            for name, base in left.items() for compared in (right[name],)}


def compare(baseline_reader, adaptive_reader, baseline_manifest, adaptive_manifest, *,
            baseline_run_key, adaptive_run_key, duration, start, end):
    qualification = validate_pair(baseline_manifest, adaptive_manifest)
    if qualification["status"] != "PASS":
        return qualification
    if baseline_manifest["simulationRunId"] == adaptive_manifest["simulationRunId"]:
        return dict(qualification, status="INCOMPATIBLE", reason="SAME_RUN")
    warmup = baseline_manifest["warmup_seconds"]
    if type(warmup) is not int or type(start) is not int or start < warmup:
        return dict(qualification, status="INCOMPATIBLE", reason="BEFORE_WARMUP")
    readers = (baseline_reader, adaptive_reader)
    keys = (baseline_run_key, adaptive_run_key)
    manifests = (baseline_manifest, adaptive_manifest)
    selections = [reader.windows(key, duration, start, end) for reader, key in zip(readers, keys)]
    for selection, manifest in zip(selections, manifests):
        actual = selection["run"]
        if (actual["simulation_run_id"], actual["run_generation"], actual["topology_digest"],
                actual["manifest_identity"]) != (
                manifest["simulationRunId"], manifest["runGeneration"], manifest["topologyDigest"],
                manifest_identity(manifest)):
            return dict(qualification, status="INCOMPATIBLE", reason="MANIFEST_IDENTITY")
    if any(window["state"] != "PUBLISHED" for selection in selections for window in selection["windows"]):
        return dict(qualification, status="INCOMPLETE_COVERAGE", selections=selections)
    populations = {window["lineage"]["population_digest"] for selection in selections for window in selection["windows"]}
    contracts = {(window["lineage"]["definition_set"], window["lineage"]["coverage_policy"],
                  window["lineage"]["source_semantics"]) for selection in selections for window in selection["windows"]}
    if len(populations) != 1 or len(contracts) != 1:
        return dict(qualification, status="INCOMPATIBLE", reason="POPULATION_OR_METRIC_CONTRACT")
    summaries, trends = {}, []
    scopes = [("network", None)] + [("intersection", node) for node in baseline_manifest["participatingIntersectionIds"]]
    for grain, node in scopes:
        series = [[next(row for row in window["rows"] if (row["grain"], row["intersection"]) == (grain, node))
                   for window in selection["windows"]] for selection in selections]
        summaries[node or "network"] = _deltas(*(_qualified_range(rows) for rows in series))
        for window, left, right in zip(selections[0]["windows"], *series):
            trends.append(dict(grain=grain, intersection=node, start=window["start"],
                               deltas=_deltas(_qualified_range([left]), _qualified_range([right]))))
    for reader, key, selection in zip(readers, keys, selections):
        if reader._pin(key, duration, start, end, latest=False)[3] != selection["fence"]:
            raise StaleGoldSelection("Experiment revisions changed during comparison")
    data_status = "COMPLETE" if all(delta["status"] == "OK" for summary in summaries.values()
                                    for delta in summary.values()) else "PARTIAL"
    return dict(qualification, data_status=data_status, summary=summaries, trends=trends,
                pressure=dict(status="CALIBRATION_REQUIRED"),
                lineage=[[window["lineage"] for window in selection["windows"]] for selection in selections])


def read_manifest(path):
    with path.open("rb") as stream:
        raw = stream.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        raise ValueError("Experiment manifest exceeds byte guard")
    return json.loads(raw)
