"""Read effective Gold Business revisions through the SQLite publication fence."""

import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

from contracts.observation_schedule import MAX_CANONICAL_INTEGER, TICKS_PER_SECOND
from de.gold.business import VERIFIED_DEFINITION
from de.gold_runtime.business_materialization import applied_window_coverage
from de.gold_runtime.business_publication import DivergentResult, _MAX_ROWS, _manifest, result_rows
from de.gold_runtime.business_state import IsolatedBusinessStore, _MANIFEST_LIMIT, _RESULT_LIMIT, _verify_canonical
from de.silver.incremental_contract import canonical_hash

_MAX_WINDOWS = 12
_MAX_RUNS = 25
_SOURCE = "silver_observation_metadata_v1"
_COVERAGE = "isolated_strict_v1"


class StaleGoldSelection(RuntimeError):
    """The effective revisions changed while physical rows were being read."""


class GoldReader:
    def __init__(self, checkpoint, physical_rows, *, namespace="live"):
        self.checkpoint = IsolatedBusinessStore(checkpoint).db_path
        self.physical_rows = physical_rows
        self.namespace = namespace

    def _connection(self):
        # Each phase uses an independent short read snapshot. No transaction spans ClickHouse I/O.
        IsolatedBusinessStore._verify_isolation(self.checkpoint)
        db = sqlite3.connect(self.checkpoint.as_uri() + "?mode=ro", uri=True, timeout=2)
        db.row_factory = sqlite3.Row
        return db

    def runs(self, *, after="", limit=10):
        if type(limit) is not int or not 1 <= limit <= _MAX_RUNS or not isinstance(after, str):
            raise ValueError("Invalid run catalog page")
        with closing(self._connection()) as db:
            rows = db.execute("""SELECT r.id,r.identity,s.lifecycle,s.cutoff,s.schedule_id,s.schedule
                FROM core_run r JOIN business_observation_schedule s ON s.run_key=r.id
                WHERE r.id>? ORDER BY r.id LIMIT ?""", (after, limit + 1)).fetchall()
        return dict(runs=[dict(json.loads(row["identity"]), run_key=row["id"], lifecycle=row["lifecycle"],
                    cutoff_tick=row["cutoff"], schedule_id=row["schedule_id"],
                    participants=json.loads(row["schedule"])["participants"]) for row in rows[:limit]],
                    after=rows[limit - 1]["id"] if len(rows) > limit else None)

    def _select(self, db, run_key, duration, start, end, *, latest):
        return db.execute("""
            SELECT w.start,w.duration,w.generation,r.id AS result_id,r.revision,r.payload,r.payload_hash,
                   r.row_manifest,r.manifest_hash,s.epoch,b.binding_id,o.input_hash,o.work_id,o.population_digest,
                   o.metadata_digest,o.definition_set,o.coverage_policy,o.source_semantics
            FROM business_scope s
            JOIN business_result r ON r.id=s.result_id AND r.scope_id=s.id
            JOIN business_publication p ON p.result_id=r.id AND p.state='PUBLISHED'
            JOIN business_obligation o ON o.id=r.obligation_id AND o.state='COMPUTED'
            JOIN core_window w ON w.id=o.window_id AND w.generation=o.generation
            JOIN core_work c ON c.window_id=w.id AND c.generation=w.generation
                AND c.state='COMPLETE' AND c.work_id=o.work_id AND c.input_hash=o.input_hash
            JOIN core_binding b ON b.binding_id=o.binding_id AND b.namespace=?
            WHERE w.run_key=? AND w.duration=? AND w.start>=? AND w.start<?
                AND o.definition_set=? AND o.source_semantics=? AND o.coverage_policy=?
            ORDER BY w.start DESC LIMIT ?
        """, (self.namespace, run_key, duration, start, end, VERIFIED_DEFINITION, _SOURCE, _COVERAGE,
              1 if latest else _MAX_WINDOWS + 1)).fetchall()

    @staticmethod
    def _fence(rows):
        return tuple((row["start"], row["result_id"], row["epoch"]) for row in rows)

    def _pin(self, run_key, duration, start, end, *, latest):
        with closing(self._connection()) as db:
            db.execute("BEGIN")
            run = db.execute("SELECT identity FROM core_run WHERE id=?", (run_key,)).fetchone()
            if run is None:
                raise ValueError("Unknown historical Gold run")
            rows = self._select(db, run_key, duration, start, end, latest=latest)
            latest_reprocessing = None
            if latest:
                unpublished = db.execute("""SELECT w.start FROM business_obligation o
                    JOIN core_window w ON w.id=o.window_id
                    JOIN business_result r ON r.obligation_id=o.id
                    JOIN business_publication p ON p.result_id=r.id AND p.state='PUBLISHED'
                    JOIN core_binding b ON b.binding_id=o.binding_id AND b.namespace=?
                    WHERE w.run_key=? AND w.duration=? AND o.state='SUPERSEDED'
                    ORDER BY w.start DESC LIMIT 1""", (self.namespace, run_key, duration)).fetchone()
                if unpublished and (not rows or unpublished[0] > rows[0]["start"]):
                    latest_reprocessing = unpublished[0]
                    rows = ()
            if len(rows) > (1 if latest else (end - start) // duration):
                raise DivergentResult("Multiple effective Gold revisions for one window selector")
            selected_starts = {row["start"] for row in rows}
            if len(selected_starts) != len(rows):
                raise DivergentResult("Multiple effective results for the same simulation window")
            gaps = {}
            if latest_reprocessing is not None:
                gaps[latest_reprocessing] = "REPROCESSING"
            if not latest:
                state = SimpleNamespace(store=SimpleNamespace(conn=db))
                for tick in range(start, end, duration):
                    if tick in selected_starts:
                        continue
                    coverage, _ = applied_window_coverage(state, run_key, tick, duration)
                    hold = db.execute("""SELECT h.reason FROM core_window w JOIN business_hold h
                        ON h.window_id=w.id AND h.generation=w.generation
                        WHERE w.run_key=? AND w.start=? AND w.duration=?""",
                        (run_key, tick, duration)).fetchone()
                    gaps[tick] = (hold[0] if hold else "REPROCESSING") if coverage == "READY" else coverage
            schedule = db.execute("SELECT schedule_id,cutoff,lifecycle,lifecycle_tick FROM business_observation_schedule "
                                  "WHERE run_key=?", (run_key,)).fetchone()
            cursor = db.execute("SELECT sequence,digest,settled_ordinal,pending_digest FROM business_metadata_cursor "
                                "WHERE run_key=? AND stream='SCHEDULE'", (run_key,)).fetchone()
            participants = db.execute("SELECT schedule FROM business_observation_schedule WHERE run_key=?",
                                      (run_key,)).fetchone()
            nodes = [item["intersectionId"] for item in json.loads(participants[0])["participants"]] if participants else []
            fence = canonical_hash([self._fence(rows), sorted(gaps.items()), nodes,
                                    tuple(schedule) if schedule else None, tuple(cursor) if cursor else None])
            return json.loads(run[0]), rows, gaps, fence, nodes

    def _physical_window(self, row):
        if (_verify_canonical(row["payload"], _RESULT_LIMIT) != row["payload_hash"]
                or _verify_canonical(row["row_manifest"], _MANIFEST_LIMIT) != row["manifest_hash"]):
            raise DivergentResult("Gold result or row manifest hash changed")
        document = json.loads(row["payload"])
        if (document["start_us"], document["duration_s"], document["definition_set"]) != (
                row["start"] * TICKS_PER_SECOND, row["duration"], row["definition_set"]):
            raise DivergentResult("Gold result differs from its durable window")
        expected = result_rows(row["payload"])
        if _manifest(expected) != row["row_manifest"]:
            raise DivergentResult("Gold result rows differ from the published manifest")
        by_key = {(item.grain, item.key): item.payload for item in expected}
        observed = self.physical_rows.read(row["result_id"], limit=2 * _MAX_ROWS + 1)
        seen = {}
        if len(observed) > 2 * _MAX_ROWS:
            raise DivergentResult("Gold physical result exceeds its duplicate inspection cap")
        for item in observed:
            key = item.grain, item.key
            if by_key.get(key) != item.payload:
                raise DivergentResult("Physical Gold row differs from its published manifest")
            seen[key] = item.payload
        if seen != by_key:
            raise DivergentResult("Published physical Gold result is incomplete")
        physical = [json.loads(payload) for payload in seen.values()]
        physical.sort(key=lambda item: (item["grain"], item["intersection"] or "", item["direction"] or ""))
        lineage = {name: row[name] for name in ("result_id", "revision", "epoch", "generation", "binding_id",
            "work_id", "input_hash", "payload_hash", "manifest_hash", "population_digest", "metadata_digest",
            "definition_set", "coverage_policy", "source_semantics")}
        return dict(start=row["start"], duration=row["duration"], state="PUBLISHED", lineage=lineage,
                    slot_states=document["slot_states"], rows=physical)

    def windows(self, run_key, duration, start=None, end=None):
        """Return latest finalized or an explicit aligned range with visible gaps."""
        maximum = MAX_CANONICAL_INTEGER // TICKS_PER_SECOND
        if (type(duration) is not int or duration not in (60, 300) or not isinstance(run_key, str)
                or len(run_key) != 64):
            raise ValueError("Expected an explicit run and a 60s or 300s interval")
        latest = start is None and end is None
        if latest:
            start, end = 0, maximum
        elif (type(start) is not int or type(end) is not int or not 0 <= start < end <= maximum
                or start % duration or end % duration or (end - start) // duration > _MAX_WINDOWS):
            raise ValueError("Expected a bounded aligned range of up to 12 Gold windows")
        for _ in range(2):
            run, selected, gaps, fence, participants = self._pin(run_key, duration, start, end, latest=latest)
            try:
                windows = [self._physical_window(row) for row in selected]
            except DivergentResult:
                if self._pin(run_key, duration, start, end, latest=latest)[3] != fence:
                    continue
                raise
            if self._pin(run_key, duration, start, end, latest=latest)[3] == fence:
                windows.extend(dict(start=tick, duration=duration, state=reason, rows=[]) for tick, reason in gaps.items())
                windows.sort(key=lambda window: window["start"])
                return dict(run=run, run_key=run_key, duration=duration, fence=fence, windows=windows,
                            participants=participants,
                            state=("AVAILABLE" if selected else "REPROCESSING"
                                   if latest and gaps else "NO_PUBLISHED_RESULT"))
        raise StaleGoldSelection("Gold revisions changed before selection could be confirmed")


_METRICS = frozenset(("moving_speed", "stopped_ratio", "observed_load", "queue_mean", "queue_p95",
                      "queue_max", "occupancy_mean", "pressure_score", "high_pressure_count"))
_PRESSURE = frozenset(("pressure_score", "high_pressure_count"))


def metric_status(row, name):
    if name in _PRESSURE:
        return row.get(name) or dict(value=None, status="CALIBRATION_REQUIRED")
    return row.get(name) or dict(value=None, status="NOT_APPLICABLE")


def compare_adjacent(points, duration):
    """Compute period-over-period change only where both aligned values exist."""
    changes = []
    for left, right in zip(points, points[1:]):
        a, b = left["value"], right["value"]
        definition_changed = left.get("definition") != right.get("definition")
        context_changed = left.get("context_key") != right.get("context_key")
        available = (left["status"] == right["status"] == "OK" and a is not None and b is not None
                     and right["start"] - left["start"] == duration and not definition_changed and not context_changed)
        changes.append(dict(start=right["start"], absolute=b-a if available else None,
                           relative_pct=100*(b-a)/a if available and a else None,
                           status=("OK" if available else "INCOMPATIBLE_DEFINITION" if definition_changed
                                   else "CONTEXT_CHANGED" if context_changed else "UNAVAILABLE")))
    return changes


def historical_trend(selection, *, metric, grain="network", intersection=None, direction=None):
    """One point per requested window with no interpolation through gaps."""
    if (metric not in _METRICS or grain not in ("network", "intersection", "direction")
            or (intersection is None) != (grain == "network")
            or (direction is None) != (grain != "direction")):
        raise ValueError("Invalid Gold metric or trend scope")
    points = []
    for window in selection["windows"]:
        state = window["state"]
        row = next((row for row in window["rows"]
                    if (row["grain"], row["intersection"], row["direction"]) == (grain, intersection, direction)), None)
        value = metric_status(row, metric) if row else dict(value=None, status=state if state != "PUBLISHED" else "NO_ENTITY")
        points.append(dict(start=window["start"], **value))
    return points


def overview(selection, *, selected_start=None):
    """Use the same pinned selection for KPI cards, ranking, heatmap and trends."""
    published = [window for window in selection["windows"] if window["state"] == "PUBLISHED"]
    if selected_start is not None:
        selected = next((item for item in selection["windows"] if item["start"] == selected_start), None)
        if selected is None:
            raise ValueError("Selected Overview window lies outside the pinned range")
    else:
        selected = published[-1] if published else (
            selection["windows"][0] if selection.get("state") == "REPROCESSING" else None)
    newest = selected if selected and selected["state"] == "PUBLISHED" else None
    network = next(row for row in newest["rows"] if row["grain"] == "network") if newest else {}
    intersections = [row for row in newest["rows"] if row["grain"] == "intersection"] if newest else []
    queue_order = sorted((row for row in intersections if row["queue_p95"]["status"] == "OK"),
                         key=lambda row: (-row["queue_p95"]["value"], row["intersection"]))
    trends = {metric: historical_trend(selection, metric=metric) for metric in
              ("moving_speed", "stopped_ratio", "observed_load", "queue_p95", "high_pressure_count")}
    calibrated = any(row.get("pressure_definition_digest") for window in published for row in window["rows"])
    participants = selection.get("participants") or sorted({row["intersection"] for window in published
        for row in window["rows"] if row["grain"] == "intersection"})
    intersection_trends = {node: {metric: [dict(point, end=point["start"] + selection["duration"])
        for point in historical_trend(selection, metric=metric, grain="intersection", intersection=node)] for metric in
        ("moving_speed", "stopped_ratio", "observed_load", "queue_p95")} for node in participants}
    heatmap = []
    markers = []
    previous = {}
    for window in selection["windows"]:
        rows = {row["intersection"]: row for row in window["rows"] if row["grain"] == "intersection"}
        for node in participants:
            row = rows.get(node)
            context = row.get("traffic_context") or {} if row else {}
            context_key = (tuple(sorted(context.get("scenario_samples", {}))),
                           tuple(sorted(context.get("control_samples", {}))))
            prior = previous.get(node)
            if row and prior and prior[0] + selection["duration"] == window["start"] and prior[1] != context_key:
                markers.append(dict(intersection=node, state="OBSERVED_WINDOW_CHANGE", start=window["start"],
                                    end=window["start"] + selection["duration"], before=prior[1], after=context_key))
            if row:
                previous[node] = window["start"], context_key
            value = (metric_status(row, "pressure_score") if row else
                     dict(value=None, status=window["state"] if window["state"] != "PUBLISHED" else "NO_ENTITY"))
            heatmap.append(dict(start=window["start"], intersection=node, **value,
                end=window["start"] + selection["duration"], publication=window.get("lineage"),
                definition=row.get("pressure_definition_digest") if row else None,
                worst_pressure_direction=row.get("worst_pressure_direction") if row else None,
                context_key=context_key, traffic_context=context))
    ranking = []
    direction_rows = {(row["intersection"], row["direction"]): row for row in newest["rows"]
                      if row["grain"] == "direction"} if newest else {}
    for row in intersections:
        score = metric_status(row, "pressure_score")
        pressure_direction = row.get("worst_pressure_direction")
        diagnostic = direction_rows.get(tuple(pressure_direction)) if pressure_direction else None
        ranking.append(dict(intersection=row["intersection"], score=score,
            worst_pressure_direction=pressure_direction, worst_queue_direction=row["worst_queue_direction"],
            worst_pressure_metrics={name: metric_status(diagnostic, name) for name in
                                    ("moving_speed", "stopped_ratio", "queue_p95", "occupancy_mean")} if diagnostic else None,
            traffic_context=row.get("traffic_context")))
    ranking.sort(key=lambda item: (item["score"]["value"] is None, -(item["score"]["value"] or 0),
                                   item["intersection"]))
    persistence = {node: summarize_persistence([cell for cell in heatmap if cell["intersection"] == node],
                                             selection["duration"]) for node in participants} if calibrated else {}
    for index, item in enumerate(ranking, 1):
        item["rank"] = index if item["score"]["status"] == "OK" else None
        node = item["intersection"]
        through_selected = [cell for cell in heatmap if cell["intersection"] == node and cell["start"] <= newest["start"]]
        if calibrated:
            item["current_high_streak"] = summarize_persistence(through_selected, selection["duration"]).get("current_streak")
            item["pressure_delta"] = next((point for point in reversed(compare_adjacent(through_selected,
                selection["duration"])) if point["start"] == newest["start"]), None)
    cards = {name: metric_status(network, name) for name in
             ("moving_speed", "stopped_ratio", "queue_p95", "high_pressure_count")}
    cards["queue_p95"] = dict(cards["queue_p95"], worst_queue_direction=network.get("worst_queue_direction"))
    return dict(selected_start=selected["start"] if selected else None,
        selected_state=selected["state"] if selected else "NO_PUBLISHED_RESULT",
        publication=newest.get("lineage") if newest else None, kpis=cards,
        queue_diagnosis=[dict(intersection=row["intersection"], p95=row["queue_p95"],
                              worst_direction=row["worst_queue_direction"]) for row in queue_order],
        pressure_ranking=dict(state=("CALIBRATION_REQUIRED" if not calibrated else
            "AVAILABLE" if all(item["rank"] is not None for item in ranking) and ranking else "INCOMPLETE_COVERAGE"),
            rows=ranking),
        bottleneck_persistence=dict(status="AVAILABLE" if calibrated else "CALIBRATION_REQUIRED", rows=persistence),
        pressure_heatmap=heatmap, context_markers=markers,
        trends=trends, intersection_trends=intersection_trends, change={metric: compare_adjacent(points, selection["duration"])
                              for metric, points in trends.items()})


def summarize_persistence(cells, duration):
    valid = high = longest = streak = 0
    first = peak = peak_start = previous = None
    definitions = {cell["definition"] for cell in cells if cell["status"] == "OK"}
    if len(definitions) > 1:
        return dict(status="INCOMPATIBLE_DEFINITION", expected=len(cells))
    for cell in cells:
        if cell["status"] != "OK":
            streak = 0
        else:
            valid += 1
            if peak is None or cell["value"] > peak:
                peak, peak_start = cell["value"], cell["start"]
            if cell["severity"] == "HIGH":
                high += 1
                streak = streak + 1 if previous is not None and cell["start"] - previous == duration else 1
                longest = max(longest, streak)
                first = cell["start"] if first is None else first
            else:
                streak = 0
        previous = cell["start"]
    complete = bool(cells) and valid == len(cells)
    changes = compare_adjacent(cells, duration)
    return dict(status="OK" if complete else "INCOMPLETE_COVERAGE", expected=len(cells), valid=valid,
                observed_high=high, high_share=high / len(cells) if complete else None,
                longest_streak=longest, current_streak=streak, first_high_start=first, peak=peak,
                peak_start=peak_start, latest_adjacent_delta=changes[-1] if changes else None)


def intersection_detail(selection, intersection):
    """Return topology-backed direction breakdown and trends for a pinned intersection."""
    directions = sorted({row["direction"] for window in selection["windows"] for row in window["rows"]
                         if row["grain"] == "direction" and row["intersection"] == intersection})
    windows = []
    for window in selection["windows"]:
        selected = [row for row in window["rows"] if row["intersection"] == intersection]
        intersection_row = next((row for row in selected if row["grain"] == "intersection"), None)
        windows.append(dict(start=window["start"], state=window["state"], rows=selected,
            worst_queue_direction=intersection_row["worst_queue_direction"] if intersection_row else None,
            traffic_context=intersection_row.get("traffic_context") if intersection_row else None,
            worst_pressure_direction=dict(value=intersection_row.get("worst_pressure_direction") if intersection_row else None,
                status=metric_status(intersection_row or {}, "pressure_score")["status"])))
    return dict(intersection=intersection, windows=windows, trends={
        direction: {metric: historical_trend(selection, grain="direction", intersection=intersection,
                     direction=direction, metric=metric) for metric in
                    ("moving_speed", "stopped_ratio", "observed_load", "queue_p95", "occupancy_mean", "pressure_score")}
        for direction in directions})
