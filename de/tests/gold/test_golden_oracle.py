import json
from dataclasses import replace
from pathlib import Path

from de.gold.engine import GoldTransformationEngine


def test_two_window_business_oracle(context, two_window_records):
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "gold2_two_window_oracle.json").read_text(encoding="utf-8")
    )
    result = GoldTransformationEngine().transform(two_window_records, context)
    kpis = {
        (item.metric_code, item.window_size_sec, item.window_start_sim_sec): item
        for item in result.kpi_results
        if item.intersection_id == "J1"
    }
    assert kpis[("CONGESTION_SCORE_WINDOW", 60, 0)].numeric_value == fixture["previous_congestion"]
    assert kpis[("CONGESTION_SCORE_WINDOW", 60, 60)].numeric_value == fixture["current_congestion"]
    assert kpis[("INTERSECTION_PRIORITY_WINDOW", 60, 60)].numeric_value == fixture["current_priority"]
    assert kpis[("PRIORITY_RANK", 60, 60)].numeric_value == fixture["current_rank"]
    comparison = next(item for item in result.comparisons if item.metric_code == "MAX_QUEUE_LENGTH_M" and item.current_window_size_sec == 60 and item.current_window_start_sim_sec == 60)
    assert (comparison.current_value, comparison.previous_value, comparison.absolute_change, comparison.percent_change) == tuple(fixture["max_queue_comparison"])
    congestion = kpis[("CONGESTION_SCORE_WINDOW", 60, 60)]
    assert sum(factor["contribution"] for factor in json.loads(congestion.explanation_json)["factors"]) == 62.5


def test_output_grains_are_unique_and_runs_never_mix(context, two_window_records):
    second_run = tuple(replace(row, simulation_run_id="run-2") for row in two_window_records)
    result = GoldTransformationEngine().transform(two_window_records + second_run, context)
    traffic_grains = {
        (item.namespace, item.simulation_run_id, item.scenario_id, item.intersection_id,
         item.direction, item.window_id, item.definition_version, item.revision_seq)
        for item in result.traffic_windows
    }
    assert len(traffic_grains) == len(result.traffic_windows)
    assert {item.simulation_run_id for item in result.traffic_windows} == {"run-1", "run-2"}
    for item in result.traffic_windows:
        assert item.window_end_sim_sec - item.window_start_sim_sec == item.window_size_sec

