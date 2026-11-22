from copy import deepcopy

from de.tests.silver.v1_fixtures import snapshot_v1 as _snapshot
from contracts.traffic_observation import build_observation
from de.silver.archive_resolver import VerifiedRunContext
from de.silver.fact_builders import build_v1_fact_rows


def test_v1_fact_cardinality_and_unavailable_preservation():
    snapshot = _snapshot()
    node = snapshot["node_id"]
    directions = tuple(snapshot["directions"])
    context = VerifiedRunContext(snapshot["simulation_run_id"], 1,
        snapshot["topology_digest"], "a" * 64, (node,), {node: directions},
        {}, {}, "b" * 64, "c" * 64, "RUNNING", "t1")
    one = directions[0]
    snapshot["directions"][one]["vehicle_count"] = None
    snapshot["directions"][one]["metric_availability"] = {"vehicle_count": "UNAVAILABLE"}
    event = build_observation(snapshot, cycle_sequence=4, simulation_step=1200)
    provenance = dict(bronze_semantic_revision=1, bronze_canonical_hash="d" * 64,
        source_raw_ingestion_id="raw", source_topic="traffic.observations.v1",
        source_partition=0, source_offset=12, source_physical_record_hash="e" * 64,
        source_byte_hash="f" * 64)
    rows = build_v1_fact_rows(event, context, provenance)
    assert (len(rows["traffic"]), len(rows["signal"]), len(rows["intersection"])) == (
        len(directions), len(directions), 1)
    assert rows["traffic"][0]["vehicle_count"] is None
    assert rows["intersection"][0]["total_vehicle_count"] is None
    assert rows["intersection"][0]["total_vehicle_count_quality"] == "PARTIAL_OR_UNAVAILABLE"
    measured = deepcopy(snapshot)
    measured["directions"][one]["vehicle_count"] = 0
    del measured["directions"][one]["metric_availability"]
    measured_event = build_observation(measured, cycle_sequence=5, simulation_step=1300)
    complete = build_v1_fact_rows(measured_event, context, provenance)
    assert complete["intersection"][0]["total_vehicle_count"] == 0
