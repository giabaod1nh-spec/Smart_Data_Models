"""Gold 1 frozen analytical contracts; no runtime I/O."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Final

GOLD_SCHEMA_VERSION: Final = "k10-gold-m1-v1"
WINDOW_SIZES_SEC: Final = (60, 300)
DIRECTION_MAPPING_VERSION: Final = "direction-v1"

# Locked current-row order for serving views (DESC on each component).
CURRENT_ROW_ORDER: Final = (
    "definition_major",
    "definition_minor",
    "revision_seq",
    "computed_at",
    "source_set_hash",
)

MAIN_DIM_TABLES: Final = (
    "gold_dim_run",
    "gold_dim_scenario",
    "gold_dim_intersection",
    "gold_dim_approach",
    "gold_dim_window",
    "gold_dim_metric_definition",
)
MAIN_FACT_TABLES: Final = (
    "gold_fact_traffic_window",
    "gold_fact_intersection_window",
    "gold_fact_traffic_comparison",
    "gold_fact_signal_operation_window",
    "gold_fact_kpi_result",
)
CONTROL_TABLES: Final = ("gold_processing_ledger",)
MART_VIEWS: Final = (
    "gold_mart_network_window_overview",
    "gold_mart_intersection_window_summary",
    "gold_mart_direction_window_summary",
    "gold_mart_congestion_window",
    "gold_mart_priority_window_ranking",
    "gold_mart_signal_operation_window",
)
ALL_GOLD_TABLES: Final = MAIN_DIM_TABLES + MAIN_FACT_TABLES + CONTROL_TABLES

MART_BUSINESS_IDENTITY: Final[dict[str, tuple[str, ...]]] = {
    "gold_mart_network_window_overview": (
        "namespace", "simulation_run_id", "scenario_id", "window_id",
    ),
    "gold_mart_intersection_window_summary": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "window_id",
    ),
    "gold_mart_direction_window_summary": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "direction",
        "window_id",
    ),
    "gold_mart_congestion_window": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "window_id",
        "metric_code",
    ),
    "gold_mart_priority_window_ranking": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "window_id",
        "metric_code",
    ),
    "gold_mart_signal_operation_window": (
        "namespace", "simulation_run_id", "scenario_id", "intersection_id", "direction",
        "window_id",
    ),
}

MART_SOURCE_FACT: Final[dict[str, str]] = {
    "gold_mart_network_window_overview": "gold_fact_intersection_window",
    "gold_mart_intersection_window_summary": "gold_fact_intersection_window",
    "gold_mart_direction_window_summary": "gold_fact_traffic_window",
    "gold_mart_congestion_window": "gold_fact_kpi_result",
    "gold_mart_priority_window_ranking": "gold_fact_kpi_result",
    "gold_mart_signal_operation_window": "gold_fact_signal_operation_window",
}

STRUCTURAL_MARTS_PENDING_GOLD2: Final = frozenset({"gold_mart_network_window_overview"})

LINEAGE_COLUMNS: Final = (
    "namespace", "source_set_hash", "source_row_count", "source_valid_row_count",
    "source_min_simulation_time", "source_max_simulation_time", "source_min_offset",
    "source_max_offset", "source_tables", "quality_status", "quality_flags",
    "analytical_freshness_status", "source_latest_simulation_time",
    "source_latest_processed_at", "computed_at", "gold_schema_version",
    "definition_version", "definition_major", "definition_minor", "revision_seq",
)

TRAFFIC_WINDOW_MEASURES: Final = (
    "avg_vehicle_count", "max_vehicle_count", "latest_vehicle_count",
    "avg_pcu_equivalent", "max_pcu_equivalent", "latest_pcu_equivalent",
    "avg_speed_kmh", "min_speed_kmh", "max_speed_kmh", "latest_speed_kmh",
    "avg_queue_length_m", "max_queue_length_m", "latest_queue_length_m",
    "avg_waiting_vehicle_count", "max_waiting_vehicle_count",
    "avg_occupancy_pct", "max_occupancy_pct", "avg_arrival_rate_pcu_per_sec",
    "max_arrival_rate_pcu_per_sec", "spillback_observation_count",
    "spillback_ratio_pct", "latest_traffic_status",
)
WINDOW_KEYS: Final = (
    "simulation_run_id", "scenario_id", "intersection_id", "window_id",
    "window_size_sec", "window_start_sim_sec", "window_end_sim_sec",
)
DIRECTION_KEYS: Final = ("direction", "source_direction", "direction_mapping_version")

TABLE_COLUMNS: Final[dict[str, tuple[str, ...]]] = {
    "gold_dim_run": (
        "simulation_run_id", "scenario_id", "seed", "producer_id", "started_at",
        "ended_at", "run_status", "contract_version", "node_count",
        "source_bronze_run_id", "source_hash", "definition_version",
        "definition_major", "definition_minor", "computed_at", "gold_schema_version",
    ),
    "gold_dim_scenario": (
        "scenario_id", "description", "source_hash", "definition_version",
        "definition_major", "definition_minor", "computed_at", "gold_schema_version",
    ),
    "gold_dim_intersection": (
        "intersection_id", "intersection_name", "latitude", "longitude", "network_zone",
        "connected_intersections", "source_hash", "definition_version",
        "definition_major", "definition_minor", "computed_at", "gold_schema_version",
    ),
    "gold_dim_approach": (
        "intersection_id", "direction", "source_direction", "direction_mapping_version",
        "source_hash", "definition_version", "definition_major", "definition_minor",
        "computed_at", "gold_schema_version",
    ),
    "gold_dim_window": (
        "window_id", "window_size_sec", "window_start_sim_sec", "window_end_sim_sec",
        "computed_at", "gold_schema_version",
    ),
    "gold_dim_metric_definition": (
        "metric_code", "metric_version", "metric_name", "description", "grain",
        "formula_identifier", "unit_code", "approval_status", "formula_json",
        "definition_version", "definition_major", "definition_minor", "computed_at",
        "gold_schema_version",
    ),
    "gold_fact_traffic_window": (
        "simulation_run_id", "scenario_id", "intersection_id", *DIRECTION_KEYS,
        "window_id", "window_size_sec", "window_start_sim_sec", "window_end_sim_sec",
        *TRAFFIC_WINDOW_MEASURES, *LINEAGE_COLUMNS,
    ),
    "gold_fact_intersection_window": (
        *WINDOW_KEYS, "avg_total_vehicle_count", "max_total_vehicle_count",
        "latest_total_vehicle_count", "latest_overall_traffic_status",
        "latest_derived_traffic_state", "latest_phase", "incident_observation_count",
        "incident_occurrence", "spillback_observation_count", "spillback_occurrence",
        "box_blocked_observation_count", "box_blocked_occurrence", *LINEAGE_COLUMNS,
    ),
    "gold_fact_traffic_comparison": (
        "simulation_run_id", "scenario_id", "intersection_id", *DIRECTION_KEYS,
        "metric_code",
        "current_window_id", "current_window_size_sec", "current_window_start_sim_sec",
        "current_window_end_sim_sec", "previous_window_id", "previous_window_start_sim_sec",
        "previous_window_end_sim_sec", "current_value", "previous_value", "absolute_change",
        "percent_change", "change_direction", "comparison_status", *LINEAGE_COLUMNS,
    ),
    "gold_fact_signal_operation_window": (
        "simulation_run_id", "scenario_id", "intersection_id", *DIRECTION_KEYS,
        "window_id", "window_size_sec", "window_start_sim_sec", "window_end_sim_sec",
        "observation_count", "green_observation_count", "red_observation_count",
        "yellow_observation_count", "other_status_count", "green_share_pct", "red_share_pct",
        "yellow_share_pct", "dominant_signal_status", "dominant_phase",
        "avg_configured_green_duration_sec", "avg_configured_red_duration_sec",
        "avg_configured_yellow_duration_sec", "latest_timing_mode",
        "ctx_avg_queue_length_m", "ctx_max_queue_length_m", *LINEAGE_COLUMNS,
    ),
    "gold_fact_kpi_result": (
        "simulation_run_id", "scenario_id", "intersection_id", *DIRECTION_KEYS,
        "window_id", "window_size_sec", "window_start_sim_sec", "window_end_sim_sec",
        "metric_code", "metric_version", "numeric_value", "unit_code", "status",
        "explanation_json", *LINEAGE_COLUMNS,
    ),
    "gold_processing_ledger": (
        "namespace", "source_set_hash", "definition_version", "definition_major",
        "definition_minor", "revision_seq", "disposition", "computed_at", "error_message",
        "gold_schema_version",
    ),
}

REALTIME_OWNED_FIELDS: Final = frozenset({
    "current_vehicle_count", "current_pcu", "current_average_speed", "current_queue_length",
    "current_waiting_vehicle_count", "current_occupancy", "current_arrival_rate",
    "current_traffic_state", "current_spillback", "current_active_incident",
    "current_signal_phase", "lamp_status", "remaining_green", "remaining_red",
    "remaining_yellow", "orion_entity_timestamp",
})
FORBIDDEN_MART_NAME_FRAGMENTS: Final = (
    "realtime", "live", "current-state", "current_state",
)
FORBIDDEN_GOLD_MEASURE_NAMES: Final = frozenset({
    "vehicle_count", "pcu_equivalent", "average_speed_kmh", "queue_length_m",
    "waiting_vehicle_count", "occupancy_pct", "arrival_rate_pcu_per_sec",
    "remaining_green_sec", "remaining_red_sec", "remaining_yellow_sec",
})

SOURCE_SILVER_TABLES: Final = (
    "silver_fact_traffic_observation", "silver_fact_intersection_state",
    "silver_fact_signal_state", "silver_fact_camera_observation", "silver_fact_run_event",
    "silver_dim_run", "silver_dim_scenario", "silver_dim_intersection", "silver_dim_approach",
)

# Gold-layer direction map (mirrors Silver producer evidence N/S/E/W; also accepts long forms).
DIRECTION_CANONICAL_MAP: Final[dict[str, str]] = {
    "N": "N", "NORTH": "N", "NORTHBOUND": "N",
    "S": "S", "SOUTH": "S", "SOUTHBOUND": "S",
    "E": "E", "EAST": "E", "EASTBOUND": "E",
    "W": "W", "WEST": "W", "WESTBOUND": "W",
}
CANONICAL_DIRECTIONS: Final = frozenset({"N", "S", "E", "W", "UNKNOWN"})
QUALITY_FLAG_NON_CANONICAL_DIRECTION: Final = "NON_CANONICAL_DIRECTION"

METRIC_SEMANTICS: Final = {
    "vehicle_count": {"category": "SNAPSHOT_GAUGE", "allowed": ("AVG", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "pcu_equivalent": {"category": "SNAPSHOT_GAUGE", "allowed": ("AVG", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "average_speed_kmh": {"category": "GAUGE", "allowed": ("AVG", "MIN", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "queue_length_m": {"category": "SNAPSHOT_GAUGE", "allowed": ("AVG", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "waiting_vehicle_count": {"category": "SNAPSHOT_GAUGE", "allowed": ("AVG", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "occupancy_pct": {"category": "SNAPSHOT_GAUGE", "allowed": ("AVG", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "arrival_rate_pcu_per_sec": {"category": "RATE", "allowed": ("AVG", "MAX", "LATEST"), "forbidden": ("SUM",)},
    "spillback_risk": {"category": "STATE_FLAG", "allowed": ("OR", "MAX", "LATEST", "RATIO"), "forbidden": ("SUM",)},
    "traffic_status": {"category": "CATEGORICAL_STATE", "allowed": ("LATEST", "DISTRIBUTION"), "forbidden": ("AVG", "SUM")},
    "derived_traffic_state": {"category": "CATEGORICAL_STATE", "allowed": ("LATEST", "WORST", "DISTRIBUTION"), "forbidden": ("AVG", "SUM")},
}

BD1: Final = {
    "metric_code": "CONGESTION_SCORE_WINDOW", "metric_version": "v1.0",
    "formula_identifier": "bd1_congestion_window_v1",
    "weights": {"queue": 0.35, "speed": 0.30, "occupancy": 0.20, "spillback": 0.15},
}
BD2: Final = {
    "metric_code": "SIGNAL_OPERATION_SUMMARY_WINDOW", "metric_version": "v1.0",
    "formula_identifier": "bd2_signal_operation_window_v1",
}
BD3: Final = {
    "metric_code": "INTERSECTION_PRIORITY_WINDOW", "metric_version": "v1.0",
    "formula_identifier": "bd3_priority_window_v1",
    "weights": {"congestion": 0.45, "queue_level": 0.20, "queue_growth": 0.15, "spillback": 0.10, "incident": 0.10},
    "quality_penalty": 0.10,
}

QUALITY_STATUS_VALUES: Final = frozenset({
    "VALID", "VALID_WITH_GAPS", "PARTIAL", "LOW_COVERAGE", "INSUFFICIENT_DATA",
    "CONFLICTED", "UNSUPPORTED",
})
ANALYTICAL_FRESHNESS_VALUES: Final = frozenset({
    "CLOSED_COMPLETE", "CLOSED_WITH_GAPS", "PARTIAL_WINDOW", "REVISED",
    "INSUFFICIENT_DATA", "CONFLICTED", "STALE_ANALYTICAL",
})
UNIT_CODES: Final = frozenset({
    "COUNT", "PCU", "KM_PER_HOUR", "METER", "PERCENT", "PCU_PER_SEC",
    "SECOND", "SCORE_0_100", "ORDINAL", "COMPOSITE_SUMMARY",
})

_VERSION_RE = re.compile(r"^v?(?P<major>\d+)\.(?P<minor>\d+)(?:\..*)?$")


def parse_definition_version(version: str) -> tuple[int, int]:
    """Parse display versions like v1.0 / v10.0 into numeric major/minor for ordering."""
    match = _VERSION_RE.match(str(version).strip())
    if match is None:
        raise ValueError(f"Unsupported definition version: {version!r}")
    return int(match.group("major")), int(match.group("minor"))


def current_row_sort_key(
    definition_major: int,
    definition_minor: int,
    revision_seq: int,
    computed_at_sort: float | int,
    source_set_hash: str,
) -> tuple:
    """Higher tuple wins. Never order by semantic-version strings."""
    return (
        int(definition_major),
        int(definition_minor),
        int(revision_seq),
        computed_at_sort,
        str(source_set_hash),
    )


def select_current_row(rows: list[dict]) -> dict:
    """Deterministic current-row selection for one business-identity group."""
    if not rows:
        raise ValueError("rows must be non-empty")
    return max(
        rows,
        key=lambda row: current_row_sort_key(
            int(row["definition_major"]),
            int(row["definition_minor"]),
            int(row["revision_seq"]),
            row["computed_at"],
            str(row["source_set_hash"]),
        ),
    )


def canonicalize_direction(raw: object) -> tuple[str, str, tuple[str, ...]]:
    """Return (canonical_direction, source_direction, quality_flags)."""
    source = "" if raw is None else str(raw)
    normalized = source.strip().upper()
    if not normalized:
        return "UNKNOWN", source, (QUALITY_FLAG_NON_CANONICAL_DIRECTION,)
    mapped = DIRECTION_CANONICAL_MAP.get(normalized)
    if mapped is None:
        return "UNKNOWN", source, (QUALITY_FLAG_NON_CANONICAL_DIRECTION,)
    return mapped, source, ()


def canonical_window_id(
    simulation_run_id: str, scenario_id: str, window_size_sec: int,
    window_start_sim_sec: float, window_end_sim_sec: float,
) -> str:
    """Return the locked SHA-256 window identity."""
    payload = {
        "scenario_id": scenario_id, "simulation_run_id": simulation_run_id,
        "window_end_sim_sec": window_end_sim_sec, "window_size_sec": window_size_sec,
        "window_start_sim_sec": window_start_sim_sec,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def assert_no_realtime_mart_names(names: tuple[str, ...] = MART_VIEWS) -> None:
    """Reject mart names that imply operational/current-state ownership."""
    bad = [
        name for name in names
        if any(fragment in name.lower() for fragment in FORBIDDEN_MART_NAME_FRAGMENTS)
    ]
    if bad:
        raise AssertionError(f"Forbidden Gold mart names: {bad}")
