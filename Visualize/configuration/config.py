"""
Central configuration for the SUMO TraCI backend.

Model numeric parameters (PCU, thresholds, detector geometry, …) come from
ParameterRegistry via model_params — do not hardcode duplicates here.
This module remains a compatibility facade for topology / publish / env wiring.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from types import MappingProxyType
from typing import Dict, List, Mapping, Optional, Tuple

from configuration.model_params import get_registry

# ── Paths ──────────────────────────────────────────────────────────
# Visualize project root (parent of configuration/)
VISUALIZE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = VISUALIZE_DIR
SUMO_ASSET_ROOT = VISUALIZE_DIR / "Visualize"
SUMO_ASSETS_DIR = SUMO_ASSET_ROOT  # compat alias
GENERATED_ROOT = VISUALIZE_DIR / "generated"
ARTIFACT_ROOT = VISUALIZE_DIR / "artifacts"
ARTIFACTS_DIR = ARTIFACT_ROOT  # compat alias
DEFAULT_SUMO_CONFIG = SUMO_ASSET_ROOT / "intersection.sumocfg"

VERSION = (
    (VISUALIZE_DIR / "VERSION").read_text(encoding="utf-8").splitlines()[0].split("=", 1)[-1].strip()
    if (VISUALIZE_DIR / "VERSION").exists()
    else "dev"
)

# Load frozen registry once at import (fail-fast if YAML invalid)
_REG = get_registry()

# ── Simulation / SUMO_CONFIG (env may override seed/step) ─────────
SIM_SEED = int(os.getenv("SUMO_SEED", str(_REG.get_value("sumo_config.seed.value"))))
SIM_BEGIN_SEC = int(_REG.get_value("sumo_config.sim_begin_sec.value"))
SIM_END_SEC = int(_REG.get_value("sumo_config.sim_end_sec.value"))
WARMUP_SEC = float(os.getenv("WARMUP_SEC", str(_REG.get_value("sumo_config.warmup_sec.value"))))
TIME_TO_TELEPORT = int(
    os.getenv("TIME_TO_TELEPORT", str(_REG.get_value("sumo_config.time_to_teleport.value")))
)

YELLOW_COMMITMENT_RATIO = float(_REG.threshold("yellow_commitment_position_ratio"))
BOX_OCCUPANCY_THRESHOLD = float(_REG.threshold("downstream_occupancy_threshold"))
BLOCKED_ENTRY_POSITION_RATIO = float(_REG.threshold("blocked_entry_position_ratio"))
MOTO_FRONT_ZONE_START_RATIO = float(_REG.threshold("moto_front_zone_start_ratio"))

CONTROL_API_PORT = int(os.getenv("CONTROL_API_PORT", "9090"))

# Live TraCI → WebSocket vehicle stream (Control API /ws/live)
LIVE_STREAM_ENABLED = os.getenv("LIVE_STREAM_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
)
LIVE_STREAM_HZ = float(os.getenv("LIVE_STREAM_HZ", "10"))
LIVE_STREAM_QUEUE_SIZE = int(os.getenv("LIVE_STREAM_QUEUE_SIZE", "3"))

# ── DetectorParameters (from registry detector profile) ───────────
_DET = _REG.detector_meta()
E1_OFFSET_FROM_END_M = float(_DET["e1_offset_from_end_m"]["value"])
E2_COVER_LENGTH_M = float(_DET["e2_cover_length_m"]["value"])
E2_OUT_POS_M = float(_DET["e2_out_pos_m"]["value"])
E2_OUT_LENGTH_M = float(_DET["e2_out_length_m"]["value"])
DETECTOR_PROFILE_ID = str(_DET.get("profile_id"))
DETECTOR_GEOMETRY_VERSION = str(_DET.get("geometry_version"))
DIR_CODE = {"North": "N", "South": "S", "East": "E", "West": "W"}

# ── NGSI node metadata ─────────────────────────────────────────────
INTERSECTION_META: Dict[str, Dict[str, object]] = {
    "A": {"name": "Nguyen Hue - Le Loi", "lat": 10.7769, "lng": 106.7009},
    "B": {"name": "Dien Bien Phu - Hai Ba Trung", "lat": 10.7889, "lng": 106.6917},
    "C": {"name": "Cong Hoa - Hoang Van Thu", "lat": 10.8005, "lng": 106.6520},
    "D": {"name": "Vo Van Kiet - Nguyen Tri Phuong", "lat": 10.7550, "lng": 106.6700},
}

DENSITY_TO_TRAFFIC_STATUS: Dict[str, str] = {
    "LOW": "LIGHT",
    "MEDIUM": "MODERATE",
    "HIGH": "HEAVY",
}

DIRECTION_TO_TRAFFIC: Dict[str, str] = {
    "North": "NORTHBOUND",
    "South": "SOUTHBOUND",
    "East": "EASTBOUND",
    "West": "WESTBOUND",
}

COLOR_TO_STATUS: Dict[str, str] = {
    "green": "GREEN",
    "yellow": "YELLOW",
    "red": "RED",
}

# ── PublishParameters / env ────────────────────────────────────────
ORION_URL = os.getenv("ORION_URL", "http://localhost:1026")
SUMO_GUI = os.getenv("SUMO_GUI", "true").lower() in ("1", "true", "yes")
SUMO_CONFIG = Path(os.getenv("SUMO_CONFIG", str(DEFAULT_SUMO_CONFIG)))
SUMO_STEP_LENGTH = float(
    os.getenv("SUMO_STEP_LENGTH", str(_REG.get_value("sumo_config.step_length.value")))
)
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))
PUBLISH_NODES = [
    n.strip()
    for n in os.getenv("PUBLISH_NODES", os.getenv("PUBLISH_NODE", "A,B,C,D,M")).split(",")
    if n.strip()
]
PUBLISH_NODE = PUBLISH_NODES[0]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Async Orion publisher (ADR-015) ────────────────────────────────
ORION_ASYNC_PUBLISH = os.getenv("ORION_ASYNC_PUBLISH", "true").lower() in (
    "1",
    "true",
    "yes",
)
ORION_PUBLISH_QUEUE_SIZE = int(os.getenv("ORION_PUBLISH_QUEUE_SIZE", "5"))
ORION_PUBLISH_BACKPRESSURE_WARN_DEPTH = int(
    os.getenv("ORION_PUBLISH_BACKPRESSURE_WARN_DEPTH", "3")
)
ORION_PUBLISH_MAX_LAG_SEC = float(os.getenv("ORION_PUBLISH_MAX_LAG_SEC", "5"))
ORION_PUBLISH_RESUME_LAG_SEC = float(os.getenv("ORION_PUBLISH_RESUME_LAG_SEC", "2"))
ORION_PUBLISH_RESUME_MAX_DEPTH = int(os.getenv("ORION_PUBLISH_RESUME_MAX_DEPTH", "1"))
ORION_PUBLISH_WORKER_COUNT = int(os.getenv("ORION_PUBLISH_WORKER_COUNT", "1"))
ORION_PUBLISH_SHUTDOWN_TIMEOUT_SEC = float(
    os.getenv("ORION_PUBLISH_SHUTDOWN_TIMEOUT_SEC", "15")
)
ORION_PUBLISH_RETRY_MAX = int(os.getenv("ORION_PUBLISH_RETRY_MAX", "10"))
ORION_PUBLISH_RETRY_BASE_SEC = float(os.getenv("ORION_PUBLISH_RETRY_BASE_SEC", "0.5"))
ORION_PUBLISH_RETRY_SLOW_SEC = float(os.getenv("ORION_PUBLISH_RETRY_SLOW_SEC", "5"))
ORION_PUBLISH_HTTP_TIMEOUT_SEC = float(os.getenv("ORION_PUBLISH_HTTP_TIMEOUT_SEC", "5"))
# Publish mode: sequential (default) | batch. Bound at AsyncOrionPublisher construct time.
_ORION_PUBLISH_MODE_RAW = os.getenv("ORION_PUBLISH_MODE", "sequential").strip().lower()
ORION_PUBLISH_MODE = (
    _ORION_PUBLISH_MODE_RAW if _ORION_PUBLISH_MODE_RAW in ("sequential", "batch") else "sequential"
)
ORION_BATCH_OPTIONS = os.getenv("ORION_BATCH_OPTIONS", "update").strip().lower() or "update"
ORION_PUBLISH_BATCH_TIMEOUT_SEC = float(
    os.getenv("ORION_PUBLISH_BATCH_TIMEOUT_SEC", "10")
)
# Runtime gate initial value only (K-4.5). Mid-run: POST /control/orion-publish.
ORION_PUBLISH_ENABLED = os.getenv("ORION_PUBLISH_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Architecture deployment profile (none|migration|final) — validation only when set
ARCHITECTURE_PROFILE = os.getenv("ARCHITECTURE_PROFILE", "none").strip().lower() or "none"
ARCHITECTURE_LOCK_SMOKE = os.getenv("ARCHITECTURE_LOCK_SMOKE", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Projector (K-3) — defaults keep migration/K-4.5 safe (shadow ON)
PROJECTOR_SHADOW_MODE = os.getenv("PROJECTOR_SHADOW_MODE", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PROJECTOR_TARGET_NAMESPACE = (
    os.getenv("PROJECTOR_TARGET_NAMESPACE", "shadow").strip().lower() or "shadow"
)
PROJECTOR_HEALTH_HOST = os.getenv("PROJECTOR_HEALTH_HOST", "0.0.0.0")
PROJECTOR_HEALTH_PORT = int(os.getenv("PROJECTOR_HEALTH_PORT", "8092"))

# Sync publish is debug-only; migration/final profiles forbid it
ORION_SYNC_PUBLISH = os.getenv("ORION_SYNC_PUBLISH", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# ── Kafka dual-publish (K-2a) — default OFF until soak ─────────────
KAFKA_PUBLISH_ENABLED = os.getenv("KAFKA_PUBLISH_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic.entity-events.v2")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "visualize-kafka-producer")
KAFKA_PRODUCER_ID = os.getenv("KAFKA_PRODUCER_ID", "visualize-traci")
KAFKA_LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))
KAFKA_DELIVERY_TIMEOUT_MS = int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "30000"))
KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"))
KAFKA_EVIDENCE_ROOT = Path(
    os.getenv(
        "KAFKA_EVIDENCE_ROOT",
        str(ARTIFACT_ROOT / "kafka_producer_ledger"),
    )
)

# ── Kafka durable outbox (K-2b) — default OFF; supersedes K-2a produce when on ─
KAFKA_OUTBOX_ENABLED = os.getenv("KAFKA_OUTBOX_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
)
KAFKA_OUTBOX_DB = Path(
    os.getenv(
        "KAFKA_OUTBOX_DB",
        str(ARTIFACT_ROOT / "kafka_outbox" / "outbox.sqlite3"),
    )
)
KAFKA_OUTBOX_MAX_IN_FLIGHT = int(os.getenv("KAFKA_OUTBOX_MAX_IN_FLIGHT", "32"))
KAFKA_OUTBOX_ACKED_RETENTION_DAYS = int(
    os.getenv("KAFKA_OUTBOX_ACKED_RETENTION_DAYS", "7")
)
KAFKA_OUTBOX_DISK_WARN_BYTES = int(
    os.getenv("KAFKA_OUTBOX_DISK_WARN_BYTES", str(512 * 1024 * 1024))
)
KAFKA_OUTBOX_DISK_FAULT_BYTES = int(
    os.getenv("KAFKA_OUTBOX_DISK_FAULT_BYTES", str(64 * 1024 * 1024))
)

PCU_FALLBACK = float(
    os.getenv("PCU_FALLBACK", str(_REG.export_effective_config().get("unknown_vtype_fallback_pce", 1.0)))
)
HALTING_SPEED_MS = float(
    os.getenv("HALTING_SPEED_MS", str(_REG.threshold("halting_speed_ms")))
)

# ── TopologyConfig (ADR-002/003) — structural, not model PCE ───────
LANES_PER_APPROACH = 3  # compatibility fallback; OSM edges use EDGE_LANE_COUNTS
MOVEMENT_BY_LANE_INDEX = {0: "right", 1: "straight", 2: "left"}

NODE_TO_TLS: Dict[str, str] = {
    "A": "411926415",
    "B": "14203086108",
    "C": "14203056181",
    "D": "411925926",
    "M": "14203056194",
}

# A physical intersection imported from OSM can be split into several SUMO TLS
# controllers.  NODE_TO_TLS keeps one primary ID for backwards-compatible
# snapshots/detectors; NODE_TO_TLS_GROUPS lists every controller that must move
# together for that physical intersection.
NODE_TO_TLS_GROUPS: Dict[str, Tuple[str, ...]] = {
    "A": ("411926415",),
    "B": ("14203086108", "14203086109", "14203086112"),  # cluster around (253.65, 236.76)
    "C": ("14203056181", "14203056185", "14203056187", "14203056189"),  # above old C
    "D": ("411925926",),
    "M": ("14203056194", "14203056195", "14203056196", "14203056197"),
}

# Physical controllers assigned to the virtual NS/EW axes used by the common
# four-phase agent action vocabulary.
GROUPED_TLS_AXES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "B": {
        "NS": ("14203086108", "14203086109"),
        "EW": ("14203086112",),
    },
    "C": {
        "NS": ("14203056181", "14203056189"),
        "EW": ("14203056185", "14203056187"),
    },
    "M": {
        "NS": ("14203056194", "14203056196"),
        "EW": ("14203056195", "14203056197"),
    },
}

TLS_TO_NODE: Dict[str, str] = {
    tls_id: node_id
    for node_id, tls_ids in NODE_TO_TLS_GROUPS.items()
    for tls_id in tls_ids
}


def tls_ids_for_node(node_id: str) -> Tuple[str, ...]:
    return NODE_TO_TLS_GROUPS.get(node_id, (NODE_TO_TLS[node_id],))

APPROACH_EDGES: Dict[str, Dict[str, str]] = {
    "411926415": {"East": "-694973742#0", "South": "408246393#6", "West": "35114553#3"},
    "14203086108": {
        "North": "35113721#14",
        "South": "-1179966753#1",
        "East": "654858471#7",
    },
    "14203056181": {
        "North": "35113721#3",
        "East": "658328100#1",
        "South": "-35113721#6",
        "West": "-658328100#4",
    },
    "411925926": {"North": "1267138238#1", "East": "-694942823", "West": "694973742#5"},
    "14203056194": {
        "North": "35113721#9",
        "East": "-694973742#4",
        "South": "-35113721#12",
        "West": "694973742#1",
    },
}

OUTGOING_EDGES: Dict[str, List[str]] = {
    "411926415": ["-35114553#3", "408246393#7", "694973742#0"],
    "14203086108": ["-35113721#14", "1179966753#1", "654858471#9"],
    "14203056181": ["-35113721#3", "-658328100#1", "35113721#6", "658328100#4"],
    "411925926": ["-694973742#5", "326771005#0", "694942823"],
    "14203056194": ["-35113721#9", "694973742#4", "35113721#12", "-694973742#1"],
    "411926403": ["-658328100#5", "408246393#8", "658328100#6"],
    "411926496": ["-1184372244#1", "1267138238#0", "658328100#0"],
}

# Exact lane counts for the controlled OSM approaches/exits. Other edges retain
# the legacy fallback so external callers remain backward-compatible.
EDGE_LANE_COUNTS: Dict[str, int] = {
    "-694973742#0": 1, "408246393#6": 3, "35114553#3": 1,
    "-35114553#3": 1, "408246393#7": 3, "694973742#0": 1,
    "35113721#14": 1, "-1179966753#1": 1, "654858471#7": 3,
    "-35113721#14": 1, "1179966753#1": 1, "654858471#9": 3,
    "35113721#3": 1, "658328100#1": 1, "-35113721#6": 1, "-658328100#4": 1,
    "-35113721#3": 1, "-658328100#1": 1, "35113721#6": 1, "658328100#4": 1,
    "1267138238#1": 3, "-694942823": 1, "694973742#5": 1,
    "-694973742#5": 1, "326771005#0": 3, "694942823": 1,
    "35113721#9": 1, "-694973742#4": 1, "-35113721#12": 1, "694973742#1": 1,
    "-35113721#9": 1, "694973742#4": 1, "35113721#12": 1, "-694973742#1": 1,
}

DIRECTIONS: List[str] = ["North", "South", "East", "West"]


def approach_lane_ids(tls_id: str, direction: str) -> List[str]:
    edge = APPROACH_EDGES.get(tls_id, {}).get(direction)
    return edge_lane_ids(edge) if edge else []


def edge_lane_ids(edge: Optional[str]) -> List[str]:
    if not edge:
        return []
    return [f"{edge}_{i}" for i in range(EDGE_LANE_COUNTS.get(edge, LANES_PER_APPROACH))]


def movement_for_lane(edge: str, lane_index: int) -> str:
    if EDGE_LANE_COUNTS.get(edge, LANES_PER_APPROACH) == 1:
        return "straight"
    return MOVEMENT_BY_LANE_INDEX.get(lane_index, "straight")


def detector_id_e1(tls_id: str, direction: str, lane_index: int) -> str:
    return f"E1_{tls_id}_{DIR_CODE[direction]}_{lane_index}"


def detector_id_e2(tls_id: str, direction: str, lane_index: int) -> str:
    return f"E2_{tls_id}_{DIR_CODE[direction]}_{lane_index}"


def detector_id_e2_out(tls_id: str, edge: str, lane_index: int) -> str:
    return f"E2OUT_{tls_id}_{edge}_{lane_index}"


TURN_BY_OD: Dict[Tuple[str, str], str] = {
    ("408246393#6", "694973742#0"): "right", ("408246393#6", "408246393#7"): "straight", ("408246393#6", "-35114553#3"): "left",
    ("35113721#14", "35113721#15"): "straight", ("-1179966753#1", "-1179966753#0"): "straight", ("654858471#7", "654858471#8"): "straight",
    ("35113721#3", "35113721#4"): "straight", ("658328100#1", "658328100#2"): "straight",
    ("-35113721#6", "-35113721#5"): "straight", ("-658328100#4", "-658328100#3"): "straight",
    ("35113721#9", "35113721#10"): "straight", ("-694973742#4", "-694973742#3"): "straight",
    ("-35113721#12", "-35113721#11"): "straight", ("694973742#1", "694973742#2"): "straight",
    ("1267138238#1", "-694973742#5"): "right", ("1267138238#1", "326771005#0"): "straight", ("1267138238#1", "694942823"): "left",
    ("408246393#7", "-658328100#5"): "right", ("408246393#7", "408246393#8"): "straight", ("408246393#7", "658328100#6"): "left",
    ("1184372243", "658328100#0"): "right", ("1184372243", "1267138238#0"): "straight", ("1184372243", "-1184372244#1"): "left",
}

PHASE_INDEX_TO_NAME: Dict[int, str] = {
    0: "EW_GREEN",
    1: "EW_YELLOW",
    2: "NS_GREEN",
    3: "NS_YELLOW",
}
PHASE_NAME_TO_INDEX: Dict[str, int] = {v: k for k, v in PHASE_INDEX_TO_NAME.items()}
PHASE_SEQUENCE: List[str] = ["NS_GREEN", "NS_YELLOW", "EW_GREEN", "EW_YELLOW"]

PHASE_COLORS: Dict[str, Dict[str, str]] = {
    "NS_GREEN": {"North": "green", "South": "green", "East": "red", "West": "red"},
    "NS_YELLOW": {"North": "yellow", "South": "yellow", "East": "red", "West": "red"},
    "EW_GREEN": {"North": "red", "South": "red", "East": "green", "West": "green"},
    "EW_YELLOW": {"North": "red", "South": "red", "East": "yellow", "West": "yellow"},
}

FORCE_VIA_YELLOW: Dict[str, str] = {
    "NS_GREEN": "NS_YELLOW",
    "EW_GREEN": "EW_YELLOW",
}

DIRECTION_TO_GREEN_PHASE: Dict[str, str] = {
    "North": "NS_GREEN",
    "South": "NS_GREEN",
    "East": "EW_GREEN",
    "West": "EW_GREEN",
}

EMERGENCY_VTYPES = frozenset({"ambulance", "police", "firetruck"})

# Read-only PCU mapping from registry (compat for legacy imports)
PCU_FACTORS: Mapping[str, float] = MappingProxyType(dict(_REG.pcu_factors()))

DENSITY_THRESHOLDS_PER_DIRECTION = _REG.traffic_load_bins_direction()
DENSITY_THRESHOLDS_INTERSECTION = _REG.traffic_load_bins_intersection()

SCENARIO_IDS = (
    "normal",
    "peak",
    "oversaturated",
    "rain",
    "incident",
    # legacy (compat — normalized to canonical)
    "morning_peak",
    "evening_peak",
    "heavy_traffic",
    "heavy_rain",
    "accident",
    "emergency",
    "blocked_intersection",
    "spillback",
)

CANONICAL_SCENARIO_IDS = frozenset(
    {"normal", "peak", "oversaturated", "rain", "incident"}
)

LEGACY_SCENARIO_ALIASES: Dict[str, str] = {
    "morning_peak": "peak",
    "evening_peak": "peak",
    "heavy_traffic": "peak",
    "heavy_rain": "rain",
    "accident": "incident",
    "blocked_intersection": "incident",
    "spillback": "peak",
    "emergency": "incident",
}

DEMAND_PROFILE_IDS = frozenset(
    {
        "normal",
        "peak",
        "oversaturated",
        # legacy demand ids (normalized to peak where applicable)
        "morning_peak",
        "evening_peak",
        "heavy_traffic",
    }
)

# Deterministic incident approach per node (no UI direction picker).
INCIDENT_APPROACH_BY_NODE: Dict[str, str] = {
    "A": "South",
    "B": "East",
    "C": "North",
    "D": "North",
    "M": "West",
}


def incident_approach_direction(node_id: str) -> str:
    return INCIDENT_APPROACH_BY_NODE.get(node_id, "North")

_log = logging.getLogger(__name__)


def normalize_demand_profile_id(profile_id: str) -> str:
    """Map legacy demand profile ids to canonical ids."""
    pid = profile_id.strip().lower()
    if pid in ("morning_peak", "evening_peak", "heavy_traffic"):
        return "peak"
    return pid


def normalize_scenario_id(scenario_id: str) -> str:
    """Normalize to canonical scenario id."""
    sid = scenario_id.strip().lower()
    return LEGACY_SCENARIO_ALIASES.get(sid, sid)


def is_known_scenario_id(scenario_id: str) -> bool:
    sid = normalize_scenario_id(scenario_id.strip().lower())
    return sid in CANONICAL_SCENARIO_IDS or scenario_id.strip().lower() in SCENARIO_IDS

_sc = _REG.export_effective_config().get("scenarios") or {}
SCENARIO_TRAFFIC_SCALE: Dict[str, float] = dict(_sc.get("traffic_scale") or {})
SCENARIO_SPEED_FACTOR: Dict[str, float] = dict(_sc.get("speed_factor") or {})

BASE_V_FREE_KMH = float(_REG.threshold("base_v_free_kmh"))
APPROACH_LENGTH_M = float(_REG.threshold("approach_length_m"))
K_JAM_PCU_PER_KM = float(_REG.threshold("k_jam_pcu_per_km"))
try:
    PASSENGER_CAR_LENGTH_M = float(_REG.threshold("passenger_car_length_m"))
except Exception:
    PASSENGER_CAR_LENGTH_M = 7.5

# Provenance exports
MODEL_SCHEMA_VERSION = _REG.schema_version
PCU_PROFILE_ID = _REG.profile_id
PCU_PROFILE_VERSION = _REG.profile_version
CONFIG_HASH = _REG.config_hash
GREEN_DURATION_FALLBACK = int(_REG.get_value("sumo_config.green_duration_fallback_sec.value"))
YELLOW_DURATION_FALLBACK = int(_REG.get_value("sumo_config.yellow_duration_fallback_sec.value"))


def resolve_sumo_binary(use_gui: Optional[bool] = None) -> str:
    import shutil

    gui = SUMO_GUI if use_gui is None else use_gui
    names = ["sumo-gui", "sumo-gui.exe"] if gui else ["sumo", "sumo.exe"]
    fallback = ["sumo", "sumo.exe"] if gui else []

    sumo_home = os.environ.get("SUMO_HOME", "").strip()
    candidates: List[Path] = []
    if sumo_home:
        bin_dir = Path(sumo_home) / "bin"
        for n in names + fallback:
            candidates.append(bin_dir / n)

    for n in names + fallback:
        found = shutil.which(n)
        if found:
            return found

    for c in candidates:
        if c.is_file():
            return str(c)

    raise RuntimeError(
        "SUMO binary not found. Set SUMO_HOME to your SUMO install "
        "(e.g. C:\\Program Files (x86)\\Eclipse\\Sumo) and ensure "
        "%SUMO_HOME%\\bin is on PATH, or install SUMO from https://eclipse.dev/sumo/"
    )


def ensure_sumo_tools_on_path() -> None:
    import sys

    sumo_home = os.environ.get("SUMO_HOME", "").strip()
    if not sumo_home:
        return
    tools = Path(sumo_home) / "tools"
    if tools.is_dir() and str(tools) not in sys.path:
        sys.path.insert(0, str(tools))


def density_label_intersection(total_pcu: float) -> str:
    """Traffic Load Class from sum of approach PCU counts — not PCU/km density."""
    for label, (lo, hi) in DENSITY_THRESHOLDS_INTERSECTION.items():
        if lo <= total_pcu < hi:
            return label
    return "HIGH"


def traffic_status_from_density(density: str) -> str:
    return DENSITY_TO_TRAFFIC_STATUS.get(density, "MODERATE")


def lane_length_m(traci_module, lane_id: str) -> float:
    try:
        return float(traci_module.lane.getLength(lane_id))
    except Exception:
        return APPROACH_LENGTH_M
