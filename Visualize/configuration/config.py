"""
Central configuration for the SUMO TraCI backend.

Model numeric parameters (PCU, thresholds, detector geometry, …) come from
ParameterRegistry via model_params — do not hardcode duplicates here.
This module remains a compatibility facade for topology / publish / env wiring.
"""
from __future__ import annotations

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
    for n in os.getenv("PUBLISH_NODES", os.getenv("PUBLISH_NODE", "A")).split(",")
    if n.strip()
]
PUBLISH_NODE = PUBLISH_NODES[0]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PCU_FALLBACK = float(
    os.getenv("PCU_FALLBACK", str(_REG.export_effective_config().get("unknown_vtype_fallback_pce", 1.0)))
)
HALTING_SPEED_MS = float(
    os.getenv("HALTING_SPEED_MS", str(_REG.threshold("halting_speed_ms")))
)

# ── TopologyConfig (ADR-002/003) — structural, not model PCE ───────
LANES_PER_APPROACH = 3
MOVEMENT_BY_LANE_INDEX = {0: "right", 1: "straight", 2: "left"}

NODE_TO_TLS: Dict[str, str] = {
    "A": "J1",
    "B": "J2",
    "C": "J3",
    "D": "J4",
}
TLS_TO_NODE: Dict[str, str] = {v: k for k, v in NODE_TO_TLS.items()}

APPROACH_EDGES: Dict[str, Dict[str, str]] = {
    "J1": {"North": "J3J1", "East": "J2J1", "South": "S1J1", "West": "W1J1"},
    "J2": {"North": "J4J2", "East": "E1J2", "South": "S2J2", "West": "J1J2"},
    "J3": {"North": "N1J3", "East": "J4J3", "South": "J1J3", "West": "W2J3"},
    "J4": {"North": "N2J4", "East": "E2J4", "South": "J2J4", "West": "J3J4"},
}

OUTGOING_EDGES: Dict[str, List[str]] = {
    "J1": ["J1J2", "J1J3", "J1S1", "J1W1"],
    "J2": ["J2J1", "J2J4", "J2E1", "J2S2"],
    "J3": ["J3J1", "J3J4", "J3N1", "J3W2"],
    "J4": ["J4J2", "J4J3", "J4N2", "J4E2"],
}

DIRECTIONS: List[str] = ["North", "South", "East", "West"]


def approach_lane_ids(tls_id: str, direction: str) -> List[str]:
    edge = APPROACH_EDGES[tls_id][direction]
    return [f"{edge}_{i}" for i in range(LANES_PER_APPROACH)]


def detector_id_e1(tls_id: str, direction: str, lane_index: int) -> str:
    return f"E1_{tls_id}_{DIR_CODE[direction]}_{lane_index}"


def detector_id_e2(tls_id: str, direction: str, lane_index: int) -> str:
    return f"E2_{tls_id}_{DIR_CODE[direction]}_{lane_index}"


def detector_id_e2_out(tls_id: str, edge: str, lane_index: int) -> str:
    return f"E2OUT_{tls_id}_{edge}_{lane_index}"


TURN_BY_OD: Dict[Tuple[str, str], str] = {
    ("J3J1", "J1W1"): "right", ("J3J1", "J1S1"): "straight", ("J3J1", "J1J2"): "left", ("J3J1", "J1J3"): "straight",
    ("J2J1", "J1J3"): "right", ("J2J1", "J1W1"): "straight", ("J2J1", "J1S1"): "left", ("J2J1", "J1J2"): "straight",
    ("S1J1", "J1J2"): "right", ("S1J1", "J1J3"): "straight", ("S1J1", "J1W1"): "left", ("S1J1", "J1S1"): "straight",
    ("W1J1", "J1S1"): "right", ("W1J1", "J1J2"): "straight", ("W1J1", "J1J3"): "left", ("W1J1", "J1W1"): "straight",
    ("J4J2", "J2J1"): "right", ("J4J2", "J2S2"): "straight", ("J4J2", "J2E1"): "left", ("J4J2", "J2J4"): "straight",
    ("E1J2", "J2J4"): "right", ("E1J2", "J2J1"): "straight", ("E1J2", "J2S2"): "left", ("E1J2", "J2E1"): "straight",
    ("S2J2", "J2E1"): "right", ("S2J2", "J2J4"): "straight", ("S2J2", "J2J1"): "left", ("S2J2", "J2S2"): "straight",
    ("J1J2", "J2S2"): "right", ("J1J2", "J2E1"): "straight", ("J1J2", "J2J4"): "left", ("J1J2", "J2J1"): "straight",
    ("N1J3", "J3W2"): "right", ("N1J3", "J3J1"): "straight", ("N1J3", "J3J4"): "left", ("N1J3", "J3N1"): "straight",
    ("J4J3", "J3N1"): "right", ("J4J3", "J3W2"): "straight", ("J4J3", "J3J1"): "left", ("J4J3", "J3J4"): "straight",
    ("J1J3", "J3J4"): "right", ("J1J3", "J3N1"): "straight", ("J1J3", "J3W2"): "left", ("J1J3", "J3J1"): "straight",
    ("W2J3", "J3J1"): "right", ("W2J3", "J3J4"): "straight", ("W2J3", "J3N1"): "left", ("W2J3", "J3W2"): "straight",
    ("N2J4", "J4J3"): "right", ("N2J4", "J4J2"): "straight", ("N2J4", "J4E2"): "left", ("N2J4", "J4N2"): "straight",
    ("E2J4", "J4N2"): "right", ("E2J4", "J4J3"): "straight", ("E2J4", "J4J2"): "left", ("E2J4", "J4E2"): "straight",
    ("J2J4", "J4E2"): "right", ("J2J4", "J4N2"): "straight", ("J2J4", "J4J3"): "left", ("J2J4", "J4J2"): "straight",
    ("J3J4", "J4J2"): "right", ("J3J4", "J4E2"): "straight", ("J3J4", "J4N2"): "left", ("J3J4", "J4J3"): "straight",
}

PHASE_INDEX_TO_NAME: Dict[int, str] = {
    0: "NS_GREEN",
    1: "NS_YELLOW",
    2: "EW_GREEN",
    3: "EW_YELLOW",
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
    "morning_peak",
    "evening_peak",
    "oversaturated",
    "rain",
    "heavy_rain",
    "accident",
    "emergency",
    "blocked_intersection",
    "spillback",
)

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
