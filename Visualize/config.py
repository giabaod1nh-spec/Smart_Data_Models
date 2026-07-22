"""
Central configuration for the SUMO TraCI backend (v1).

All NGSI ↔ SUMO ID mappings and PCU tables live here — do not scatter them.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Paths ──────────────────────────────────────────────────────────
VISUALIZE_DIR = Path(__file__).resolve().parent
SUMO_ASSETS_DIR = VISUALIZE_DIR / "Visualize"
SIMULATOR_DIR = VISUALIZE_DIR.parent / "simulator"

DEFAULT_SUMO_CONFIG = SUMO_ASSETS_DIR / "intersection.sumocfg"

# ── Environment / runtime ──────────────────────────────────────────
ORION_URL = os.getenv("ORION_URL", "http://localhost:1026")
SUMO_GUI = os.getenv("SUMO_GUI", "true").lower() in ("1", "true", "yes")
SUMO_CONFIG = Path(os.getenv("SUMO_CONFIG", str(DEFAULT_SUMO_CONFIG)))
# Prefer env; otherwise keep sumocfg step-length (0.01). TraCI can override via --step-length.
SUMO_STEP_LENGTH = float(os.getenv("SUMO_STEP_LENGTH", "0.01"))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))  # simulation seconds
PUBLISH_NODE = os.getenv("PUBLISH_NODE", "A")  # NGSI node id
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PCU_FALLBACK = float(os.getenv("PCU_FALLBACK", "1.0"))
HALTING_SPEED_MS = float(os.getenv("HALTING_SPEED_MS", "0.1"))

# ── NGSI node ↔ SUMO TLS ───────────────────────────────────────────
# v1: only A ↔ J1 is published; other junctions reserved for later.
NODE_TO_TLS: Dict[str, str] = {
    "A": "J1",
    # Future:
    # "B": "J2",
    # "C": "J3",
    # "D": "J4",
}
TLS_TO_NODE: Dict[str, str] = {v: k for k, v in NODE_TO_TLS.items()}

PUBLISH_TLS = NODE_TO_TLS[PUBLISH_NODE]  # J1 when PUBLISH_NODE=A

# ── Approach edges for J1 (incoming) ────────────────────────────────
# Compass = direction vehicles ARRIVE from (matches simulator DIRECTIONS).
APPROACH_EDGES: Dict[str, Dict[str, str]] = {
    "J1": {
        "North": "J3J1",
        "East": "J2J1",
        "South": "S1J1",
        "West": "W1J1",
    },
}

# Outgoing edges (for spillback pressure)
OUTGOING_EDGES: Dict[str, List[str]] = {
    "J1": ["J1J2", "J1J3", "J1S1", "J1W1"],
}

DIRECTIONS: List[str] = ["North", "South", "East", "West"]

# Lane IDs for each approach (2 lanes in current net)
def approach_lane_ids(tls_id: str, direction: str) -> List[str]:
    edge = APPROACH_EDGES[tls_id][direction]
    return [f"{edge}_0", f"{edge}_1"]


# (from_edge, to_edge) → movement type for J1 (from intersection.net.xml connections)
TURN_BY_OD: Dict[Tuple[str, str], str] = {
    # North approach J3J1
    ("J3J1", "J1W1"): "right",
    ("J3J1", "J1S1"): "straight",
    ("J3J1", "J1J2"): "left",
    ("J3J1", "J1J3"): "straight",  # u-turn → treat as straight for counts
    # East approach J2J1
    ("J2J1", "J1J3"): "right",
    ("J2J1", "J1W1"): "straight",
    ("J2J1", "J1S1"): "left",
    ("J2J1", "J1J2"): "straight",
    # South approach S1J1
    ("S1J1", "J1J2"): "right",
    ("S1J1", "J1J3"): "straight",
    ("S1J1", "J1W1"): "left",
    ("S1J1", "J1S1"): "straight",
    # West approach W1J1
    ("W1J1", "J1S1"): "right",
    ("W1J1", "J1J2"): "straight",
    ("W1J1", "J1J3"): "left",
    ("W1J1", "J1W1"): "straight",
}

# ── TLS phase vocabulary (J1 programID=0) ───────────────────────────
# Verified against linkIndex groups in intersection.net.xml
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

# Safe force_phase transitions: from → list of intermediate phases before target
# When jumping NS↔EW, insert the yellow of the current axis first.
FORCE_VIA_YELLOW: Dict[str, str] = {
    "NS_GREEN": "NS_YELLOW",
    "EW_GREEN": "EW_YELLOW",
}

# ── PCU factors (vType id from intersection.rou.xml) ────────────────
# Base values for motorcycle/car/bus/truck match simulator/models.py.
# Extra SUMO types mapped explicitly (not inferred silently).
PCU_FACTORS: Dict[str, float] = {
    "motorcycle": 0.24,  # same as simulator/models.py
    "car": 1.00,         # same as simulator/models.py
    "bus": 2.50,         # same as simulator/models.py
    "truck": 2.50,       # same as simulator/models.py
    "container": 2.50,   # SUMO-only; treat as heavy vehicle
    "ambulance": 1.00,   # SUMO-only; light emergency
    "police": 1.00,      # SUMO-only; light authority
    "firetruck": 2.50,   # SUMO-only; heavy emergency
}

# Density thresholds (same as simulator/scenarios.py)
DENSITY_THRESHOLDS_PER_DIRECTION = {
    "LOW": (0, 15),
    "MEDIUM": (15, 35),
    "HIGH": (35, float("inf")),
}

# Scenario identifiers (same names as simulator/scenarios.py)
SCENARIO_IDS = (
    "normal",
    "morning_peak",
    "evening_peak",
    "rain",
    "heavy_rain",
    "accident",
)

# Traffic scale / speed factors applied via TraCI for scenarios
SCENARIO_TRAFFIC_SCALE: Dict[str, float] = {
    "normal": 1.0,
    "morning_peak": 2.5,
    "evening_peak": 2.2,
    "rain": 0.85,
    "heavy_rain": 0.55,
    "accident": 1.2,
}

SCENARIO_SPEED_FACTOR: Dict[str, float] = {
    "normal": 1.0,
    "morning_peak": 0.85,
    "evening_peak": 0.80,
    "rain": 0.85,
    "heavy_rain": 0.65,
    "accident": 0.90,
}

# Base free-flow speed (km/h) for Greenshields theoretical_speed reporting
BASE_V_FREE_KMH = 50.0
APPROACH_LENGTH_M = 100.0  # matches edge length in nod/edg (~100m stubs)
K_JAM_PCU_PER_KM = 200.0


def resolve_sumo_binary(use_gui: Optional[bool] = None) -> str:
    """Locate sumo or sumo-gui binary. Raises RuntimeError with clear message."""
    import shutil

    gui = SUMO_GUI if use_gui is None else use_gui
    names = ["sumo-gui", "sumo-gui.exe"] if gui else ["sumo", "sumo.exe"]
    # Also allow fallback to sumo if gui requested but missing
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
    """Add SUMO tools to sys.path so `import traci` works."""
    import sys

    sumo_home = os.environ.get("SUMO_HOME", "").strip()
    if not sumo_home:
        return
    tools = Path(sumo_home) / "tools"
    if tools.is_dir() and str(tools) not in sys.path:
        sys.path.insert(0, str(tools))


def ensure_simulator_on_path() -> None:
    """Allow importing entity_generator / client / scenarios from simulator/."""
    import sys

    path = str(SIMULATOR_DIR)
    if path not in sys.path:
        sys.path.insert(0, path)
