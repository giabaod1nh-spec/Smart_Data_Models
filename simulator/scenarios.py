"""
scenarios.py — Scenario Engine + cau hinh giao thong theo giao lo
"""

DIRECTIONS = ["North", "South", "East", "West"]

SCENARIOS = {
    "normal": {
        "description": "Luu luong binh thuong",
        # Tong ~7.5 xe/s, duoi nguong capacity do duoc (~10 xe/s cho mang 4 giao lo nay)
        # -> he thong dat gan trang thai on dinh thay vi tang vo han
        "boundary_spawn_rate": {"N1": 1.5, "W1": 1.5, "S1": 1, "E1": 2, "E2": 2},
        "base_speed_kmh": {"min": 35, "max": 55},
        "weather": "SUNNY",
    },
    "morning_peak": {
        "description": "Gio cao diem sang 7-9h",
        "boundary_spawn_rate": {"N1": 9, "W1": 9, "S1": 6, "E1": 12, "E2": 12},
        "base_speed_kmh": {"min": 20, "max": 35},
        "weather": "SUNNY",
    },
    "evening_peak": {
        "description": "Gio cao diem chieu 17-19h",
        "boundary_spawn_rate": {"N1": 8, "W1": 8, "S1": 5, "E1": 10, "E2": 10},
        "base_speed_kmh": {"min": 20, "max": 32},
        "weather": "SUNNY",
    },
    "rain": {
        "description": "Troi mua",
        "boundary_spawn_rate": {"N1": 4, "W1": 4, "S1": 3, "E1": 5, "E2": 5},
        "base_speed_kmh": {"min": 25, "max": 40},
        "weather": "LIGHT_RAIN",
    },
    "heavy_rain": {
        "description": "Mua to",
        "boundary_spawn_rate": {"N1": 3, "W1": 3, "S1": 2, "E1": 4, "E2": 4},
        "base_speed_kmh": {"min": 15, "max": 30},
        "weather": "HEAVY_RAIN",
    },
    "accident": {
        "description": "Tai nan tai 1 giao lo",
        "boundary_spawn_rate": {"N1": 5, "W1": 5, "S1": 3, "E1": 6, "E2": 6},
        "base_speed_kmh": {"min": 15, "max": 30},
        "weather": "SUNNY",
    },
}

TURN_PROBABILITIES = {"straight": 0.70, "left": 0.20, "right": 0.10}

VALID_DESTINATIONS = {
    "North": ["South", "East", "West"],
    "South": ["North", "West", "East"],
    "East":  ["West",  "North", "South"],
    "West":  ["East",  "South", "North"],
}

MOVEMENT_TYPE = {
    "North": {"South": "straight", "East": "left",  "West": "right"},
    "South": {"North": "straight", "West": "left",  "East": "right"},
    "East":  {"West":  "straight", "North": "left", "South": "right"},
    "West":  {"East":  "straight", "South": "left", "North": "right"},
}

INTERSECTION_TURN_RULES = {
    "A": {"right_on_red": True,  "u_turn_allowed": False},
    "B": {"right_on_red": False, "u_turn_allowed": True},
    "C": {"right_on_red": True,  "u_turn_allowed": True},
    "D": {"right_on_red": False, "u_turn_allowed": False},
}

REROUTE_PROBABILITY = {
    "motorcycle": 0.35,
    "car":        0.15,
    "bus":        0.05,
    "truck":      0.10,
}

DENSITY_THRESHOLDS_PER_DIRECTION = {
    "LOW":    (0, 15),
    "MEDIUM": (15, 35),
    "HIGH":   (35, float("inf")),
}

# Tong 4 huong — dung cho re-route / Intersection entity (calibrate ~4x per-direction)
DENSITY_THRESHOLDS_INTERSECTION = {
    "LOW":    (0, 50),
    "MEDIUM": (50, 140),
    "HIGH":   (140, float("inf")),
}

# Backward compat alias (per-direction)
DENSITY_THRESHOLDS_PCU = DENSITY_THRESHOLDS_PER_DIRECTION

# Spawn curve (P7-B): chi morning_peak / evening_peak
SCENARIOS_WITH_RAMP = {"morning_peak", "evening_peak"}
SPAWN_CURVE_DURATION_SEC = 300.0

PHASE_SEQUENCE = ["NS_GREEN", "NS_YELLOW", "EW_GREEN", "EW_YELLOW"]
PHASE_DURATIONS = {"NS_GREEN": 45, "NS_YELLOW": 5, "EW_GREEN": 45, "EW_YELLOW": 5}

PHASE_COLORS = {
    "NS_GREEN":  {"North": "green",  "South": "green",  "East": "red",    "West": "red"},
    "NS_YELLOW": {"North": "yellow", "South": "yellow", "East": "red",    "West": "red"},
    "EW_GREEN":  {"North": "red",    "South": "red",    "East": "green",  "West": "green"},
    "EW_YELLOW": {"North": "red",    "South": "red",    "East": "yellow", "West": "yellow"},
}

INTER_VEHICLE_GAP_M = 1.5
MOTORCYCLE_LATERAL_FACTOR = 2.5
APPROACH_LANE_LENGTH_M = 100.0
CROSSING_LENGTH_M = 30.0

PRIORITY_VEHICLE_PROBABILITY = 0.002

BUS_DWELL_PROBABILITY_PER_SEC = 0.01
BUS_DWELL_DURATION_SEC = 4.0
BUS_DWELL_SPEED_KMH = 5.0

# Phase 3 U3 — discharge headway theo loại xe (nghiên cứu HN / EASTS)
DISCHARGE_HEADWAY_SEC = {
    "motorcycle": 1.2,
    "car":        2.2,
    "bus":        3.0,
    "truck":      2.8,
}

# Phase 3 U2 — ngưỡng PCU edge ra để chặn vào hộp nút
BOX_OCCUPANCY_THRESHOLD = 0.85

# Phase 3 U5 — hướng xung đột khi rẽ phải đèn đỏ
RIGHT_ON_RED_CONFLICT = {
    "North": "East",
    "East":  "South",
    "South": "West",
    "West":  "North",
}

# Phase 3 U6 — đã qua điểm này khi vàng → tiếp tục đi
YELLOW_COMMITMENT_POINT = 0.85

# Phase 3 U8 — preemption
DIRECTION_TO_GREEN_PHASE = {
    "North": "NS_GREEN",
    "South": "NS_GREEN",
    "East":  "EW_GREEN",
    "West":  "EW_GREEN",
}

# Phase 3 U7 — OD matrix (trọng số theo cặp nguồn→đích boundary)
# Phần còn lại được normalize đều cho các boundary khác trong network.pick_destination
OD_MATRIX = {
    "morning_peak": {
        ("W1", "E1"): 0.40,
        ("W1", "E2"): 0.25,
        ("W1", "S1"): 0.10,
        ("W1", "N1"): 0.05,
        ("N1", "S1"): 0.25,
        ("N1", "E1"): 0.20,
        ("N1", "E2"): 0.15,
        ("N1", "W1"): 0.10,
        ("S1", "N1"): 0.20,
        ("S1", "E1"): 0.15,
        ("E1", "W1"): 0.15,
        ("E2", "W1"): 0.15,
    },
    "evening_peak": {
        ("E1", "W1"): 0.30,
        ("E2", "W1"): 0.20,
        ("S1", "N1"): 0.25,
        ("S1", "W1"): 0.15,
        ("N1", "S1"): 0.15,
        ("W1", "E1"): 0.15,
        ("E1", "N1"): 0.10,
        ("E2", "N1"): 0.10,
    },
}
