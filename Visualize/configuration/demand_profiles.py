"""
demand_profiles.py — Experimental vehicle composition (ADR-007).

Weights from ParameterRegistry composition (YAML SSOT) — NOT field-calibrated VN data.
"""
from __future__ import annotations

from typing import Dict

from configuration.model_params import get_registry

_reg = get_registry()
_comp = _reg.export_effective_config().get("composition") or {}
_weights = dict(_comp.get("weights") or {})
_emergency = dict(_comp.get("emergency_share") or {})
if not _weights:
    raise RuntimeError("composition.weights missing from ParameterRegistry")

# experimental — values from registry composition (YAML SSOT)
VEHICLE_CLASS_WEIGHTS: Dict[str, float] = {
    "motorcycle": float(_weights["motorcycle"]),
    "car": float(_weights["car"]),
    "bus": float(_weights["bus"]),
    "truck": float(_weights["truck"]),
}

EMERGENCY_SHARE: Dict[str, float] = {
    "ambulance": float(_emergency["ambulance"]),
    "police": float(_emergency["police"]),
    "firetruck": float(_emergency["firetruck"]),
    "container": float(_emergency["container"]),
}

# Base trunk volume per bidirectional corridor (veh/h) before class split
BASE_TRUNK_VEH_PER_HOUR = float(_comp["base_trunk_veh_per_hour"])
DIAGONAL_VEH_PER_HOUR = float(_comp["diagonal_veh_per_hour"])


def split_flow(total_vph: float) -> Dict[str, int]:
    """Return integer vehsPerHour per class summing ≈ total_vph."""
    core = {k: max(0, int(round(total_vph * w))) for k, w in VEHICLE_CLASS_WEIGHTS.items()}
    drift = int(round(total_vph)) - sum(core.values())
    core["motorcycle"] = max(0, core["motorcycle"] + drift)
    for k, w in EMERGENCY_SHARE.items():
        # At least 1 veh/h for emergency/container so preemption paths are exerciseable
        core[k] = max(1, int(round(total_vph * w)))
    return core


def moto_vtype_attrs() -> Dict[str, str]:
    """vType attributes for motorcycle (no sublane — ADR-001). Keeps GUI imgFile."""
    return {
        "vClass": "motorcycle",
        "guiShape": "motorcycle",
        "length": "2.0",
        "width": "0.9",
        "maxSpeed": "16.7",
        "accel": "3.0",
        "decel": "5.0",
        "sigma": "0.5",
        "minGap": "0.5",
        "speedFactor": "1.1",
        "impatience": "0.4",
        "lcStrategic": "1.2",
        "lcSpeedGain": "1.2",
        "lcKeepRight": "0.5",
        "imgFile": "images/bike_bg.png",
    }


# SUMO GUI sprites (relative to sumocfg / Visualize/Visualize/)
VTYPE_GUI: Dict[str, Dict[str, str]] = {
    "car": {
        "vClass": "passenger",
        "guiShape": "passenger/sedan",
        "length": "4.5",
        "width": "2.2",
        "maxSpeed": "13.9",
        "accel": "2.5",
        "decel": "4.5",
        "sigma": "0.5",
        "minGap": "2.5",
        "imgFile": "images/car_final.png",
    },
    "bus": {
        "vClass": "bus",
        "guiShape": "bus",
        "length": "12.0",
        "width": "2.8",
        "maxSpeed": "11.0",
        "accel": "1.5",
        "decel": "3.5",
        "sigma": "0.5",
        "minGap": "3.0",
        "imgFile": "images/bus_bg.png",
    },
    "truck": {
        "vClass": "truck",
        "guiShape": "truck",
        "length": "7.1",
        "width": "2.4",
        "maxSpeed": "11.1",
        "accel": "1.2",
        "decel": "3.0",
        "sigma": "0.5",
        "minGap": "3.0",
        "imgFile": "images/truck_bg.png",
    },
    "container": {
        "vClass": "trailer",
        "guiShape": "truck/semitrailer",
        "length": "16.5",
        "width": "2.5",
        "maxSpeed": "10.0",
        "accel": "0.8",
        "decel": "2.5",
        "sigma": "0.5",
        "minGap": "3.5",
        "imgFile": "images/tractor_head.png",
    },
    "ambulance": {
        "vClass": "emergency",
        "guiShape": "emergency",
        "length": "5.5",
        "width": "2.2",
        "maxSpeed": "18.0",
        "accel": "3.0",
        "decel": "5.0",
        "sigma": "0.3",
        "minGap": "2.0",
        "imgFile": "images/ambulance_bg.png",
    },
    "police": {
        "vClass": "authority",
        "guiShape": "police",
        "length": "5.0",
        "width": "2.2",
        "maxSpeed": "19.0",
        "accel": "3.5",
        "decel": "5.5",
        "sigma": "0.3",
        "minGap": "2.0",
        "imgFile": "images/police_bg.png",
    },
    "firetruck": {
        "vClass": "emergency",
        "guiShape": "firebrigade",
        "length": "8.0",
        "width": "2.5",
        "maxSpeed": "16.0",
        "accel": "2.5",
        "decel": "4.0",
        "sigma": "0.4",
        "minGap": "3.0",
        "imgFile": "images/firetruck_bg.png",
    },
}

# Container trailer params (SUMO multi-carriage rendering)
CONTAINER_PARAMS = {
    "locomotiveLength": "4.0",
    "carriageLength": "12.0",
    "carriageGap": "0.5",
    "carriageImages": "images/trailer_box.png",
}
