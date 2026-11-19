"""
detector_manager.py — E1/E2 helpers (ADR-004, naming ADR §33).
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import configuration.config as cfg

log = logging.getLogger(__name__)


def build_detectors_xml(net_lane_lengths: Optional[Dict[str, float]] = None) -> str:
    """
    Build detectors.add.xml content.
    Approach E2 = E2_STOPLINE cover (cfg.E2_COVER_LENGTH_M, default 100 m).
    Full-link spillback is measured via TraCI lane metrics, not a longer E2.
    net_lane_lengths: optional lane_id -> length; default APPROACH_LENGTH_M.
    """
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<!-- Detector naming: E1_{TLS}_{DIR}_{LANE} / E2_... / E2OUT_... -->",
        "<additional>",
    ]
    for tls_id, approaches in cfg.APPROACH_EDGES.items():
        for direction, edge in approaches.items():
            sensor_edge = cfg.sensing_edge_for_direction(tls_id, direction)
            for i, lane in enumerate(cfg.edge_lane_ids(edge)):
                length = (net_lane_lengths or {}).get(lane, cfg.APPROACH_LENGTH_M)
                margin = min(0.1, length / 4)
                e1_pos = max(margin, length - cfg.E1_OFFSET_FROM_END_M)
                e1_pos = min(e1_pos, length - margin)
                sensor_lane = f"{sensor_edge}_{i}"
                sensor_length = (net_lane_lengths or {}).get(sensor_lane, cfg.APPROACH_LENGTH_M)
                sensor_margin = min(0.1, sensor_length / 4)
                e2_pos = max(sensor_margin, sensor_length - cfg.E2_COVER_LENGTH_M)
                e2_end = sensor_length - sensor_margin
                e1 = cfg.detector_id_e1(tls_id, direction, i)
                e2 = cfg.detector_id_e2(tls_id, direction, i)
                lines.append(
                    f'    <inductionLoop id="{e1}" lane="{lane}" pos="{e1_pos:.2f}" freq="1" file="NUL"/>'
                )
                lines.append(
                    f'    <laneAreaDetector id="{e2}" lane="{sensor_lane}" pos="{e2_pos:.3f}" '
                    f'endPos="{e2_end:.3f}" freq="1" file="NUL"/>'
                )
        for edge in cfg.OUTGOING_EDGES.get(tls_id, []):
            for i, lane in enumerate(cfg.edge_lane_ids(edge)):
                did = cfg.detector_id_e2_out(tls_id, edge, i)
                length = (net_lane_lengths or {}).get(lane, cfg.APPROACH_LENGTH_M)
                margin = min(0.02, length / 10)
                pos = min(cfg.E2_OUT_POS_M, max(margin, length - cfg.E2_OUT_LENGTH_M - margin))
                end = min(length - margin, pos + cfg.E2_OUT_LENGTH_M)
                lines.append(
                    f'    <laneAreaDetector id="{did}" lane="{lane}" pos="{pos:.2f}" '
                    f'endPos="{end:.2f}" freq="1" file="NUL"/>'
                )
    lines.append("</additional>")
    return "\n".join(lines) + "\n"


class DetectorManager:
    """Read E1/E2 via TraCI with fallback flags."""

    def __init__(self, tls_id: str):
        self.tls_id = tls_id
        self.available = False
        self._checked = False

    def ensure_checked(self, traci_module) -> bool:
        if self._checked:
            return self.available
        self._checked = True
        try:
            approaches = cfg.APPROACH_EDGES.get(self.tls_id, {})
            sample_direction = next(iter(approaches), None)
            if sample_direction is None:
                self.available = False
                return False
            sample = cfg.detector_id_e2(self.tls_id, sample_direction, 0)
            ids = set(traci_module.lanearea.getIDList())
            self.available = sample in ids
            if not self.available:
                log.warning(
                    "E2 detectors not loaded for %s (missing %s). Using TraCI vehicle fallback.",
                    self.tls_id, sample,
                )
        except Exception as e:
            log.warning("Detector check failed: %s", e)
            self.available = False
        return self.available

    def approach_queue_by_movement(self, traci_module, direction: str, *, quality: Optional[dict] = None) -> Dict[str, float]:
        """Jam length per movement from E2 lanes; empty dict if unavailable."""
        if not self.ensure_checked(traci_module):
            return {}
        out = {"right": 0.0, "straight": 0.0, "left": 0.0}
        edge = cfg.APPROACH_EDGES.get(self.tls_id, {}).get(direction)
        if not edge:
            return out
        if any(cfg.movement_for_lane(edge, i) == "mixed"
               for i, _ in enumerate(cfg.edge_lane_ids(edge))):
            # E2 measures a lane, not its per-vehicle turn. Use route-based
            # queue attribution when a lane serves multiple movements.
            return {}
        for i, _lane in enumerate(cfg.edge_lane_ids(edge)):
            did = cfg.detector_id_e2(self.tls_id, direction, i)
            movement = cfg.movement_for_lane(edge, i)
            try:
                jam = float(traci_module.lanearea.getJamLengthMeters(did))
                out[movement] = round(out[movement] + jam, 1)
            except Exception:
                if quality is not None:
                    quality["queue_detector_incomplete"] = True
                pass
        return out

    def approach_occupancy_pct(self, traci_module, direction: str, *, quality: Optional[dict] = None) -> Optional[float]:
        if not self.ensure_checked(traci_module):
            return None
        vals: List[float] = []
        edge = cfg.APPROACH_EDGES.get(self.tls_id, {}).get(direction)
        if not edge:
            return None
        for i, _lane in enumerate(cfg.edge_lane_ids(edge)):
            did = cfg.detector_id_e2(self.tls_id, direction, i)
            try:
                occ = float(traci_module.lanearea.getLastStepOccupancy(did))
                if occ <= 1.0:
                    occ *= 100.0
                vals.append(max(0.0, min(100.0, occ)))
            except Exception:
                if quality is not None:
                    quality["occupancy_detector_incomplete"] = True
                pass
        if not vals:
            return None
        return round(sum(vals) / len(vals), 1)

    def outbound_occupancy_ratio(self, traci_module, *, quality: Optional[dict] = None) -> Tuple[float, bool]:
        """Mean outbound E2 occupancy ratio 0–1 and whether any edge is full."""
        if not self.ensure_checked(traci_module):
            return 0.0, False
        vals: List[float] = []
        for edge in cfg.OUTGOING_EDGES.get(self.tls_id, []):
            for i, _lane in enumerate(cfg.edge_lane_ids(edge)):
                did = cfg.detector_id_e2_out(self.tls_id, edge, i)
                try:
                    occ = float(traci_module.lanearea.getLastStepOccupancy(did))
                    if occ > 1.0:
                        occ /= 100.0
                    vals.append(max(0.0, min(1.0, occ)))
                except Exception:
                    if quality is not None:
                        quality["spillback_detector_incomplete"] = True
                    pass
        if not vals:
            if quality is not None:
                quality["spillback_detector_incomplete"] = True
            return 0.0, False
        mean = sum(vals) / len(vals)
        full = any(v >= cfg.BOX_OCCUPANCY_THRESHOLD for v in vals)
        return round(mean, 3), full
