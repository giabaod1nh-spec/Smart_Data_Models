"""
SUMO TLS signal controller — maps TraCI phase index ↔ simulator vocabulary.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import config as cfg

log = logging.getLogger(__name__)


class SumoSignalController:
    """Controls one TLS (default J1) with safe yellow transitions."""

    def __init__(self, tls_id: str = "J1"):
        self.tls_id = tls_id
        self._custom_green_sec: Optional[int] = None
        self._pending_target: Optional[str] = None  # phase name after yellow

    def current_phase_name(self, traci_module) -> str:
        idx = int(traci_module.trafficlight.getPhase(self.tls_id))
        return cfg.PHASE_INDEX_TO_NAME.get(idx, "NS_GREEN")

    def next_phase_name(self, traci_module) -> str:
        current = self.current_phase_name(traci_module)
        if self._pending_target:
            return self._pending_target
        seq = cfg.PHASE_SEQUENCE
        i = seq.index(current) if current in seq else 0
        return seq[(i + 1) % len(seq)]

    def phase_remaining_seconds(self, traci_module) -> int:
        try:
            next_switch = float(traci_module.trafficlight.getNextSwitch(self.tls_id))
            now = float(traci_module.simulation.getTime())
            return max(0, int(next_switch - now))
        except Exception as e:
            log.warning("getNextSwitch failed for %s: %s", self.tls_id, e)
            return 0

    def phase_duration(self, traci_module) -> int:
        try:
            phases = traci_module.trafficlight.getAllProgramLogics(self.tls_id)
            if not phases:
                return 42
            logic = phases[0]
            idx = int(traci_module.trafficlight.getPhase(self.tls_id))
            if 0 <= idx < len(logic.phases):
                dur = int(logic.phases[idx].duration)
                name = cfg.PHASE_INDEX_TO_NAME.get(idx, "")
                if self._custom_green_sec and "GREEN" in name:
                    return self._custom_green_sec
                return dur
        except Exception as e:
            log.warning("phase_duration failed for %s: %s", self.tls_id, e)
        return 42

    def colors(self, traci_module) -> Dict[str, str]:
        name = self.current_phase_name(traci_module)
        return dict(cfg.PHASE_COLORS.get(name, cfg.PHASE_COLORS["NS_GREEN"]))

    def tick_pending(self, traci_module) -> None:
        """Advance pending force_phase after yellow completes."""
        if not self._pending_target:
            return
        current = self.current_phase_name(traci_module)
        # If we are still on a yellow that leads to target, wait.
        via = None
        for green, yellow in cfg.FORCE_VIA_YELLOW.items():
            if self._pending_target in ("EW_GREEN", "EW_YELLOW") and green == "NS_GREEN":
                via = yellow
            if self._pending_target in ("NS_GREEN", "NS_YELLOW") and green == "EW_GREEN":
                via = yellow
        # Simpler: when current phase is the pending target, clear.
        if current == self._pending_target:
            self._pending_target = None
            return
        # If yellow finished and we landed on wrong green, set target now.
        if "YELLOW" not in current and current != self._pending_target:
            target_idx = cfg.PHASE_NAME_TO_INDEX.get(self._pending_target)
            if target_idx is not None:
                traci_module.trafficlight.setPhase(self.tls_id, target_idx)
                log.info("Applied pending phase %s on %s", self._pending_target, self.tls_id)
            self._pending_target = None

    def force_phase(self, traci_module, phase_name: str) -> None:
        """
        Force TLS to a named phase.

        Safety: if switching from a green on one axis to the other axis,
        first go through that axis's yellow, then set the target on the
        next call to tick_pending / or immediately set yellow then schedule target.
        """
        if phase_name not in cfg.PHASE_NAME_TO_INDEX:
            raise ValueError(f"Unknown phase '{phase_name}'. Expected one of {list(cfg.PHASE_NAME_TO_INDEX)}")

        current = self.current_phase_name(traci_module)
        target_idx = cfg.PHASE_NAME_TO_INDEX[phase_name]

        same_axis = (
            (current.startswith("NS") and phase_name.startswith("NS"))
            or (current.startswith("EW") and phase_name.startswith("EW"))
        )

        # Already there
        if current == phase_name:
            self._pending_target = None
            return

        # Same axis or already yellow / target is yellow → set directly
        if same_axis or "YELLOW" in current or "YELLOW" in phase_name:
            traci_module.trafficlight.setPhase(self.tls_id, target_idx)
            self._pending_target = None
            log.info("force_phase %s → %s on %s", current, phase_name, self.tls_id)
            return

        # Cross-axis from green: insert yellow of current axis first
        via_yellow = cfg.FORCE_VIA_YELLOW.get(current)
        if via_yellow:
            yellow_idx = cfg.PHASE_NAME_TO_INDEX[via_yellow]
            traci_module.trafficlight.setPhase(self.tls_id, yellow_idx)
            self._pending_target = phase_name
            log.info(
                "force_phase safe: %s → %s (pending %s) on %s",
                current, via_yellow, phase_name, self.tls_id,
            )
        else:
            traci_module.trafficlight.setPhase(self.tls_id, target_idx)
            self._pending_target = None
            log.info("force_phase %s → %s on %s", current, phase_name, self.tls_id)

    def set_green_duration(self, traci_module, seconds: int) -> None:
        """Set custom green duration (clamped 10–120) for green phases."""
        seconds = max(10, min(120, int(seconds)))
        self._custom_green_sec = seconds
        try:
            logics = traci_module.trafficlight.getAllProgramLogics(self.tls_id)
            if not logics:
                log.info("set_green_duration stored=%ss (no program logic)", seconds)
                return
            logic = logics[0]
            for i, ph in enumerate(logic.phases):
                name = cfg.PHASE_INDEX_TO_NAME.get(i, "")
                if "GREEN" in name:
                    ph.duration = float(seconds)
                    if hasattr(ph, "minDur"):
                        ph.minDur = float(seconds)
                    if hasattr(ph, "maxDur"):
                        ph.maxDur = float(seconds)
            traci_module.trafficlight.setProgramLogic(self.tls_id, logic)
            # If currently in a green phase, also set remaining via setPhaseDuration
            current = self.current_phase_name(traci_module)
            if "GREEN" in current:
                try:
                    traci_module.trafficlight.setPhaseDuration(self.tls_id, float(seconds))
                except Exception:
                    pass
            log.info("set_green_duration %ss on %s", seconds, self.tls_id)
        except Exception as e:
            log.warning(
                "Could not rewrite TLS program for green duration (%s); "
                "reporting will use custom value. Error: %s",
                seconds, e,
            )
