"""
SUMO TLS signal controller — maps TraCI phase index ↔ simulator vocabulary.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional, Sequence

import configuration.config as cfg

log = logging.getLogger(__name__)

# Officer / MANUAL mode: keep TraCI from advancing the TLS program.
_MANUAL_HOLD_SEC = 3600.0
_MANUAL_HOLD_REFRESH_BELOW_SEC = 60


class SumoSignalController:
    """Controls one TLS or one synchronized group of OSM TLS controllers."""

    def __init__(
        self,
        tls_id: str = "411926415",
        *,
        node_id: Optional[str] = None,
        tls_ids: Optional[Sequence[str]] = None,
        grouped_axes: Optional[Dict[str, Sequence[str]]] = None,
    ):
        self.tls_id = tls_id
        self.node_id = node_id or cfg.TLS_TO_NODE.get(tls_id)
        self.tls_ids = tuple(tls_ids or (tls_id,))
        self.grouped_axes = {
            axis: tuple(ids) for axis, ids in (grouped_axes or {}).items()
        }
        self._grouped = len(self.tls_ids) > 1
        self._group_phase_name = "NS_GREEN"
        self._group_next_switch = 0.0
        self._custom_green_sec: Optional[int] = None
        self._pending_target: Optional[str] = None
        self.preemption_active: bool = False
        self._preempt_restore: Optional[str] = None
        self.preemption_enabled: bool = True  # FIXED mode sets False
        self.manual_mode: bool = False

    def initialize(self, traci_module) -> None:
        """Synchronize a split OSM intersection immediately after TraCI starts."""
        if self._grouped:
            self._set_group_phase(traci_module, self._group_phase_name)

    def _group_phase_duration(self, traci_module, phase_name: str) -> int:
        if "YELLOW" in phase_name:
            return self._duration_for_kind(traci_module, "YELLOW")
        return self._duration_for_kind(traci_module, "GREEN")

    def _set_group_phase(
        self,
        traci_module,
        phase_name: str,
        duration: Optional[float] = None,
    ) -> None:
        """Map the virtual four-phase state onto each physical 3-phase TLS."""
        active_axis = "NS" if phase_name.startswith("NS") else "EW"
        is_yellow = "YELLOW" in phase_name
        axis_by_tls = {
            tls: axis
            for axis, tls_ids in self.grouped_axes.items()
            for tls in tls_ids
        }
        hold = float(duration or self._group_phase_duration(traci_module, phase_name))
        for tls_id in self.tls_ids:
            axis = axis_by_tls.get(tls_id, "NS")
            physical_phase = 2  # all red
            if axis == active_axis:
                physical_phase = 1 if is_yellow else 0
            traci_module.trafficlight.setPhase(tls_id, physical_phase)
            traci_module.trafficlight.setPhaseDuration(tls_id, hold)
        self._group_phase_name = phase_name
        now = float(traci_module.simulation.getTime())
        self._group_next_switch = now + hold

    def set_manual_mode(self, traci_module, enabled: bool) -> None:
        """Enter/leave traffic-officer MANUAL hold (countdown stops until force_phase)."""
        self.manual_mode = bool(enabled)
        if enabled:
            self.preemption_enabled = False
            self.preemption_active = False
            self.apply_manual_hold(traci_module)
            log.info("TLS %s entered MANUAL hold", self.tls_id)
        else:
            self._restore_cycle_duration(traci_module)
            log.info("TLS %s left MANUAL — cycle timing restored", self.tls_id)

    def apply_manual_hold(self, traci_module) -> None:
        """Re-extend current phase so SUMO does not auto-advance (officer holds)."""
        if not self.manual_mode:
            return
        current = self.current_phase_name(traci_module)
        # Let yellow-safe transitions finish under tick_pending.
        if "YELLOW" in current and self._pending_target:
            return
        remaining = self.phase_remaining_seconds(traci_module)
        if remaining >= _MANUAL_HOLD_REFRESH_BELOW_SEC:
            return
        try:
            if self._grouped:
                self._set_group_phase(
                    traci_module, self._group_phase_name, _MANUAL_HOLD_SEC
                )
            else:
                traci_module.trafficlight.setPhaseDuration(self.tls_id, _MANUAL_HOLD_SEC)
        except Exception as e:
            log.debug("manual hold setPhaseDuration failed on %s: %s", self.tls_id, e)

    def _restore_cycle_duration(self, traci_module) -> None:
        name = self.current_phase_name(traci_module)
        dur = self.yellow_duration(traci_module) if "YELLOW" in name else self.green_duration(traci_module)
        try:
            if self._grouped:
                self._set_group_phase(traci_module, name, float(max(1, int(dur))))
            else:
                traci_module.trafficlight.setPhaseDuration(self.tls_id, float(max(1, int(dur))))
        except Exception as e:
            log.debug("restore cycle duration failed on %s: %s", self.tls_id, e)

    def current_phase_name(self, traci_module) -> str:
        if self._grouped:
            return self._group_phase_name
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
        if self._grouped:
            now = float(traci_module.simulation.getTime())
            return max(0, int(self._group_next_switch - now))
        try:
            next_switch = float(traci_module.trafficlight.getNextSwitch(self.tls_id))
            now = float(traci_module.simulation.getTime())
            return max(0, int(next_switch - now))
        except Exception as e:
            log.warning("getNextSwitch failed for %s: %s", self.tls_id, e)
            return 0

    def _duration_for_kind(self, traci_module, kind: str) -> int:
        try:
            logics = traci_module.trafficlight.getAllProgramLogics(self.tls_id)
            if not logics:
                return {"GREEN": cfg.GREEN_DURATION_FALLBACK, "YELLOW": cfg.YELLOW_DURATION_FALLBACK, "RED": cfg.GREEN_DURATION_FALLBACK}.get(kind, cfg.GREEN_DURATION_FALLBACK)
            logic = logics[0]
            if self._grouped:
                if kind == "YELLOW" and len(logic.phases) > 1:
                    return int(logic.phases[1].duration)
                if kind in ("GREEN", "RED") and logic.phases:
                    if kind == "GREEN" and self._custom_green_sec:
                        return self._custom_green_sec
                    return int(logic.phases[0].duration)
            for i, ph in enumerate(logic.phases):
                name = cfg.PHASE_INDEX_TO_NAME.get(i, "")
                if kind == "GREEN" and "GREEN" in name:
                    if self._custom_green_sec:
                        return self._custom_green_sec
                    return int(ph.duration)
                if kind == "YELLOW" and "YELLOW" in name:
                    return int(ph.duration)
            if kind == "RED":
                # Opposite axis green duration as red for this approach
                return self._duration_for_kind(traci_module, "GREEN")
        except Exception:
            pass
        return {"GREEN": cfg.GREEN_DURATION_FALLBACK, "YELLOW": cfg.YELLOW_DURATION_FALLBACK, "RED": cfg.GREEN_DURATION_FALLBACK}.get(kind, cfg.GREEN_DURATION_FALLBACK)

    def phase_duration(self, traci_module) -> int:
        name = self.current_phase_name(traci_module)
        if "YELLOW" in name:
            return self.yellow_duration(traci_module)
        return self.green_duration(traci_module)

    def green_duration(self, traci_module) -> int:
        return self._duration_for_kind(traci_module, "GREEN")

    def yellow_duration(self, traci_module) -> int:
        return self._duration_for_kind(traci_module, "YELLOW")

    def red_duration(self, traci_module) -> int:
        return self._duration_for_kind(traci_module, "RED")

    def colors(self, traci_module) -> Dict[str, str]:
        name = self.current_phase_name(traci_module)
        return dict(cfg.PHASE_COLORS.get(name, cfg.PHASE_COLORS["NS_GREEN"]))

    def tick_pending(self, traci_module) -> None:
        if self._grouped:
            now = float(traci_module.simulation.getTime())
            if self._pending_target:
                if now >= self._group_next_switch:
                    target = self._pending_target
                    self._pending_target = None
                    self._set_group_phase(traci_module, target)
                    if self.manual_mode and "YELLOW" not in target:
                        self.apply_manual_hold(traci_module)
                return
            if not self.manual_mode and now >= self._group_next_switch:
                seq = cfg.PHASE_SEQUENCE
                idx = seq.index(self._group_phase_name)
                self._set_group_phase(traci_module, seq[(idx + 1) % len(seq)])
            return
        if not self._pending_target:
            return
        current = self.current_phase_name(traci_module)
        if current == self._pending_target:
            self._pending_target = None
            if self.manual_mode:
                self.apply_manual_hold(traci_module)
            return
        # Stay on yellow until it expires; then apply the pending green.
        if "YELLOW" in current:
            remaining = self.phase_remaining_seconds(traci_module)
            if remaining > 0:
                return
            # Yellow expired (or nextSwitch already past) — force target now
            target_idx = cfg.PHASE_NAME_TO_INDEX.get(self._pending_target)
            if target_idx is not None:
                traci_module.trafficlight.setPhase(self.tls_id, target_idx)
                log.info("Applied pending phase %s on %s after yellow", self._pending_target, self.tls_id)
            self._pending_target = None
            if self.manual_mode:
                self.apply_manual_hold(traci_module)
            return
        # Not yellow and not yet on target — apply immediately
        target_idx = cfg.PHASE_NAME_TO_INDEX.get(self._pending_target)
        if target_idx is not None:
            traci_module.trafficlight.setPhase(self.tls_id, target_idx)
            log.info("Applied pending phase %s on %s", self._pending_target, self.tls_id)
        self._pending_target = None
        if self.manual_mode:
            self.apply_manual_hold(traci_module)

    def update_preemption(self, traci_module) -> None:
        """Detect emergency vehicles on approaches; force green axis yellow-safely."""
        if not self.preemption_enabled:
            if self.preemption_active:
                self.preemption_active = False
            return
        ev_direction: Optional[str] = None
        for direction in cfg.DIRECTIONS:
            for lid in cfg.approach_lane_ids(self.tls_id, direction):
                try:
                    for vid in traci_module.lane.getLastStepVehicleIDs(lid):
                        vt = traci_module.vehicle.getTypeID(vid)
                        if vt in cfg.EMERGENCY_VTYPES:
                            ev_direction = direction
                            break
                except Exception:
                    pass
            if ev_direction:
                break

        if ev_direction:
            target = cfg.DIRECTION_TO_GREEN_PHASE[ev_direction]
            if not self.preemption_active:
                self._preempt_restore = self.current_phase_name(traci_module)
            self.preemption_active = True
            current = self.current_phase_name(traci_module)
            # Do not fight an in-progress yellow transition already heading to target
            if current == target or self._pending_target == target:
                return
            if current != target:
                self.force_phase(traci_module, target)
        elif self.preemption_active:
            self.preemption_active = False
            # Return to normal cycle — do not hard-jump; leave TLS program running
            self._preempt_restore = None

    def force_phase(self, traci_module, phase_name: str) -> None:
        """
        Force TLS to a named phase.

        Safety: if switching from a green on one axis to the other axis,
        first go through that axis's yellow, then set the target on the
        next call to tick_pending / or immediately set yellow then schedule target.
        """
        if phase_name not in cfg.PHASE_NAME_TO_INDEX:
            raise ValueError(f"Unknown phase '{phase_name}'. Expected one of {list(cfg.PHASE_NAME_TO_INDEX)}")

        if self._grouped:
            current = self._group_phase_name
            if current == phase_name:
                self._pending_target = None
                return
            same_axis = current[:2] == phase_name[:2]
            if "GREEN" in current and "GREEN" in phase_name and not same_axis:
                via_yellow = cfg.FORCE_VIA_YELLOW.get(current)
                if via_yellow:
                    self._pending_target = phase_name
                    self._set_group_phase(traci_module, via_yellow)
                    return
            self._pending_target = None
            self._set_group_phase(traci_module, phase_name)
            return

        current = self.current_phase_name(traci_module)
        target_idx = cfg.PHASE_NAME_TO_INDEX[phase_name]

        same_axis = (
            (current.startswith("NS") and phase_name.startswith("NS"))
            or (current.startswith("EW") and phase_name.startswith("EW"))
        )

        # Already there
        if current == phase_name:
            self._pending_target = None
            if self.manual_mode:
                self.apply_manual_hold(traci_module)
            return

        # From yellow: finish yellow-safe path.
        # - same axis green (e.g. NS_YELLOW → NS_GREEN): set directly
        # - cross-axis green while pending already that green: keep waiting
        # - cross-axis to other green: replace pending, stay on current yellow
        if "YELLOW" in current:
            if same_axis or "YELLOW" in phase_name:
                traci_module.trafficlight.setPhase(self.tls_id, target_idx)
                self._pending_target = None
                log.info("force_phase %s → %s on %s", current, phase_name, self.tls_id)
            else:
                # Keep yellow, retarget pending (preemption or new API command)
                self._pending_target = phase_name
                try:
                    traci_module.trafficlight.setPhaseDuration(
                        self.tls_id, float(self.yellow_duration(traci_module))
                    )
                except Exception:
                    pass
                log.info(
                    "force_phase while yellow: pending %s on %s (was %s)",
                    phase_name, self.tls_id, current,
                )
            return

        # Same axis green↔ or target is yellow → set directly
        if same_axis or "YELLOW" in phase_name:
            traci_module.trafficlight.setPhase(self.tls_id, target_idx)
            self._pending_target = None
            log.info("force_phase %s → %s on %s", current, phase_name, self.tls_id)
            if self.manual_mode and "YELLOW" not in phase_name:
                self.apply_manual_hold(traci_module)
            return

        # Cross-axis from green: insert yellow of current axis first
        via_yellow = cfg.FORCE_VIA_YELLOW.get(current)
        if via_yellow:
            yellow_idx = cfg.PHASE_NAME_TO_INDEX[via_yellow]
            traci_module.trafficlight.setPhase(self.tls_id, yellow_idx)
            self._pending_target = phase_name
            try:
                traci_module.trafficlight.setPhaseDuration(
                    self.tls_id, float(self.yellow_duration(traci_module))
                )
            except Exception:
                pass
            log.info(
                "force_phase safe: %s → %s (pending %s) on %s",
                current, via_yellow, phase_name, self.tls_id,
            )
        else:
            traci_module.trafficlight.setPhase(self.tls_id, target_idx)
            self._pending_target = None
            log.info("force_phase %s → %s on %s", current, phase_name, self.tls_id)
            if self.manual_mode:
                self.apply_manual_hold(traci_module)

    def set_green_duration(self, traci_module, seconds: int) -> None:
        """Set custom green duration (clamped 10–120) for green phases."""
        seconds = max(10, min(120, int(seconds)))
        self._custom_green_sec = seconds
        if self._grouped:
            if "GREEN" in self._group_phase_name:
                self._set_group_phase(traci_module, self._group_phase_name, seconds)
            return
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
