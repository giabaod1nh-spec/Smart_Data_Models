"""Map discrete RL actions to SumoSignalController API (yellow-safe)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Set

from AI_Control_Traffic_Light.environment.topology import ACTION_NAMES

log = logging.getLogger(__name__)

KEEP = 0
SWITCH = 1
EXTEND_5 = 2
EXTEND_10 = 3


class ActionExecutor:
    """Apply agent actions via existing signal controller — respects yellow transitions."""

    def __init__(self) -> None:
        self._last_actions: Dict[str, int] = {}

    def last_action(self, node_id: str) -> int:
        return self._last_actions.get(node_id, KEEP)

    def action_name(self, action: int) -> str:
        if 0 <= action < len(ACTION_NAMES):
            return ACTION_NAMES[action]
        return "UNKNOWN"

    def valid_actions(self, signal_controller, traci_module) -> Set[int]:
        valid = {KEEP, SWITCH}
        phase = signal_controller.current_phase_name(traci_module)
        pending = getattr(signal_controller, "_pending_target", None)
        if "GREEN" in phase and not pending:
            valid.add(EXTEND_5)
            valid.add(EXTEND_10)
        return valid

    def apply(
        self,
        node_id: str,
        action: int,
        signal_controller,
        traci_module,
        *,
        adaptive_mode: bool = True,
    ) -> int:
        """Returns the action actually applied (may differ if invalid)."""
        valid = self.valid_actions(signal_controller, traci_module)
        applied = action if action in valid else KEEP
        if action not in valid:
            log.debug("Invalid action %s at %s — fallback KEEP", action, node_id)

        phase = signal_controller.current_phase_name(traci_module)

        if applied == KEEP:
            if adaptive_mode:
                signal_controller.apply_manual_hold(traci_module)
        elif applied == SWITCH:
            nxt = signal_controller.next_phase_name(traci_module)
            signal_controller.force_phase(traci_module, nxt)
            if adaptive_mode:
                signal_controller.apply_manual_hold(traci_module)
        elif applied in (EXTEND_5, EXTEND_10):
            delta = 5 if applied == EXTEND_5 else 10
            if "GREEN" in phase:
                rem = signal_controller.phase_remaining_seconds(traci_module)
                signal_controller.set_green_duration(traci_module, int(rem + delta))
                if adaptive_mode:
                    signal_controller.apply_manual_hold(traci_module)
            else:
                applied = KEEP
                if adaptive_mode:
                    signal_controller.apply_manual_hold(traci_module)
        else:
            if adaptive_mode:
                signal_controller.apply_manual_hold(traci_module)

        self._last_actions[node_id] = applied
        return applied

    def enter_adaptive_mode(self, signals: Dict[str, Any], traci_module) -> None:
        for sig in signals.values():
            sig.set_manual_mode(traci_module, True)
            sig.preemption_enabled = False
            sig.preemption_active = False

    def exit_adaptive_mode(self, signals: Dict[str, Any], traci_module) -> None:
        for sig in signals.values():
            sig.set_manual_mode(traci_module, False)
            sig.preemption_enabled = False
