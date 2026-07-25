"""NetworkRuntimeController — demand/overlays/emergency + context orchestration."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import configuration.config as cfg
from context_engine.coordinator import NetworkContextCoordinator
from actuators.emergency import EmergencyActuator
from configuration.model_params import get_registry
from runtime.state import NetworkRuntimeState
from actuators.capacity import ScenarioCapacityActuator
from actuators.demand import ScenarioDemandActuator

log = logging.getLogger(__name__)


class NetworkRuntimeController:
    def __init__(self, publish_nodes: List[str], artifacts_run_dir: Optional[Path] = None):
        reg = get_registry()
        policy = reg.insertion_policy()
        seed = int(policy.get("demand_seed", 42))
        self.demand = ScenarioDemandActuator(seed=seed)
        self.capacity = ScenarioCapacityActuator()
        self.emergency = EmergencyActuator()
        catalog = None
        cat_path = cfg.GENERATED_ROOT / "network_topology_catalog.json"
        if cat_path.is_file():
            catalog = json.loads(cat_path.read_text(encoding="utf-8"))
        self.coordinator = NetworkContextCoordinator(catalog)
        self.state = NetworkRuntimeState()
        self.state.ensure_nodes(publish_nodes)
        self.artifacts_run_dir = artifacts_run_dir
        self._event_path: Optional[Path] = None
        self._last_t = 0.0
        self.set_demand_profile("normal")

    def attach_run_dir(self, run_dir: Path) -> None:
        self.artifacts_run_dir = run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        self._event_path = run_dir / "runtime_events.jsonl"

    def _emit(self, event_type: str, payload: Dict[str, Any]) -> None:
        if not self._event_path:
            return
        rec = {"event_type": event_type, **payload}
        with self._event_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def set_demand_profile(self, profile_id: str) -> Dict[str, Any]:
        info = self.demand.set_profile(profile_id)
        self.state.demand_profile_id = profile_id
        self._emit("demand_profile", {"profile_id": profile_id, **info})
        return info

    def set_control_mode(self, mode: str) -> None:
        if mode not in ("FIXED", "PREEMPTION_ENABLED"):
            raise ValueError("control_mode must be FIXED|PREEMPTION_ENABLED")
        self.state.control_mode = mode
        self._emit("control", {"control_mode": mode})

    def add_overlay(self, traci_module, **kwargs) -> Dict[str, Any]:
        inst = self.capacity.add_overlay(traci_module, **kwargs)
        if inst.overlay_type == "emergency":
            self.emergency.maybe_insert(
                traci_module,
                target_intersection=inst.intersection_id,
                sim_t=kwargs.get("sim_t", 0.0),
                force=True,
            )
        self.state.overlays = self.capacity.active_list()
        for o in self.state.overlays:
            if o["overlay_id"] == inst.overlay_id:
                o["created_at_s"] = inst.created_at_s
        self._emit(
            "overlay",
            {
                "action": "add",
                "overlay_id": inst.overlay_id,
                "type": inst.overlay_type,
                "state": inst.state.value,
                "segment_role": inst.segment_role,
                "target_edge": inst.target_edge,
            },
        )
        return {
            "overlay_id": inst.overlay_id,
            "type": inst.overlay_type,
            "state": inst.state.value,
            "segment_role": inst.segment_role,
            "target_edge": inst.target_edge,
        }

    def remove_overlay(self, traci_module, overlay_id: str) -> bool:
        ok = self.capacity.remove_overlay(traci_module, overlay_id)
        self.state.overlays = self.capacity.active_list()
        self._emit("overlay", {"action": "remove", "overlay_id": overlay_id, "ok": ok})
        return ok

    def on_start(self, traci_module) -> None:
        try:
            traci_module.simulation.setScale(1.0)
        except Exception as e:
            log.warning("setScale(1.0) failed: %s", e)

    def pre_step_actuators(self, traci_module, sim_t: float) -> None:
        """Pre-simulationStep: insertion schedule, overlay expiry, emergency insert."""
        dt = max(0.0, sim_t - self._last_t) if self._last_t > 0 else cfg_dt_fallback()
        # Note: sim_t here is previous step time; caller may pass last known time
        self.demand.tick(traci_module, dt if dt > 0 else 0.01)
        self.capacity.tick_expiry(traci_module, sim_t)
        self.emergency.tick(traci_module, sim_t)
        for ov in self.capacity.active_list():
            if ov["type"] == "emergency":
                self.emergency.maybe_insert(
                    traci_module, target_intersection=ov["intersection_id"], sim_t=sim_t
                )
        self.state.overlays = self.capacity.active_list()
        self.state.source_stats = self.demand.source_stats()
        self.state.insertion_stats = dict(self.demand.stats)

    def context_tick(
        self,
        traci_module,
        sim_t: float,
        snapshots: Dict[str, dict],
        signals: Dict[str, Any],
    ) -> None:
        """Post-observation: derive local + network context from fresh snapshots."""
        self._last_t = sim_t
        overlays = self.capacity.active_list()
        for o in overlays:
            inst = self.capacity.overlays.get(o["overlay_id"])
            if inst:
                o["created_at_s"] = inst.created_at_s
                o["state"] = inst.state.value
        preempt = {
            nid: bool(getattr(sig, "preemption_active", False)) for nid, sig in signals.items()
        }
        self.coordinator.update_from_snapshots(
            self.state, snapshots, overlays, sim_t, preemption_by_node=preempt
        )
        self.state.overlays = overlays
        self._emit(
            "context_tick",
            {
                "sim_t": sim_t,
                "network_summary": dict(self.state.network_summary),
                "probable_cause_count": sum(
                    len(n.probable_causes) for n in self.state.nodes.values()
                ),
            },
        )

    def tick(
        self,
        traci_module,
        sim_t: float,
        snapshots: Dict[str, dict],
        signals: Dict[str, Any],
    ) -> None:
        """Compat: actuators + context (prefer split pre_step / context_tick)."""
        self.pre_step_actuators(traci_module, sim_t)
        if snapshots:
            self.context_tick(traci_module, sim_t, snapshots, signals)

    def network_state(self) -> Dict[str, Any]:
        return self.state.to_dict()


def cfg_dt_fallback() -> float:
    try:
        import configuration.config as cfg

        return float(cfg.SUMO_STEP_LENGTH)
    except Exception:
        return 0.01
