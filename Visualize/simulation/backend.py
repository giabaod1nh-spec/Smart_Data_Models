"""
SumoBackend — TraCI facade with multi-node snapshot/control + command queue.
"""
from __future__ import annotations

import logging
import json
import os
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import configuration.config as cfg
from runtime.command_queue import CommandQueue
from configuration.model_params import get_registry
from runtime.run_manifest import build_and_write_manifest
from simulation.scenario_manager import SumoScenarioManager
from simulation.signal_controller import SumoSignalController
from observation.snapshot_provider import SumoSnapshotProvider
from runtime.network_controller import NetworkRuntimeController
from observation.trip_collector import TripCollector

log = logging.getLogger(__name__)


class SumoBackend:
    def __init__(
        self,
        sumo_config: Optional[os.PathLike] = None,
        use_gui: Optional[bool] = None,
        publish_node: Optional[str] = None,
        publish_nodes: Optional[List[str]] = None,
    ):
        self.sumo_config = cfg.SUMO_CONFIG if sumo_config is None else sumo_config
        self.use_gui = cfg.SUMO_GUI if use_gui is None else use_gui
        nodes = publish_nodes or cfg.PUBLISH_NODES
        for n in nodes:
            if n not in cfg.NODE_TO_TLS:
                raise ValueError(f"Unknown publish node '{n}'. Known: {list(cfg.NODE_TO_TLS)}")
        self.publish_nodes = list(nodes)
        self.publish_node = publish_node or self.publish_nodes[0]

        self._traci = None
        self._started = False
        self.commands = CommandQueue()
        self.trips = TripCollector()

        self.signals: Dict[str, SumoSignalController] = {}
        self.scenarios: Dict[str, SumoScenarioManager] = {}
        self.snapshots: Dict[str, SumoSnapshotProvider] = {}
        for node in self.publish_nodes:
            tls = cfg.NODE_TO_TLS[node]
            self.signals[node] = SumoSignalController(tls)
            self.scenarios[node] = SumoScenarioManager(tls)
            self.snapshots[node] = SumoSnapshotProvider(tls, node)

        # Compat aliases for primary node
        self.tls_id = cfg.NODE_TO_TLS[self.publish_node]
        self.signal = self.signals[self.publish_node]
        self.scenario = self.scenarios[self.publish_node]
        self.snapshot_provider = self.snapshots[self.publish_node]

        self.current_scenario = "normal"
        self.per_node_scenario: Dict[str, str] = {}
        self.trip_records: list = self.trips.records
        self.last_spawn_count = 0
        self.simulation_time_sec = 0.0
        self._exited_total = 0
        # Last snapshots / stats built on the TraCI simulation thread.
        # Control API must read these caches — TraCI is not thread-safe.
        self._snapshot_cache: Dict[str, dict] = {}
        self._stats_cache: dict = {}
        self._last_cache_sim_t: float = -1e9
        # Observation/context cadence (not every TraCI micro-step when step_length≪1s)
        self._cache_interval_sec: float = float(os.getenv("OBSERVATION_INTERVAL_SEC", "1.0"))
        self._last_obs_sim_t: float = -1e9
        self._observation_seq: int = 0
        self.simulation_run_id: Optional[str] = None
        self.run_manifest: Optional[dict] = None
        self.runtime = NetworkRuntimeController(self.publish_nodes)
        self.control_mode = "FIXED"
        self.runtime.set_control_mode("FIXED")
        for sig in self.signals.values():
            sig.preemption_enabled = False

    def start(self) -> None:
        cfg.ensure_sumo_tools_on_path()
        if not os.environ.get("SUMO_HOME", "").strip():
            log.warning("SUMO_HOME is not set. TraCI import may fail.")
        try:
            import traci
        except ImportError as e:
            raise RuntimeError(
                "Cannot import traci. Set SUMO_HOME. Detail: " + str(e)
            ) from e

        binary = cfg.resolve_sumo_binary(self.use_gui)
        config_path = str(self.sumo_config)
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"SUMO config not found: {config_path}")

        self.simulation_run_id = str(uuid.uuid4())
        for snap in self.snapshots.values():
            snap.simulation_run_id = self.simulation_run_id

        reg = get_registry()
        allow_missing = os.getenv("ALLOW_MISSING_RUN_MANIFEST", "").lower() in (
            "1", "true", "yes",
        )
        topo_hash = None
        cat_path = cfg.GENERATED_ROOT / "network_topology_catalog.json"
        if cat_path.is_file():
            try:
                topo_hash = json.loads(cat_path.read_text(encoding="utf-8")).get("topology_hash")
            except Exception:
                topo_hash = None
        self.run_manifest = build_and_write_manifest(
            registry=reg,
            artifacts_root=cfg.ARTIFACTS_DIR,
            simulation_run_id=self.simulation_run_id,
            scenario=self.runtime.state.demand_profile_id,
            sumo_seed=cfg.SIM_SEED,
            python_random_seed=cfg.SIM_SEED,
            allow_missing_manifest=allow_missing,
            network_hash_mismatch_mode=os.getenv("NETWORK_HASH_MODE", "compat"),
            demand_seed=int(reg.insertion_policy().get("demand_seed", cfg.SIM_SEED)),
            control_mode=self.control_mode,
            topology_hash=topo_hash,
        )
        run_dir = cfg.ARTIFACTS_DIR / "runs" / self.simulation_run_id
        self.runtime.attach_run_dir(run_dir)
        log.info(
            "Run provenance: run_id=%s demand_profile=%s config_hash=%s schema=%s",
            self.simulation_run_id,
            self.runtime.state.demand_profile_id,
            reg.config_hash[:12],
            reg.schema_version,
        )

        sumo_cmd = [binary, "-c", config_path, "--start", "--seed", str(cfg.SIM_SEED)]
        if not self.use_gui:
            sumo_cmd.append("--quit-on-end")
        if os.getenv("SUMO_STEP_LENGTH"):
            sumo_cmd.extend(["--step-length", str(cfg.SUMO_STEP_LENGTH)])

        log.info("Starting SUMO: %s", " ".join(sumo_cmd))
        traci.start(sumo_cmd)
        self._traci = traci
        self._started = True
        self.simulation_time_sec = float(traci.simulation.getTime())
        self.runtime.on_start(traci)
        log.info(
            "SUMO started. nodes=%s primary=%s/%s",
            self.publish_nodes, self.publish_node, self.tls_id,
        )

    def step(self) -> bool:
        """
        Locked tick lifecycle (Phase 2.0A):
          pre_step → simulationStep → post_step_observation
          → context_tick → update_caches → (publish external)
        """
        self._require_started()
        traci = self._traci
        phases: List[str] = []

        # --- pre_step ---
        phases.append("pre_step")
        self.commands.drain({
            "force_phase": lambda node_id, phase: self.force_phase(node_id, phase),
            "set_green_duration": lambda node_id, seconds: self.set_green_duration(node_id, seconds),
            "set_scenario": lambda scenario, target_intersection=None, target_direction=None: self.set_scenario(
                scenario, target_intersection, target_direction
            ),
            "set_demand_profile": lambda profile: self.set_demand_profile(profile),
            "add_overlay": lambda **kw: self.add_overlay(**kw),
            "remove_overlay": lambda overlay_id: self.remove_overlay(overlay_id),
            "set_control_mode": lambda mode: self.set_control_mode(mode),
        })
        # Actuators that schedule insertions / expire overlays before SUMO advances
        try:
            self.runtime.pre_step_actuators(traci, self.simulation_time_sec)
        except Exception as e:
            log.debug("pre_step_actuators: %s", e)

        # --- simulationStep ---
        phases.append("simulationStep")
        traci.simulationStep()
        for sig in self.signals.values():
            sig.tick_pending(traci)
            sig.update_preemption(traci)
        self.simulation_time_sec = float(traci.simulation.getTime())

        # --- post_step bookkeeping (every TraCI step) ---
        self.trips.on_step(traci, self.simulation_time_sec)
        self.trip_records = self.trips.records
        try:
            self._exited_total += int(traci.simulation.getArrivedNumber())
        except Exception:
            pass

        # Observation + context at OBSERVATION_INTERVAL (same-cycle, not prior-cycle lag).
        # TraCI step may be 0.01s; full snapshot every micro-step is not viable.
        do_observe = (
            self.simulation_time_sec - self._last_obs_sim_t >= self._cache_interval_sec
            or self._last_obs_sim_t < 0
        )
        if do_observe:
            phases.append("post_step_observation")
            self._observation_seq += 1
            obs_seq = self._observation_seq
            fresh_snaps: Dict[str, dict] = {}
            for node_id in self.publish_nodes:
                try:
                    phys = self._build_physical_snapshot_dict(node_id)
                    phys["observation_seq"] = obs_seq
                    phys["source_observation_seq"] = obs_seq
                    fresh_snaps[node_id] = phys
                except Exception as e:
                    log.debug("physical snapshot %s: %s", node_id, e)

            phases.append("context_tick")
            try:
                self.runtime.context_tick(
                    traci, self.simulation_time_sec, fresh_snaps, self.signals
                )
            except Exception as e:
                log.debug("context_tick: %s", e)

            phases.append("update_caches")
            for node_id, phys in fresh_snaps.items():
                merged = self._merge_context_into_snapshot(node_id, phys)
                merged["observation_seq"] = obs_seq
                merged["source_observation_seq"] = obs_seq
                merged["context_source_observation_seq"] = obs_seq
                self._snapshot_cache[node_id] = merged
            try:
                self._stats_cache = self._build_stats_now()
                self._stats_cache["last_tick_phases"] = list(phases)
                self._stats_cache["last_context_sim_t"] = self.runtime.state.last_context_sim_t
                self._stats_cache["observation_interval_sec"] = self._cache_interval_sec
                self._stats_cache["observation_seq"] = obs_seq
            except Exception as e:
                log.debug("stats cache: %s", e)
            self._last_cache_sim_t = self.simulation_time_sec
            self._last_obs_sim_t = self.simulation_time_sec
            self.runtime.state.last_tick_phases = list(phases)
        else:
            phases.append("skip_observation")

        # publish_tick is owned by traci_runner (interval Orion) — not here
        try:
            if traci.simulation.getMinExpectedNumber() <= 0 and self.simulation_time_sec >= cfg.SIM_END_SEC:
                return False
        except Exception:
            pass
        return True

    def stop(self) -> None:
        if self._traci is not None:
            try:
                self._traci.close()
                log.info("TraCI closed.")
            except Exception as e:
                log.warning("traci.close() error: %s", e)
            finally:
                self._traci = None
                self._started = False

    def get_snapshot(self, node_id: str = "A", *, fresh: bool = False) -> dict:
        """
        Return snapshot for node.

        fresh=False (default): prefer TraCI-thread cache (safe for Control API).
        fresh=True: rebuild now — call only from the simulation thread.
        """
        self._require_started()
        if node_id not in self.snapshots:
            raise KeyError(f"Node '{node_id}' not in publish_nodes {self.publish_nodes}")
        if not fresh:
            cached = self._snapshot_cache.get(node_id)
            if cached is not None:
                return dict(cached)
        return self._build_snapshot_now(node_id)

    def get_snapshot_fresh(self, node_id: str = "A") -> dict:
        """Force rebuild from TraCI — call only from the simulation thread."""
        return self.get_snapshot(node_id, fresh=True)

    def _build_physical_snapshot_dict(self, node_id: str) -> dict:
        """Post-step physical observation only (no context merge yet)."""
        snap = self.snapshots[node_id].build_snapshot(
            self._traci, self.signals[node_id], self.scenarios[node_id],
        )
        snap["node_id"] = node_id
        return snap

    def _merge_context_into_snapshot(self, node_id: str, phys: dict) -> dict:
        """Additive Strategy C: attach local/network context onto physical snapshot dict."""
        snap = dict(phys)
        try:
            from runtime.layer_contracts import (
                CONTEXT_SCHEMA_VERSION,
                PHYSICAL_SNAPSHOT_SCHEMA_VERSION,
            )

            nstate = self.runtime.state.nodes.get(node_id)
            if nstate:
                snap["derived_traffic_state"] = nstate.aggregate_traffic_state
                snap["aggregate_traffic_state"] = nstate.aggregate_traffic_state
                snap["derived_aggregate_context"] = nstate.aggregate_traffic_state
                snap["derived_phenomena"] = nstate.derived_phenomena.to_dict()
                snap["operational_state"] = dict(nstate.operational_state)
                snap["probable_causes"] = list(nstate.probable_causes)
                snap["direction_contexts"] = {
                    k: v.traffic_state for k, v in nstate.directions.items()
                }
                snap["direction_states"] = {
                    k: {
                        "traffic_state": v.traffic_state,
                        "queue_length_m": v.queue_length_m,
                        "pcu": v.pcu,
                    }
                    for k, v in nstate.directions.items()
                }
            snap["physical_snapshot_schema_version"] = PHYSICAL_SNAPSHOT_SCHEMA_VERSION
            snap["context_schema_version"] = CONTEXT_SCHEMA_VERSION
            snap["link_states"] = {
                lid: st
                for lid, st in (self.runtime.state.link_states or {}).items()
                if st.get("upstream_node") == node_id or st.get("downstream_node") == node_id
            }
        except Exception as e:
            log.debug("merge context %s: %s", node_id, e)
        return snap

    def _build_snapshot_now(self, node_id: str) -> dict:
        phys = self._build_physical_snapshot_dict(node_id)
        # Preserve last observation_seq when rebuilding mid-cycle (do not invent a new cycle).
        phys["observation_seq"] = self._observation_seq
        phys["source_observation_seq"] = self._observation_seq
        snap = self._merge_context_into_snapshot(node_id, phys)
        snap["observation_seq"] = self._observation_seq
        snap["source_observation_seq"] = self._observation_seq
        snap["context_source_observation_seq"] = self._observation_seq
        self._snapshot_cache[node_id] = snap
        return dict(snap)

    def build_entity_mapping_input(self, node_id: str) -> dict:
        """Legacy-flat EntityMappingInput for Orion (dict form)."""
        from runtime.layer_contracts import (
            CONTEXT_SCHEMA_VERSION,
            PHYSICAL_SNAPSHOT_SCHEMA_VERSION,
        )

        snap = self.get_snapshot(node_id, fresh=False)
        return {
            "physical_snapshot": snap,
            "local_context": (self.runtime.state.nodes.get(node_id).to_dict()
                              if node_id in self.runtime.state.nodes else None),
            "network_context": {
                "probable_causes": snap.get("probable_causes") or [],
                "link_states": snap.get("link_states") or {},
                "network_summary": self.runtime.state.network_summary,
            },
            "provenance": {
                "simulation_run_id": self.simulation_run_id,
                "observation_seq": snap.get("observation_seq", self._observation_seq),
                "source_observation_seq": snap.get("source_observation_seq", self._observation_seq),
                "simulation_time_s": snap.get("simulation_time_sec"),
                "config_hash": get_registry().config_hash,
                "physical_snapshot_schema_version": PHYSICAL_SNAPSHOT_SCHEMA_VERSION,
                "context_schema_version": CONTEXT_SCHEMA_VERSION,
            },
        }

    def _refresh_caches(self) -> None:
        """Rebuild snapshot/stats caches on the TraCI thread."""
        for node_id in self.publish_nodes:
            try:
                self._build_snapshot_now(node_id)
            except Exception as e:
                log.debug("snapshot cache refresh %s: %s", node_id, e)
        try:
            self._stats_cache = self._build_stats_now()
        except Exception as e:
            log.debug("stats cache refresh: %s", e)

    def _invalidate_caches(self) -> None:
        self._snapshot_cache.clear()
        self._stats_cache = {}
        self._last_cache_sim_t = -1e9

    def force_phase(self, node_id: str, phase: str) -> None:
        self._require_started()
        self._assert_node(node_id)
        self.signals[node_id].force_phase(self._traci, phase)
        self._invalidate_caches()

    def set_green_duration(self, node_id: str, seconds: int) -> None:
        self._require_started()
        self._assert_node(node_id)
        self.signals[node_id].set_green_duration(self._traci, seconds)
        self._invalidate_caches()

    def set_scenario(
        self,
        scenario: str,
        target_intersection: Optional[str] = None,
        target_direction: Optional[str] = None,
    ) -> None:
        """Compat facade: map legacy scenario id → demand profile and/or overlay."""
        self._require_started()
        node = target_intersection or self.publish_node
        self._assert_node(node)
        self.scenarios[node].set_scenario(self._traci, scenario, target_direction)
        self.current_scenario = scenario
        self.per_node_scenario[node] = scenario

        demand_ids = {"normal", "morning_peak", "evening_peak", "oversaturated"}
        if scenario in demand_ids:
            self.runtime.set_demand_profile(scenario)
        elif scenario in ("accident", "blocked_intersection"):
            self.runtime.add_overlay(
                self._traci,
                overlay_type=scenario,
                intersection_id=node,
                direction=target_direction or "North",
                segment_role="incoming_approach",
                sim_t=self.simulation_time_sec,
            )
        elif scenario == "spillback":
            self.runtime.add_overlay(
                self._traci,
                overlay_type="downstream_restriction",
                intersection_id=node,
                direction=target_direction or "West",
                segment_role="downstream_exit",
                sim_t=self.simulation_time_sec,
            )
        elif scenario in ("rain", "heavy_rain"):
            self.runtime.add_overlay(
                self._traci,
                overlay_type="heavy_rain",
                intersection_id=node,
                direction=target_direction,
                sim_t=self.simulation_time_sec,
            )
        elif scenario == "emergency":
            self.set_control_mode("PREEMPTION_ENABLED")
            self.runtime.add_overlay(
                self._traci,
                overlay_type="emergency",
                intersection_id=node,
                direction=target_direction,
                sim_t=self.simulation_time_sec,
            )
        self._invalidate_caches()

    def set_demand_profile(self, profile: str) -> dict:
        self._require_started()
        info = self.runtime.set_demand_profile(profile)
        self.current_scenario = profile
        try:
            self._traci.simulation.setScale(1.0)
        except Exception:
            pass
        self._invalidate_caches()
        return info

    def add_overlay(
        self,
        *,
        overlay_type: str,
        intersection_id: str,
        direction: Optional[str] = None,
        segment_role: Optional[str] = None,
        target_edge: Optional[str] = None,
        target_lanes: Optional[list] = None,
        duration_s: Optional[float] = None,
        overlay_id: Optional[str] = None,
    ) -> dict:
        self._require_started()
        self._assert_node(intersection_id)
        return self.runtime.add_overlay(
            self._traci,
            overlay_type=overlay_type,
            intersection_id=intersection_id,
            direction=direction,
            segment_role=segment_role,
            target_edge=target_edge,
            target_lanes=target_lanes,
            duration_s=duration_s,
            sim_t=self.simulation_time_sec,
            overlay_id=overlay_id,
        )

    def remove_overlay(self, overlay_id: str) -> bool:
        self._require_started()
        ok = self.runtime.remove_overlay(self._traci, overlay_id)
        self._invalidate_caches()
        return ok

    def set_control_mode(self, mode: str) -> None:
        self.runtime.set_control_mode(mode)
        self.control_mode = mode
        enabled = mode == "PREEMPTION_ENABLED"
        for sig in self.signals.values():
            sig.preemption_enabled = enabled
            if not enabled:
                sig.preemption_active = False

    def get_network_state(self) -> dict:
        return self.runtime.network_state()

    def get_intersection_state(self, node_id: str) -> dict:
        self._assert_node(node_id)
        nodes = self.runtime.network_state().get("nodes") or {}
        return nodes.get(node_id) or {"intersection_id": node_id}

    def get_stats(self, *, fresh: bool = False) -> dict:
        self._require_started()
        if not fresh and self._stats_cache:
            return dict(self._stats_cache)
        if not fresh and not self._stats_cache:
            # API-safe stub when cache not yet warm (no TraCI from other threads)
            return {
                "total_active_vehicles": 0,
                "total_exited_network": self._exited_total,
                "last_spawn_count": self.last_spawn_count,
                "simulation_time_sec": self.simulation_time_sec,
                "per_node_scenario": dict(self.per_node_scenario),
                "scenario": self.current_scenario,
                "publish_node": self.publish_node,
                "publish_nodes": list(self.publish_nodes),
                "tls_id": self.tls_id,
                "version": cfg.VERSION,
                "cache_warm": False,
            }
        return self._build_stats_now()

    def _build_stats_now(self) -> dict:
        traci = self._traci
        try:
            active = len(traci.vehicle.getIDList())
        except Exception:
            active = 0
        waiting = self.trips.snapshot_waiting_stats(traci)
        net = self.runtime.network_state()
        out = {
            "total_active_vehicles": active,
            "total_exited_network": self._exited_total,
            "last_spawn_count": self.last_spawn_count,
            "simulation_time_sec": self.simulation_time_sec,
            "per_node_scenario": dict(self.per_node_scenario),
            "scenario": self.current_scenario,
            "demand_profile_id": net.get("demand_profile_id"),
            "control_mode": net.get("control_mode") or self.control_mode,
            "overlays": net.get("overlays") or [],
            "insertion_stats": net.get("insertion_stats") or {},
            "source_stats": net.get("source_stats") or {},
            "publish_node": self.publish_node,
            "publish_nodes": list(self.publish_nodes),
            "tls_id": self.tls_id,
            "version": cfg.VERSION,
            "simulation_run_id": self.simulation_run_id,
            "schema_version": cfg.MODEL_SCHEMA_VERSION,
            "pcu_profile_id": cfg.PCU_PROFILE_ID,
            "config_hash": cfg.CONFIG_HASH,
            **waiting,
        }
        self._stats_cache = out
        return dict(out)

    def count_total_vehicles(self) -> int:
        if not self._started:
            return 0
        return len(self._traci.vehicle.getIDList())

    def count_exited_network(self) -> int:
        return self._exited_total

    @property
    def intersections(self) -> dict:
        backend = self

        class _PhaseProxy:
            def __init__(self, node: str):
                self._node = node

            def force_phase(self, phase: str) -> None:
                backend.force_phase(self._node, phase)

            def set_green_duration(self, seconds: int) -> None:
                backend.set_green_duration(self._node, seconds)

        class _Ix:
            def __init__(self, node: str):
                self.phase_controller = _PhaseProxy(node)

        return {n: _Ix(n) for n in self.publish_nodes}

    def _require_started(self) -> None:
        if not self._started or self._traci is None:
            raise RuntimeError("SumoBackend is not started. Call start() first.")

    def _assert_node(self, node_id: str) -> None:
        if node_id not in self.publish_nodes:
            raise KeyError(
                f"Node {node_id} not controlled; publish_nodes={self.publish_nodes}"
            )
