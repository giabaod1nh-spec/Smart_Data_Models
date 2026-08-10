"""ScenarioDemandActuator — deterministic bucket scheduler; owns profile−baseline only."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from configuration.model_params import get_registry
from configuration.turn_routes import to_edge_for_route_key

log = logging.getLogger(__name__)

# Try freer departure placements when the entry edge is contested.
_DEPART_TRIES = (
    ("free", "base"),
    ("free", "free"),
    ("best", "random_free"),
)

# Higher rank wins when a source feeds multiple intersections with different profiles.
_PROFILE_SEVERITY = {
    "normal": 0,
    "morning_peak": 1,
    "evening_peak": 1,
    "heavy_traffic": 2,
    "oversaturated": 3,
}


@dataclass
class SourceBucket:
    source_id: str
    target_delta_vph: float
    interval_s: float
    accumulator_s: float = 0.0
    scheduled: int = 0
    inserted: int = 0
    failed: int = 0
    pending: int = 0


class ScenarioDemandActuator:
    """
    Hybrid ownership: static rou owns baseline_veh_per_hour;
    this actuator inserts only max(0, profile_target - baseline).

    Demand can be scoped per intersection: only boundary sources whose
    nodes_on_path include that intersection are boosted for that profile.
    Sources feeding multiple nodes take the max target among active node profiles.
    """

    def __init__(self, seed: int = 42):
        self.seed = int(seed)
        self.profile_id: str = "normal"
        self.default_profile_id: str = "normal"
        self.node_profiles: Dict[str, str] = {}
        self._buckets: Dict[str, SourceBucket] = {}
        self._seq = 0
        self.enabled = True
        self._last_logged_failed = 0
        self.stats: Dict[str, Any] = {
            "inserted_total": 0,
            "failed_total": 0,
            "pending_total": 0,
            "scheduled_total": 0,
        }

    def set_profile(
        self,
        profile_id: str,
        target_intersection: Optional[str] = None,
    ) -> Dict[str, Any]:
        if target_intersection:
            self.node_profiles[str(target_intersection)] = profile_id
            self.profile_id = profile_id
        else:
            # Network-wide: one default profile for every source.
            self.default_profile_id = profile_id
            self.profile_id = profile_id
            self.node_profiles.clear()
        return self._rebuild_buckets(active_profile=profile_id, scope=target_intersection)

    def _profile_for_node(self, node_id: str) -> str:
        return self.node_profiles.get(node_id, self.default_profile_id)

    def _effective_target_vph(
        self,
        source_id: str,
        src: Dict[str, Any],
        profile_targets: Dict[str, Dict[str, float]],
    ) -> Tuple[float, str]:
        """Return (target_vph, winning_profile_id) for a boundary source."""
        nodes = [str(n) for n in (src.get("nodes_on_path") or [])]
        if not nodes:
            pid = self.default_profile_id
            return float(profile_targets.get(pid, {}).get(source_id, src["baseline_veh_per_hour"])), pid

        best_pid = self._profile_for_node(nodes[0])
        best_tgt = float(
            profile_targets.get(best_pid, {}).get(source_id, src["baseline_veh_per_hour"])
        )
        best_sev = _PROFILE_SEVERITY.get(best_pid, 0)
        for nid in nodes[1:]:
            pid = self._profile_for_node(nid)
            tgt = float(profile_targets.get(pid, {}).get(source_id, src["baseline_veh_per_hour"]))
            sev = _PROFILE_SEVERITY.get(pid, 0)
            if sev > best_sev or (sev == best_sev and tgt > best_tgt):
                best_sev = sev
                best_tgt = tgt
                best_pid = pid
        return best_tgt, best_pid

    def _rebuild_buckets(
        self,
        *,
        active_profile: str,
        scope: Optional[str],
    ) -> Dict[str, Any]:
        reg = get_registry()
        sources = reg.boundary_sources()
        # Collect all profiles we might need.
        needed = {self.default_profile_id, *self.node_profiles.values(), active_profile}
        profile_targets: Dict[str, Dict[str, float]] = {}
        for pid in needed:
            prof = reg.demand_profile(pid)
            profile_targets[pid] = {
                k: float(v) for k, v in (prof.get("source_targets") or {}).items()
            }

        prev_pending = {sid: b.pending for sid, b in self._buckets.items()}
        prev_acc = {sid: b.accumulator_s for sid, b in self._buckets.items()}
        self._buckets.clear()
        self._last_logged_failed = 0
        winning: Dict[str, str] = {}
        for sid, src in sources.items():
            base = float(src["baseline_veh_per_hour"])
            tgt_f, win_pid = self._effective_target_vph(sid, src, profile_targets)
            if tgt_f < base:
                raise ValueError(
                    f"Reject profile mix: {sid} target {tgt_f} < baseline {base}"
                )
            delta = tgt_f - base
            interval = 3600.0 / delta if delta > 0 else 1e9
            bucket = SourceBucket(
                source_id=sid,
                target_delta_vph=delta,
                interval_s=interval,
            )
            # Preserve owed inserts / accumulator across profile rebuilds.
            bucket.pending = int(prev_pending.get(sid, 0))
            bucket.accumulator_s = float(prev_acc.get(sid, 0.0))
            self._buckets[sid] = bucket
            winning[sid] = win_pid

        log.info(
            "Demand rebuild active=%s scope=%s node_profiles=%s deltas=%s winners=%s",
            active_profile,
            scope or "network",
            dict(self.node_profiles),
            {k: round(v.target_delta_vph, 1) for k, v in self._buckets.items()},
            winning,
        )
        return {
            "profile_id": active_profile,
            "scope": scope or "network",
            "node_profiles": dict(self.node_profiles),
            "deltas": {k: v.target_delta_vph for k, v in self._buckets.items()},
            "source_profile_winners": winning,
        }

    def _pick_route_vtype(self, source_id: str, n: int) -> Tuple[str, str, str, str]:
        reg = get_registry()
        src = reg.boundary_sources()[source_id]
        comp = reg.export_effective_config().get("composition") or {}
        weights = dict(comp.get("weights") or {})
        h = int(hashlib.md5(f"{self.seed}:{source_id}:{n}".encode()).hexdigest()[:8], 16)
        rd = src.get("route_distribution") or {source_id: 1.0}
        items = list(rd.items())
        r = (h % 10000) / 10000.0
        cum = 0.0
        route_key = items[-1][0]
        for k, p in items:
            cum += float(p)
            if r <= cum:
                route_key = k
                break
        wv = list(weights.items()) or [("motorcycle", 1.0)]
        r2 = ((h // 10000) % 10000) / 10000.0
        cum = 0.0
        vtype = wv[-1][0]
        for k, p in wv:
            cum += float(p)
            if r2 <= cum:
                vtype = k
                break
        return route_key, src["source_edge"], to_edge_for_route_key(source_id, route_key, src), vtype

    def tick(self, traci_module, dt: float) -> int:
        if not self.enabled or dt <= 0:
            return 0
        policy = get_registry().insertion_policy()
        max_pending = int(policy.get("max_pending_per_source", 120))
        max_retries = int(policy.get("max_retries", 3))
        inserted = 0
        pending = 0
        for sid, bucket in self._buckets.items():
            if bucket.target_delta_vph <= 0:
                continue
            bucket.accumulator_s += dt
            if bucket.pending >= max_pending:
                bucket.accumulator_s = min(bucket.accumulator_s, bucket.interval_s)
            while bucket.accumulator_s >= bucket.interval_s and bucket.pending < max_pending:
                bucket.accumulator_s -= bucket.interval_s
                bucket.scheduled += 1
                self.stats["scheduled_total"] += 1
                bucket.pending += 1
            attempts = 0
            max_attempts = max(1, max_retries * 2)
            while bucket.pending > 0 and attempts < max_attempts:
                attempts += 1
                ok = self._try_insert(traci_module, sid, bucket)
                if ok:
                    bucket.pending -= 1
                    bucket.inserted += 1
                    inserted += 1
                    self.stats["inserted_total"] += 1
                else:
                    bucket.failed += 1
                    self.stats["failed_total"] += 1
                    break
            pending += bucket.pending
        self.stats["pending_total"] = pending
        failed = int(self.stats["failed_total"])
        if failed - self._last_logged_failed >= 250:
            log.warning(
                "Demand insert pressure profile=%s inserted=%s failed=%s pending=%s",
                self.profile_id,
                self.stats["inserted_total"],
                failed,
                pending,
            )
            self._last_logged_failed = failed
        return inserted

    def _try_insert(self, traci_module, source_id: str, bucket: SourceBucket) -> bool:
        self._seq += 1
        route_key, fr, to, vtype = self._pick_route_vtype(source_id, self._seq)
        rid = f"r_{source_id}_{self._seq}"
        vid = f"d_{source_id}_{self._seq}"
        edges = [fr, to]
        try:
            found = traci_module.simulation.findRoute(fr, to)
            if found and getattr(found, "edges", None):
                edges = list(found.edges)
        except Exception:
            pass
        try:
            traci_module.route.add(rid, edges)
        except Exception:
            pass
        last_err: Exception | None = None
        for depart_lane, depart_pos in _DEPART_TRIES:
            try:
                traci_module.vehicle.add(
                    vid,
                    rid,
                    typeID=vtype,
                    depart="now",
                    departLane=depart_lane,
                    departPos=depart_pos,
                    departSpeed="0",
                )
                return True
            except Exception as e:
                last_err = e
                continue
        log.debug("insert fail %s route=%s: %s", vid, route_key, last_err)
        return False

    def source_stats(self) -> Dict[str, Any]:
        return {
            sid: {
                "target_delta_vph": b.target_delta_vph,
                "interval_s": round(b.interval_s, 3),
                "scheduled": b.scheduled,
                "inserted": b.inserted,
                "failed": b.failed,
                "pending": b.pending,
            }
            for sid, b in self._buckets.items()
        }
