"""ScenarioDemandActuator — deterministic bucket scheduler; owns profile−baseline only."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from configuration.model_params import get_registry

log = logging.getLogger(__name__)


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
    Never uses simulation.setScale for demand.
    """

    def __init__(self, seed: int = 42):
        self.seed = int(seed)
        self.profile_id: str = "normal"
        self._buckets: Dict[str, SourceBucket] = {}
        self._seq = 0
        self.enabled = True
        self.stats: Dict[str, Any] = {
            "inserted_total": 0,
            "failed_total": 0,
            "pending_total": 0,
            "scheduled_total": 0,
        }

    def set_profile(self, profile_id: str) -> Dict[str, Any]:
        reg = get_registry()
        prof = reg.demand_profile(profile_id)
        sources = reg.boundary_sources()
        self.profile_id = profile_id
        self._buckets.clear()
        targets = prof.get("source_targets") or {}
        for sid, tgt in targets.items():
            src = sources[sid]
            base = float(src["baseline_veh_per_hour"])
            tgt_f = float(tgt)
            if tgt_f < base:
                raise ValueError(f"Reject profile {profile_id}: {sid} target {tgt_f} < baseline {base}")
            delta = tgt_f - base
            interval = 3600.0 / delta if delta > 0 else 1e9
            self._buckets[sid] = SourceBucket(
                source_id=sid,
                target_delta_vph=delta,
                interval_s=interval,
            )
        log.info(
            "Demand profile %s active; sources=%d deltas=%s",
            profile_id,
            len(self._buckets),
            {k: round(v.target_delta_vph, 1) for k, v in self._buckets.items()},
        )
        return {"profile_id": profile_id, "deltas": {k: v.target_delta_vph for k, v in self._buckets.items()}}

    def _pick_route_vtype(self, source_id: str, n: int) -> Tuple[str, str, str, str]:
        reg = get_registry()
        src = reg.boundary_sources()[source_id]
        comp = reg.export_effective_config().get("composition") or {}
        weights = dict(comp.get("weights") or {})
        # Deterministic pick from seed + n
        h = int(hashlib.md5(f"{self.seed}:{source_id}:{n}".encode()).hexdigest()[:8], 16)
        # route
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
        # vtype
        wv = list(weights.items()) or [("motorcycle", 1.0)]
        r2 = ((h // 10000) % 10000) / 10000.0
        cum = 0.0
        vtype = wv[-1][0]
        for k, p in wv:
            cum += float(p)
            if r2 <= cum:
                vtype = k
                break
        return route_key, src["source_edge"], src["to_edge"], vtype

    def tick(self, traci_module, dt: float) -> int:
        if not self.enabled or dt <= 0:
            return 0
        policy = get_registry().insertion_policy()
        max_pending = int(policy.get("max_pending_per_source", 40))
        max_retries = int(policy.get("max_retries", 3))
        inserted = 0
        pending = 0
        for sid, bucket in self._buckets.items():
            if bucket.target_delta_vph <= 0:
                continue
            bucket.accumulator_s += dt
            while bucket.accumulator_s >= bucket.interval_s and bucket.pending < max_pending:
                bucket.accumulator_s -= bucket.interval_s
                bucket.scheduled += 1
                self.stats["scheduled_total"] += 1
                bucket.pending += 1
            # drain pending
            attempts = 0
            while bucket.pending > 0 and attempts < max_retries * 2:
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
        return inserted

    def _try_insert(self, traci_module, source_id: str, bucket: SourceBucket) -> bool:
        self._seq += 1
        route_key, fr, to, vtype = self._pick_route_vtype(source_id, self._seq)
        rid = f"r_{source_id}_{self._seq}"
        vid = f"d_{source_id}_{self._seq}"
        try:
            edges = [fr, to]
            try:
                found = traci_module.simulation.findRoute(fr, to)
                if found and getattr(found, "edges", None):
                    edges = list(found.edges)
            except Exception:
                pass
            if rid not in traci_module.route.getIDList():
                traci_module.route.add(rid, edges)
            traci_module.vehicle.add(vid, rid, typeID=vtype, depart="now")
            return True
        except Exception as e:
            log.debug("insert fail %s route=%s: %s", vid, route_key, e)
            return False

    def source_stats(self) -> Dict[str, Any]:
        return {
            sid: {
                "target_delta_vph": b.target_delta_vph,
                "scheduled": b.scheduled,
                "inserted": b.inserted,
                "failed": b.failed,
                "pending": b.pending,
            }
            for sid, b in self._buckets.items()
        }
