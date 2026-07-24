"""
trip_collector.py — Accumulate trip / waiting metrics from TraCI.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Set

log = logging.getLogger(__name__)


class TripCollector:
    def __init__(self, max_records: int = 5000):
        self.max_records = max_records
        self.records: List[dict] = []
        self.total_exited = 0
        self._seen_depart: Set[str] = set()
        self._depart_time: Dict[str, float] = {}

    def on_step(self, traci_module, sim_t: float) -> None:
        try:
            for vid in traci_module.simulation.getDepartedIDList():
                self._depart_time[vid] = sim_t
                self._seen_depart.add(vid)
        except Exception:
            pass

        try:
            arrived = list(traci_module.simulation.getArrivedIDList())
        except Exception:
            arrived = []

        for vid in arrived:
            self.total_exited += 1
            depart = self._depart_time.pop(vid, None)
            travel = round(sim_t - depart, 2) if depart is not None else None
            waiting = None
            time_loss = None
            vtype = None
            try:
                # Arrived vehicles may already be gone; best-effort from last known
                pass
            except Exception:
                pass
            rec = {
                "vehicle_id": vid,
                "depart_sim_t": depart,
                "arrive_sim_t": sim_t,
                "travel_time_sec": travel,
                "waiting_time_sec": waiting,
                "time_loss_sec": time_loss,
                "vehicle_type": vtype,
            }
            self.records.append(rec)
            if len(self.records) > self.max_records:
                self.records = self.records[-self.max_records :]

        # Sample waiting time for active vehicles into optional side channel
        try:
            for vid in traci_module.vehicle.getIDList():
                if vid not in self._depart_time:
                    self._depart_time[vid] = sim_t
        except Exception:
            pass

    def snapshot_waiting_stats(self, traci_module) -> Dict[str, Any]:
        waits: List[float] = []
        try:
            for vid in traci_module.vehicle.getIDList():
                try:
                    waits.append(float(traci_module.vehicle.getAccumulatedWaitingTime(vid)))
                except Exception:
                    pass
        except Exception:
            pass
        if not waits:
            return {"active_waiting_mean_sec": 0.0, "active_waiting_max_sec": 0.0}
        return {
            "active_waiting_mean_sec": round(sum(waits) / len(waits), 2),
            "active_waiting_max_sec": round(max(waits), 2),
        }
