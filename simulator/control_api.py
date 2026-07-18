"""
control_api.py — Control API cho Dashboard (multi-intersection)
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Optional

from scenarios import SCENARIOS, PHASE_SEQUENCE, DIRECTIONS
import network as net

app = FastAPI(title="Simulator Control API (Multi-Intersection)")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

engine = None


class ScenarioRequest(BaseModel):
    scenario: str
    target_intersection: Optional[str] = None
    target_direction: Optional[str] = None
    node_overrides: Optional[Dict[str, str]] = None  # {"B": "morning_peak"}


class PhaseRequest(BaseModel):
    intersection_id: str
    phase: str


class GreenDurationRequest(BaseModel):
    intersection_id: str
    seconds: int


@app.get("/scenario")
def get_scenario():
    available = [{"id": k, "description": v["description"]} for k, v in SCENARIOS.items()]
    return {
        "current": engine.current_scenario if engine else "normal",
        "available": available,
        "intersections": net.INTERSECTION_NODES,
        "directions": DIRECTIONS,
        "per_node": engine.per_node_scenario if engine else {},
    }


@app.post("/scenario")
def set_scenario(req: ScenarioRequest):
    if req.scenario not in SCENARIOS:
        raise HTTPException(400, f"Unknown scenario '{req.scenario}'")
    if req.node_overrides:
        for node_id, sc in req.node_overrides.items():
            if node_id not in net.INTERSECTION_NODES:
                raise HTTPException(400, f"Unknown intersection '{node_id}'")
            if sc not in SCENARIOS:
                raise HTTPException(400, f"Unknown scenario override '{sc}' for {node_id}")
    if engine:
        engine.set_scenario(req.scenario, req.target_intersection, req.target_direction)
        if req.node_overrides:
            engine.per_node_scenario.update(req.node_overrides)
    return {
        "current": req.scenario,
        "per_node": engine.per_node_scenario if engine else {},
    }


@app.post("/phase")
def set_phase(req: PhaseRequest):
    if req.phase not in PHASE_SEQUENCE:
        raise HTTPException(400, f"Unknown phase '{req.phase}'")
    if req.intersection_id not in net.INTERSECTION_NODES:
        raise HTTPException(400, f"Unknown intersection '{req.intersection_id}'")
    if engine:
        engine.intersections[req.intersection_id].phase_controller.force_phase(req.phase)
    return {"intersection_id": req.intersection_id, "phase": req.phase}


@app.post("/green-duration")
def set_green_duration(req: GreenDurationRequest):
    if req.intersection_id not in net.INTERSECTION_NODES:
        raise HTTPException(400, f"Unknown intersection '{req.intersection_id}'")
    if engine:
        engine.intersections[req.intersection_id].phase_controller.set_green_duration(req.seconds)
    return {"intersection_id": req.intersection_id, "green_duration_seconds": req.seconds}


@app.get("/snapshot/{intersection_id}")
def get_snapshot(intersection_id: str):
    if not engine:
        return {"error": "engine not initialized"}
    if intersection_id not in net.INTERSECTION_NODES:
        raise HTTPException(404, f"Unknown intersection '{intersection_id}'")
    return engine.get_snapshot(intersection_id)


@app.get("/trip-records")
def get_trip_records(limit: int = 50):
    if not engine:
        return {"records": []}
    return {"records": engine.trip_records[-limit:]}


@app.get("/stats")
def get_stats():
    if not engine:
        return {"error": "engine not initialized"}
    return {
        "total_active_vehicles": engine.count_total_vehicles(),
        "total_exited_network":  engine.count_exited_network(),
        "last_spawn_count":      engine.last_spawn_count,
        "simulation_time_sec":   engine.simulation_time_sec,
        "per_node_scenario":     engine.per_node_scenario,
    }


@app.get("/health")
def health():
    return {"status": "ok", "scenario": engine.current_scenario if engine else None}
