"""
control_api.py — FastAPI Control API for SumoBackend (ADR-005).

Mutating endpoints enqueue commands; TraCI thread drains via SumoBackend.step().
"""
from __future__ import annotations

from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import configuration.config as cfg

app = FastAPI(title="Visualize SUMO Control API", version=cfg.VERSION)
# NOTE: Origins are intentionally restricted to local development frontends;
# configure allow_origins appropriately before production deployment.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = None  # SumoBackend instance set by traci_runner


class ScenarioRequest(BaseModel):
    scenario: str
    target_intersection: Optional[str] = None
    target_direction: Optional[str] = None
    node_overrides: Optional[Dict[str, str]] = None


class PhaseRequest(BaseModel):
    intersection_id: str
    phase: str


class GreenDurationRequest(BaseModel):
    intersection_id: str
    seconds: int = Field(..., ge=10, le=120)


class DemandProfileRequest(BaseModel):
    profile: str


class OverlayRequest(BaseModel):
    overlay_type: str
    intersection_id: str
    direction: Optional[str] = None
    segment_role: Optional[str] = None
    target_edge: Optional[str] = None
    target_lanes: Optional[List[str]] = None
    duration_s: Optional[float] = None
    overlay_id: Optional[str] = None


class ControlModeRequest(BaseModel):
    mode: str = Field(..., pattern="^(FIXED|PREEMPTION_ENABLED)$")


def _require_engine():
    if engine is None:
        raise HTTPException(503, "engine not started")
    return engine


@app.get("/health")
def health():
    eng = engine
    return {
        "status": "ok" if eng and eng._started else "starting",
        "scenario": eng.current_scenario if eng else None,
        "demand_profile_id": (
            eng.runtime.state.demand_profile_id if eng and hasattr(eng, "runtime") else None
        ),
        "control_mode": eng.control_mode if eng else None,
        "version": cfg.VERSION,
        "publish_nodes": eng.publish_nodes if eng else cfg.PUBLISH_NODES,
    }


@app.get("/scenario")
def get_scenario():
    eng = _require_engine()
    available = [{"id": s, "description": s} for s in cfg.SCENARIO_IDS]
    net = eng.get_network_state() if eng._started else {}
    return {
        "current": eng.current_scenario,
        "demand_profile_id": net.get("demand_profile_id"),
        "control_mode": net.get("control_mode"),
        "overlays": net.get("overlays") or [],
        "available": available,
        "intersections": list(cfg.NODE_TO_TLS.keys()),
        "directions": cfg.DIRECTIONS,
        "per_node": eng.per_node_scenario,
    }


@app.post("/scenario")
def set_scenario(req: ScenarioRequest):
    eng = _require_engine()
    if req.scenario not in cfg.SCENARIO_IDS:
        raise HTTPException(400, f"Unknown scenario '{req.scenario}'")
    if req.target_intersection and req.target_intersection not in eng.publish_nodes:
        raise HTTPException(400, f"Unknown intersection '{req.target_intersection}'")
    eng.commands.enqueue(
        "set_scenario",
        scenario=req.scenario,
        target_intersection=req.target_intersection,
        target_direction=req.target_direction,
    )
    if req.node_overrides:
        for node_id, sc in req.node_overrides.items():
            if node_id not in eng.publish_nodes:
                raise HTTPException(400, f"Unknown intersection '{node_id}'")
            if sc not in cfg.SCENARIO_IDS:
                raise HTTPException(400, f"Unknown scenario '{sc}'")
            eng.commands.enqueue(
                "set_scenario",
                scenario=sc,
                target_intersection=node_id,
                target_direction=None,
            )
    return {"queued": True, "current": req.scenario}


@app.post("/demand-profile")
def set_demand_profile(req: DemandProfileRequest):
    eng = _require_engine()
    try:
        from configuration.model_params import get_registry

        get_registry().demand_profile(req.profile)
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    eng.commands.enqueue("set_demand_profile", profile=req.profile)
    return {"queued": True, "profile": req.profile}


@app.post("/overlays")
def add_overlay(req: OverlayRequest):
    eng = _require_engine()
    if req.intersection_id not in eng.publish_nodes:
        raise HTTPException(400, f"Unknown intersection '{req.intersection_id}'")
    eng.commands.enqueue(
        "add_overlay",
        overlay_type=req.overlay_type,
        intersection_id=req.intersection_id,
        direction=req.direction,
        segment_role=req.segment_role,
        target_edge=req.target_edge,
        target_lanes=req.target_lanes,
        duration_s=req.duration_s,
        overlay_id=req.overlay_id,
    )
    return {"queued": True, "overlay_type": req.overlay_type}


@app.delete("/overlays/{overlay_id}")
def delete_overlay(overlay_id: str):
    eng = _require_engine()
    eng.commands.enqueue("remove_overlay", overlay_id=overlay_id)
    return {"queued": True, "overlay_id": overlay_id}


@app.get("/overlays")
def list_overlays():
    eng = _require_engine()
    return {"overlays": eng.get_network_state().get("overlays") or []}


@app.get("/network-state")
def network_state():
    eng = _require_engine()
    return eng.get_network_state()


@app.get("/intersections/{intersection_id}/state")
def intersection_state(intersection_id: str):
    eng = _require_engine()
    if intersection_id not in eng.publish_nodes:
        raise HTTPException(404, f"Unknown intersection '{intersection_id}'")
    return eng.get_intersection_state(intersection_id)


@app.get("/links/{link_id}/state")
def link_state(link_id: str):
    eng = _require_engine()
    net = eng.get_network_state()
    causes = []
    for node in (net.get("nodes") or {}).values():
        for c in node.get("probable_causes") or []:
            if c.get("link") == link_id:
                causes.append(c)
    return {"link_id": link_id, "probable_causes": causes}


@app.post("/control-mode")
def set_control_mode(req: ControlModeRequest):
    eng = _require_engine()
    eng.commands.enqueue("set_control_mode", mode=req.mode)
    return {"queued": True, "mode": req.mode}


@app.post("/phase")
def set_phase(req: PhaseRequest):
    eng = _require_engine()
    if req.phase not in cfg.PHASE_SEQUENCE:
        raise HTTPException(400, f"Unknown phase '{req.phase}'")
    if req.intersection_id not in eng.publish_nodes:
        raise HTTPException(400, f"Unknown intersection '{req.intersection_id}'")
    eng.commands.enqueue("force_phase", node_id=req.intersection_id, phase=req.phase)
    return {"queued": True, "intersection_id": req.intersection_id, "phase": req.phase}


@app.post("/green-duration")
def set_green_duration(req: GreenDurationRequest):
    eng = _require_engine()
    if req.intersection_id not in eng.publish_nodes:
        raise HTTPException(400, f"Unknown intersection '{req.intersection_id}'")
    eng.commands.enqueue(
        "set_green_duration", node_id=req.intersection_id, seconds=req.seconds
    )
    return {
        "queued": True,
        "intersection_id": req.intersection_id,
        "green_duration_seconds": req.seconds,
    }


@app.get("/snapshot/{intersection_id}")
def get_snapshot(intersection_id: str):
    eng = _require_engine()
    if intersection_id not in eng.publish_nodes:
        raise HTTPException(404, f"Unknown intersection '{intersection_id}'")
    try:
        return eng.get_snapshot(intersection_id)
    except Exception as e:
        raise HTTPException(500, str(e)) from e


@app.get("/trip-records")
def get_trip_records(limit: int = 50):
    eng = _require_engine()
    return {"records": eng.trip_records[-limit:]}


@app.get("/stats")
def get_stats():
    eng = _require_engine()
    return eng.get_stats()
