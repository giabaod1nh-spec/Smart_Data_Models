"""Live vehicle WebSocket stream — TraCI collect + non-blocking fan-out.

TraCI thread only calls LiveStreamHub.publish(); WebSocket handlers never run
on the simulation thread. Client disconnect must not affect SUMO.
"""
from __future__ import annotations

import logging
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

log = logging.getLogger(__name__)

# TraCI subscription variable codes (avoid importing traci at module load).
# Values must match traci.constants — see sumo/tools/traci/constants.py.
_VAR_POSITION = 0x42
_VAR_SPEED = 0x40
_VAR_ANGLE = 0x43
_VAR_LANEPOSITION = 0x56
_VAR_LENGTH = 0x44
_VAR_ROAD_ID = 0x50
_VAR_LANE_ID = 0x51
_VAR_TYPE = 0x4F
_VAR_WIDTH = 0x4D

_SUB_VARS = (
    _VAR_POSITION,
    _VAR_SPEED,
    _VAR_ANGLE,
    _VAR_LANEPOSITION,
    _VAR_LENGTH,
    _VAR_WIDTH,
    _VAR_ROAD_ID,
    _VAR_LANE_ID,
    _VAR_TYPE,
)

_DEFAULT_LANE_WIDTH_M = 3.2


def _parse_shape(shape_str: str) -> List[List[float]]:
    pts: List[List[float]] = []
    for token in (shape_str or "").split():
        if "," not in token:
            continue
        try:
            xs, ys = token.split(",", 1)
            pts.append([float(xs), float(ys)])
        except ValueError:
            continue
    return pts


def _lane_width_from_shapes(lane_shapes: List[List[List[float]]]) -> float:
    if len(lane_shapes) < 2:
        return _DEFAULT_LANE_WIDTH_M
    a0, a1 = lane_shapes[0], lane_shapes[1]
    if len(a0) < 1 or len(a1) < 1:
        return _DEFAULT_LANE_WIDTH_M
    dx = a1[0][0] - a0[0][0]
    dy = a1[0][1] - a0[0][1]
    w = (dx * dx + dy * dy) ** 0.5
    return w if w > 0.5 else _DEFAULT_LANE_WIDTH_M


def load_vtypes(rou_xml: Path) -> Dict[str, Dict[str, Any]]:
    """Parse SUMO vType dimensions from .rou.xml for frontend rendering."""
    if not rou_xml.is_file():
        return {}
    root = ET.parse(rou_xml).getroot()
    out: Dict[str, Dict[str, Any]] = {}
    for vt in root.findall("vType"):
        vid = vt.get("id") or ""
        if not vid:
            continue
        try:
            length = float(vt.get("length") or 4.5)
            width = float(vt.get("width") or 2.2)
        except (TypeError, ValueError):
            continue
        out[vid] = {
            "length": length,
            "width": width,
            "vClass": vt.get("vClass") or "",
            "guiShape": vt.get("guiShape") or "",
        }
    return out

_WAITING_SPEED_MPS = 0.1


def load_network_geometry(net_xml: Path) -> Dict[str, Any]:
    """Parse SUMO .net.xml into canvas-friendly geometry (cartesian only)."""
    root = ET.parse(net_xml).getroot()
    loc = root.find("location")
    conv = (loc.get("convBoundary") if loc is not None else "0,0,800,800") or "0,0,800,800"
    parts = [float(x) for x in conv.split(",")]
    bounds = {
        "minX": parts[0],
        "minY": parts[1],
        "maxX": parts[2],
        "maxY": parts[3],
        "projParameter": (loc.get("projParameter") if loc is not None else "!") or "!",
        "geoReferenced": False,
    }
    # projParameter "!" means no geo projection — never invent lat/lon.
    if loc is not None and (loc.get("projParameter") or "!").strip() not in ("!", ""):
        bounds["geoReferenced"] = True

    junctions: List[Dict[str, Any]] = []
    for j in root.findall("junction"):
        jid = j.get("id") or ""
        if not jid or jid.startswith(":"):
            continue
        try:
            junctions.append(
                {
                    "id": jid,
                    "x": float(j.get("x", 0)),
                    "y": float(j.get("y", 0)),
                    "type": j.get("type") or "",
                }
            )
        except (TypeError, ValueError):
            continue

    edges: List[Dict[str, Any]] = []
    for e in root.findall("edge"):
        eid = e.get("id") or ""
        if not eid or eid.startswith(":"):
            continue
        lanes = e.findall("lane")
        if not lanes:
            continue
        lane_entries: List[Dict[str, Any]] = []
        lane_shapes: List[List[List[float]]] = []
        for lane in lanes:
            pts = _parse_shape(lane.get("shape") or "")
            if len(pts) < 2:
                continue
            lane_shapes.append(pts)
            try:
                length = float(lane.get("length") or 0.0)
            except (TypeError, ValueError):
                length = 0.0
            lane_entries.append(
                {
                    "id": lane.get("id") or "",
                    "index": int(lane.get("index") or len(lane_entries)),
                    "shape": pts,
                    "length": length,
                }
            )
        if not lane_entries:
            continue
        lane_width = _lane_width_from_shapes(lane_shapes)
        for le in lane_entries:
            le["width"] = lane_width
        mid = lane_entries[len(lane_entries) // 2]["shape"]
        edges.append(
            {
                "id": eid,
                "from": e.get("from"),
                "to": e.get("to"),
                "numLanes": len(lane_entries),
                "shape": mid,
                "lanes": lane_entries,
            }
        )

    return {"bounds": bounds, "junctions": junctions, "edges": edges}


class VehicleStreamCollector:
    """Throttled TraCI vehicle subscription → compact live frame."""

    def __init__(
        self,
        *,
        hz: float = 10.0,
        tls_ids: Optional[List[str]] = None,
        node_to_tls: Optional[Dict[str, str]] = None,
        signals: Optional[Dict[str, Any]] = None,
    ):
        self.hz = max(1.0, float(hz))
        self._min_interval = 1.0 / self.hz
        self._last_wall = 0.0
        self._subscribed: Set[str] = set()
        self.tls_ids = list(tls_ids or [])
        self.node_to_tls = dict(node_to_tls or {})
        self.signals = signals or {}
        self._frames_built = 0
        self._last_frame: Optional[Dict[str, Any]] = None

    def reset(self) -> None:
        self._subscribed.clear()
        self._last_wall = 0.0
        self._last_frame = None

    def maybe_collect(self, traci_module, sim_t: float) -> Optional[Dict[str, Any]]:
        now = time.monotonic()
        if self._last_wall > 0 and (now - self._last_wall) < self._min_interval:
            return None
        self._last_wall = now
        try:
            frame = self._build_frame(traci_module, sim_t)
        except Exception as e:
            log.warning("live stream collect failed: %s", e)
            return None
        self._frames_built += 1
        self._last_frame = frame
        return frame

    def _sync_subscriptions(self, traci_module, vehicle_ids: Set[str]) -> None:
        # Subscribe new vehicles
        for vid in vehicle_ids:
            if vid in self._subscribed:
                continue
            try:
                traci_module.vehicle.subscribe(vid, _SUB_VARS)
                self._subscribed.add(vid)
            except Exception:
                pass
        # Drop departed — SUMO removes their subscriptions automatically, so an
        # explicit unsubscribe would only spam "subscription to remove not found".
        self._subscribed.intersection_update(vehicle_ids)

    def _build_frame(self, traci_module, sim_t: float) -> Dict[str, Any]:
        try:
            ids = set(traci_module.vehicle.getIDList())
        except Exception:
            ids = set()

        self._sync_subscriptions(traci_module, ids)

        vehicles: List[Dict[str, Any]] = []
        speed_sum = 0.0
        waiting = 0
        results = {}
        try:
            results = traci_module.vehicle.getAllSubscriptionResults() or {}
        except Exception as e:
            log.debug("getAllSubscriptionResults: %s", e)

        for vid in ids:
            data = results.get(vid) or {}
            try:
                if data:
                    pos = data.get(_VAR_POSITION)
                    speed = float(data.get(_VAR_SPEED, 0.0))
                    angle = float(data.get(_VAR_ANGLE, 0.0))
                    road = str(data.get(_VAR_ROAD_ID, "") or "")
                    lane = str(data.get(_VAR_LANE_ID, "") or "")
                    vtype = str(data.get(_VAR_TYPE, "") or "")
                    lane_pos = float(data.get(_VAR_LANEPOSITION, 0.0))
                    length = float(data.get(_VAR_LENGTH, 0.0))
                    width = float(data.get(_VAR_WIDTH, 0.0))
                    if pos is None:
                        pos = traci_module.vehicle.getPosition(vid)
                    x, y = float(pos[0]), float(pos[1])
                else:
                    # Fallback per-vehicle getters if subscription miss
                    x, y = traci_module.vehicle.getPosition(vid)
                    speed = float(traci_module.vehicle.getSpeed(vid))
                    angle = float(traci_module.vehicle.getAngle(vid))
                    road = str(traci_module.vehicle.getRoadID(vid))
                    lane = str(traci_module.vehicle.getLaneID(vid))
                    vtype = str(traci_module.vehicle.getTypeID(vid))
                    lane_pos = float(traci_module.vehicle.getLanePosition(vid))
                    length = float(traci_module.vehicle.getLength(vid))
                    width = float(traci_module.vehicle.getWidth(vid))
            except Exception as e:
                log.debug("live frame: vehicle %s skipped: %s", vid, e)
                continue

            vehicles.append(
                {
                    "id": vid,
                    "type": vtype,
                    "x": round(x, 2),
                    "y": round(y, 2),
                    "speed": round(speed, 3),
                    "angle": round(angle, 2),
                    "lane": lane,
                    "road": road,
                    "lanePos": round(lane_pos, 2),
                    "length": round(length, 2),
                    "width": round(width, 2),
                }
            )
            speed_sum += max(0.0, speed)
            if speed < _WAITING_SPEED_MPS:
                waiting += 1

        n = len(vehicles)
        avg_mps = (speed_sum / n) if n else 0.0
        avg_kmh = avg_mps * 3.6

        traffic_lights: List[Dict[str, Any]] = []
        for node_id, tls_id in self.node_to_tls.items():
            if self.tls_ids and tls_id not in self.tls_ids:
                continue
            entry: Dict[str, Any] = {
                "id": tls_id,
                "intersectionId": node_id,
            }
            try:
                entry["state"] = str(
                    traci_module.trafficlight.getRedYellowGreenState(tls_id)
                )
            except Exception:
                entry["state"] = ""
            sig = self.signals.get(node_id)
            if sig is not None:
                try:
                    entry["phase"] = sig.current_phase_name(traci_module)
                    entry["colors"] = sig.colors(traci_module)
                except Exception:
                    entry["phase"] = None
                    entry["colors"] = {}
            else:
                entry["phase"] = None
                entry["colors"] = {}
            traffic_lights.append(entry)

        return {
            "simulationTime": round(float(sim_t), 3),
            "vehicles": vehicles,
            "trafficLights": traffic_lights,
            "statistics": {
                "vehicleCount": n,
                "averageSpeed": round(avg_kmh, 2),
                "waitingVehicles": waiting,
            },
        }


class LiveStreamHub:
    """Thread-safe latest-frame store for WebSocket fan-out (never blocks TraCI)."""

    def __init__(self, *, enabled: bool = True):
        self.enabled = bool(enabled)
        self._lock = threading.Lock()
        self._latest: Optional[Dict[str, Any]] = None
        self._seq = 0
        self._clients = 0
        self._published = 0
        self._last_publish_wall = 0.0
        self._network: Optional[Dict[str, Any]] = None

    def set_network_geometry(self, geometry: Dict[str, Any]) -> None:
        with self._lock:
            self._network = geometry

    def get_network_geometry(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._network

    def publish(self, frame: Dict[str, Any]) -> None:
        if not self.enabled or frame is None:
            return
        with self._lock:
            self._seq += 1
            out = dict(frame)
            out["seq"] = self._seq
            self._latest = out
            self._published += 1
            self._last_publish_wall = time.monotonic()

    def get_latest(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._latest

    def client_connected(self) -> None:
        with self._lock:
            self._clients += 1
            log.info("live WS client connected clients=%s", self._clients)

    def client_disconnected(self) -> None:
        with self._lock:
            self._clients = max(0, self._clients - 1)
            log.info("live WS client disconnected clients=%s", self._clients)

    def health(self) -> Dict[str, Any]:
        with self._lock:
            age_ms = None
            if self._last_publish_wall > 0:
                age_ms = int((time.monotonic() - self._last_publish_wall) * 1000)
            vc = 0
            if self._latest:
                stats = self._latest.get("statistics") or {}
                vc = int(stats.get("vehicleCount") or 0)
            return {
                "status": "ok" if self.enabled else "disabled",
                "enabled": self.enabled,
                "clients": self._clients,
                "seq": self._seq,
                "published": self._published,
                "lastFrameAgeMs": age_ms,
                "vehicleCount": vc,
            }


# Process-wide hub (Control API + SumoBackend share this instance).
_HUB: Optional[LiveStreamHub] = None
_COLLECTOR: Optional[VehicleStreamCollector] = None
_HUB_LOCK = threading.Lock()


def get_live_stream_hub() -> LiveStreamHub:
    global _HUB
    with _HUB_LOCK:
        if _HUB is None:
            import configuration.config as cfg

            _HUB = LiveStreamHub(enabled=bool(getattr(cfg, "LIVE_STREAM_ENABLED", True)))
        return _HUB


def get_vehicle_stream_collector(
    *,
    signals: Optional[Dict[str, Any]] = None,
    publish_nodes: Optional[List[str]] = None,
) -> VehicleStreamCollector:
    global _COLLECTOR
    with _HUB_LOCK:
        if _COLLECTOR is None:
            import configuration.config as cfg

            nodes = list(publish_nodes or getattr(cfg, "PUBLISH_NODES", ["A"]))
            node_to_tls = {
                n: cfg.NODE_TO_TLS[n] for n in nodes if n in cfg.NODE_TO_TLS
            }
            _COLLECTOR = VehicleStreamCollector(
                hz=float(getattr(cfg, "LIVE_STREAM_HZ", 10.0)),
                tls_ids=list(node_to_tls.values()),
                node_to_tls=node_to_tls,
                signals=signals or {},
            )
        elif signals is not None:
            _COLLECTOR.signals = signals
        return _COLLECTOR


def init_live_stream(
    *,
    net_xml: Path,
    signals: Dict[str, Any],
    publish_nodes: List[str],
) -> LiveStreamHub:
    """Called once when SUMO starts — loads geometry and prepares collector."""
    import configuration.config as cfg

    hub = get_live_stream_hub()
    hub.enabled = bool(getattr(cfg, "LIVE_STREAM_ENABLED", True))
    try:
        net_path = Path(net_xml)
        geom = load_network_geometry(net_path)
        rou_path = net_path.with_name("intersection.rou.xml")
        vtypes = load_vtypes(rou_path)
        if vtypes:
            geom["vTypes"] = vtypes
        hub.set_network_geometry(geom)
        log.info(
            "live stream geometry edges=%s junctions=%s vTypes=%s geo=%s",
            len(geom.get("edges") or []),
            len(geom.get("junctions") or []),
            len(vtypes),
            geom.get("bounds", {}).get("geoReferenced"),
        )
    except Exception as e:
        log.warning("live stream geometry load failed: %s", e)

    global _COLLECTOR
    with _HUB_LOCK:
        node_to_tls = {
            n: cfg.NODE_TO_TLS[n] for n in publish_nodes if n in cfg.NODE_TO_TLS
        }
        _COLLECTOR = VehicleStreamCollector(
            hz=float(getattr(cfg, "LIVE_STREAM_HZ", 10.0)),
            tls_ids=list(node_to_tls.values()),
            node_to_tls=node_to_tls,
            signals=signals,
        )
    return hub
