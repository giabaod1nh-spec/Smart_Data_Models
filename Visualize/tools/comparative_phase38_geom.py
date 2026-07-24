"""Phase 3.8 comparative experiment: geometry factor only (phase3.8-geom).

Builds a temporary pre-geometry (120/100) network beside current post (500/300),
runs short TraCI probes for arrival-on-green + insertion pending/departDelay,
and writes artifacts/phase3.8/comparative_phase38_geom.json.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import configuration.config as cfg

ASSETS = ROOT / "Visualize"
OUT = ROOT / "artifacts" / "phase3.8"
SPEED = 13.9
GREEN_S = 42.0


def _sha(p: Path) -> str:
    import hashlib

    return hashlib.sha256(p.read_bytes()).hexdigest()


def idealized_aog(ff_s: float, green_s: float = GREEN_S) -> float:
    return max(0.0, (green_s - ff_s) / green_s)


def lane_length_sets(net_path: Path) -> dict:
    net = ET.parse(net_path)
    internal, external = [], []
    for edge in net.findall("edge"):
        eid = edge.get("id") or ""
        if edge.get("function") == "internal" or eid.startswith(":"):
            continue
        for lane in edge.findall("lane"):
            L = float(lane.get("length"))
            if len(eid) == 4 and eid[0] == "J" and eid[2] == "J":
                internal.append(L)
            else:
                external.append(L)
    return {
        "internal_m": sorted(set(round(x, 2) for x in internal)),
        "external_m": sorted(set(round(x, 2) for x in external)),
    }


def build_pre_workspace(work: Path) -> None:
    """Create 120 m / 100 m nod + net + TLS + detectors + copied rou/sumocfg."""
    work.mkdir(parents=True, exist_ok=True)
    nod = """<nodes>
    <node id="J1" x="0"   y="0"   type="traffic_light"/>
    <node id="J2" x="120" y="0"   type="traffic_light"/>
    <node id="J3" x="0"   y="120" type="traffic_light"/>
    <node id="J4" x="120" y="120" type="traffic_light"/>
    <node id="N1" x="0"   y="220"/>
    <node id="N2" x="120" y="220"/>
    <node id="S1" x="0"   y="-100"/>
    <node id="S2" x="120" y="-100"/>
    <node id="W1" x="-100" y="0"/>
    <node id="W2" x="-100" y="120"/>
    <node id="E1" x="220" y="0"/>
    <node id="E2" x="220" y="120"/>
</nodes>
"""
    (work / "intersection.nod.xml").write_text(nod, encoding="utf-8")
    shutil.copy(ASSETS / "intersection.edg.xml", work / "intersection.edg.xml")
    env = os.environ.copy()
    env["PATH"] = str(Path(os.environ["SUMO_HOME"]) / "bin") + os.pathsep + env.get("PATH", "")
    subprocess.check_call(
        [
            "netconvert",
            f"--node-files={work / 'intersection.nod.xml'}",
            f"--edge-files={work / 'intersection.edg.xml'}",
            f"--output-file={work / 'intersection.net.xml'}",
            "--no-turnarounds.except-deadend",
            "true",
        ],
        env=env,
    )
    subprocess.check_call(
        [
            sys.executable,
            str(ROOT / "tools" / "reapply_tls.py"),
            "--net",
            str(work / "intersection.net.xml"),
        ]
    )
    # detectors for pre lengths
    import re
    from observation.detector_manager import build_detectors_xml

    text = (work / "intersection.net.xml").read_text(encoding="utf-8")
    lengths = {
        m.group(1): float(m.group(2))
        for m in re.finditer(r'<lane id="([^"]+)"[^>]*length="([0-9.]+)"', text)
    }
    (work / "detectors.add.xml").write_text(build_detectors_xml(lengths), encoding="utf-8")
    for name in ("intersection.rou.xml", "intersection.sumocfg", "viewsettings.xml"):
        shutil.copy(ASSETS / name, work / name)


def probe_traci(sumocfg: Path, end_s: float = 180.0, seed: int = 42) -> dict:
    """Empirical AoG + insertion stats via TraCI."""
    cfg.ensure_sumo_tools_on_path()
    import traci

    sumo_bin = cfg.resolve_sumo_binary(False)
    cmd = [
        sumo_bin,
        "-c",
        str(sumocfg),
        "--begin",
        "0",
        "--end",
        str(end_s),
        "--seed",
        str(seed),
        "--step-length",
        "0.1",
        "--no-step-log",
        "true",
        "--time-to-teleport",
        str(cfg.TIME_TO_TELEPORT),
    ]
    traci.start(cmd)
    arrivals_green = 0
    arrivals_total = 0
    seen: set[str] = set()
    max_pending = 0
    try:
        while traci.simulation.getTime() < end_s:
            traci.simulationStep()
            try:
                pending_list = traci.simulation.getPendingVehicles()
                pending = len(pending_list) if pending_list is not None else 0
            except Exception:
                pending = 0
            max_pending = max(max_pending, pending)
            for vid in traci.vehicle.getIDList():
                if vid in seen:
                    continue
                try:
                    lane = traci.vehicle.getLaneID(vid)
                    if lane.startswith(":"):
                        continue
                    L = float(traci.lane.getLength(lane))
                    pos = float(traci.vehicle.getLanePosition(vid))
                    if L <= 0 or pos / L < 0.92:
                        continue
                    # first time near stop-line = arrival event
                    seen.add(vid)
                    arrivals_total += 1
                    edge = lane.rsplit("_", 1)[0]
                    # map edge to TLS via approach edges
                    tls_id = None
                    for tid, approaches in cfg.APPROACH_EDGES.items():
                        if edge in approaches.values():
                            tls_id = tid
                            break
                    if not tls_id:
                        continue
                    state = traci.trafficlight.getRedYellowGreenState(tls_id)
                    # crude: if any 'G'/'g' in state treat as some green present;
                    # refine by linkIndex would be better — use vehicle next TLS
                    links = traci.vehicle.getNextTLS(vid)
                    if links:
                        _, link_idx, dist, link_state = links[0]
                        if link_state in ("G", "g"):
                            arrivals_green += 1
                    elif "G" in state or "g" in state:
                        arrivals_green += 1
                except Exception:
                    continue
        teleports = int(traci.simulation.getParameter("", "device.teleport.total") or 0) if False else 0
        # SUMO does not expose teleport total reliably via empty param; count from ending
        ending = int(traci.simulation.getEndingTeleportNumber())
    finally:
        traci.close()
    aog = (arrivals_green / arrivals_total) if arrivals_total else None
    return {
        "end_s": end_s,
        "seed": seed,
        "arrivals_total": arrivals_total,
        "arrivals_on_green": arrivals_green,
        "arrival_on_green_ratio": round(aog, 4) if aog is not None else None,
        "max_pending": max_pending,
        "ending_teleports": ending,
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    post_lens = lane_length_sets(ASSETS / "intersection.net.xml")
    post_ff_int = post_lens["internal_m"][0] / SPEED
    post_ff_ext = post_lens["external_m"][0] / SPEED

    with tempfile.TemporaryDirectory(prefix="phase38_pre_") as tmp:
        pre_dir = Path(tmp) / "assets"
        build_pre_workspace(pre_dir)
        pre_lens = lane_length_sets(pre_dir / "intersection.net.xml")
        pre_ff_int = pre_lens["internal_m"][0] / SPEED
        pre_ff_ext = pre_lens["external_m"][0] / SPEED
        pre_probe = probe_traci(pre_dir / "intersection.sumocfg", end_s=180.0)
        # rewrite sumocfg in temp already points to local files by relative name

    post_probe = probe_traci(ASSETS / "intersection.sumocfg", end_s=180.0)

    report = {
        "baseline_tag": "phase3.8-geom",
        "pre_tag": "phase3.8-pre",
        "written_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "experimental_factor": "geometry_only",
        "label": "EXPERIMENTAL / UNCALIBRATED",
        "pre": {
            "design_center_m": 120,
            "design_external_m": 100,
            "lane_lengths": pre_lens,
            "ff_internal_s": round(pre_ff_int, 3),
            "ff_external_s": round(pre_ff_ext, 3),
            "idealized_aog_band": round(idealized_aog(pre_ff_int), 4),
            "probe": pre_probe,
            "nominal_storage_pce_geometric": round((pre_lens["internal_m"][0] / 7.5) * 3, 1),
        },
        "post": {
            "design_center_m": 500,
            "design_external_m": 300,
            "lane_lengths": post_lens,
            "ff_internal_s": round(post_ff_int, 3),
            "ff_external_s": round(post_ff_ext, 3),
            "idealized_aog_band": round(idealized_aog(post_ff_int), 4),
            "probe": post_probe,
            "net_sha256": _sha(ASSETS / "intersection.net.xml"),
            "nominal_storage_pce_geometric": round((post_lens["internal_m"][0] / 7.5) * 3, 1),
        },
        "delta": {
            "idealized_aog_pre_to_post": [
                round(idealized_aog(pre_ff_int), 4),
                round(idealized_aog(post_ff_int), 4),
            ],
            "empirical_aog_pre_to_post": [
                pre_probe.get("arrival_on_green_ratio"),
                post_probe.get("arrival_on_green_ratio"),
            ],
            "max_pending_pre_to_post": [pre_probe["max_pending"], post_probe["max_pending"]],
        },
        "gates": {
            "pending_no_unexpected_spike": post_probe["max_pending"] <= max(40, pre_probe["max_pending"] + 5),
            "no_ending_teleports_post": post_probe["ending_teleports"] == 0,
        },
    }
    out_path = OUT / "comparative_phase38_geom.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["delta"], indent=2))
    print(f"Wrote {out_path}")
    if not report["gates"]["pending_no_unexpected_spike"]:
        print("WARNING: pending spike vs pre")
        sys.exit(2)
    if not report["gates"]["no_ending_teleports_post"]:
        print("WARNING: teleports on post")
        sys.exit(3)


if __name__ == "__main__":
    main()
