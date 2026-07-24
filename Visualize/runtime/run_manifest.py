"""
run_manifest.py — Atomic run_manifest.json writer for simulation replayability.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import configuration.config as cfg

log = logging.getLogger(__name__)

VISUALIZE_DIR = cfg.PROJECT_ROOT


def _sha256_file(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> Optional[str]:
    try:
        import subprocess

        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(VISUALIZE_DIR.parent),
            capture_output=True,
            text=True,
            timeout=5,
        )
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return os.getenv("GIT_COMMIT")


def _sumo_version() -> Optional[str]:
    try:
        import subprocess

        r = subprocess.run(["sumo", "--version"], capture_output=True, text=True, timeout=10)
        line = (r.stdout or r.stderr or "").splitlines()
        return line[0].strip() if line else None
    except Exception:
        return None


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".manifest_", suffix=".json", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp_path), str(path))
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise


def build_and_write_manifest(
    *,
    registry,
    artifacts_root: Path,
    simulation_run_id: Optional[str] = None,
    scenario: str = "normal",
    sumo_seed: Optional[int] = None,
    python_random_seed: Optional[int] = None,
    numpy_random_seed: Optional[int] = None,
    route_generation_seed: Optional[int] = None,
    experiment_description: str = "",
    allow_missing_manifest: bool = False,
    network_hash_mismatch_mode: str = "compat",
    demand_seed: Optional[int] = None,
    control_mode: Optional[str] = None,
    topology_hash: Optional[str] = None,
) -> Dict[str, Any]:
    run_id = simulation_run_id or str(uuid.uuid4())
    net = registry.export_effective_config().get("network") or {}
    det = registry.detector_meta()
    assets = {
        "net_file": "Visualize/intersection.net.xml",
        "rou_file": "Visualize/intersection.rou.xml",
        "sumocfg_file": "Visualize/intersection.sumocfg",
    }
    hashes = {}
    for key, rel in assets.items():
        candidate = VISUALIZE_DIR / "Visualize" / Path(rel).name
        hashes[key] = {
            "path": str(Path(rel).as_posix()),
            "sha256": _sha256_file(candidate),
        }

    actual_network_hash = hashes.get("net_file", {}).get("sha256")
    expected = det.get("expected_network_hash")
    mismatch = None
    if expected and actual_network_hash and expected.lower() != actual_network_hash.lower():
        mismatch = {
            "expected_network_hash": expected,
            "actual_network_hash": actual_network_hash,
        }
        msg = f"Detector expected_network_hash mismatch: {mismatch}"
        if network_hash_mismatch_mode == "strict":
            raise RuntimeError(msg)
        log.warning(msg)

    manifest: Dict[str, Any] = {
        "run_id": run_id,
        "simulation_run_id": run_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scenario": scenario,
        "seed": sumo_seed,
        "sumo_seed": sumo_seed,
        "python_random_seed": python_random_seed,
        "numpy_random_seed": numpy_random_seed,
        "route_generation_seed": route_generation_seed,
        "network_hash": actual_network_hash,
        "route_hash": hashes.get("rou_file", {}).get("sha256"),
        "sumocfg_hash": hashes.get("sumocfg_file", {}).get("sha256"),
        "config_hash": registry.config_hash,
        "profile_id": registry.profile_id,
        "profile_version": registry.profile_version,
        "parameter_profile": registry.profile_id,
        "sumo_version": _sumo_version(),
        "traci_version": None,
        "git_commit": _git_commit(),
        "schema_version": registry.schema_version,
        "motorcycle_pcu": float(registry.get_pcu("motorcycle")),
        "K_JAM": float(registry.threshold("k_jam_pcu_per_km")),
        "threshold_profile": "thresholds",
        "detector_profile_id": det.get("profile_id"),
        "detector_geometry_version": det.get("geometry_version"),
        "network_id": det.get("network_id") or net.get("network_id"),
        "network_version": det.get("network_version") or net.get("network_version"),
        "actual_network_hash": actual_network_hash,
        "network_hash_mismatch": mismatch,
        "assets": hashes,
        "scenario_profile": (registry.export_effective_config().get("scenarios") or {}).get(
            "profile_id"
        ),
        "experiment_description": experiment_description or "",
        "demand_seed": demand_seed,
        "control_mode": control_mode or "FIXED",
        "topology_hash": topology_hash,
        "demand_profile_id": scenario,
    }
    try:
        import traci  # type: ignore

        manifest["traci_version"] = getattr(traci, "__version__", None)
    except Exception:
        pass

    out_dir = artifacts_root / "runs" / run_id
    out_path = out_dir / "run_manifest.json"
    try:
        atomic_write_json(out_path, manifest)
        log.info("Wrote run manifest %s", out_path)
    except Exception as e:
        if allow_missing_manifest:
            log.error("Failed to write run manifest (compat): %s", e)
        else:
            raise RuntimeError(f"Failed to write run_manifest.json: {e}") from e
    return manifest
