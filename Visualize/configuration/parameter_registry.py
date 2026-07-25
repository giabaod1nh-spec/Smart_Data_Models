"""
parameter_registry.py — Immutable ParameterRegistry (true SSOT).

YAML is storage only. Lifecycle: load → validate → freeze → runtime use.
No hot-reload / public mutation after freeze.
"""
from __future__ import annotations

import copy
import hashlib
import json
import logging
import math
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Protocol

log = logging.getLogger(__name__)

# Visualize project root; YAML lives beside this package
_CONFIGURATION_DIR = Path(__file__).resolve().parent
VISUALIZE_DIR = _CONFIGURATION_DIR.parent
DEFAULT_YAML = _CONFIGURATION_DIR / "model_parameters.yaml"

LOCKED_RUNTIME_MOTORCYCLE_PCE = 0.24
FORBIDDEN_RUNTIME_MOTORCYCLE_PCE = 0.30


class ParameterBackend(Protocol):
    def load_raw(self) -> Dict[str, Any]:
        ...


class YamlParameterBackend:
    """Only class allowed to read model_parameters.yaml."""

    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path else DEFAULT_YAML

    def load_raw(self) -> Dict[str, Any]:
        import yaml

        if not self.path.is_file():
            raise FileNotFoundError(f"Model parameters file not found: {self.path}")
        with self.path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid model parameters root in {self.path}")
        return data


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _deep_freeze(obj: Any) -> Any:
    if isinstance(obj, dict):
        return MappingProxyType({k: _deep_freeze(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return tuple(_deep_freeze(v) for v in obj)
    return obj


def _as_plain(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {k: _as_plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_as_plain(v) for v in obj]
    return obj


class ParameterRegistry:
    """
    Immutable registry after validate()+freeze().

    Create a new instance to switch profiles — no in-place mutation.
    """

    def __init__(
        self,
        raw: Dict[str, Any],
        *,
        profile_id: Optional[str] = None,
        source_path: Optional[Path] = None,
    ):
        self._raw = copy.deepcopy(raw)
        self._source_path = source_path
        self._frozen = False
        self._validated = False
        self._profile_id = profile_id
        self._effective: Dict[str, Any] = {}
        self._pcu_factors: Dict[str, float] = {}
        self._param_index: Dict[str, Dict[str, Any]] = {}
        self._config_hash: str = ""
        self._warnings: List[str] = []

    # ── construction ───────────────────────────────────────────────
    @classmethod
    def load(
        cls,
        backend: Optional[ParameterBackend] = None,
        *,
        profile_id: Optional[str] = None,
        path: Path | str | None = None,
    ) -> "ParameterRegistry":
        be = backend or YamlParameterBackend(path)
        raw = be.load_raw()
        src = getattr(be, "path", None)
        reg = cls(raw, profile_id=profile_id, source_path=Path(src) if src else None)
        reg.validate()
        reg.freeze()
        return reg

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    @property
    def is_validated(self) -> bool:
        return self._validated

    @property
    def profile_id(self) -> str:
        self._require_ready()
        return str(self._effective["profile"]["id"])

    @property
    def profile_version(self) -> str:
        self._require_ready()
        return str(self._effective["profile"]["version"])

    @property
    def schema_version(self) -> str:
        self._require_ready()
        return str(self._effective.get("schema_version", ""))

    @property
    def config_hash(self) -> str:
        self._require_ready()
        return self._config_hash

    @property
    def warnings(self) -> tuple:
        return tuple(self._warnings)

    def _require_ready(self) -> None:
        if not self._validated or not self._frozen:
            raise RuntimeError("ParameterRegistry is not validated/frozen; refuse runtime use")

    def _require_mutable(self) -> None:
        if self._frozen:
            raise RuntimeError("ParameterRegistry is frozen; create a new instance to change profile")

    # ── validate / freeze ──────────────────────────────────────────
    def validate(self) -> None:
        self._require_mutable()
        self._warnings.clear()
        profiles = self._raw.get("profiles") or {}
        if not profiles:
            raise ValueError("profiles section missing")

        defaults = [pid for pid, p in profiles.items() if p.get("runtime_default")]
        if len(defaults) != 1:
            raise ValueError(f"Exactly one runtime_default profile required; found {defaults}")

        selected = self._profile_id or defaults[0]
        if selected not in profiles:
            raise ValueError(f"Unknown profile_id '{selected}'")
        profile = copy.deepcopy(profiles[selected])
        kind = profile.get("kind", "runtime")
        if profile.get("runtime_default") and kind not in ("runtime",):
            self._warnings.append(f"runtime_default profile kind={kind}")

        # Build effective runtime config (excludes pcu_reference / non-active profiles)
        pcu_runtime = copy.deepcopy(self._raw.get("pcu_runtime") or {})
        if "motorcycle" not in pcu_runtime:
            raise ValueError("pcu_runtime.motorcycle is required")
        moto = pcu_runtime["motorcycle"]
        moto_val = float(moto["value"])
        if kind == "runtime" or profile.get("runtime_default"):
            if not math.isclose(moto_val, LOCKED_RUNTIME_MOTORCYCLE_PCE, rel_tol=0, abs_tol=1e-9):
                raise ValueError(
                    f"Locked runtime motorcycle PCE must be {LOCKED_RUNTIME_MOTORCYCLE_PCE}, got {moto_val}"
                )
        for vtype, meta in pcu_runtime.items():
            val = float(meta["value"])
            if not math.isfinite(val) or val <= 0:
                raise ValueError(f"Invalid PCE for {vtype}: {val}")
            if vtype == "motorcycle" and math.isclose(val, FORBIDDEN_RUNTIME_MOTORCYCLE_PCE, abs_tol=1e-9):
                if profile.get("runtime_default") or kind == "runtime":
                    raise ValueError("Runtime PCU factors must not contain motorcycle=0.30")
            self._check_param_meta(f"pcu_runtime.{vtype}", meta)

        # If explicitly loading reference profile that wants TCVN 0.30, swap motorcycle from reference
        if kind in ("reference", "sensitivity") and selected.startswith("tcvn"):
            ref = (self._raw.get("pcu_reference") or {}).get("motorcycle_tcvn_13592_2022")
            if ref:
                pcu_runtime = copy.deepcopy(pcu_runtime)
                pcu_runtime["motorcycle"] = copy.deepcopy(ref)
                pcu_runtime["motorcycle"]["runtime_default"] = False

        vtype_mapping = copy.deepcopy(self._raw.get("vtype_mapping") or {})
        for vt in vtype_mapping:
            if vt not in pcu_runtime:
                raise ValueError(f"vType '{vt}' in mapping missing from pcu_runtime")

        thresholds = copy.deepcopy(self._raw.get("thresholds") or {})
        for key, meta in thresholds.items():
            self._check_param_meta(f"thresholds.{key}", meta)
            val = meta.get("value")
            if key.endswith("_ratio") or "occupancy_threshold" in key:
                f = float(val)
                if not 0.0 <= f <= 1.0:
                    raise ValueError(f"{key} must be in [0,1], got {f}")
            if key == "k_jam_pcu_per_km":
                if float(val) <= 0:
                    raise ValueError("K_JAM must be > 0")
            if key.startswith("traffic_load_bins"):
                self._validate_bins(key, val)

        detectors = copy.deepcopy(self._raw.get("detectors") or {})
        for geom_key in (
            "e1_offset_from_end_m",
            "e2_cover_length_m",
            "e2_out_pos_m",
            "e2_out_length_m",
        ):
            if geom_key not in detectors:
                raise ValueError(f"detectors.{geom_key} required")
            self._check_param_meta(f"detectors.{geom_key}", detectors[geom_key])
            if float(detectors[geom_key]["value"]) <= 0:
                raise ValueError(f"detectors.{geom_key} must be > 0")

        sumo_cfg = copy.deepcopy(self._raw.get("sumo_config") or {})
        for key, meta in sumo_cfg.items():
            self._check_param_meta(f"sumo_config.{key}", meta)
            v = meta["value"]
            if isinstance(v, (int, float)) and float(v) < 0:
                raise ValueError(f"sumo_config.{key} must not be negative")
        if "green_duration_fallback_sec" in sumo_cfg:
            if float(sumo_cfg["green_duration_fallback_sec"]["value"]) <= 0:
                raise ValueError("green_duration_fallback_sec must be > 0")
        if "yellow_duration_fallback_sec" in sumo_cfg:
            if float(sumo_cfg["yellow_duration_fallback_sec"]["value"]) < 0:
                raise ValueError("yellow_duration_fallback_sec must be >= 0")

        composition = copy.deepcopy(self._raw.get("composition") or {})
        scenarios = copy.deepcopy(self._raw.get("scenarios") or {})
        network = copy.deepcopy(self._raw.get("network") or {})
        metrics_provenance = copy.deepcopy(self._raw.get("metrics_provenance") or {})

        self._pcu_factors = {k: float(v["value"]) for k, v in pcu_runtime.items()}
        if math.isclose(self._pcu_factors.get("motorcycle", -1), FORBIDDEN_RUNTIME_MOTORCYCLE_PCE):
            if profile.get("runtime_default") or kind == "runtime":
                raise ValueError("Runtime motorcycle PCE must not be 0.30")

        self._effective = {
            "schema_version": self._raw.get("schema_version"),
            "model_schema_version": self._raw.get("model_schema_version"),
            "unknown_vtype_mode": self._raw.get("unknown_vtype_mode", "compat"),
            "unknown_vtype_fallback_pce": float(
                (self._raw.get("unknown_vtype_fallback_pce") or {}).get("value", 1.0)
            ),
            "profile": {
                "id": profile["id"],
                "version": profile["version"],
                "kind": kind,
                "runtime_default": bool(profile.get("runtime_default")),
                "source_reference_id": profile.get("source_reference_id"),
            },
            "pcu_runtime": pcu_runtime,
            "vtype_mapping": vtype_mapping,
            "taxonomy": copy.deepcopy(self._raw.get("taxonomy") or {}),
            "thresholds": thresholds,
            "detectors": {
                k: detectors[k]
                for k in detectors
                if k
                in (
                    "profile_id",
                    "geometry_version",
                    "network_id",
                    "network_version",
                    "expected_network_hash",
                    "e1_offset_from_end_m",
                    "e2_cover_length_m",
                    "e2_out_pos_m",
                    "e2_out_length_m",
                )
            },
            "sumo_config": sumo_cfg,
            "composition": {
                "profile_id": composition.get("profile_id"),
                "weights": composition.get("weights"),
                "emergency_share": composition.get("emergency_share"),
                "base_trunk_veh_per_hour": composition.get("base_trunk_veh_per_hour"),
                "diagonal_veh_per_hour": composition.get("diagonal_veh_per_hour"),
                "source_type": composition.get("source_type"),
            },
            "scenarios": {
                "profile_id": scenarios.get("profile_id"),
                "traffic_scale": scenarios.get("traffic_scale"),
                "speed_factor": scenarios.get("speed_factor"),
                "source_type": scenarios.get("source_type"),
            },
            "demand_baselines": copy.deepcopy(self._raw.get("demand_baselines") or {}),
            "boundary_sources": copy.deepcopy(self._raw.get("boundary_sources") or {}),
            "network_demand_profiles": copy.deepcopy(self._raw.get("network_demand_profiles") or {}),
            "insertion_policy": copy.deepcopy(self._raw.get("insertion_policy") or {}),
            "context_derivation": copy.deepcopy(self._raw.get("context_derivation") or {}),
            "causal_inference": copy.deepcopy(self._raw.get("causal_inference") or {}),
            "local_overlay_types": copy.deepcopy(self._raw.get("local_overlay_types") or {}),
            "demo_profile": copy.deepcopy(self._raw.get("demo_profile") or {}),
            "network": {
                "network_id": network.get("network_id"),
                "network_version": network.get("network_version"),
            },
            "metrics_provenance": metrics_provenance,
        }
        self._validate_hybrid_demand()
        # Build flat parameter index for catalog
        self._param_index = {}
        for vtype, meta in pcu_runtime.items():
            self._param_index[f"pcu.{vtype}"] = meta
        for key, meta in thresholds.items():
            self._param_index[f"threshold.{key}"] = meta
        for key in (
            "e1_offset_from_end_m",
            "e2_cover_length_m",
            "e2_out_pos_m",
            "e2_out_length_m",
        ):
            self._param_index[f"detector.{key}"] = detectors[key]
        for key, meta in sumo_cfg.items():
            self._param_index[f"sumo_config.{key}"] = meta

        self._config_hash = _sha256_text(_canonical_json(self.export_effective_config()))
        self._validated = True

    def _check_param_meta(self, path: str, meta: Mapping[str, Any]) -> None:
        if "value" not in meta:
            raise ValueError(f"{path}: missing value")
        for req in ("source_type", "calibrated", "description"):
            if req not in meta:
                raise ValueError(f"{path}: missing {req}")
        if "unit" not in meta:
            raise ValueError(f"{path}: missing unit (use 'unitless' if N/A)")
        st = meta["source_type"]
        if st in ("ACADEMIC", "STANDARD") and not meta.get("source_reference_id"):
            raise ValueError(f"{path}: {st} requires source_reference_id")
        if meta.get("calibrated") is True and st in (
            "HEURISTIC",
            "EXPERIMENTAL",
            "DESIGN_DECISION",
            "PLACEHOLDER",
        ):
            raise ValueError(f"{path}: calibrated=true invalid for source_type={st}")

    def _validate_bins(self, key: str, bins: Any) -> None:
        if not isinstance(bins, dict):
            raise ValueError(f"{key} must be a mapping")
        edges = []
        for label, pair in bins.items():
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise ValueError(f"{key}.{label} must be [lo, hi]")
            lo, hi = pair
            lo_f = float(lo)
            hi_f = float("inf") if hi is None else float(hi)
            if lo_f < 0 or hi_f <= lo_f:
                raise ValueError(f"{key}.{label} invalid range")
            edges.append((lo_f, hi_f))
        edges.sort()
        for i in range(1, len(edges)):
            if edges[i][0] < edges[i - 1][0]:
                raise ValueError(f"{key} bins not increasing")

    def freeze(self) -> None:
        if not self._validated:
            raise RuntimeError("Cannot freeze before validate()")
        self._effective = _as_plain(self._effective)  # ensure plain then freeze copies on read
        self._frozen = True

    # ── accessors ──────────────────────────────────────────────────
    def export_effective_config(self) -> Dict[str, Any]:
        """Plain dict used for config_hash (no reference-only profiles)."""
        if not self._validated and not self._effective:
            raise RuntimeError("export_effective_config requires validate()")
        return copy.deepcopy(_as_plain(self._effective))

    def get_value(self, key: str) -> Any:
        self._require_ready()
        if key in self._param_index:
            return copy.deepcopy(self._param_index[key]["value"])
        # dotted paths into effective
        cur: Any = self._effective
        for part in key.split("."):
            if not isinstance(cur, Mapping) or part not in cur:
                raise KeyError(key)
            cur = cur[part]
        return copy.deepcopy(_as_plain(cur))

    def get_metadata(self, key: str) -> Dict[str, Any]:
        self._require_ready()
        if key in self._param_index:
            return copy.deepcopy(_as_plain(self._param_index[key]))
        raise KeyError(key)

    def get_pcu(self, vtype: str) -> float:
        self._require_ready()
        if vtype in self._pcu_factors:
            return float(self._pcu_factors[vtype])
        mode = self._effective.get("unknown_vtype_mode", "compat")
        if mode == "strict":
            raise KeyError(f"Unknown SUMO vType '{vtype}' (strict mode)")
        fb = float(self._effective.get("unknown_vtype_fallback_pce", 1.0))
        log.warning(
            "Unknown SUMO vType '%s' — using fallback PCE=%.2f (compat mode).",
            vtype,
            fb,
        )
        return fb

    def pcu_factors(self) -> Mapping[str, float]:
        self._require_ready()
        return MappingProxyType(dict(self._pcu_factors))

    def list_parameters(self) -> List[Dict[str, Any]]:
        self._require_ready()
        out = []
        for key, meta in sorted(self._param_index.items()):
            rec = {
                "key": key,
                "value": meta.get("value"),
                "unit": meta.get("unit"),
                "source_type": meta.get("source_type"),
                "source_reference_id": meta.get("source_reference_id"),
                "calibrated": meta.get("calibrated"),
                "description": meta.get("description"),
                "profile_id": self.profile_id,
                "profile_version": self.profile_version,
                "runtime_default": bool(self._effective["profile"].get("runtime_default")),
            }
            out.append(rec)
        return out

    def threshold(self, name: str) -> Any:
        self._require_ready()
        return copy.deepcopy(self._effective["thresholds"][name]["value"])

    def detector_meta(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(self._effective["detectors"])

    def _validate_hybrid_demand(self) -> None:
        sources = self._effective.get("boundary_sources") or {}
        profiles = self._effective.get("network_demand_profiles") or {}
        for sid, src in sources.items():
            if "source_edge" not in src or "baseline_veh_per_hour" not in src:
                raise ValueError(f"boundary_sources.{sid} needs source_edge and baseline_veh_per_hour")
            rd = src.get("route_distribution") or {}
            if rd:
                s = sum(float(v) for v in rd.values())
                if abs(s - 1.0) > 1e-6:
                    raise ValueError(f"boundary_sources.{sid} route_distribution must sum to 1, got {s}")
        for pid, prof in profiles.items():
            if pid in ("active_default",):
                continue
            if not isinstance(prof, dict) or "source_targets" not in prof:
                continue
            for sid, tgt in (prof.get("source_targets") or {}).items():
                if sid not in sources:
                    raise ValueError(f"profile {pid} unknown source {sid}")
                base = float(sources[sid]["baseline_veh_per_hour"])
                if float(tgt) < base:
                    raise ValueError(
                        f"profile {pid} source {sid} target {tgt} < baseline {base}"
                    )
        ctx = self._effective.get("context_derivation") or {}
        ar = (ctx.get("aggregate_rule") or {}).get("type", "worst_state")
        if ar not in ("worst_state", "weighted"):
            raise ValueError(f"aggregate_rule.type must be worst_state|weighted, got {ar}")

    def demand_profile(self, profile_id: Optional[str] = None) -> Dict[str, Any]:
        self._require_ready()
        profiles = self._effective.get("network_demand_profiles") or {}
        pid = profile_id or profiles.get("active_default") or "normal"
        if pid not in profiles or pid == "active_default":
            raise KeyError(f"Unknown demand profile '{pid}'")
        return copy.deepcopy(_as_plain(profiles[pid]))

    def boundary_sources(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(_as_plain(self._effective.get("boundary_sources") or {}))

    def insertion_policy(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(_as_plain(self._effective.get("insertion_policy") or {}))

    def context_derivation(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(_as_plain(self._effective.get("context_derivation") or {}))

    def causal_inference(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(_as_plain(self._effective.get("causal_inference") or {}))

    def local_overlay_types(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(_as_plain(self._effective.get("local_overlay_types") or {}))

    def metrics_provenance(self) -> Dict[str, Any]:
        self._require_ready()
        return copy.deepcopy(self._effective.get("metrics_provenance") or {})

    def traffic_load_bins_direction(self) -> Dict[str, tuple]:
        raw = self.threshold("traffic_load_bins_per_direction")
        return {
            k: (float(v[0]), float("inf") if v[1] is None else float(v[1]))
            for k, v in raw.items()
        }

    def traffic_load_bins_intersection(self) -> Dict[str, tuple]:
        raw = self.threshold("traffic_load_bins_intersection")
        return {
            k: (float(v[0]), float("inf") if v[1] is None else float(v[1]))
            for k, v in raw.items()
        }


# Process-wide default registry (frozen)
_REGISTRY: Optional[ParameterRegistry] = None


def get_registry(*, reload: bool = False, profile_id: Optional[str] = None) -> ParameterRegistry:
    global _REGISTRY
    if _REGISTRY is None or reload or (profile_id and profile_id != getattr(_REGISTRY, "_profile_id", None) and profile_id != _REGISTRY.profile_id):
        # New instance when profile_id differs
        if profile_id is None and _REGISTRY is not None and not reload:
            return _REGISTRY
        _REGISTRY = ParameterRegistry.load(profile_id=profile_id)
    return _REGISTRY


def reset_registry_for_tests() -> None:
    global _REGISTRY
    _REGISTRY = None
