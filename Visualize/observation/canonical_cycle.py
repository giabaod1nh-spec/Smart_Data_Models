"""Immutable post-step capture and non-publishing V1 observation boundary."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from contracts.traffic_observation import build_observation, validate_observation
from contracts.topology.v1_digest import topology_digest


def _freeze(value: Any) -> bytes:
    # Bytes own the complete nested value; no mutable TraCI/context references escape.
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")


@dataclass(frozen=True)
class RunTopologyContext:
    simulation_run_id: str
    catalog_json: bytes
    topology_digest: str
    archive_path: Path
    archive_ready: bool
    error: str | None = None

    @property
    def catalog(self) -> dict:
        return json.loads(self.catalog_json)


def freeze_run_catalog(run_id: str, catalog: Mapping[str, Any], archive_path: Path) -> RunTopologyContext:
    frozen = _freeze(catalog)
    digest = topology_digest(json.loads(frozen))
    try:
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        # V2 already copied this run package file. Verify that exact content
        # without rewriting its bytes or changing legacy archive behavior.
        if not archive_path.exists():
            archive_path.write_bytes(frozen)
        archived = archive_path.read_bytes()
        archived_catalog = json.loads(archived)
        if archived_catalog != json.loads(frozen) or topology_digest(archived_catalog) != digest:
            raise ValueError("archived catalog differs from frozen runtime catalog")
        return RunTopologyContext(run_id, frozen, digest, archive_path, True)
    except (OSError, ValueError, TypeError) as exc:
        return RunTopologyContext(run_id, frozen, digest, archive_path, False, str(exc))


@dataclass(frozen=True)
class CanonicalIntersection:
    intersection_id: str
    source_json: bytes
    quality_json: bytes

    @property
    def source(self) -> dict:
        return json.loads(self.source_json)

    @property
    def quality(self) -> dict:
        return json.loads(self.quality_json)


@dataclass(frozen=True)
class CanonicalCycleSnapshot:
    simulation_run_id: str
    simulation_time_seconds: float
    simulation_step: int
    cycle_sequence: int
    captured_at: str
    topology_digest: str
    intersections: tuple[CanonicalIntersection, ...]

    def to_dict(self) -> dict:
        return {
            "simulationRunId": self.simulation_run_id,
            "simulationTimeSeconds": self.simulation_time_seconds,
            "simulationStep": self.simulation_step,
            "cycleSequence": self.cycle_sequence,
            "capturedAt": self.captured_at,
            "topologyDigest": self.topology_digest,
            "intersections": [
                {"intersectionId": item.intersection_id, "source": item.source, "quality": item.quality}
                for item in self.intersections
            ],
        }


@dataclass(frozen=True)
class V1CycleResult:
    cycle_sequence: int
    snapshot: CanonicalCycleSnapshot | None = None
    observations: tuple[dict, ...] = ()
    failure_code: str | None = None
    failure_reason: str | None = None

    @property
    def success(self) -> bool:
        return self.snapshot is not None and self.failure_code is None


class V1CycleError(ValueError):
    def __init__(self, code: str, reason: str):
        self.code = code
        super().__init__(reason)


def capture_cycle(
    *, context: RunTopologyContext | None, participating_nodes: tuple[str, ...],
    sources: Mapping[str, dict], simulation_time_seconds: float,
    simulation_step: int, cycle_sequence: int,
) -> CanonicalCycleSnapshot:
    if context is None or not context.archive_ready:
        raise V1CycleError("CATALOG_NOT_READY", context.error if context else "run catalog absent")
    if not context.simulation_run_id or simulation_step < 1 or cycle_sequence < 1:
        raise V1CycleError("MISSING_METADATA", "run ID or completed step/sequence absent")
    catalog = context.catalog
    if len(set(participating_nodes)) != len(participating_nodes) or not participating_nodes:
        raise V1CycleError("INVALID_PARTICIPANTS", "participating nodes must be unique and nonempty")
    captured = []
    for node in participating_nodes:
        tls = catalog["node_to_tls"].get(node)
        group = catalog["node_to_tls_groups"].get(node)
        if not tls or not group:
            raise V1CycleError("MISSING_TOPOLOGY", f"{node}: primary TLS or group missing")
        if len(group) != 1 or group[0] != tls:
            raise V1CycleError("UNSUPPORTED_TOPOLOGY", f"{node}: V1 requires one physical TLS")
        approaches = set(catalog["approach_edges"].get(tls, {}))
        if not approaches or not approaches <= {"North", "South", "East", "West"}:
            raise V1CycleError("UNSUPPORTED_TOPOLOGY", f"{node}: approaches cannot be represented")
        if node not in sources:
            raise V1CycleError("MISSING_INTERSECTION", f"{node}: physical capture absent")
        raw = sources[node]
        if raw.get("node_id") != node or raw.get("simulation_run_id") != context.simulation_run_id:
            raise V1CycleError("MISSING_IDENTITY", f"{node}: capture identity differs from run")
        if not isinstance(raw.get("phase"), str) or not raw["phase"]:
            raise V1CycleError("MISSING_TLS_STATE", f"{node}: TLS phase unavailable")
        directions = raw.get("directions")
        colors = raw.get("colors")
        if not isinstance(directions, dict) or not isinstance(colors, dict) or set(directions) != approaches or set(colors) != approaches:
            raise V1CycleError("MISSING_TOPOLOGY", f"{node}: captured directions differ from catalog")
        if any(color not in ("red", "yellow", "green") for color in colors.values()):
            raise V1CycleError("MISSING_TLS_STATE", f"{node}: invalid TLS color")
        if not raw.get("scenario") or not raw.get("control_mode") or not isinstance(raw.get("incidents"), list):
            raise V1CycleError("MISSING_METADATA", f"{node}: required scenario/control/incident state absent")
        quality = raw.get("_measurement_quality")
        if not isinstance(quality, dict) or set(quality) - {"_top_level"} != approaches:
            raise V1CycleError("MISSING_QUALITY", f"{node}: measurement quality absent")
        source = dict(raw)
        source.pop("_measurement_quality", None)
        source["simulation_time_sec"] = simulation_time_seconds
        captured.append(CanonicalIntersection(node, _freeze(source), _freeze(quality)))
    return CanonicalCycleSnapshot(
        context.simulation_run_id, simulation_time_seconds, simulation_step,
        cycle_sequence, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        context.topology_digest, tuple(captured),
    )


def build_v1_cycle(snapshot: CanonicalCycleSnapshot) -> tuple[dict, ...]:
    """Pure transformation; Phase 1 owns wire selection, identity and validation."""
    events = []
    for intersection in snapshot.intersections:
        source = intersection.source
        quality = intersection.quality
        top_quality = quality.pop("_top_level", {})
        if top_quality.get("spillback_unavailable"):
            for name in (
                "downstream_edge_full", "spillback_pressure", "spillback_detected",
                "intersection_box_blocked", "vehicles_blocked_at_entry",
            ):
                source.pop(name, None)
        elif top_quality.get("blocked_entry_unavailable"):
            source.pop("vehicles_blocked_at_entry", None)
        if top_quality.get("yellow_commitment_unavailable"):
            source.pop("yellow_commitment_count", None)
        for direction, status in quality.items():
            metrics = source["directions"][direction]
            unavailable = status.get("unavailable_core_metrics", [])
            for metric in unavailable:
                metrics[metric] = None
            if unavailable:
                metrics["metric_availability"] = {metric: "UNAVAILABLE" for metric in unavailable}
                # Optional derived fields may depend on incomplete aggregates.
                if any(metric in unavailable for metric in ("vehicle_count", "pcu_equivalent", "waiting_vehicle_count")):
                    for optional in ("queue_length_vehicles", "density", "estimated_density_veh_per_km", "waiting_reason_counts", "dominant_waiting_reason", "theoretical_speed_kmh", "moto_front_pct", "vehicle_class_composition", "storage_utilization_pcu"):
                        metrics.pop(optional, None)
                if "queue_length_m" in unavailable:
                    metrics.pop("queue_by_movement", None)
                    metrics.pop("queue_length_vehicles", None)
                if any(metric in unavailable for metric in ("left_count", "straight_count", "right_count")):
                    metrics.pop("queue_by_movement", None)
            if status.get("arrival_rate_unavailable"):
                metrics.pop("arrival_rate_pcu_per_sec", None)
            if status.get("moto_front_unavailable"):
                metrics.pop("moto_front_pct", None)
            if status.get("full_link_incomplete"):
                for optional in (
                    "approach_length_m", "full_link_halting_count", "full_link_jam_length_m",
                    "storage_utilization_lane", "storage_utilization_pcu",
                    "nominal_storage_pcu_geometric",
                ):
                    metrics.pop(optional, None)
        event = build_observation(
            source, cycle_sequence=snapshot.cycle_sequence,
            simulation_step=snapshot.simulation_step,
            captured_at=snapshot.captured_at, topology_digest=snapshot.topology_digest,
        )
        validate_observation(event)
        events.append(event)
    return tuple(events)


def generate_v1_cycle(**kwargs) -> V1CycleResult:
    sequence = kwargs["cycle_sequence"]
    try:
        snapshot = capture_cycle(**kwargs)
        observations = build_v1_cycle(snapshot)
        return V1CycleResult(sequence, snapshot, observations)
    except V1CycleError as exc:
        return V1CycleResult(sequence, failure_code=exc.code, failure_reason=str(exc))
    except Exception as exc:
        return V1CycleResult(sequence, failure_code="INVALID_OBSERVATION", failure_reason=str(exc))
