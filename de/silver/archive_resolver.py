"""Resolve immutable V1 run context from its archived producer files."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from contracts.topology.v1_digest import topology_digest


def manifest_identity(manifest: Mapping[str, Any]) -> str:
    """Match producer active_run_registry.manifest_identity without runtime imports."""
    for key in ("simulationRunId", "topologyDigest", "participatingIntersectionIds", "runGeneration"):
        if key not in manifest:
            raise ValueError(f"run manifest lacks {key}")
    immutable = {key: value for key, value in manifest.items()
                 if key not in {"runStatus", "failedCycleCount"}}
    encoded = json.dumps(immutable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class ArchiveUnavailable(RuntimeError):
    """An archive or registry cannot currently be read."""


class ArchiveIntegrityError(ValueError):
    """Archived immutable identity or topology is inconsistent."""


@dataclass(frozen=True)
class VerifiedRunContext:
    run_id: str
    generation: int
    topology_digest: str
    manifest_identity: str
    participants: tuple[str, ...]
    directions: Mapping[str, tuple[str, ...]]
    manifest: Mapping[str, Any]
    catalog: Mapping[str, Any]
    manifest_file_hash: str
    catalog_file_hash: str
    registry_status: str | None
    registry_updated_at: str | None

    def validate_event(self, event: Mapping[str, Any]) -> None:
        if (event.get("simulationRunId") != self.run_id or
                event.get("topologyDigest") != self.topology_digest):
            raise ArchiveIntegrityError("event run identity differs from archive")
        node = event.get("intersectionId")
        if node not in self.directions:
            raise ArchiveIntegrityError("event intersection is not a run participant")
        actual = set(event.get("payload", {}).get("directions", {}))
        expected = set(self.directions[node])
        if actual != expected:
            raise ArchiveIntegrityError(
                f"event directions differ from archived approaches for {node}: "
                f"missing={sorted(expected - actual)} unexpected={sorted(actual - expected)}")
        if self.registry_status is None:
            raise ArchiveIntegrityError("RUN_NOT_PUBLISHED")


class ArchiveResolver:
    def __init__(self, artifacts_root: Path):
        self.root = Path(artifacts_root)

    @staticmethod
    def _load(path: Path) -> tuple[dict, str]:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ArchiveUnavailable(str(exc)) from exc
        try:
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("JSON root must be an object")
        except (UnicodeError, ValueError) as exc:
            raise ArchiveIntegrityError(f"invalid archive {path}") from exc
        return value, hashlib.sha256(raw).hexdigest()

    def registry_row(self, run_id: str) -> dict | None:
        path = self.root / "runs" / "active_run.sqlite3"
        try:
            db = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro", uri=True)
            db.row_factory = sqlite3.Row
            try:
                db.execute("PRAGMA query_only=ON")
                row = db.execute("SELECT generation,run_id,manifest_identity,status,"
                                 "activated_at,updated_at FROM runs WHERE run_id=?", (run_id,)).fetchone()
                return dict(row) if row else None
            finally:
                db.close()
        except (OSError, sqlite3.Error) as exc:
            raise ArchiveUnavailable(str(exc)) from exc

    def verify_registry(self) -> None:
        """Verify a read-only historical lookup is possible at startup."""
        path = self.root / "runs" / "active_run.sqlite3"
        try:
            db = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro", uri=True)
            try:
                db.execute("PRAGMA query_only=ON")
                db.execute("SELECT generation,run_id,manifest_identity,status,"
                           "activated_at,updated_at FROM runs LIMIT 1").fetchone()
            finally:
                db.close()
        except (OSError, sqlite3.Error) as exc:
            raise ArchiveUnavailable(str(exc)) from exc

    def resolve(self, simulation_run_id: str, expected_digest: str,
                expected_generation: int | None = None) -> VerifiedRunContext:
        if not simulation_run_id or simulation_run_id in {".", ".."} or any(
                char in simulation_run_id for char in ("/", "\\", "\0")):
            raise ArchiveIntegrityError("invalid run ID")
        folder = self.root / "runs" / simulation_run_id
        manifest, manifest_hash = self._load(folder / "run_manifest.json")
        catalog, catalog_hash = self._load(folder / "network_topology_catalog.json")
        try:
            identity = manifest_identity(manifest)
            generation = manifest["runGeneration"]
            actual_digest = topology_digest(catalog)
            participants = tuple(manifest["participatingIntersectionIds"])
            if (not isinstance(generation, int) or isinstance(generation, bool) or generation < 1 or
                    not participants or len(set(participants)) != len(participants)):
                raise ValueError("invalid generation or participants")
            if (manifest["simulationRunId"] != simulation_run_id or
                    manifest["topologyDigest"] != expected_digest or actual_digest != expected_digest or
                    (expected_generation is not None and generation != expected_generation)):
                raise ValueError("run identity or digest mismatch")
            node_to_tls = catalog["node_to_tls"]
            approaches = catalog["approach_edges"]
            if (not isinstance(node_to_tls, dict) or not node_to_tls or
                    not isinstance(approaches, dict)):
                raise ValueError("catalog node and approach maps must be non-empty objects")
            topology_directions = {}
            for node in sorted(node_to_tls):
                tls_id = node_to_tls[node]
                values = approaches[tls_id]
                if (not isinstance(node, str) or not node or not isinstance(tls_id, str) or
                        not tls_id or not isinstance(values, dict) or not values or
                        any(not isinstance(value, str) or not value for value in values) or
                        len(values) != len(set(values))):
                    raise ValueError(f"invalid archived approaches for catalog node {node!r}")
                topology_directions[node] = tuple(values)
            if any(node not in topology_directions for node in participants):
                raise ValueError("run participant is absent from the full catalog")
            directions = {node: topology_directions[node] for node in participants}
        except (KeyError, TypeError, ValueError) as exc:
            raise ArchiveIntegrityError(str(exc)) from exc
        registry = self.registry_row(simulation_run_id)
        if registry is None:
            if manifest.get("runStatus") != "READY":
                raise ArchiveIntegrityError("published manifest has no registry row")
        elif (registry["generation"] != generation or registry["manifest_identity"] != identity or
              registry["status"] not in {"RUNNING", "COMPLETE", "INCOMPLETE", "STOPPED"}):
            raise ArchiveIntegrityError("registry identity or status mismatch")
        return VerifiedRunContext(simulation_run_id, generation, actual_digest, identity,
                                  participants, directions, manifest, catalog, manifest_hash,
                                  catalog_hash, registry["status"] if registry else None,
                                  registry["updated_at"] if registry else None)
