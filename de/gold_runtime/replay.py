"""Manifest-driven replay/backfill in an isolated namespace (Plan §18, Appendix T).

Replay writes the same physical namespace-bearing Gold fact/comparison/signal/KPI
and processing-ledger tables with ``namespace='replay:<id>'``. It uses its own
SQLite runtime database and instance lock, verifies existing dimensions read-only
by hash, and never writes a ``live`` row or advances a live cursor.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from de.gold.contracts import GOLD_SCHEMA_VERSION
from de.gold_runtime.config import (
    DEFINITION_VERSION,
    GOLD_DATABASE,
    LIVE_NAMESPACE,
    GoldConfigError,
    GoldSettings,
    is_replay_namespace,
    replay_namespace,
)

MANIFEST_VERSION = "gold-replay-manifest-v1"


class ReplayGuardError(Exception):
    """A replay run attempted a live write, live cursor advance or shared state."""


class ReplayManifestError(ValueError):
    """Malformed or mismatched replay manifest."""


@dataclass(frozen=True)
class ReplayTableWindow:
    source_table: str
    window_start_sim_sec: float
    window_end_sim_sec: float
    cursor_json: str

    def to_dict(self) -> dict:
        return {
            "source_table": self.source_table,
            "window_start_sim_sec": float(self.window_start_sim_sec),
            "window_end_sim_sec": float(self.window_end_sim_sec),
            "cursor_json": self.cursor_json,
        }


@dataclass(frozen=True)
class ReplayManifest:
    replay_id: str
    source_database: str
    destination_namespace: str
    table_windows: tuple[ReplayTableWindow, ...]
    source_set_hash: str
    definition_version: str = DEFINITION_VERSION
    gold_schema_version: str = GOLD_SCHEMA_VERSION
    manifest_version: str = MANIFEST_VERSION
    dimension_hashes: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "manifest_version": self.manifest_version,
            "replay_id": self.replay_id,
            "source_database": self.source_database,
            "destination_namespace": self.destination_namespace,
            "table_windows": [window.to_dict() for window in self.table_windows],
            "source_set_hash": self.source_set_hash,
            "definition_version": self.definition_version,
            "gold_schema_version": self.gold_schema_version,
            "dimension_hashes": {
                str(key): str(value) for key, value in sorted(self.dimension_hashes.items())
            },
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )

    def manifest_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def validate(self) -> "ReplayManifest":
        if self.manifest_version != MANIFEST_VERSION:
            raise ReplayManifestError(f"unsupported manifest_version {self.manifest_version!r}")
        if self.source_database != GOLD_DATABASE:
            raise ReplayManifestError("replay must read the migration-005 database")
        if self.destination_namespace != replay_namespace(self.replay_id):
            raise ReplayManifestError(
                "destination_namespace must be replay:<replay_id>"
            )
        if not self.table_windows:
            raise ReplayManifestError("replay manifest requires at least one table window")
        for window in self.table_windows:
            if window.window_start_sim_sec >= window.window_end_sim_sec:
                raise ReplayManifestError(
                    f"{window.source_table}: start must be < end"
                )
        if self.definition_version != DEFINITION_VERSION:
            raise ReplayManifestError("definition_version must match the Gold 2 context")
        if self.gold_schema_version != GOLD_SCHEMA_VERSION:
            raise ReplayManifestError("gold_schema_version must match migration 005")
        return self

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ReplayManifest":
        return cls(
            replay_id=str(payload["replay_id"]),
            source_database=str(payload["source_database"]),
            destination_namespace=str(payload["destination_namespace"]),
            table_windows=tuple(
                ReplayTableWindow(
                    source_table=str(item["source_table"]),
                    window_start_sim_sec=float(item["window_start_sim_sec"]),
                    window_end_sim_sec=float(item["window_end_sim_sec"]),
                    cursor_json=str(item["cursor_json"]),
                )
                for item in payload["table_windows"]
            ),
            source_set_hash=str(payload["source_set_hash"]),
            definition_version=str(payload.get("definition_version", DEFINITION_VERSION)),
            gold_schema_version=str(payload.get("gold_schema_version", GOLD_SCHEMA_VERSION)),
            manifest_version=str(payload.get("manifest_version", MANIFEST_VERSION)),
            dimension_hashes=dict(payload.get("dimension_hashes", {})),
        )

    @classmethod
    def from_json(cls, payload: str) -> "ReplayManifest":
        return cls.from_dict(json.loads(payload))


@dataclass(frozen=True)
class ReplayReport:
    replay_id: str
    namespace: str
    manifest_hash: str
    windows_processed: int
    rows_written: int
    dimension_mismatches: tuple[tuple, ...] = ()
    quarantined: bool = False
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "replay_id": self.replay_id,
            "namespace": self.namespace,
            "manifest_hash": self.manifest_hash,
            "windows_processed": int(self.windows_processed),
            "rows_written": int(self.rows_written),
            "dimension_mismatches": [list(item) for item in self.dimension_mismatches],
            "quarantined": bool(self.quarantined),
            "started_at": None if self.started_at is None else self.started_at.isoformat(),
            "finished_at": None if self.finished_at is None else self.finished_at.isoformat(),
        }


def assert_replay_settings(settings: GoldSettings, manifest: ReplayManifest) -> None:
    """Namespace, credentials-scope and runtime-state isolation guards."""
    manifest.validate()
    if not settings.is_replay():
        raise ReplayGuardError("replay manifest supplied to a live-mode runtime")
    if settings.namespace != manifest.destination_namespace:
        raise ReplayGuardError(
            f"runtime namespace {settings.namespace!r} != manifest "
            f"{manifest.destination_namespace!r}"
        )
    if not is_replay_namespace(settings.namespace):
        raise ReplayGuardError(f"invalid replay namespace {settings.namespace!r}")


def assert_namespace_isolation(live: GoldSettings, replay: GoldSettings) -> None:
    """Live and replay may share physical tables, never logical/runtime state."""
    if live.namespace == replay.namespace:
        raise ReplayGuardError("live and replay namespaces must differ")
    if live.namespace != LIVE_NAMESPACE:
        raise ReplayGuardError("the live runtime must use namespace 'live'")
    if not is_replay_namespace(replay.namespace):
        raise ReplayGuardError("the replay runtime must use namespace 'replay:<id>'")
    if Path(live.checkpoint_path) == Path(replay.checkpoint_path):
        raise ReplayGuardError("live and replay must not share a SQLite runtime database")
    if Path(live.instance_lock_path) == Path(replay.instance_lock_path):
        raise ReplayGuardError("live and replay must not share an instance lock")


def assert_no_live_write(settings: GoldSettings, rows: Sequence[Any]) -> None:
    for row in rows:
        namespace = getattr(row, "namespace", None)
        if namespace is not None and namespace != settings.namespace:
            raise ReplayGuardError(
                f"replay attempted to write namespace {namespace!r}"
            )


def verify_dimensions(repository: Any, manifest: ReplayManifest) -> tuple[tuple, ...]:
    """Dimensions are read-only in replay; a hash mismatch quarantines the batch."""
    if not manifest.dimension_hashes:
        return ()
    expected = {
        tuple(json.loads(identity)): source_hash
        for identity, source_hash in manifest.dimension_hashes.items()
    }
    return repository.verify_dimension_hashes(expected)


def dimension_hash_key(identity: Sequence[str]) -> str:
    return json.dumps(list(identity), separators=(",", ":"), ensure_ascii=False)


def build_manifest(
    *,
    replay_id: str,
    table_windows: Sequence[ReplayTableWindow],
    source_set_hash: str,
    dimension_hashes: Optional[Mapping[str, str]] = None,
) -> ReplayManifest:
    return ReplayManifest(
        replay_id=replay_id,
        source_database=GOLD_DATABASE,
        destination_namespace=replay_namespace(replay_id),
        table_windows=tuple(table_windows),
        source_set_hash=source_set_hash,
        dimension_hashes=dict(dimension_hashes or {}),
    ).validate()


def replay_settings_from(live: GoldSettings, replay_id: str, **overrides: Any) -> GoldSettings:
    """Derive an isolated replay configuration from a validated live configuration."""
    namespace = replay_namespace(replay_id)
    data = live.model_dump()
    data.update(
        {
            "namespace": namespace,
            "destination_mode": "replay",
            "replay_id": replay_id,
            "replay_namespace": namespace,
            "checkpoint_path": str(
                Path(live.checkpoint_path).with_name(f"replay.{replay_id}.sqlite3")
            ),
            "instance_lock_path": str(
                Path(live.instance_lock_path).with_name(f"replay.{replay_id}.lock")
            ),
        }
    )
    data.update(overrides)
    settings = GoldSettings(**data)
    try:
        settings.validate_all()
    except GoldConfigError as exc:
        raise ReplayGuardError(str(exc)) from exc
    assert_namespace_isolation(live, settings)
    return settings


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
