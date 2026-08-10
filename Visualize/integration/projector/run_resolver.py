"""Resolve TraCI simulation_run_id against Projector current-run (fail-safe)."""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Optional

log = logging.getLogger(__name__)

DEFAULT_PROJECTOR_BASE_URL = "http://localhost:8093"
DEFAULT_SYNC_TIMEOUT_SEC = 60.0
DEFAULT_SYNC_POLL_SEC = 2.0


def projector_sync_timeout_sec() -> float:
    raw = os.getenv("PROJECTOR_SYNC_TIMEOUT_SEC", "").strip()
    if not raw:
        return DEFAULT_SYNC_TIMEOUT_SEC
    try:
        return max(0.0, float(raw))
    except ValueError:
        log.warning(
            "Invalid PROJECTOR_SYNC_TIMEOUT_SEC=%r; using default %.0fs",
            raw,
            DEFAULT_SYNC_TIMEOUT_SEC,
        )
        return DEFAULT_SYNC_TIMEOUT_SEC


class ProjectorUnreachableError(OSError):
    """Projector /current-run could not be reached."""


class RunIdConflictError(RuntimeError):
    """TraCI would start a conflicting run while Projector has an active run."""

    def __init__(self, active_run_id: str) -> None:
        self.active_run_id = active_run_id
        super().__init__(
            f"Projector active run: {active_run_id}\n"
            "TraCI would start a new run and cause dashboard mismatch.\n"
            "  --new-run              start fresh run (Projector will switch after RunStarted)\n"
            "  --simulation-run-id …  resume explicit run "
            "(see README: time regression risk if SUMO restarts from t=0)"
        )


@dataclass(frozen=True)
class RunResolution:
    simulation_run_id: str
    source: str  # explicit | new-run | cold-start
    projector_sync_pending: bool


def projector_base_url() -> str:
    raw = os.getenv("PROJECTOR_BASE_URL", DEFAULT_PROJECTOR_BASE_URL).strip()
    return raw.rstrip("/") or DEFAULT_PROJECTOR_BASE_URL


def _http_current_run(url: str, *, timeout: float = 5.0) -> tuple[int, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            code = resp.status
            raw = resp.read().decode("utf-8")
            body = json.loads(raw) if raw else None
            return code, body
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = raw
        return e.code, body
    except urllib.error.URLError as e:
        raise ProjectorUnreachableError(str(e)) from e


def fetch_projector_current_run(
    base_url: Optional[str] = None,
    *,
    timeout: float = 5.0,
) -> Optional[dict[str, Any]]:
    """Return current-run JSON body, or None when Projector is idle (204)."""
    root = (base_url or projector_base_url()).rstrip("/")
    code, body = _http_current_run(f"{root}/current-run", timeout=timeout)
    if code == 204:
        return None
    if code == 200 and isinstance(body, dict):
        return body
    if code == 503:
        raise ProjectorUnreachableError(f"projector current-run unavailable (503)")
    raise ProjectorUnreachableError(
        f"unexpected projector current-run response: HTTP {code}"
    )


def _active_run_id(current: Optional[dict[str, Any]]) -> Optional[str]:
    if not current:
        return None
    run_id = current.get("simulationRunId")
    if run_id is None:
        return None
    text = str(run_id).strip()
    return text or None


def resolve_simulation_run_id(
    *,
    explicit_id: Optional[str] = None,
    new_run_flag: bool = False,
    projector_url: Optional[str] = None,
) -> RunResolution:
    """Fail-safe run selection before SUMO starts."""
    if explicit_id and new_run_flag:
        raise ValueError("--simulation-run-id and --new-run are mutually exclusive")

    if explicit_id:
        run_id = str(explicit_id).strip()
        if not run_id:
            raise ValueError("--simulation-run-id must not be blank")
        try:
            current = fetch_projector_current_run(projector_url)
        except ProjectorUnreachableError:
            log.warning(
                "Projector unreachable while resolving explicit run id; "
                "continuing with simulation_run_id=%s",
                run_id,
            )
            return RunResolution(
                simulation_run_id=run_id,
                source="explicit",
                projector_sync_pending=True,
            )
        active = _active_run_id(current)
        pending = active is None or active != run_id
        return RunResolution(
            simulation_run_id=run_id,
            source="explicit",
            projector_sync_pending=pending,
        )

    if new_run_flag:
        run_id = str(uuid.uuid4())
        return RunResolution(
            simulation_run_id=run_id,
            source="new-run",
            projector_sync_pending=True,
        )

    try:
        current = fetch_projector_current_run(projector_url)
    except ProjectorUnreachableError as e:
        run_id = str(uuid.uuid4())
        log.warning(
            "Projector unreachable (%s); cold-start with new simulation_run_id=%s",
            e,
            run_id,
        )
        return RunResolution(
            simulation_run_id=run_id,
            source="cold-start",
            projector_sync_pending=True,
        )

    active = _active_run_id(current)
    if active:
        raise RunIdConflictError(active)

    run_id = str(uuid.uuid4())
    return RunResolution(
        simulation_run_id=run_id,
        source="cold-start",
        projector_sync_pending=True,
    )


def wait_for_projector_run(
    run_id: str,
    base_url: Optional[str] = None,
    *,
    timeout_sec: Optional[float] = None,
    poll_sec: float = DEFAULT_SYNC_POLL_SEC,
) -> bool:
    """Poll /current-run until simulationRunId matches run_id. Never raises."""
    target = str(run_id).strip()
    if not target:
        return False

    effective_timeout = (
        projector_sync_timeout_sec() if timeout_sec is None else max(0.0, float(timeout_sec))
    )
    root = (base_url or projector_base_url()).rstrip("/")
    deadline = time.monotonic() + effective_timeout
    last_seen: Optional[str] = None

    while time.monotonic() < deadline:
        try:
            current = fetch_projector_current_run(root)
            seen = _active_run_id(current)
            last_seen = seen
            if seen == target:
                log.info("Projector current-run aligned: simulationRunId=%s", target)
                return True
        except ProjectorUnreachableError as e:
            log.warning("Projector sync poll failed: %s", e)
        except Exception as e:
            log.warning("Projector sync poll unexpected error: %s", e, exc_info=True)

        time.sleep(poll_sec)

    log.warning(
        "Projector sync gate timeout after %.0fs: TraCI run=%s projector run=%s. "
        "Dashboard may show empty intersections until Projector catches up. "
        "Check: Invoke-RestMethod %s/current-run",
        effective_timeout,
        target,
        last_seen,
        root,
    )
    return False
