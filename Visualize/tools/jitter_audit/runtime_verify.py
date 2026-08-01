"""Runtime verification before jitter audit."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RuntimeVerification:
    ok: bool
    checks: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"ok": self.ok, "checks": self.checks, "errors": self.errors}


def _http_json(url: str, timeout: float = 3.0) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _count_sumo_processes() -> int:
    if sys.platform != "win32":
        try:
            out = subprocess.check_output(["pgrep", "-c", "sumo"], text=True)
            return int(out.strip() or "0")
        except Exception:
            return -1
    try:
        out = subprocess.check_output(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-Process -Name sumo*,sumo-gui -ErrorAction SilentlyContinue | Measure-Object).Count",
            ],
            text=True,
        )
        return int(out.strip() or "0")
    except Exception:
        return -1


def verify_runtime(env: Dict[str, str]) -> RuntimeVerification:
    rv = RuntimeVerification(ok=True)
    c = rv.checks

    c["architecture_profile"] = env.get("ARCHITECTURE_PROFILE", "")
    c["orion_publish_enabled"] = env.get("ORION_PUBLISH_ENABLED", "")
    c["kafka_outbox_enabled"] = env.get("KAFKA_OUTBOX_ENABLED", "")
    c["orion_sync_publish"] = env.get("ORION_SYNC_PUBLISH", env.get("SYNC_PUBLISH", ""))
    c["orion_perf_audit"] = env.get("ORION_PERF_AUDIT", "")
    c["log_level"] = env.get("LOG_LEVEL", "")

    if c["orion_publish_enabled"].lower() in ("true", "1", "yes"):
        rv.ok = False
        rv.errors.append("ORION_PUBLISH_ENABLED must be false for Kafka-only audit")
    if c["kafka_outbox_enabled"].lower() not in ("true", "1", "yes") and env.get("AUDIT_CASE") not in ("A",):
        rv.ok = False
        rv.errors.append("KAFKA_OUTBOX_ENABLED must be true for Kafka audit cases")
    if c["orion_sync_publish"].lower() in ("true", "1", "yes"):
        rv.ok = False
        rv.errors.append("sync publish forbidden")

    c["sumo_process_count"] = _count_sumo_processes()
    if c["sumo_process_count"] > 1:
        rv.ok = False
        rv.errors.append(f"multiple SUMO processes detected: {c['sumo_process_count']}")

    try:
        subs = _http_json("http://localhost:1026/ngsi-ld/v1/subscriptions")
        c["orion_subscriptions"] = len(subs) if isinstance(subs, list) else None
        c["subscription_on_traci_hot_path"] = False
    except Exception as e:
        c["orion_subscriptions"] = None
        c["orion_subscriptions_error"] = str(e)

    try:
        c["projector_health"] = _http_json("http://localhost:8092/health")
    except Exception as e:
        c["projector_health"] = {"error": str(e)}

    try:
        c["raw_consumer_ready"] = _http_json("http://localhost:8091/ready")
    except Exception:
        try:
            c["raw_consumer_ready"] = _http_json("http://localhost:1027/ready")
        except Exception as e:
            c["raw_consumer_ready"] = {"error": str(e)}

    try:
        out = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}}"],
            text=True,
            timeout=10,
        )
        names = [n.strip() for n in out.splitlines() if n.strip()]
        c["docker_services"] = names
        dup_projectors = [n for n in names if "projector" in n.lower()]
        if len(dup_projectors) > 1:
            rv.ok = False
            rv.errors.append(f"multiple projector containers: {dup_projectors}")
    except Exception as e:
        c["docker_services_error"] = str(e)

    return rv
