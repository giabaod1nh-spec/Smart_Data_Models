#!/usr/bin/env python3
"""Host-side projector launcher for K-5 isolation live A/B (Orion stub optional)."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from types import SimpleNamespace


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True)
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--topic", default="traffic.entity-events.v2")
    p.add_argument("--group", default="projector-k5-isolation")
    p.add_argument("--namespace", default="production")
    p.add_argument("--write-mode", default="disabled", choices=["disabled", "armed", "active"])
    p.add_argument("--start-offsets-file", required=True)
    p.add_argument("--health-port", type=int, default=8093)
    p.add_argument("--orion-stub", action="store_true", help="No Orion HTTP; instant success")
    p.add_argument("--orion-stub-delay-ms", type=float, default=0.0)
    args = p.parse_args(argv)

    # Force profile compatible with production namespace cutover tooling.
    os.environ.setdefault("ARCHITECTURE_PROFILE", "k5-cutover")
    os.environ["PROJECTOR_SHADOW_MODE"] = "false"
    os.environ["PROJECTOR_TARGET_NAMESPACE"] = args.namespace
    os.environ["PROJECTOR_WRITE_MODE"] = args.write_mode
    os.environ["PROJECTOR_HEALTH_PORT"] = str(args.health_port)
    os.environ["PROJECTOR_DB"] = str(Path(args.db))
    os.environ["PROJECTOR_FENCE_MANIFEST"] = str(Path(args.start_offsets_file))
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = args.bootstrap

    if args.orion_stub:
        import time
        import Visualize.tools.projector_live_consumer as plc
        from integration import orion as orion_pkg  # noqa: F401
        import integration.orion.client as orion_client

        delay = max(0.0, float(args.orion_stub_delay_ms) / 1000.0)

        def _stub_wait(*_a, **_k):
            return True

        def _stub_upsert(entities, *a, **k):
            if delay:
                time.sleep(delay)
            return SimpleNamespace(
                http_status=204,
                success_ids=tuple(e["id"] for e in entities),
                permanent_errors=(),
                retryable_error_ids=(),
                ambiguous_ids=(),
            )

        orion_client.wait_orion_ready = _stub_wait  # type: ignore
        orion_client.batch_upsert_entities = _stub_upsert  # type: ignore

    sys.argv = [
        "projector_live_consumer",
        "--db",
        str(args.db),
        "--bootstrap",
        args.bootstrap,
        "--topic",
        args.topic,
        "--group",
        args.group,
        "--namespace",
        args.namespace,
        "--no-shadow",
        "--write-mode",
        args.write_mode,
        "--start-offsets-file",
        str(args.start_offsets_file),
        "--health-host",
        "127.0.0.1",
        "--health-port",
        str(args.health_port),
    ]
    from Visualize.tools.projector_live_consumer import main as live_main

    return int(live_main())


if __name__ == "__main__":
    raise SystemExit(main())
