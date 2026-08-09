"""Entrypoint: health HTTP + Bronze processor worker thread."""
from __future__ import annotations

import logging
import sys
import threading
from pathlib import Path

import uvicorn

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.bronze.config import get_settings  # noqa: E402
from de.bronze.health_api import app, bind_processor  # noqa: E402
from de.bronze.instance_lock import BronzeInstanceAlreadyRunning, InstanceLock  # noqa: E402
from de.bronze.processor import BronzeProcessor  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("de.bronze.main")


def main() -> int:
    settings = get_settings()
    if settings.worker_count != 1:
        log.error("BRONZE_WORKER_COUNT must be 1")
        return 1
    if not settings.single_instance:
        log.error("BRONZE_SINGLE_INSTANCE must be true")
        return 1

    lock = InstanceLock(Path(settings.checkpoint_path))
    try:
        lock.acquire()
    except BronzeInstanceAlreadyRunning as e:
        log.error("%s", e)
        return 1

    processor = BronzeProcessor(settings)
    bind_processor(processor)
    try:
        processor.start()
    except Exception as e:
        log.error("processor start failed: %s", e)
        lock.release()
        return 1

    def _serve() -> None:
        uvicorn.run(
            app,
            host=settings.health_host,
            port=settings.health_port,
            log_level="info",
        )

    t = threading.Thread(target=_serve, name="k7-health-http", daemon=True)
    t.start()
    log.info(
        "Bronze processor health on %s:%s namespace=%s",
        settings.health_host,
        settings.health_port,
        settings.checkpoint_namespace,
    )
    try:
        while processor.state.value not in ("STOPPED",):
            if processor._thread and not processor._thread.is_alive():
                if processor.state.value == "FAULTED":
                    return 1
                break
            threading.Event().wait(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        processor.stop()
        lock.release()
    return 0 if processor.state.value != "FAULTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
