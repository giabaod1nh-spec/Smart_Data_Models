"""Silver Plan 3 — process entrypoint."""
from __future__ import annotations

import signal
import threading
import time

import uvicorn

from de.silver.config import ProcessorState, SilverSettings, get_settings
from de.silver.health_api import app, bind_processor
from de.silver.instance_lock import InstanceLock
from de.silver.processor import SilverProcessor


def main() -> int:
    settings = get_settings()
    settings.validate_mode_guards()
    lock = InstanceLock(settings.checkpoint_path, settings.namespace)
    lock.acquire()
    processor = SilverProcessor(settings, lock_held=True)
    bind_processor(processor, max_age_sec=settings.health_snapshot_max_age_sec)
    processor.start()

    def _handle_sig(_signum, _frame) -> None:
        processor.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    config = uvicorn.Config(
        app,
        host=settings.health_host,
        port=settings.health_port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="silver-health", daemon=True)
    thread.start()

    exit_code = 0
    try:
        while not processor._stop.is_set():  # noqa: SLF001
            if processor.state == ProcessorState.FAULTED:
                exit_code = 2
                break
            if processor.state == ProcessorState.STOPPED:
                break
            time.sleep(0.2)
    finally:
        processor.stop()
        lock.release()
        server.should_exit = True
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
