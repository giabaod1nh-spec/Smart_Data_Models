"""Gold 3 process entrypoint: ``python -m de.gold_runtime.main``.

Composition order is validate config → acquire namespace lock → open storage →
recover → serve health → run. Shutdown stops intake, drains the bounded batch,
writes ledger/checkpoint state for completed work only, and releases the lock.
"""
from __future__ import annotations

import signal
import threading
import time
from pathlib import Path

from de.gold_runtime.checkpoint_store import GoldRuntimeStore
from de.gold_runtime.config import GoldSettings, ProcessorState, get_settings
from de.gold_runtime.health_api import app, bind_processor
from de.gold_runtime.instance_lock import InstanceLock
from de.gold_runtime.processor import GoldProcessor


def build_processor(settings: GoldSettings) -> tuple[GoldProcessor, InstanceLock]:
    settings.validate_all()
    store = GoldRuntimeStore(Path(settings.checkpoint_path))
    store.open()
    lock = InstanceLock(
        settings.instance_lock_path,
        settings.namespace,
        store,
        processor_version=settings.processor_version,
    )
    lock.acquire()
    processor = GoldProcessor(settings, store=store, lock=lock, lock_held=True)
    return processor, lock


def main() -> int:
    settings = get_settings()
    processor, lock = build_processor(settings)
    bind_processor(processor, max_age_sec=settings.health_snapshot_max_age_sec)

    def _handle_signal(_signum, _frame) -> None:
        processor.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    import uvicorn

    server = uvicorn.Server(
        uvicorn.Config(
            app, host=settings.health_host, port=settings.health_port, log_level="warning"
        )
    )
    health_thread = threading.Thread(target=server.run, name="gold-runtime-health", daemon=True)
    health_thread.start()

    exit_code = 0
    try:
        processor.start()
        while True:
            state = processor.state
            if state is ProcessorState.FAULTED:
                exit_code = 2
                break
            if state in {ProcessorState.STOPPED, ProcessorState.STOPPING}:
                break
            if processor.health_snapshot().shutdown_requested:
                break
            time.sleep(0.2)
    finally:
        processor.stop()
        lock.release()
        server.should_exit = True
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
