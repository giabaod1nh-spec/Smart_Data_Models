"""Active Gold V1 foundation entrypoint."""

from __future__ import annotations

import logging
import signal
import threading

from de.gold_runtime.checkpoint_store import GoldCoreStore
from de.gold_runtime.config import GoldCoreSettings
from de.gold_runtime.health_api import app, bind_processor
from de.gold_runtime.instance_lock import InstanceLock
from de.gold_runtime.processor import CORE_PROCESSOR_VERSION, GoldCoreProcessor
from de.gold_runtime.replay import CoreReplayManifest
from de.silver.config import SilverSettings


def build_processor(settings: GoldCoreSettings, *, silver=None):
    settings.validate_core()
    silver = silver or SilverSettings()
    replay = CoreReplayManifest.load(settings.replay_manifest) if settings.replay_manifest else None
    checkpoint, lock_path = settings.state_paths(silver, replay)
    store = GoldCoreStore(checkpoint)
    lock = InstanceLock(lock_path, replay.namespace if replay else "live", store,
                        processor_version=CORE_PROCESSOR_VERSION, exact_path=True)
    try:
        lock.acquire(open_store=True)
        return GoldCoreProcessor(settings, silver, store, lock, replay=replay), lock
    except BaseException:
        lock.release()
        store.close()
        raise


def main() -> int:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    settings = GoldCoreSettings()
    processor, lock = build_processor(settings)
    bind_processor(processor, max_age_sec=settings.health_snapshot_max_age_sec)

    def stop(_signum, _frame):
        processor.request_shutdown()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server = uvicorn.Server(uvicorn.Config(
        app, host=settings.health_host, port=settings.health_port, log_level="warning"))
    server_thread = threading.Thread(target=server.run, name="gold-core-health", daemon=True)
    server_thread.start()
    try:
        return processor.run()
    finally:
        lock.release()
        processor.store.close()
        server.should_exit = True
        server_thread.join(timeout=2)


if __name__ == "__main__":
    raise SystemExit(main())
