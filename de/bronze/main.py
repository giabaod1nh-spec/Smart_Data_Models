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


def observation_main():
    import json
    import signal
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from de.bronze.config import ObservationSettings
    from de.bronze.processor import ObservationWorker
    from de.kafka_raw.v1_ledger_store import IntegrityFault
    settings = ObservationSettings()
    settings.validate_runtime()
    lock = InstanceLock(Path(settings.checkpoint_path))
    lock.acquire()
    worker = ObservationWorker(settings)
    running = True
    def stop(*args):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM,stop)
    signal.signal(signal.SIGINT,stop)
    snapshot = dict(ready=False,state='STARTING')
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ('/health','/ready','/metrics'):
                self.send_error(404)
                return
            self.send_response(503 if self.path=='/ready' and not snapshot['ready'] else 200)
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(json.dumps(snapshot).encode())
        def log_message(self,*args):
            pass
    server = ThreadingHTTPServer(('0.0.0.0',settings.health_port),Handler)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        worker.start()
        while running:
            try:
                worker.step()
            except IntegrityFault as exc:
                worker.state,worker.error = 'FAULTED',str(exc)
                snapshot.update(ready=False,state=worker.state,error=worker.error)
                return 1
            except Exception as exc:
                worker.state,worker.error = 'DEGRADED',str(exc)
                log.exception('V1 Bronze processing retry')
            snapshot.update(ready=worker.state=='READY',state=worker.state,error=worker.error,
                            partitions=worker.progress,processed=worker.processed,**worker.metrics)
            time.sleep(settings.poll_interval_sec)
    finally:
        server.shutdown()
        worker.close()
        lock.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(observation_main())
