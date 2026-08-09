"""Entrypoint: health HTTP + Raw Kafka consumer worker thread."""
from __future__ import annotations

import logging
import sys
import threading
from pathlib import Path

import uvicorn

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from de.kafka_raw.config import get_settings  # noqa: E402
from de.kafka_raw.consumer import RawKafkaConsumer  # noqa: E402
from de.kafka_raw.health_api import app, bind_consumer  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("de.kafka_raw.main")


def main() -> int:
    settings = get_settings()
    consumer = RawKafkaConsumer(settings)
    bind_consumer(consumer)
    consumer.start()

    def _serve() -> None:
        uvicorn.run(
            app,
            host=settings.health_host,
            port=settings.health_port,
            log_level="info",
        )

    t = threading.Thread(target=_serve, name="k4-health-http", daemon=True)
    t.start()
    log.info(
        "health on %s:%s client_id=%s",
        settings.health_host,
        settings.health_port,
        settings.client_id,
    )
    try:
        while consumer.state.value not in ("STOPPED",):
            t.join(timeout=1.0)
            if not consumer._thread or not consumer._thread.is_alive():
                if consumer.state.value == "FAULTED":
                    return 1
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.stop()
    return 0 if consumer.state.value != "FAULTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
