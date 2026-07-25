"""
TEMPORARY Notification Receiver for Orion Subscription delivery verification.

NOT a Data Engineering webhook.
- receive POST
- log
- optionally save raw JSON under integration/captured/
- return HTTP 204

Does NOT parse, transform, insert DB, or run ETL.
"""
from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger("temp_receiver")

DEFAULT_CAPTURED = (
    Path(__file__).resolve().parents[1] / "captured" / "notification.captured.example.json"
)


class TemporaryNotificationReceiver:
    """In-process receiver usable by the verification harness."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        capture_path: Optional[Path] = None,
        save_every: bool = True,
    ):
        self.host = host
        self.port = port
        self.capture_path = Path(capture_path) if capture_path else DEFAULT_CAPTURED
        self.save_every = save_every
        self.bodies: List[Dict[str, Any]] = []
        self.raw_bodies: List[bytes] = []
        self.request_count = 0
        self._httpd: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def url(self) -> str:
        assert self._httpd is not None
        return f"http://{self.host}:{self._httpd.server_port}/webhook/ngsi"

    @property
    def bound_port(self) -> int:
        assert self._httpd is not None
        return int(self._httpd.server_port)

    def start(self) -> None:
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                with owner._lock:
                    owner.request_count += 1
                    owner.raw_bodies.append(raw)
                    try:
                        parsed = json.loads(raw.decode("utf-8"))
                    except Exception:
                        parsed = {"_raw": raw.decode("utf-8", errors="replace")}
                    owner.bodies.append(parsed)
                    if owner.save_every:
                        owner.capture_path.parent.mkdir(parents=True, exist_ok=True)
                        owner.capture_path.write_text(
                            json.dumps(parsed, indent=2), encoding="utf-8"
                        )
                log.info(
                    "notification received count=%s bytes=%s path=%s",
                    owner.request_count,
                    len(raw),
                    self.path,
                )
                self.send_response(204)
                self.end_headers()

            def log_message(self, format, *args):  # noqa: A003
                return

        self._httpd = HTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        log.info("Temporary receiver listening on %s", self.url)

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread:
            self._thread.join(timeout=5)

    def wait_for_body(self, timeout: float = 45.0) -> Dict[str, Any]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self.bodies:
                    return self.bodies[-1]
            time.sleep(0.2)
        raise TimeoutError(f"No notification received within {timeout}s")

    def save_last(self, path: Optional[Path] = None) -> Path:
        out = Path(path) if path else self.capture_path
        with self._lock:
            if not self.bodies:
                raise RuntimeError("No notification body to save")
            body = self.bodies[-1]
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(body, indent=2), encoding="utf-8")
        return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="TEMPORARY Orion notification receiver")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=18080)
    p.add_argument(
        "--capture",
        type=Path,
        default=DEFAULT_CAPTURED,
        help="Path to write last raw notification JSON",
    )
    args = p.parse_args()
    rx = TemporaryNotificationReceiver(
        host=args.host, port=args.port, capture_path=args.capture
    )
    rx.start()
    log.info("TEMPORARY receiver — Ctrl+C to stop. URL=%s", rx.url)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        rx.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
