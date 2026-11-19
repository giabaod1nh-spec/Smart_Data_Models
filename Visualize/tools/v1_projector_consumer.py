"""Consume V1 observations and project current state into Orion-LD.

Run from Visualize with ``python -m tools.v1_projector_consumer``.  Historical
replay must supply a separate Orion URL, group, and ledger database.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from integration.projector.v1_orion import PermanentOrionError, RetryableOrionError, V1Orion
from integration.projector.v1_runtime import InvalidObservation, ProjectionFault, RetryLater, V1Projector
from integration.projector.v1_store import V1Store

log = logging.getLogger(__name__)
VIS = Path(__file__).resolve().parents[1]


class FileLock:
    """Kernel-released single-writer lock; survives an unclean process exit."""

    def __init__(self, db_path: Path) -> None:
        self.path = Path(str(db_path) + ".lock")
        self.handle = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            raise RuntimeError(f"V1 Projector ledger already has a writer: {self.path}")
        self.handle = handle

    def release(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None


def health_server(projector: V1Projector, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):  # noqa: N802
            if self.path.split("?", 1)[0] not in ("/health", "/ready"):
                self.send_error(404)
                return
            health = projector.health()
            body = json.dumps(health).encode("utf-8")
            status = 200 if self.path.startswith("/health") or health["projectionReady"] else 503
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer((os.getenv("V1_PROJECTOR_HEALTH_HOST", "0.0.0.0"), port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("V1_KAFKA_TOPIC", "traffic.observations.v1"))
    parser.add_argument("--group", default=os.getenv("V1_PROJECTOR_GROUP", "projector-v1-production"))
    parser.add_argument("--db", type=Path, default=Path(os.getenv("V1_PROJECTOR_DB", str(VIS / "artifacts/projector/v1-production.sqlite3"))))
    parser.add_argument("--runs-dir", type=Path, default=Path(os.getenv("V1_RUNS_DIR", str(VIS / "artifacts/runs"))))
    parser.add_argument("--replay-run", default=None)
    parser.add_argument("--replay-orion-url", default=None)
    parser.add_argument("--health-port", type=int, default=int(os.getenv("V1_PROJECTOR_HEALTH_PORT", "8092")))
    parser.add_argument("--max-messages", type=int, default=0, help="0 runs continuously")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    live_url = os.getenv("ORION_URL", "http://localhost:1026").rstrip("/")
    production_group = os.getenv("V1_PROJECTOR_GROUP", "projector-v1-production")
    production_db = Path(os.getenv("V1_PROJECTOR_DB", str(VIS / "artifacts/projector/v1-production.sqlite3")))
    if args.topic != "traffic.observations.v1":
        parser.error("V1 Projector accepts only traffic.observations.v1")
    if args.replay_run:
        if not args.replay_orion_url or args.replay_orion_url.rstrip("/") == live_url:
            parser.error("replay requires a separate Orion instance URL")
        if args.group == production_group or args.db.resolve() == production_db.resolve():
            parser.error("replay requires a separate consumer group and ledger path")
    elif args.replay_orion_url:
        parser.error("--replay-orion-url requires --replay-run")
    orion = V1Orion.from_env()
    if args.replay_run:
        orion.base_url = args.replay_orion_url.rstrip("/")
    store = V1Store(args.db)
    lock = FileLock(args.db)
    lock.acquire()
    projector = V1Projector(store=store, orion=orion, runs_dir=args.runs_dir, replay_run=args.replay_run)
    server = health_server(projector, args.health_port)
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    paused: dict[int, list[object]] = {}
    next_retry: dict[int, float] = {}
    assignment: set[int] = set()

    def on_assign(_consumer, partitions):
        assignment.clear()
        assignment.update(tp.partition for tp in partitions)
        paused.clear()
        next_retry.clear()
        log.info("assigned V1 partitions %s", sorted(assignment))

    def on_revoke(_consumer, partitions):
        assignment.clear()
        paused.clear()
        next_retry.clear()
        log.info("revoked V1 partitions %s", [tp.partition for tp in partitions])

    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
    completed = 0

    def apply(message) -> bool:
        nonlocal completed
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        try:
            outcome = projector.process(topic=topic, partition=partition, offset=offset, key=message.key(), value=message.value())
        except InvalidObservation as exc:
            # Invalid input is terminal only before any Orion write.  A known
            # partial event must be repaired rather than silently skipped.
            partial = False
            try:
                event_id = json.loads(message.value() or b"{}").get("eventId")
                partial = bool(event_id and store.completed_ops(event_id))
            except (ValueError, TypeError):
                pass
            if partial:
                projector.fault = str(exc)
                log.error("partial event cannot be quarantined: %s", exc)
                return False
            store.quarantine(topic=topic, partition=partition, offset=offset, payload=message.value() or b"", reason=str(exc))
            outcome = "QUARANTINED"
        except (ProjectionFault, PermanentOrionError, RetryLater, RetryableOrionError) as exc:
            if isinstance(exc, (ProjectionFault, PermanentOrionError)):
                projector.fault = str(exc)
                log.error("projection fault at %s[%s]@%s: %s", topic, partition, offset, exc)
            else:
                log.warning("V1 projection retry at %s[%s]@%s: %s", topic, partition, offset, exc)
            return False
        consumer.commit(message=message, asynchronous=False)
        completed += 1
        log.info("V1 %s %s[%s]@%s", outcome, topic, partition, offset)
        return True

    try:
        while True:
            # Run activation is independent of Kafka arrival.  Reconcile even
            # when a newly selected run has not yet published a message.
            try:
                projector.ensure_reconciled()
            except (RetryLater, RetryableOrionError) as exc:
                log.warning("reconciliation retry: %s", exc)
            except (ProjectionFault, PermanentOrionError, InvalidObservation) as exc:
                projector.fault = str(exc)
                log.error("reconciliation fault: %s", exc)
            now = time.monotonic()
            for partition, queue in list(paused.items()):
                if partition not in assignment or now < next_retry[partition]:
                    continue
                if apply(queue[0]):
                    queue.pop(0)
                    if not queue:
                        consumer.resume([TopicPartition(args.topic, partition)])
                        del paused[partition]
                        del next_retry[partition]
                    else:
                        next_retry[partition] = now
                else:
                    next_retry[partition] = now + 3.0
            if args.max_messages and completed >= args.max_messages:
                return 0
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                log.warning("Kafka consumer error: %s", message.error())
                continue
            if message.partition() not in assignment:
                continue
            if message.partition() in paused:
                # A broker client may still yield records it prefetched before
                # pause. Preserve partition order until the failed offset ends.
                paused[message.partition()].append(message)
                continue
            if not apply(message):
                partition = message.partition()
                consumer.pause([TopicPartition(args.topic, partition)])
                paused[partition] = [message]
                next_retry[partition] = time.monotonic() + 3.0
    finally:
        consumer.close()
        server.shutdown()
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
