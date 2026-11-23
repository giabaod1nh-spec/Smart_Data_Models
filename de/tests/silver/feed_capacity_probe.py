"""Isolated process for the Stage A capacity profile. Requires psutil."""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path

import psutil

from de.silver.canonical_reader import CanonicalReadSession
from de.silver.config import SilverSettings
from de.silver.incremental_contract import (
    CanonicalChangePageRequest, CanonicalFeedError, CanonicalInvalidationPageRequest,
    SourceDiscoveryRequest, canonical_bytes,
)
from de.tests.silver.feed_consumer_probe import acknowledge, initialize


def run(settings, output, consumer_db, ready, schedule):
    counts = {"sqlite_statements": 0, "clickhouse_queries": 0}
    base_connect = sqlite3.connect

    def trace(_):
        counts["sqlite_statements"] += 1

    def connect(*args, **kwargs):
        db = base_connect(*args, **kwargs)
        db.set_trace_callback(trace)
        return db

    def instrument(repo):
        query = repo.client.query

        def counted(*args, **kwargs):
            counts["clickhouse_queries"] += 1
            return query(*args, **kwargs)

        repo.client.query = counted

    proc = psutil.Process()
    # The instrumentation is scoped to this dedicated process.
    sqlite3.connect = connect
    with CanonicalReadSession(settings) as session, output.open("w", encoding="utf-8", buffering=1) as fp:
        instrument(session.bronze_repo)
        instrument(session.silver_repo)
        discover = session.discover_canonical_source(SourceDiscoveryRequest("live"))
        binding = discover.source_binding_id
        initialize(consumer_db, binding)
        ready.touch()
        while not schedule.exists():
            time.sleep(0.05)
        begin, steady, burst, end = json.loads(schedule.read_text())
        cursor = invalidation_cursor = total_bytes = pages = 0
        last_sample = 0

        def record(kind, **values):
            fp.write(json.dumps(dict(kind=kind, time=time.time(), **values)) + "\n")

        while time.time() < end:
            now = time.time()
            if begin <= now < steady or burst <= now:
                started = time.perf_counter()
                try:
                    page = session.read_canonical_changes_page(CanonicalChangePageRequest(
                        namespace="live", expected_source_binding_id=binding, after_change_sequence=cursor))
                    if page.receipt:
                        watermark = page.receipt.safe_invalidation_through_sequence
                        while invalidation_cursor < watermark:
                            invalidation_page = session.read_canonical_invalidations_page(
                                CanonicalInvalidationPageRequest(namespace="live",
                                    expected_source_binding_id=binding, after_invalidation_sequence=invalidation_cursor,
                                    through_invalidation_sequence=watermark))
                            acknowledge(consumer_db, canonical_bytes(invalidation_page))
                            invalidation_cursor = invalidation_page.through_invalidation_sequence
                        acknowledge(consumer_db, canonical_bytes(page))
                        cursor = page.through_change_sequence
                        total_bytes += page.encoded_bytes
                        pages += 1
                        record("page", latency_ms=(time.perf_counter() - started) * 1000,
                               bytes=page.encoded_bytes, entries=page.entry_count, cursor=cursor)
                        if pages == 1:
                            output.with_suffix(".first-receipt.json").write_bytes(canonical_bytes(page))
                        if page.has_more:
                            continue
                except CanonicalFeedError as exc:
                    record("fault", code=exc.code, retryable=exc.retryable, cursor=cursor)
                    if not exc.retryable:
                        raise
            if now - last_sample >= 1:
                rss = proc.memory_info().rss
                peak = getattr(proc.memory_info(), "peak_wset", rss)
                cpu = proc.cpu_times()
                io = proc.io_counters()
                record("sample", cursor=cursor, invalidation_cursor=invalidation_cursor, rss=rss,
                       peak_rss=peak, cpu_s=cpu.user + cpu.system, read_bytes=io.read_bytes,
                       write_bytes=io.write_bytes, **counts, encoded_bytes=total_bytes)
                last_sample = now
            time.sleep(0.20)
        record("end", cursor=cursor, invalidation_cursor=invalidation_cursor, **counts,
               encoded_bytes=total_bytes, pages=pages)


if __name__ == "__main__":
    run(SilverSettings.model_validate_json(sys.stdin.read()), *(Path(arg) for arg in sys.argv[1:]))
