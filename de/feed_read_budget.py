"""Opt-in query ceilings for canonical sequence page evidence reads."""
from contextlib import contextmanager
from contextvars import ContextVar
import time


class FeedEvidenceDeadline(TimeoutError):
    pass


class FeedEvidenceSize(Exception):
    pass


_budget = ContextVar("canonical_feed_clickhouse_budget", default=None)


@contextmanager
def feed_query_budget(deadline, max_bytes):
    token = _budget.set((deadline, max_bytes))
    try:
        yield
    finally:
        _budget.reset(token)


def evidence_query(client, query, **kwargs):
    budget = _budget.get()
    if budget is None:
        return client.query(query, **kwargs)
    deadline, max_bytes = budget
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise FeedEvidenceDeadline("ClickHouse evidence query would exceed deadline")
    kwargs["settings"] = {
        **kwargs.get("settings", {}),
        "max_execution_time": max(0.001, remaining),
        "max_result_bytes": max_bytes,
        "max_result_rows": 1001,
        "result_overflow_mode": "throw",
        "timeout_overflow_mode": "throw",
    }
    try:
        result = client.query(query, **kwargs)
    except Exception as exc:
        # These are server exception names, independent of the client's error
        # subclass. Preserve all other existing repository errors.
        message = str(exc)
        if "TIMEOUT_EXCEEDED" in message or time.monotonic() >= deadline:
            raise FeedEvidenceDeadline("ClickHouse evidence query exceeded deadline") from exc
        if "TOO_MANY_ROWS_OR_BYTES" in message:
            raise FeedEvidenceSize("ClickHouse evidence result exceeded the configured bound") from exc
        raise
    if time.monotonic() > deadline:
        raise FeedEvidenceDeadline("ClickHouse evidence query exceeded deadline")
    return result
