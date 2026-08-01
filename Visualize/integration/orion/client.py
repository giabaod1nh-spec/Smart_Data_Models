"""
orion_client.py — NGSI-LD Context Broker REST client for Visualize.

Sequential: POST once to create; subsequent updates use PATCH attrs.
Batch: POST entityOperations/upsert?options=update with structured BatchUpsertResult.
Raises OrionTransientError / OrionPermanentError / OrionBatchProtocolError.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import requests

import configuration.config as cfg

log = logging.getLogger(__name__)

HEADERS = {"Content-Type": "application/ld+json"}

_created: set[str] = set()
_created_lock = threading.Lock()
_session: Optional[requests.Session] = None
_session_lock = threading.Lock()

TRANSIENT_STATUS = frozenset({429, 502, 503, 504})
PERMANENT_STATUS = frozenset({400, 401, 403})


class OrionPublishError(Exception):
    """Base Orion publish failure."""

    def __init__(self, message: str, *, status: int = 0, entity_id: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.entity_id = entity_id


class OrionTransientError(OrionPublishError):
    """Retryable: network, timeout, 429/502/503/504."""


class OrionPermanentError(OrionPublishError):
    """Non-retryable: 400/401/403 or invalid payload."""


class OrionBatchProtocolError(OrionPermanentError):
    """Broker batch response untrustworthy (malformed 207 / invariant break) — FAULTED."""


@dataclass(frozen=True)
class BatchEntityError:
    entity_id: str
    status: int
    title: str = ""
    detail: str = ""


@dataclass(frozen=True)
class BatchUpsertResult:
    http_status: Optional[int]
    success_ids: tuple[str, ...]
    retryable_error_ids: tuple[str, ...]
    permanent_errors: tuple[BatchEntityError, ...]
    ambiguous_ids: tuple[str, ...]

def _perf_enabled() -> bool:
    try:
        from integration.orion.perf_probe import enabled

        return enabled()
    except ImportError:
        return os.getenv("ORION_PERF_AUDIT", "").lower() in ("1", "true", "yes")


def _record_http(entity_id: str, method: str, status: int, t0: float, ok: bool) -> None:
    if not _perf_enabled():
        return
    from integration.orion.perf_probe import record_entity

    record_entity(entity_id, method, status, (time.perf_counter() - t0) * 1000.0, ok)


def _entities_url() -> str:
    return f"{cfg.ORION_URL.rstrip('/')}/ngsi-ld/v1/entities"


def _http_timeout() -> float:
    return float(getattr(cfg, "ORION_PUBLISH_HTTP_TIMEOUT_SEC", 5))


def get_session() -> requests.Session:
    """Lazy shared Session with connection pooling."""
    global _session
    with _session_lock:
        if _session is None:
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=4, pool_maxsize=8, max_retries=0
            )
            s.mount("http://", adapter)
            s.mount("https://", adapter)
            _session = s
        return _session


def close_session() -> None:
    global _session
    with _session_lock:
        if _session is not None:
            try:
                _session.close()
            except Exception:
                pass
            _session = None


def _raise_for_status(entity_id: str, method: str, status: int, body: str) -> None:
    if status in TRANSIENT_STATUS:
        raise OrionTransientError(
            f"{method} {entity_id} -> {status}: {body[:150]}",
            status=status,
            entity_id=entity_id,
        )
    if status in PERMANENT_STATUS:
        raise OrionPermanentError(
            f"{method} {entity_id} -> {status}: {body[:150]}",
            status=status,
            entity_id=entity_id,
        )
    raise OrionTransientError(
        f"{method} {entity_id} -> unexpected status {status}: {body[:150]}",
        status=status,
        entity_id=entity_id,
    )


def wait_orion_ready(retries: int = 30, delay: float = 3.0) -> bool:
    version_url = f"{cfg.ORION_URL.rstrip('/')}/version"
    session = get_session()
    for i in range(retries):
        try:
            r = session.get(version_url, timeout=3)
            if r.status_code == 200:
                log.info("Context Broker is ready at %s", cfg.ORION_URL)
                return True
        except requests.exceptions.RequestException:
            pass
        log.info("Waiting for Context Broker... (%d/%d)", i + 1, retries)
        time.sleep(delay)
    raise RuntimeError(
        f"Context Broker at {cfg.ORION_URL} did not become ready in time"
    )


def upsert_entity(entity: dict) -> None:
    eid = entity["id"]
    with _created_lock:
        already = eid in _created
    if already:
        _patch(entity)
    else:
        _post_then_fallback(entity)


def _mark_created(eid: str) -> None:
    with _created_lock:
        _created.add(eid)


def _post_then_fallback(entity: dict) -> None:
    eid = entity["id"]
    session = get_session()
    timeout = _http_timeout()
    t0 = time.perf_counter()
    try:
        r = session.post(_entities_url(), json=entity, headers=HEADERS, timeout=timeout)
        ok = r.status_code in (201, 409)
        _record_http(eid, "POST", r.status_code, t0, ok)
        if r.status_code == 201:
            _mark_created(eid)
            return
        if r.status_code == 409:
            _mark_created(eid)
            _patch(entity)
            return
        _raise_for_status(eid, "POST", r.status_code, r.text)
    except OrionPublishError:
        raise
    except requests.exceptions.Timeout as e:
        _record_http(eid, "POST", 0, t0, False)
        raise OrionTransientError(f"POST timeout for {eid}: {e}", entity_id=eid) from e
    except requests.exceptions.RequestException as e:
        _record_http(eid, "POST", 0, t0, False)
        raise OrionTransientError(f"POST failed for {eid}: {e}", entity_id=eid) from e


def _patch(entity: dict) -> None:
    eid = entity["id"]
    attrs = {k: v for k, v in entity.items() if k not in ("id", "type")}
    url = f"{_entities_url()}/{eid}/attrs"
    session = get_session()
    timeout = _http_timeout()
    t0 = time.perf_counter()
    try:
        r = session.patch(url, json=attrs, headers=HEADERS, timeout=timeout)
        ok = r.status_code in (200, 204, 207)
        _record_http(eid, "PATCH", r.status_code, t0, ok)
        if r.status_code in (200, 204, 207):
            if r.status_code == 207:
                try:
                    body = r.json() or {}
                    not_updated = body.get("notUpdated") or []
                    missing = []
                    for item in not_updated:
                        reason = str(item.get("reason") or "").lower()
                        name = item.get("attributeName")
                        if name and "doesn't exist" in reason:
                            missing.append(name)
                    if missing:
                        append_payload = {k: attrs[k] for k in missing if k in attrs}
                        if "@context" in attrs:
                            append_payload["@context"] = attrs["@context"]
                        t1 = time.perf_counter()
                        ar = session.post(
                            url, json=append_payload, headers=HEADERS, timeout=timeout
                        )
                        append_ok = ar.status_code in (201, 204, 207, 200)
                        _record_http(eid, "POST_APPEND", ar.status_code, t1, append_ok)
                        if ar.status_code not in (201, 204, 207, 200):
                            _raise_for_status(eid, "POST_APPEND", ar.status_code, ar.text)
                        else:
                            t2 = time.perf_counter()
                            pr = session.patch(
                                url, json=attrs, headers=HEADERS, timeout=timeout
                            )
                            _record_http(
                                eid,
                                "PATCH",
                                pr.status_code,
                                t2,
                                pr.status_code in (200, 204, 207),
                            )
                            if pr.status_code not in (200, 204, 207):
                                _raise_for_status(eid, "PATCH", pr.status_code, pr.text)
                    elif not_updated:
                        log.warning(
                            "PATCH %s partial notUpdated=%s", eid, not_updated
                        )
                except OrionPublishError:
                    raise
                except Exception as e:
                    raise OrionTransientError(
                        f"PATCH {eid} follow-up failed: {e}", entity_id=eid
                    ) from e
            return
        # Entity deleted externally while still in process cache → recreate via POST.
        if r.status_code == 404:
            with _created_lock:
                _created.discard(eid)
            log.warning("PATCH %s -> 404; recreating via POST", eid)
            _post_then_fallback(entity)
            return
        _raise_for_status(eid, "PATCH", r.status_code, r.text)
    except OrionPublishError:
        raise
    except requests.exceptions.Timeout as e:
        _record_http(eid, "PATCH", 0, t0, False)
        raise OrionTransientError(f"PATCH timeout for {eid}: {e}", entity_id=eid) from e
    except requests.exceptions.RequestException as e:
        _record_http(eid, "PATCH", 0, t0, False)
        raise OrionTransientError(f"PATCH failed for {eid}: {e}", entity_id=eid) from e


def reset_created_cache() -> None:
    """Clear in-memory create cache (call at start of each simulation run)."""
    with _created_lock:
        _created.clear()


def is_in_created_cache(eid: str) -> bool:
    """Test/ops helper: whether entity id is marked created for sequential path."""
    with _created_lock:
        return eid in _created


def created_cache_snapshot() -> frozenset[str]:
    with _created_lock:
        return frozenset(_created)


def _mark_created_many(ids: Iterable[str]) -> None:
    with _created_lock:
        _created.update(ids)


def _batch_options() -> str:
    opt = str(getattr(cfg, "ORION_BATCH_OPTIONS", "update") or "update").strip().lower()
    return opt or "update"


def _batch_upsert_url() -> str:
    return (
        f"{cfg.ORION_URL.rstrip('/')}/ngsi-ld/v1/entityOperations/upsert"
        f"?options={_batch_options()}"
    )


def _batch_http_timeout() -> float:
    return float(getattr(cfg, "ORION_PUBLISH_BATCH_TIMEOUT_SEC", 10))


def _parse_error_status(err_obj: dict) -> int:
    """Extract HTTP-like status from Orion 207 error entry."""
    nested = err_obj.get("error") if isinstance(err_obj.get("error"), dict) else {}
    raw = nested.get("status")
    if raw is None:
        raw = err_obj.get("status")
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise OrionBatchProtocolError(
            f"unparseable error status in 207 entry: {err_obj!r}"
        )


def _validate_and_split_207(
    requested_ids: Sequence[str], body: dict
) -> BatchUpsertResult:
    """Parse 207 body with full coverage + invariant checks. May raise protocol error."""
    if not isinstance(body, dict):
        raise OrionBatchProtocolError("207 body is not a JSON object")

    success_raw = body.get("success")
    errors_raw = body.get("errors")
    if success_raw is None:
        success_raw = []
    if errors_raw is None:
        errors_raw = []
    if not isinstance(success_raw, list) or not isinstance(errors_raw, list):
        raise OrionBatchProtocolError("207 success/errors must be lists")

    requested_set = set(requested_ids)
    success_ids: List[str] = []
    seen_success: set[str] = set()
    for item in success_raw:
        if not isinstance(item, str) or not item:
            raise OrionBatchProtocolError(f"207 success entry not a string id: {item!r}")
        if item in seen_success:
            raise OrionBatchProtocolError(f"duplicate success id in 207: {item}")
        if item not in requested_set:
            raise OrionBatchProtocolError(f"unknown success id not in request: {item}")
        seen_success.add(item)
        success_ids.append(item)

    permanent: List[BatchEntityError] = []
    retryable: List[str] = []
    seen_error: set[str] = set()
    for err in errors_raw:
        if not isinstance(err, dict):
            raise OrionBatchProtocolError(f"207 error entry not an object: {err!r}")
        eid = err.get("entityId")
        if not isinstance(eid, str) or not eid:
            raise OrionBatchProtocolError(f"207 error missing entityId: {err!r}")
        if eid in seen_error:
            raise OrionBatchProtocolError(f"duplicate error id in 207: {eid}")
        if eid not in requested_set:
            raise OrionBatchProtocolError(f"unknown error id not in request: {eid}")
        if eid in seen_success:
            raise OrionBatchProtocolError(
                f"id appears in both success and errors: {eid}"
            )
        seen_error.add(eid)
        status = _parse_error_status(err)
        nested = err.get("error") if isinstance(err.get("error"), dict) else {}
        title = str(nested.get("title") or "")
        detail = str(nested.get("detail") or "")
        be = BatchEntityError(
            entity_id=eid, status=status, title=title, detail=detail
        )
        if status in PERMANENT_STATUS:
            permanent.append(be)
        else:
            # transient or unexpected → retryable subset
            retryable.append(eid)

    accounted = seen_success | seen_error
    unaccounted = [eid for eid in requested_ids if eid not in accounted]
    # Unaccounted → unknown / retryable (never success)
    retryable.extend(unaccounted)

    return BatchUpsertResult(
        http_status=207,
        success_ids=tuple(success_ids),
        retryable_error_ids=tuple(retryable),
        permanent_errors=tuple(permanent),
        ambiguous_ids=tuple(unaccounted),
    )


def batch_upsert_entities(entities: Sequence[dict]) -> BatchUpsertResult:
    """
    Batch upsert via entityOperations/upsert?options=update.

    Returns BatchUpsertResult for HTTP outcomes and transport ambiguity.
    Raises OrionBatchProtocolError when the 207 body/invariants are untrustworthy.
    Does not read _created; warms _created only for confirmed success IDs.
    """
    if not entities:
        return BatchUpsertResult(
            http_status=204,
            success_ids=(),
            retryable_error_ids=(),
            permanent_errors=(),
            ambiguous_ids=(),
        )

    requested_ids = [str(e["id"]) for e in entities]
    url = _batch_upsert_url()
    session = get_session()
    timeout = _batch_http_timeout()
    t0 = time.perf_counter()
    try:
        r = session.post(url, json=list(entities), headers=HEADERS, timeout=timeout)
    except requests.exceptions.Timeout:
        _record_http("batch", "BATCH_UPSERT", 0, t0, False)
        return BatchUpsertResult(
            http_status=None,
            success_ids=(),
            retryable_error_ids=(),
            permanent_errors=(),
            ambiguous_ids=tuple(requested_ids),
        )
    except requests.exceptions.RequestException:
        _record_http("batch", "BATCH_UPSERT", 0, t0, False)
        return BatchUpsertResult(
            http_status=None,
            success_ids=(),
            retryable_error_ids=(),
            permanent_errors=(),
            ambiguous_ids=tuple(requested_ids),
        )

    status = r.status_code
    _record_http("batch", "BATCH_UPSERT", status, t0, status in (201, 204, 207))

    if status in (201, 204):
        _mark_created_many(requested_ids)
        return BatchUpsertResult(
            http_status=status,
            success_ids=tuple(requested_ids),
            retryable_error_ids=(),
            permanent_errors=(),
            ambiguous_ids=(),
        )

    if status == 207:
        try:
            body = r.json()
        except Exception as e:
            raise OrionBatchProtocolError(
                f"207 body unparseable as JSON: {e}"
            ) from e
        result = _validate_and_split_207(requested_ids, body)
        if result.success_ids:
            _mark_created_many(result.success_ids)
        return result

    if status in PERMANENT_STATUS:
        permanent = tuple(
            BatchEntityError(entity_id=eid, status=status) for eid in requested_ids
        )
        return BatchUpsertResult(
            http_status=status,
            success_ids=(),
            retryable_error_ids=(),
            permanent_errors=permanent,
            ambiguous_ids=(),
        )

    if status in TRANSIENT_STATUS:
        return BatchUpsertResult(
            http_status=status,
            success_ids=(),
            retryable_error_ids=tuple(requested_ids),
            permanent_errors=(),
            ambiguous_ids=(),
        )

    # Unexpected status → treat all as retryable (not success)
    return BatchUpsertResult(
        http_status=status,
        success_ids=(),
        retryable_error_ids=tuple(requested_ids),
        permanent_errors=(),
        ambiguous_ids=(),
    )
