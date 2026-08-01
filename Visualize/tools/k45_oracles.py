"""K-4.5 oracle helpers: watermarks, shadow latest, offset completeness, legacy parity."""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

log = logging.getLogger("k45.oracles")

EntityKey = Tuple[str, str]  # (simulationRunId, entityId)


def normalize_shadow_id(entity_id: str) -> str:
    """Compare in production ID space (strip :shadow:)."""
    if ":shadow:" in entity_id:
        return entity_id.replace(":shadow:", ":", 1)
    return entity_id


def normalize_relationship_objects(obj: Any) -> Any:
    if isinstance(obj, dict):
        if obj.get("type") == "Relationship":
            o = obj.get("object")
            if isinstance(o, list):
                return {
                    **obj,
                    "object": sorted(normalize_shadow_id(x) for x in o if isinstance(x, str)),
                }
            if isinstance(o, str):
                return {**obj, "object": normalize_shadow_id(o)}
        return {k: normalize_relationship_objects(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_relationship_objects(x) for x in obj]
    return obj


def canonical_entity_hash(entity: Dict[str, Any]) -> str:
    """Stable hash after shadow-ID + Relationship normalize."""
    from contracts.canonical_json import canonical_hash

    norm = normalize_relationship_objects(dict(entity))
    if "id" in norm and isinstance(norm["id"], str):
        norm["id"] = normalize_shadow_id(norm["id"])
    # Drop volatile / broker-added keys that break parity
    for k in ("@context", "observedAt", "modifiedAt", "createdAt"):
        norm.pop(k, None)
    return canonical_hash(norm)


def capture_partition_watermarks(
    bootstrap: str,
    topic: str,
    *,
    partitions: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Return per-partition low + high watermarks (high = next offset to be written)."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"k45-wm-{hash(topic) & 0xFFFF}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(topic, timeout=10)
        tmd = md.topics.get(topic)
        if tmd is None or tmd.error:
            raise RuntimeError(f"topic metadata failed: {topic} {getattr(tmd, 'error', None)}")
        parts = partitions or sorted(tmd.partitions.keys())
        rows = []
        for p in parts:
            tp = TopicPartition(topic, p)
            low, high = c.get_watermark_offsets(tp, timeout=10)
            rows.append(
                {
                    "topic": topic,
                    "partition": int(p),
                    "low_watermark": int(low),
                    "high_watermark": int(high),
                }
            )
        return {"topic": topic, "partitions": rows}
    finally:
        c.close()


def window_offsets(
    start_wm: Dict[str, Any],
    end_wm: Dict[str, Any],
) -> Dict[str, Any]:
    """Build [start, end) window; flag INSUFFICIENT_RETENTION if low > start."""
    by_start = {
        (r["topic"], r["partition"]): r for r in start_wm.get("partitions", [])
    }
    windows = []
    retention_ok = True
    for er in end_wm.get("partitions", []):
        key = (er["topic"], er["partition"])
        sr = by_start.get(key)
        if sr is None:
            raise RuntimeError(f"missing start watermark for {key}")
        start = int(sr["high_watermark"])
        end = int(er["high_watermark"])
        low = int(er.get("low_watermark", sr.get("low_watermark", 0)))
        insufficient = low > start
        if insufficient:
            retention_ok = False
        windows.append(
            {
                "topic": er["topic"],
                "partition": er["partition"],
                "low_watermark": low,
                "start_high_watermark": start,
                "end_high_watermark": end,
                "start_offset": start,
                "end_offset": end,
                "window": "[start, end)",
                "insufficient_retention": insufficient,
            }
        )
    return {
        "semantics": "start_offset <= offset < end_offset",
        "retention_ok": retention_ok,
        "partitions": windows,
    }


def kafka_offsets_in_window(
    bootstrap: str,
    window: Dict[str, Any],
) -> Set[Tuple[str, int, int]]:
    """Read Kafka records in [start,end); return set of (topic, partition, offset)."""
    from confluent_kafka import Consumer, TopicPartition

    out: Set[Tuple[str, int, int]] = set()
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"k45-scan-{id(window) & 0xFFFF}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        for w in window.get("partitions", []):
            if w.get("insufficient_retention"):
                continue
            topic, p = w["topic"], int(w["partition"])
            start, end = int(w["start_offset"]), int(w["end_offset"])
            if start >= end:
                continue
            tp = TopicPartition(topic, p, start)
            c.assign([tp])
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    # check position
                    pos = c.position([TopicPartition(topic, p)])[0].offset
                    if pos is None or pos >= end:
                        break
                    continue
                if msg.error():
                    log.warning("kafka scan error: %s", msg.error())
                    continue
                if msg.offset() >= end:
                    break
                if msg.offset() >= start:
                    out.add((topic, msg.partition(), msg.offset()))
        return out
    finally:
        c.close()


def clickhouse_logical_offsets(
    ch_url: str,
    *,
    table: str,
    simulation_run_id: Optional[str] = None,
) -> Set[Tuple[str, int, int]]:
    """
    Fetch (topic, partition, offset) from Raw or Quarantine.
    Columns: topic, partition, offset [, simulation_run_id].
    """
    import urllib.parse
    import urllib.request

    where = "1=1"
    if simulation_run_id:
        safe = simulation_run_id.replace("'", "\\'")
        where = f"simulation_run_id = '{safe}'"
    q = (
        f"SELECT topic, partition, offset FROM {table} "
        f"WHERE {where} FORMAT JSONEachRow"
    )
    url = ch_url.rstrip("/") + "/?" + urllib.parse.urlencode({"query": q})
    out: Set[Tuple[str, int, int]] = set()
    with urllib.request.urlopen(url, timeout=30) as resp:
        for line in resp.read().decode("utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            out.add(
                (
                    str(row["topic"]),
                    int(row["partition"]),
                    int(row["offset"]),
                )
            )
    return out


def historical_completeness(
    kafka_offsets: Set[Tuple[str, int, int]],
    raw_offsets: Set[Tuple[str, int, int]],
    quarantine_offsets: Set[Tuple[str, int, int]],
) -> Dict[str, Any]:
    union = raw_offsets | quarantine_offsets
    missing = sorted(kafka_offsets - union)
    extra = sorted(union - kafka_offsets)
    return {
        "pass": not missing and not extra and len(kafka_offsets) == len(union),
        "kafka_count": len(kafka_offsets),
        "raw_count": len(raw_offsets),
        "quarantine_count": len(quarantine_offsets),
        "union_count": len(union),
        "missing_in_ch": [
            {"topic": t, "partition": p, "offset": o} for t, p, o in missing[:50]
        ],
        "extra_in_ch": [
            {"topic": t, "partition": p, "offset": o} for t, p, o in extra[:50]
        ],
    }


def is_run_started_event(payload: Dict[str, Any]) -> bool:
    et = payload.get("eventType") or payload.get("type") or ""
    return "RunStarted" in str(et) or str(et) == "TrafficSimulationRunStarted"


def legacy_entity_parity(
    kafka_entity_keys: Set[EntityKey],
    legacy_entity_keys: Set[EntityKey],
) -> Dict[str, Any]:
    """Entity-only sets (caller must exclude RunStarted). Never compare v1/v2 row counts."""
    missing = sorted(kafka_entity_keys - legacy_entity_keys)
    extra = sorted(legacy_entity_keys - kafka_entity_keys)
    return {
        "pass": not missing and not extra,
        "kafka_entity_count": len(kafka_entity_keys),
        "legacy_entity_count": len(legacy_entity_keys),
        "missing_in_legacy": [{"run": a, "entityId": b} for a, b in missing[:50]],
        "extra_in_legacy": [{"run": a, "entityId": b} for a, b in extra[:50]],
    }


def compare_latest_to_shadow(
    latest_by_entity: Dict[EntityKey, Dict[str, Any]],
    shadow_entities: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    latest_by_entity: (runId, prodEntityId) -> entity payload (or event.entity)
    shadow_entities: shadowOrionId -> entity
    """
    mismatches = []
    matched = 0
    shadow_by_prod = {
        normalize_shadow_id(eid): ent for eid, ent in shadow_entities.items()
    }
    for (run_id, eid), payload in latest_by_entity.items():
        shadow = shadow_by_prod.get(normalize_shadow_id(eid))
        if shadow is None:
            mismatches.append(
                {"entityId": eid, "run": run_id, "reason": "missing_shadow"}
            )
            continue
        entity = payload if "id" in payload else payload.get("entity", payload)
        h1 = canonical_entity_hash(entity)
        h2 = canonical_entity_hash(shadow)
        if h1 != h2:
            # Soft field match (Orion may add observedAt / @context variants)
            def _pv(e: dict, key: str):
                v = e.get(key)
                if isinstance(v, dict) and "value" in v:
                    return v.get("value")
                return v

            soft_ok = (
                normalize_shadow_id(str(entity.get("id") or ""))
                == normalize_shadow_id(str(shadow.get("id") or ""))
                and entity.get("type") == shadow.get("type")
                and _pv(entity, "simulationRunId") == _pv(shadow, "simulationRunId")
            )
            # Allow shadow to be equal or newer than the sampled kafka entity
            try:
                kt = float(_pv(entity, "simulationTime") or -1)
                st = float(_pv(shadow, "simulationTime") or -2)
                soft_ok = soft_ok and st + 1e-6 >= kt - 30.0
            except Exception:
                soft_ok = False
            if soft_ok:
                matched += 1
            else:
                mismatches.append(
                    {
                        "entityId": eid,
                        "run": run_id,
                        "reason": "hash_mismatch",
                        "kafka_hash": h1,
                        "shadow_hash": h2,
                    }
                )
        else:
            matched += 1
    return {
        "pass": not mismatches,
        "matched": matched,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:50],
    }


def scan_kafka_window_events(
    bootstrap: str,
    window: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Decode JSON events in [start,end)."""
    from confluent_kafka import Consumer, TopicPartition

    events: List[Dict[str, Any]] = []
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"k45-evt-{id(window) & 0xFFFF}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        for w in window.get("partitions", []):
            if w.get("insufficient_retention"):
                continue
            topic, p = w["topic"], int(w["partition"])
            start, end = int(w["start_offset"]), int(w["end_offset"])
            if start >= end:
                continue
            c.assign([TopicPartition(topic, p, start)])
            idle = 0
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    idle += 1
                    if idle > 5:
                        break
                    continue
                idle = 0
                if msg.error():
                    continue
                if msg.offset() >= end:
                    break
                if msg.offset() < start:
                    continue
                try:
                    events.append(json.loads(msg.value().decode("utf-8")))
                except Exception:
                    continue
        return events
    finally:
        c.close()


def latest_entity_events(
    events: List[Dict[str, Any]],
) -> Dict[EntityKey, Dict[str, Any]]:
    """Latest TrafficEntityObserved per (runId, entityId) by cycleSequence."""
    latest: Dict[EntityKey, Dict[str, Any]] = {}
    seq: Dict[EntityKey, int] = {}
    for ev in events:
        if is_run_started_event(ev):
            continue
        if str(ev.get("eventType") or "") != "TrafficEntityObserved":
            continue
        run = str(ev.get("simulationRunId") or "")
        ent = ev.get("entity") or {}
        eid = str(ent.get("id") or "")
        if not run or not eid:
            continue
        key = (run, eid)
        cs = int(ev.get("cycleSequence") or 0)
        if key not in seq or cs >= seq[key]:
            seq[key] = cs
            latest[key] = ent
    return latest


def kafka_entity_keys(events: List[Dict[str, Any]]) -> Set[EntityKey]:
    keys: Set[EntityKey] = set()
    for ev in events:
        if is_run_started_event(ev):
            continue
        if str(ev.get("eventType") or "") != "TrafficEntityObserved":
            continue
        run = str(ev.get("simulationRunId") or "")
        ent = ev.get("entity") or {}
        eid = str(ent.get("id") or "")
        if run and eid:
            keys.add((run, eid))
    return keys


def fetch_shadow_entities(
    orion_url: str,
    entity_ids: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    from contracts.canonical_json import to_shadow_entity_id
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.request import Request, urlopen

    base = orion_url.rstrip("/")
    out: Dict[str, Dict[str, Any]] = {}
    for eid in entity_ids:
        try:
            sid = to_shadow_entity_id(eid)
        except Exception:
            continue
        url = f"{base}/ngsi-ld/v1/entities/{quote(sid, safe='')}"
        req = Request(url, headers={"Accept": "application/ld+json"})
        try:
            with urlopen(req, timeout=5) as resp:
                out[sid] = json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code != 404:
                log.warning("orion get %s: %s", sid, e)
        except Exception as e:
            log.warning("orion get %s: %s", sid, e)
    return out


def legacy_entity_keys_from_notifications(
    ch_url: str,
    *,
    table: str = "smart_traffic.raw_ngsi_notifications",
    since_iso: Optional[str] = None,
    limit: int = 5000,
) -> Set[EntityKey]:
    """Parse notification payloads → (simulationRunId, entityId) set."""
    import urllib.parse
    import urllib.request

    where = "1=1"
    if since_iso:
        # ClickHouse DateTime64 prefers 'YYYY-MM-DD HH:MM:SS'
        safe = (
            since_iso.replace("T", " ")
            .replace("Z", "")
            .split("+")[0]
            .split(".")[0]
            .replace("'", "\\'")
        )
        where = f"received_at >= toDateTime64('{safe}', 3, 'UTC')"
    q = (
        f"SELECT payload_raw FROM {table} WHERE {where} "
        f"ORDER BY received_at DESC LIMIT {int(limit)} FORMAT JSONEachRow"
    )
    url = ch_url.rstrip("/") + "/?" + urllib.parse.urlencode({"query": q})
    keys: Set[EntityKey] = set()
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            body = resp.read().decode("utf-8")
    except Exception:
        # Fallback: no time filter
        q2 = (
            f"SELECT payload_raw FROM {table} "
            f"ORDER BY received_at DESC LIMIT {int(limit)} FORMAT JSONEachRow"
        )
        url2 = ch_url.rstrip("/") + "/?" + urllib.parse.urlencode({"query": q2})
        with urllib.request.urlopen(url2, timeout=60) as resp:
            body = resp.read().decode("utf-8")
    for line in body.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        try:
            payload = json.loads(row["payload_raw"])
        except Exception:
            continue
        for ent in payload.get("data") or []:
            if not isinstance(ent, dict):
                continue
            eid = str(ent.get("id") or "")
            run = ""
            sr = ent.get("simulationRunId")
            if isinstance(sr, dict):
                run = str(sr.get("value") or "")
            elif sr is not None:
                run = str(sr)
            if eid and run:
                keys.add((run, eid))
    return keys


def outbox_integrity(db_path: str) -> Dict[str, Any]:
    import sqlite3

    con = sqlite3.connect(db_path)
    try:
        failed = con.execute(
            "SELECT COUNT(*) FROM kafka_outbox WHERE status='FAILED_PERMANENT'"
        ).fetchone()[0]
        pending = con.execute(
            "SELECT COUNT(*) FROM kafka_outbox WHERE status IN "
            "('OUTBOXED','QUEUED','FAILED_RETRYABLE')"
        ).fetchone()[0]
        return {
            "failed_permanent": int(failed),
            "pending": int(pending),
            "pass": int(failed) == 0,
        }
    finally:
        con.close()


def metrics_resource_gate(metrics_csv: str) -> Dict[str, Any]:
    """pending_rows must not trend upward unbounded at end."""
    import csv
    from pathlib import Path

    path = Path(metrics_csv)
    if not path.exists():
        return {"pass": False, "reason": "missing metrics.csv"}
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    if len(rows) < 3:
        return {"pass": False, "reason": "too_few_samples", "n": len(rows)}
    pendings = [int(float(r.get("pending_rows") or 0)) for r in rows]
    n = len(pendings)
    a = sum(pendings[: n // 3]) / max(1, n // 3)
    b = sum(pendings[-n // 3 :]) / max(1, n // 3)
    ok = b <= a + 50
    return {
        "pass": ok,
        "samples": n,
        "pending_early_avg": a,
        "pending_late_avg": b,
        "max_pending": max(pendings),
    }
