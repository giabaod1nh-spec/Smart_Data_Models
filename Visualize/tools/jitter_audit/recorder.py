"""JSONL metric recorder for TraCI jitter audit."""
from __future__ import annotations

import gc
import json
import statistics
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


def _ns_ms(t0_ns: int, t1_ns: Optional[int] = None) -> float:
    end = t1_ns if t1_ns is not None else time.perf_counter_ns()
    return (end - t0_ns) / 1e6


@dataclass
class StepRecord:
    ts_wall: float
    sim_t: float
    step_index: int
    phase: str = "step"
    cycle_sequence: Optional[int] = None
    backend_step_ms: float = 0.0
    snapshot_capture_ms: float = 0.0
    entity_mapping_ms: float = 0.0
    deepcopy_ms: float = 0.0
    publish_cycle_build_ms: float = 0.0
    event_envelope_build_ms: float = 0.0
    canonical_hash_ms: float = 0.0
    json_serialize_ms: float = 0.0
    outbox_lock_wait_ms: float = 0.0
    outbox_begin_tx_ms: float = 0.0
    outbox_insert_rows_ms: float = 0.0
    outbox_commit_ms: float = 0.0
    outbox_append_total_ms: float = 0.0
    logging_ms: float = 0.0
    explicit_sleep_ms: float = 0.0
    loop_total_ms: float = 0.0
    step_gap_ms: float = 0.0
    gc_gen0: int = 0
    gc_gen1: int = 0
    gc_gen2: int = 0
    gc_pause_ms: float = 0.0
    cpu_pct: Optional[float] = None
    disk_busy: Optional[bool] = None
    outbox_pending_rows: Optional[int] = None
    outbox_lock_owner: Optional[str] = None
    sqlite_op: Optional[str] = None
    notes: Dict[str, Any] = field(default_factory=dict)

    def max_component(self) -> tuple[str, float]:
        pairs = {
            "backend_step_ms": self.backend_step_ms,
            "snapshot_capture_ms": self.snapshot_capture_ms,
            "entity_mapping_ms": self.entity_mapping_ms,
            "event_envelope_build_ms": self.event_envelope_build_ms,
            "canonical_hash_ms": self.canonical_hash_ms,
            "json_serialize_ms": self.json_serialize_ms,
            "outbox_lock_wait_ms": self.outbox_lock_wait_ms,
            "outbox_commit_ms": self.outbox_commit_ms,
            "outbox_append_total_ms": self.outbox_append_total_ms,
            "explicit_sleep_ms": self.explicit_sleep_ms,
            "logging_ms": self.logging_ms,
            "gc_pause_ms": self.gc_pause_ms,
        }
        k = max(pairs, key=pairs.get)
        return k, pairs[k]


class JitterRecorder:
    def __init__(self, out_path: Path) -> None:
        self.out_path = Path(out_path)
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._fh = self.out_path.open("a", encoding="utf-8")
        self.step_index = 0
        self._last_step_ns: Optional[int] = None
        self._loop_start_ns: Optional[int] = None
        self._gc_pause_ms = 0.0
        self._gc_lock = threading.Lock()
        self.outbox_stats: Dict[str, Any] = {
            "traci_lock_wait_ms": 0.0,
            "worker_lock_wait_ms": 0.0,
            "traci_lock_hold_ms": 0.0,
            "worker_lock_hold_ms": 0.0,
            "mark_queued_calls": 0,
            "mark_acked_calls": 0,
            "mark_queued_batch_rows": 0,
            "mark_acked_batch_rows": 0,
            "worker_tx_count": 0,
            "checkpoint_count": 0,
            "checkpoint_ms": 0.0,
            "cleanup_count": 0,
            "shared_connection": True,
            "shared_python_lock": False,
            "lock_type": "_WriteGate(_writer Lock + priority/background)",
        }
        self._install_gc_callback()

    def _install_gc_callback(self) -> None:
        def _cb(phase: str, info: dict) -> None:
            if phase == "stop":
                dur = info.get("elapsed", 0.0)
                with self._gc_lock:
                    self._gc_pause_ms += float(dur) * 1000.0

        gc.callbacks.append(_cb)

    def close(self) -> None:
        with self._lock:
            self._fh.close()

    def mark_loop_start(self) -> None:
        self._loop_start_ns = time.perf_counter_ns()

    def mark_sleep(self, ms: float) -> None:
        self._pending_sleep_ms = ms  # type: ignore[attr-defined]

    def record_step(
        self,
        *,
        sim_t: float,
        backend_step_ms: float,
        phase: str = "step",
        cycle_sequence: Optional[int] = None,
        publish: Optional[Dict[str, float]] = None,
        outbox: Optional[Dict[str, float]] = None,
        cpu_pct: Optional[float] = None,
        disk_busy: Optional[bool] = None,
        outbox_pending_rows: Optional[int] = None,
        outbox_lock_owner: Optional[str] = None,
        sqlite_op: Optional[str] = None,
        notes: Optional[Dict[str, Any]] = None,
    ) -> None:
        now_ns = time.perf_counter_ns()
        step_gap = 0.0
        if self._last_step_ns is not None:
            step_gap = _ns_ms(self._last_step_ns, now_ns)
        loop_total = 0.0
        if self._loop_start_ns is not None:
            loop_total = _ns_ms(self._loop_start_ns, now_ns)
        sleep_ms = getattr(self, "_pending_sleep_ms", 0.0)
        self._pending_sleep_ms = 0.0  # type: ignore[attr-defined]

        pub = publish or {}
        ob = outbox or {}
        with self._gc_lock:
            gc_pause = self._gc_pause_ms
            self._gc_pause_ms = 0.0

        rec = StepRecord(
            ts_wall=time.time(),
            sim_t=sim_t,
            step_index=self.step_index,
            phase=phase,
            cycle_sequence=cycle_sequence,
            backend_step_ms=backend_step_ms,
            snapshot_capture_ms=pub.get("snapshot_capture_ms", 0.0),
            entity_mapping_ms=pub.get("entity_mapping_ms", 0.0),
            deepcopy_ms=pub.get("deepcopy_ms", 0.0),
            publish_cycle_build_ms=pub.get("publish_cycle_build_ms", 0.0),
            event_envelope_build_ms=pub.get("event_envelope_build_ms", 0.0),
            canonical_hash_ms=pub.get("canonical_hash_ms", 0.0),
            json_serialize_ms=pub.get("json_serialize_ms", 0.0),
            outbox_lock_wait_ms=ob.get("outbox_lock_wait_ms", 0.0),
            outbox_begin_tx_ms=ob.get("outbox_begin_tx_ms", 0.0),
            outbox_insert_rows_ms=ob.get("outbox_insert_rows_ms", 0.0),
            outbox_commit_ms=ob.get("outbox_commit_ms", 0.0),
            outbox_append_total_ms=ob.get("outbox_append_total_ms", 0.0),
            explicit_sleep_ms=sleep_ms,
            loop_total_ms=loop_total,
            step_gap_ms=step_gap,
            gc_gen0=gc.get_count()[0],
            gc_gen1=gc.get_count()[1],
            gc_gen2=gc.get_count()[2],
            gc_pause_ms=gc_pause,
            cpu_pct=cpu_pct,
            disk_busy=disk_busy,
            outbox_pending_rows=outbox_pending_rows,
            outbox_lock_owner=outbox_lock_owner,
            sqlite_op=sqlite_op,
            notes=notes or {},
        )
        self.step_index += 1
        self._last_step_ns = now_ns
        self._loop_start_ns = now_ns
        with self._lock:
            self._fh.write(json.dumps(asdict(rec), ensure_ascii=True) + "\n")
            self._fh.flush()

    @staticmethod
    def load_records(path: Path, *, skip_warmup_sec: float = 5.0) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not path.is_file():
            return rows
        t0: Optional[float] = None
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            ts = float(row.get("ts_wall") or 0)
            if t0 is None:
                t0 = ts
            if skip_warmup_sec and (ts - t0) < skip_warmup_sec:
                continue
            rows.append(row)
        return rows

    @staticmethod
    def percentile(values: List[float], p: float) -> Optional[float]:
        if not values:
            return None
        s = sorted(values)
        idx = min(len(s) - 1, max(0, int(round(p * (len(s) - 1)))))
        return s[idx]

    @staticmethod
    def summarize(records: List[Dict[str, Any]], key: str) -> Dict[str, Optional[float]]:
        vals = [float(r[key]) for r in records if key in r and r[key] is not None]
        if not vals:
            return {"p50": None, "p95": None, "p99": None, "max": None, "n": 0}
        return {
            "p50": JitterRecorder.percentile(vals, 0.50),
            "p95": JitterRecorder.percentile(vals, 0.95),
            "p99": JitterRecorder.percentile(vals, 0.99),
            "max": max(vals),
            "n": len(vals),
        }

    @staticmethod
    def top_spikes(records: List[Dict[str, Any]], *, threshold_ms: float = 50.0, n: int = 20):
        spikes = [r for r in records if float(r.get("step_gap_ms") or 0) >= threshold_ms]
        spikes.sort(key=lambda r: float(r.get("step_gap_ms") or 0), reverse=True)
        out = []
        for r in spikes[:n]:
            rec = StepRecord(**{k: r.get(k) for k in StepRecord.__dataclass_fields__})  # type: ignore
            comp, val = rec.max_component()
            out.append({**r, "max_component": comp, "max_component_ms": val})
        return out


_RECORDER: Optional[JitterRecorder] = None


def get_recorder() -> Optional[JitterRecorder]:
    return _RECORDER


def init_recorder(out_path: Path) -> JitterRecorder:
    global _RECORDER
    _RECORDER = JitterRecorder(out_path)
    return _RECORDER
