"""
Live rolling-window inference for TraCI snapshots.

Enabled when:
  - env TRAFFIC_RF_ENABLED=true (default true if model file exists)
  - model artifact is present at artifacts/ml/traffic_anomaly_rf.joblib

Attaches to snapshot:
  anomaly_label, anomaly_score, anomaly_probabilities, ml_traffic_status_hint
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque, Dict, Mapping, Optional

from .classifier import DEFAULT_MODEL_PATH, TrafficAnomalyRF
from .features import aggregate_snapshot_tick, extract_features_from_snapshot_history

log = logging.getLogger(__name__)

_WINDOW = int(os.getenv("TRAFFIC_RF_WINDOW", "12"))
_MIN_TICKS = int(os.getenv("TRAFFIC_RF_MIN_TICKS", "6"))


def _env_enabled() -> Optional[bool]:
    raw = os.getenv("TRAFFIC_RF_ENABLED")
    if raw is None:
        return None
    return raw.strip().lower() in ("1", "true", "yes", "on")


class RollingAnomalyClassifier:
    def __init__(
        self,
        model_path: Optional[Path] = None,
        window: int = _WINDOW,
        min_ticks: int = _MIN_TICKS,
    ):
        self.model_path = Path(model_path or DEFAULT_MODEL_PATH)
        self.window = window
        self.min_ticks = min_ticks
        self._hist: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )
        self._clf: Optional[TrafficAnomalyRF] = None
        self._load_attempted = False

    @property
    def enabled(self) -> bool:
        flag = _env_enabled()
        if flag is False:
            return False
        if flag is True:
            return self.model_path.is_file()
        # Auto: enable only if artifact exists
        return self.model_path.is_file()

    def _ensure_model(self) -> bool:
        if self._clf is not None:
            return True
        if self._load_attempted:
            return False
        self._load_attempted = True
        if not self.model_path.is_file():
            log.info("Traffic RF model not found at %s — classifier idle", self.model_path)
            return False
        try:
            self._clf = TrafficAnomalyRF.load(self.model_path)
            log.info("Loaded traffic RF model from %s", self.model_path)
            return True
        except Exception as e:
            log.warning("Failed to load traffic RF model: %s", e)
            return False

    def update_node(self, node_id: str, snapshot: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        """Push one observation tick; return prediction dict or None if not ready."""
        if not self.enabled:
            return None
        if not self._ensure_model() or self._clf is None:
            return None
        tick = aggregate_snapshot_tick(snapshot)
        self._hist[node_id].append(tick)
        hist = list(self._hist[node_id])
        if len(hist) < self.min_ticks:
            return None
        feat = extract_features_from_snapshot_history(hist)
        pred = self._clf.predict_features(feat)
        return pred.to_dict()

    def annotate_snapshot(self, node_id: str, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """Mutate snapshot with anomaly fields when a prediction is available."""
        pred = self.update_node(node_id, snapshot)
        if not pred:
            return snapshot
        snapshot["anomaly_label"] = pred["anomalyLabel"]
        snapshot["anomaly_score"] = pred["anomalyScore"]
        snapshot["anomaly_probabilities"] = pred["probabilities"]
        snapshot["ml_traffic_status_hint"] = pred["trafficStatusHint"]
        return snapshot


# Process-wide singleton used by SumoBackend observation path
_ROLLING: Optional[RollingAnomalyClassifier] = None


def get_rolling_classifier() -> RollingAnomalyClassifier:
    global _ROLLING
    if _ROLLING is None:
        _ROLLING = RollingAnomalyClassifier()
    return _ROLLING
