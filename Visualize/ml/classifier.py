"""Random Forest traffic anomaly classifier — train / load / predict."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split

from .features import FEATURE_NAMES, extract_features_from_series, features_to_vector
from .labels import LABEL_NAMES, LABEL_TO_TRAFFIC_STATUS

DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[1] / "artifacts" / "ml" / "traffic_anomaly_rf.joblib"
DEFAULT_META_PATH = Path(__file__).resolve().parents[1] / "artifacts" / "ml" / "traffic_anomaly_rf.meta.json"


@dataclass
class Prediction:
    label: str
    anomaly_score: float
    probabilities: Dict[str, float]
    traffic_status_hint: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anomalyLabel": self.label,
            "anomalyScore": self.anomaly_score,
            "probabilities": dict(self.probabilities),
            "trafficStatusHint": self.traffic_status_hint,
        }


class TrafficAnomalyRF:
    """Thin wrapper around sklearn RandomForestClassifier."""

    def __init__(self, model: Optional[RandomForestClassifier] = None):
        self.model = model
        self.feature_names: List[str] = list(FEATURE_NAMES)
        self.classes_: List[str] = list(LABEL_NAMES)

    @staticmethod
    def build_estimator(
        *,
        n_estimators: int = 200,
        max_depth: Optional[int] = 12,
        random_state: int = 42,
        class_weight: str = "balanced_subsample",
    ) -> RandomForestClassifier:
        return RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=2,
            random_state=random_state,
            n_jobs=-1,
            class_weight=class_weight,
        )

    def fit(
        self,
        X: np.ndarray,
        y: np.ndarray,
        *,
        test_size: float = 0.2,
        random_state: int = 42,
        **rf_kwargs: Any,
    ) -> Dict[str, Any]:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
        self.model = self.build_estimator(random_state=random_state, **rf_kwargs)
        self.model.fit(X_train, y_train)
        self.classes_ = [str(c) for c in self.model.classes_]
        y_pred = self.model.predict(X_test)
        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        cm = confusion_matrix(y_test, y_pred, labels=self.classes_).tolist()
        importances = {
            name: float(imp)
            for name, imp in zip(self.feature_names, self.model.feature_importances_)
        }
        return {
            "n_train": int(X_train.shape[0]),
            "n_test": int(X_test.shape[0]),
            "accuracy": float(report.get("accuracy", 0.0)),
            "classification_report": report,
            "confusion_matrix": {"labels": self.classes_, "matrix": cm},
            "feature_importances": dict(
                sorted(importances.items(), key=lambda kv: kv[1], reverse=True)
            ),
        }

    def predict_vector(self, x: Union[np.ndarray, Sequence[float]]) -> Prediction:
        if self.model is None:
            raise RuntimeError("Model not loaded/trained")
        arr = np.asarray(x, dtype=float).reshape(1, -1)
        if arr.shape[1] != len(self.feature_names):
            raise ValueError(
                f"Expected {len(self.feature_names)} features, got {arr.shape[1]}"
            )
        label = str(self.model.predict(arr)[0])
        proba = self.model.predict_proba(arr)[0]
        probs = {str(c): float(p) for c, p in zip(self.model.classes_, proba)}
        # anomalyScore = 1 - P(NORMAL); fallback to max non-normal if NORMAL absent
        p_normal = float(probs.get("NORMAL", 0.0))
        anomaly_score = float(max(0.0, min(1.0, 1.0 - p_normal)))
        return Prediction(
            label=label,
            anomaly_score=anomaly_score,
            probabilities=probs,
            traffic_status_hint=LABEL_TO_TRAFFIC_STATUS.get(label, "UNKNOWN"),
        )

    def predict_features(self, feat: Mapping[str, float]) -> Prediction:
        return self.predict_vector(features_to_vector(feat))

    def predict_series(
        self,
        *,
        speeds: Sequence[Any],
        occupancies: Sequence[Any],
        queues_m: Sequence[Any],
        vehicle_counts: Sequence[Any],
        arrival_rates: Optional[Sequence[Any]] = None,
        approach_speeds: Optional[Sequence[Any]] = None,
    ) -> Prediction:
        feat = extract_features_from_series(
            speeds=speeds,
            occupancies=occupancies,
            queues_m=queues_m,
            vehicle_counts=vehicle_counts,
            arrival_rates=arrival_rates,
            approach_speeds=approach_speeds,
        )
        return self.predict_features(feat)

    def save(self, path: Union[str, Path] = DEFAULT_MODEL_PATH) -> Path:
        if self.model is None:
            raise RuntimeError("Nothing to save")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "model": self.model,
            "feature_names": self.feature_names,
            "classes": self.classes_,
        }
        joblib.dump(payload, path)
        return path

    @classmethod
    def load(cls, path: Union[str, Path] = DEFAULT_MODEL_PATH) -> "TrafficAnomalyRF":
        path = Path(path)
        payload = joblib.load(path)
        obj = cls(model=payload["model"])
        obj.feature_names = list(payload.get("feature_names") or FEATURE_NAMES)
        obj.classes_ = list(payload.get("classes") or LABEL_NAMES)
        return obj

    @staticmethod
    def write_meta(meta: Mapping[str, Any], path: Union[str, Path] = DEFAULT_META_PATH) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return path
