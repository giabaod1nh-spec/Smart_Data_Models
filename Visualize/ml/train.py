#!/usr/bin/env python3
"""
Train Random Forest traffic anomaly classifier.

Usage (from Visualize/):
  .venv/bin/python -m ml.train
  .venv/bin/python -m ml.train --n-per-class 600 --out artifacts/ml/traffic_anomaly_rf.joblib
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow `python -m ml.train` from Visualize/
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ml.classifier import DEFAULT_META_PATH, DEFAULT_MODEL_PATH, TrafficAnomalyRF
from ml.features import FEATURE_NAMES
from ml.synthetic import generate_dataset


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Train traffic anomaly Random Forest")
    p.add_argument("--n-per-class", type=int, default=500)
    p.add_argument("--window", type=int, default=12, help="Nominal window length for synthetic series")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--test-size", type=float, default=0.2)
    p.add_argument("--n-estimators", type=int, default=200)
    p.add_argument("--max-depth", type=int, default=12)
    p.add_argument("--out", type=Path, default=DEFAULT_MODEL_PATH)
    p.add_argument("--meta-out", type=Path, default=DEFAULT_META_PATH)
    args = p.parse_args(argv)

    print(
        f"Generating synthetic dataset: n_per_class={args.n_per_class} "
        f"window~{args.window} features={len(FEATURE_NAMES)}"
    )
    X, y = generate_dataset(
        n_per_class=args.n_per_class,
        window=args.window,
        seed=args.seed,
    )
    print(f"Dataset shape: X={X.shape} classes={sorted(set(y.tolist()))}")

    clf = TrafficAnomalyRF()
    metrics = clf.fit(
        X,
        y,
        test_size=args.test_size,
        random_state=args.seed,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
    )
    out = clf.save(args.out)
    meta = {
        "model_path": str(out),
        "feature_names": FEATURE_NAMES,
        "labels": clf.classes_,
        "train_params": {
            "n_per_class": args.n_per_class,
            "window": args.window,
            "seed": args.seed,
            "n_estimators": args.n_estimators,
            "max_depth": args.max_depth,
        },
        "metrics": {
            "accuracy": metrics["accuracy"],
            "n_train": metrics["n_train"],
            "n_test": metrics["n_test"],
            "feature_importances": metrics["feature_importances"],
            "confusion_matrix": metrics["confusion_matrix"],
        },
    }
    TrafficAnomalyRF.write_meta(meta, args.meta_out)

    print(f"Saved model → {out}")
    print(f"Saved meta  → {args.meta_out}")
    print(f"Test accuracy: {metrics['accuracy']:.4f}")
    print("Top features:")
    for name, imp in list(metrics["feature_importances"].items())[:8]:
        print(f"  {name:28s} {imp:.4f}")
    print("Per-class report:")
    print(json.dumps(metrics["classification_report"], indent=2)[:1200])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
