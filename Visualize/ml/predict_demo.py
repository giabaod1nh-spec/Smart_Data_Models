#!/usr/bin/env python3
"""Quick demo: load trained RF and classify three hand-crafted windows."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ml.classifier import DEFAULT_MODEL_PATH, TrafficAnomalyRF


def main() -> int:
    path = DEFAULT_MODEL_PATH
    if not path.is_file():
        print(f"Model not found: {path}\nTrain first: python -m ml.train")
        return 1
    clf = TrafficAnomalyRF.load(path)
    cases = {
        "normal": dict(
            speeds=[42, 41, 40, 43, 42],
            occupancies=[18, 20, 19, 21, 20],
            queues_m=[8, 9, 7, 10, 8],
            vehicle_counts=[40, 42, 41, 43, 40],
            arrival_rates=[0.2] * 5,
            approach_speeds=[42, 40, 41, 43],
        ),
        "congestion": dict(
            speeds=[40, 34, 28, 22, 16, 12],
            occupancies=[25, 35, 45, 55, 65, 72],
            queues_m=[15, 30, 45, 60, 80, 100],
            vehicle_counts=[60, 80, 100, 130, 160, 190],
            arrival_rates=[0.3, 0.35, 0.4, 0.42, 0.45, 0.48],
            approach_speeds=[12, 14, 13, 15],
        ),
        "accident": dict(
            speeds=[45, 44, 43, 6, 4, 3],
            occupancies=[22, 24, 26, 75, 85, 90],
            queues_m=[12, 14, 16, 90, 120, 140],
            vehicle_counts=[50, 52, 55, 160, 180, 200],
            arrival_rates=[0.3, 0.3, 0.28, 0.05, 0.04, 0.03],
            approach_speeds=[4, 28, 30, 32],
        ),
    }
    for name, kwargs in cases.items():
        pred = clf.predict_series(**kwargs)
        print(f"[{name}] → {json.dumps(pred.to_dict(), ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
