"""Ensure repo root and Visualize/ are on sys.path for SUMO backend imports."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VISUALIZE_DIR = _REPO_ROOT / "Visualize"


def ensure_paths() -> None:
    for p in (_REPO_ROOT, _VISUALIZE_DIR):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)


def repo_root() -> Path:
    return _REPO_ROOT


def visualize_dir() -> Path:
    return _VISUALIZE_DIR
