"""Reproducible random seeds for Python, NumPy, TensorFlow."""
from __future__ import annotations

import os
import random

import numpy as np


def set_global_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)
    try:
        import tensorflow as tf

        tf.random.set_seed(seed)
    except ImportError:
        pass
