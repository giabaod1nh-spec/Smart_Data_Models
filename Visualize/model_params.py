"""Compatibility shim — same module as configuration.model_params."""
import importlib
import sys

sys.modules[__name__] = importlib.import_module("configuration.model_params")
