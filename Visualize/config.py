"""Permanent compatibility shim — same module as configuration.config."""
import importlib
import sys

sys.modules[__name__] = importlib.import_module("configuration.config")
