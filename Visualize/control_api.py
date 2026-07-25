"""Compatibility shim — same module object as api.control_api (mutable engine)."""
import sys

from api import control_api as _impl

sys.modules[__name__] = _impl
