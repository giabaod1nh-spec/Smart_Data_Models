"""Gold 3 runtime package.

Isolated from the pure Gold 2 package ``de/gold``: every module here may perform
I/O, but the only Gold 2 entry point it is allowed to call is
``GoldTransformationEngine.transform(records, context)``.
"""
from __future__ import annotations

from typing import Final

PROCESSOR_NAME: Final = "de-gold-runtime"
PROCESSOR_VERSION: Final = "gold-runtime-v1"
RUNTIME_MIGRATION_VERSION: Final = "gold-runtime-v1"
LINEAGE_HASH_CONTRACT: Final = "gold-lineage-hash-v1"

__all__ = [
    "PROCESSOR_NAME",
    "PROCESSOR_VERSION",
    "RUNTIME_MIGRATION_VERSION",
    "LINEAGE_HASH_CONTRACT",
]
