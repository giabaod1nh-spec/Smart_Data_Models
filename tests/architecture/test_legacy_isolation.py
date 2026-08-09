from __future__ import annotations

from arch_utils import collect_imports, iter_py_files, read_text
from ownership_matrix import (
    PROJECTOR_PACKAGES,
    RAW_CONSUMER_PACKAGES,
    REPO_ROOT,
    WEBHOOK_PACKAGES,
)


def test_webhook_marked_legacy():
    init = read_text(REPO_ROOT / "de" / "webhook" / "__init__.py")
    assert "LEGACY = True" in init
    assert "K-8" in init
    assert "K-6b" in init
    main = read_text(REPO_ROOT / "de" / "webhook" / "main.py")
    assert "LEGACY" in main


def test_raw_does_not_import_webhook():
    for p in iter_py_files(RAW_CONSUMER_PACKAGES):
        imports = collect_imports(p)
        assert not any(i.startswith("de.webhook") for i in imports)


def test_webhook_does_not_bridge_to_kafka():
    for p in iter_py_files(WEBHOOK_PACKAGES):
        text = read_text(p).lower()
        assert "confluent_kafka" not in text
        assert "produce(" not in text or "notification" in text
