from __future__ import annotations

from pathlib import Path

from arch_utils import collect_imports, iter_py_files, read_text
from ownership_matrix import REPO_ROOT

RAW_V1_TABLE = "raw_ngsi_notifications"
SCAN_EXTENSIONS = {".py", ".md", ".yml", ".yaml", ".env", ".sql", ".json"}
ALLOWED_RAW_V1_ROOTS = (Path("de/webhook"),)
ALLOWED_RAW_V1_FILES = {
    Path("de/migrations/001_create_raw_ngsi_notifications.sql"),
    Path("Visualize/tools/k45_oracles.py"),
    Path("Visualize/tests/orion_async/helpers/clickhouse_probe.py"),
    Path("tests/architecture/ownership_matrix.py"),
    Path("tests/architecture/test_dependency_boundaries.py"),
    Path("tests/architecture/test_k6_historical_ownership.py"),
    Path("tests/architecture/test_k6b_static_runtime.py"),
    Path("docs/architecture/file_plan/K6_HISTORICAL_CUTOVER_IMPLEMENTATION_PLAN.md"),
    Path("docs/architecture/FINAL_RUNTIME_MANIFEST.md"),
    Path("docs/architecture/K6_LEGACY_INVENTORY.md"),
    Path("docs/Architecture_lock/COMPONENT_OWNERSHIP.md"),
    Path("docs/Architecture_lock/FINAL_PRODUCTION_TARGET.md"),
    Path("docs/de/de0_signoff.md"),
    Path("docs/de1/DE1_IMPLEMENTATION_PLAN.md"),
    Path("docs/de1/DE1_PHASE_OVERVIEW.md"),
    Path("docs/de1/README.md"),
    Path("docs/implementation/ASYNC_ORION_PUBLISHER_TEST_PLAN.md"),
    Path("docs/implementation/ASYNC_ORION_PUBLISHER_TEST_REPORT_TEMPLATE.md"),
    Path("docs/implementation/BATCH_UPSERT_COMPATIBILITY_SPIKE.md"),
    Path("docs/implementation/orion_publish_performance_audit.md"),
    Path("docs/implementation/batch_upsert_evidence/batch_spike_runner.py"),
    Path("docs/implementation/batch_upsert_evidence/retest_notification_coverage.py"),
    Path("docs/implementation/stage_c_evidence/ch_full_verify.py"),
    Path("docs/implementation/stage_c_evidence/chaos_runner.py"),
    Path("docs/implementation/stage_c_evidence/stage_c_traci_run.py"),
    Path(".cursor/plans/architecture_lock_plan_1ee73088.plan.md"),
    Path(".cursor/plans/de-1_implementation_plan_defa7bdd.plan.md"),
    Path(".cursor/plans/k45_soak_execution_c8ab1c4e.plan.md"),
    Path(".cursor/plans/k4_raw_implementation_ea568a17.plan.md"),
    Path(".cursor/plans/kafka_architecture_migration_9f9452b4.plan.md"),
}


def _is_allowed(path: Path) -> bool:
    rel = path.relative_to(REPO_ROOT)
    return rel in ALLOWED_RAW_V1_FILES or any(
        rel == root or root in rel.parents for root in ALLOWED_RAW_V1_ROOTS
    )


def test_raw_v1_reference_allowlist_is_repository_wide_and_exact():
    hits: list[str] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in SCAN_EXTENSIONS:
            continue
        rel = path.relative_to(REPO_ROOT)
        if any(part in {".git", "__pycache__", "artifacts", "k5_evidence", "k7_bronze_evidence"} for part in rel.parts):
            continue
        try:
            text = read_text(path)
        except (OSError, UnicodeDecodeError):
            continue
        if RAW_V1_TABLE in text and not _is_allowed(path):
            hits.append(str(rel))
    assert hits == [], f"non-allowlisted Raw-v1 references: {hits}"


def test_k6_operator_tools_cannot_import_legacy_replay():
    tools = REPO_ROOT / "de" / "tools"
    hits = []
    for path in iter_py_files([tools]):
        imports = collect_imports(path)
        if any(name.startswith("de.webhook.scripts.replay_raw") for name in imports):
            hits.append(str(path.relative_to(REPO_ROOT)))
    assert hits == []
    replay = read_text(REPO_ROOT / "de" / "webhook" / "scripts" / "replay_raw.py")
    assert "LEGACY_INTERNAL_ONLY" in replay
    assert "--source auto" not in replay
    assert "--fallback-v1" not in replay
