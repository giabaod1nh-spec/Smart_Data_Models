#!/usr/bin/env python3
"""AST magic-number gate — forbid hardcoded model literals outside allowlist.

Primary checker uses Python AST (not grep). Scans assignments, comparisons,
calls, and container literals. Docs/comments/strings are skipped naturally.
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

VISUALIZE = Path(__file__).resolve().parent.parent

# Always forbidden anywhere outside allowlist (PCU / PCE)
FORBIDDEN_PCU = {0.24, 0.30}

# Model thresholds / detectors / signal fallbacks — checked in application modules
FORBIDDEN_MODEL = {
    0.8,
    0.85,
    0.9,
    15,
    15.0,
    35,
    35.0,
    50,
    50.0,
    140,
    140.0,
    20,
    20.0,
    40,
    40.0,
    30,
    30.0,
    42,
    42.0,
}

# Files where FORBIDDEN_MODEL applies (PCU always applies globally)
MODEL_SCAN_FILES = {
    "configuration/config.py",
    "simulation/signal_controller.py",
    "observation/snapshot_provider.py",
    "observation/detector_manager.py",
    "configuration/demand_profiles.py",
    "integration/orion/entity_mapper.py",
    "simulation/backend.py",
    "simulation/scenario_manager.py",
}

ALLOWLIST_FILES = {
    "configuration/parameter_registry.py",
    "configuration/model_params.py",
    "model_params.py",  # public facade for simulator/
    "tools/check_magic_numbers.py",
    "tools/generate_catalogs.py",
    "tools/generate_health_report.py",
    "tools/generate_rou.py",
    "tools/generate_detectors.py",
}

ALLOWLIST_PREFIXES = (
    "tests/",
    "artifacts/",
    "docs/",
    "__pycache__/",
)

# Call names where trailing int args are precision/retry, not model params
PRECISION_CALLS = {"round", "sleep", "Timeout", "timeout", "settimeout"}


def _is_allowlisted(rel: str) -> bool:
    rel = rel.replace("\\", "/")
    if rel in ALLOWLIST_FILES:
        return True
    return any(rel.startswith(p) for p in ALLOWLIST_PREFIXES)


def _num_value(node: ast.AST):
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        v = _num_value(node.operand)
        if isinstance(v, (int, float)):
            return -v
    return None


def _is_forbidden(val, *, model_scan: bool) -> bool:
    if isinstance(val, float) and val in FORBIDDEN_PCU:
        return True
    if isinstance(val, (int, float)) and float(val) in FORBIDDEN_PCU:
        return True
    if not model_scan:
        return False
    if val in FORBIDDEN_MODEL:
        return True
    if isinstance(val, float) and val in FORBIDDEN_MODEL:
        return True
    return False


class MagicVisitor(ast.NodeVisitor):
    def __init__(self, filename: str, model_scan: bool):
        self.filename = filename
        self.model_scan = model_scan
        self.findings: list[tuple[int, str, object]] = []

    def _check(self, node: ast.AST, ctx: str) -> None:
        v = _num_value(node)
        if v is None:
            return
        if _is_forbidden(v, model_scan=self.model_scan):
            self.findings.append((node.lineno, ctx, v))

    def visit_Assign(self, node: ast.Assign) -> None:
        # Topology lane counts are structural (ADR-002/003), not signal yellow=3
        for t in node.targets:
            if isinstance(t, ast.Name) and t.id in (
                "LANES_PER_APPROACH",
                "MOVEMENT_BY_LANE_INDEX",
            ):
                self.generic_visit(node)
                return
        self._check(node.value, "assign")
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        self._check(node.left, "compare")
        for c in node.comparators:
            self._check(c, "compare")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        name = ""
        if isinstance(node.func, ast.Name):
            name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            name = node.func.attr
        # Skip precision/retry trailing ints: round(x, 3), sleep(5)
        skip_last_int = name in PRECISION_CALLS or name.endswith("timeout")
        args = list(node.args)
        if skip_last_int and args and isinstance(_num_value(args[-1]), int):
            args = args[:-1]
        for a in args:
            self._check(a, "call")
        for kw in node.keywords:
            if kw.value is not None:
                self._check(kw.value, "call")
        self.generic_visit(node)

    def visit_Dict(self, node: ast.Dict) -> None:
        for v in node.values:
            self._check(v, "dict")
        self.generic_visit(node)

    def visit_List(self, node: ast.List) -> None:
        for elt in node.elts:
            self._check(elt, "list")
        self.generic_visit(node)

    def visit_Tuple(self, node: ast.Tuple) -> None:
        for elt in node.elts:
            self._check(elt, "tuple")
        self.generic_visit(node)

    def visit_Return(self, node: ast.Return) -> None:
        if node.value is not None:
            self._check(node.value, "return")
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        # Ignore integer divisors used for sampling (e.g. len // 5)
        if isinstance(node.op, ast.FloorDiv):
            self.generic_visit(node)
            return
        self._check(node.left, "binop")
        self._check(node.right, "binop")
        self.generic_visit(node)


def scan_file(path: Path, *, force_model_scan: bool = False) -> list[str]:
    path = path.resolve()
    try:
        rel = str(path.relative_to(VISUALIZE)).replace("\\", "/")
    except ValueError:
        rel = path.name
    if _is_allowlisted(rel) and not force_model_scan:
        return []
    model_scan = (
        force_model_scan
        or Path(rel).name in MODEL_SCAN_FILES
        or rel in MODEL_SCAN_FILES
    )
    try:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=rel)
    except SyntaxError as e:
        return [f"{rel}: syntax error: {e}"]
    visitor = MagicVisitor(rel, model_scan=model_scan)
    visitor.visit(tree)
    out = []
    for lineno, ctx, val in visitor.findings:
        out.append(f"{rel}:{lineno}: forbidden model literal {val!r} in {ctx}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AST magic-number gate")
    parser.add_argument("--root", type=Path, default=VISUALIZE)
    parser.add_argument("--fixture", type=Path, help="Optional fixture file expected to fail")
    args = parser.parse_args(argv)

    if args.fixture:
        findings = scan_file(args.fixture.resolve(), force_model_scan=True)
        if not findings:
            print("FAIL: fixture produced no findings", file=sys.stderr)
            return 1
        print("\n".join(findings))
        print("OK: fixture detected")
        return 0

    findings: list[str] = []
    for path in sorted(args.root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        findings.extend(scan_file(path))

    if findings:
        print("AST magic-number gate FAILED:", file=sys.stderr)
        for f in findings:
            print(f, file=sys.stderr)
        return 1
    print("AST magic-number gate PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
