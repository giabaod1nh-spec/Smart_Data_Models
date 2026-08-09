"""Shared helpers for architecture conformance tests."""
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Iterable, List, Set


def iter_py_files(roots: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(sorted(root.rglob("*.py")))
    return files


def collect_imports(path: Path) -> Set[str]:
    """Return dotted module prefixes imported by a Python file (best-effort AST)."""
    src = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src, filename=str(path))
    except SyntaxError:
        return set()
    found: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                found.add(node.module)
    return found


def module_matches(imported: str, forbidden: str) -> bool:
    return imported == forbidden or imported.startswith(forbidden + ".")


def scan_forbidden_imports(roots: Iterable[Path], forbidden: Iterable[str]) -> List[str]:
    hits: List[str] = []
    for path in iter_py_files(roots):
        for imported in collect_imports(path):
            for bad in forbidden:
                if module_matches(imported, bad):
                    hits.append(f"{path}:{imported} (forbidden {bad})")
    return hits


def load_dotenv(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env[key.strip()] = val.strip().strip('"').strip("'")
    return env


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def yaml_service_names(compose_text: str) -> Set[str]:
    """Minimal service-name extractor (no PyYAML dependency)."""
    names: Set[str] = set()
    in_services = False
    for line in compose_text.splitlines():
        if re.match(r"^services:\s*$", line):
            in_services = True
            continue
        if in_services:
            if re.match(r"^[A-Za-z0-9].*:\s*$", line) and not line.startswith(" "):
                # top-level key other than indented service
                if not line.startswith(" ") and line.rstrip(":") != "services":
                    # volumes: etc.
                    key = line.split(":", 1)[0].strip()
                    if key in ("volumes", "networks", "secrets", "configs", "name"):
                        break
            m = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
            if m:
                names.add(m.group(1))
    return names


def service_has_excluding_profile(compose_override_text: str, service: str) -> bool:
    """True if override assigns a non-empty profiles list to service (excluded by default)."""
    # crude block parse
    lines = compose_override_text.splitlines()
    i = 0
    while i < len(lines):
        if re.match(rf"^  {re.escape(service)}:\s*$", lines[i]):
            i += 1
            while i < len(lines) and (lines[i].startswith("    ") or lines[i].strip() == ""):
                if "profiles:" in lines[i]:
                    # next lines or inline
                    block = lines[i]
                    j = i + 1
                    while j < len(lines) and (
                        lines[j].startswith("      ") or lines[j].strip().startswith("-")
                    ):
                        block += "\n" + lines[j]
                        j += 1
                    if re.search(r"\[\s*['\"]?\w+", block) or "-" in block:
                        return True
                i += 1
            return False
        i += 1
    return False
