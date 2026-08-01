"""TraCI may only stall for Orion backpressure while the direct Orion path is live.

Under the final (Kafka-only) profile the simulation must never be paused by the
publishing pipeline, so every `should_pause = True` has to sit inside the guard
that requires an active direct-Orion publisher.
"""
from __future__ import annotations

import ast

from arch_utils import read_text
from ownership_matrix import REPO_ROOT

TRACI_RUNNER = REPO_ROOT / "Visualize" / "app" / "traci_runner.py"
REQUIRED_GUARD_NAMES = {"publish_orion", "use_async", "orion_gate_on"}


def _pause_assignments(tree: ast.AST) -> list[ast.Assign]:
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not (isinstance(node.value, ast.Constant) and node.value.value is True):
            continue
        if any(
            isinstance(t, ast.Name) and t.id == "should_pause" for t in node.targets
        ):
            found.append(node)
    return found


def _guarded_by_orion_publisher(node: ast.If) -> bool:
    names = {n.id for n in ast.walk(node.test) if isinstance(n, ast.Name)}
    return REQUIRED_GUARD_NAMES.issubset(names) and "publisher" in names


def test_backpressure_pause_requires_active_orion_publisher():
    tree = ast.parse(read_text(TRACI_RUNNER))
    pauses = _pause_assignments(tree)
    assert pauses, "no `should_pause = True` found — update this test with the loop"

    guards = [n for n in ast.walk(tree) if isinstance(n, ast.If) and _guarded_by_orion_publisher(n)]
    assert len(guards) == 1, f"expected exactly one Orion backpressure guard, got {len(guards)}"
    guarded_lines = {
        child.lineno
        for stmt in guards[0].body
        for child in ast.walk(stmt)
        if hasattr(child, "lineno")
    }

    unguarded = [p.lineno for p in pauses if p.lineno not in guarded_lines]
    assert not unguarded, (
        f"lines {unguarded} pause TraCI outside the direct-Orion guard: "
        "Kafka-only runs would stall the simulation"
    )


def test_kafka_fanout_reachable_without_orion_publisher():
    """The Kafka-only branch must exist and not require `publish_orion`."""
    tree = ast.parse(read_text(TRACI_RUNNER))
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test_src = ast.dump(node.test)
        if "kafka_outbox" in test_src and "publish_orion" in test_src:
            # `not publish_orion and (kafka_outbox is not None or ...)`
            has_negation = any(
                isinstance(sub, ast.UnaryOp) and isinstance(sub.op, ast.Not)
                for sub in ast.walk(node.test)
            )
            if has_negation:
                return
    raise AssertionError("no Kafka-only fanout branch found in traci_runner")
