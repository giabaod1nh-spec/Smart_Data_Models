"""
Cross-layer data verification tool (test/evidence only — not production pipeline).

Publisher ledger → Orion → Server realtime → ClickHouse raw → Compare

Exit codes:
  0 = all mandatory checks pass
  1 = mismatch or missing data
  2 = configuration / connectivity error
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow running as script from Visualize/
_VIS = Path(__file__).resolve().parent.parent
if str(_VIS) not in sys.path:
    sys.path.insert(0, str(_VIS))

from tests.orion_async.helpers.clickhouse_probe import (  # noqa: E402
    ClickHouseProbe,
    ClickHouseProbeError,
)
from tests.orion_async.helpers.orion_probe import OrionProbe, OrionProbeError  # noqa: E402
from tests.orion_async.helpers.publisher_ledger import LedgerEntry  # noqa: E402
from tests.orion_async.helpers.server_probe import ServerProbe, ServerProbeError  # noqa: E402


@dataclass
class CheckResult:
    layer: str
    entity_id: str
    verdict: str  # PASS WARNING FAIL
    message: str = ""


@dataclass
class VerificationReport:
    run_id: str
    from_sim_time: Optional[float] = None
    to_sim_time: Optional[float] = None
    expected_cycles: int = 0
    expected_entities: int = 0
    orion_matched: int = 0
    server_matched: int = 0
    clickhouse_matched: int = 0
    missing_entities: List[str] = field(default_factory=list)
    duplicate_events: List[str] = field(default_factory=list)
    ordering_violations: List[str] = field(default_factory=list)
    payload_mismatches: List[str] = field(default_factory=list)
    checks: List[CheckResult] = field(default_factory=list)
    connectivity_error: Optional[str] = None
    final_result: str = "PASS"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "from_sim_time": self.from_sim_time,
            "to_sim_time": self.to_sim_time,
            "expected_cycles": self.expected_cycles,
            "expected_entities": self.expected_entities,
            "orion_matched": self.orion_matched,
            "server_matched": self.server_matched,
            "clickhouse_matched": self.clickhouse_matched,
            "missing_entities": self.missing_entities,
            "duplicate_events": self.duplicate_events,
            "ordering_violations": self.ordering_violations,
            "payload_mismatches": self.payload_mismatches,
            "checks": [c.__dict__ for c in self.checks],
            "connectivity_error": self.connectivity_error,
            "final_result": self.final_result,
        }

    def format_text(self) -> str:
        lines = [
            f"Run ID: {self.run_id}",
            f"Simulation time range: {self.from_sim_time} .. {self.to_sim_time}",
            f"Expected cycles: {self.expected_cycles}",
            f"Expected entities: {self.expected_entities}",
            f"Orion matched: {self.orion_matched}",
            f"Server matched: {self.server_matched}",
            f"ClickHouse matched: {self.clickhouse_matched}",
            f"Missing entities: {self.missing_entities}",
            f"Duplicate events: {self.duplicate_events}",
            f"Ordering violations: {self.ordering_violations}",
            f"Payload mismatches: {self.payload_mismatches}",
            f"Final result: {self.final_result}",
        ]
        if self.connectivity_error:
            lines.append(f"Connectivity error: {self.connectivity_error}")
        return "\n".join(lines)


def load_ledger_jsonl(path: Path) -> List[LedgerEntry]:
    if not path.exists():
        raise FileNotFoundError(f"ledger not found: {path}")
    entries: List[LedgerEntry] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        entries.append(LedgerEntry(**d))
    return entries


def _prop(entity: dict, key: str) -> Any:
    attr = entity.get(key)
    if isinstance(attr, dict) and "value" in attr:
        return attr["value"]
    return attr


def filter_entries(
    entries: List[LedgerEntry],
    *,
    run_id: str,
    from_sim: Optional[float],
    to_sim: Optional[float],
    entity_id: Optional[str],
    cycle_sequence: Optional[int],
) -> List[LedgerEntry]:
    out = []
    for e in entries:
        if e.simulationRunId != run_id:
            continue
        if entity_id and e.entityId != entity_id:
            continue
        if cycle_sequence is not None and e.cycleSequence != cycle_sequence:
            continue
        if from_sim is not None and e.simulationTime < from_sim:
            continue
        if to_sim is not None and e.simulationTime > to_sim:
            continue
        out.append(e)
    return out


def verify(
    *,
    run_id: str,
    ledger_entries: List[LedgerEntry],
    from_sim_time: Optional[float] = None,
    to_sim_time: Optional[float] = None,
    entity_id: Optional[str] = None,
    cycle_sequence: Optional[int] = None,
    nodes: Optional[List[str]] = None,
    orion: Optional[OrionProbe] = None,
    server: Optional[ServerProbe] = None,
    clickhouse: Optional[ClickHouseProbe] = None,
    skip_orion: bool = False,
    skip_server: bool = False,
    skip_clickhouse: bool = False,
    strict: bool = False,
) -> VerificationReport:
    entries = filter_entries(
        ledger_entries,
        run_id=run_id,
        from_sim=from_sim_time,
        to_sim=to_sim_time,
        entity_id=entity_id,
        cycle_sequence=cycle_sequence,
    )
    report = VerificationReport(
        run_id=run_id,
        from_sim_time=from_sim_time,
        to_sim_time=to_sim_time,
        expected_entities=len(entries),
        expected_cycles=len({e.cycleSequence for e in entries}),
    )

    # Ordering: sequence monotonic within filtered set
    seqs = [e.cycleSequence for e in entries]
    if seqs != sorted(seqs):
        # allow same seq repeated for entities; check unique seq order
        uniq = []
        for s in seqs:
            if not uniq or uniq[-1] != s:
                uniq.append(s)
        if uniq != sorted(uniq):
            report.ordering_violations.append("cycleSequence not monotonic")
            report.checks.append(
                CheckResult("publisher", "*", "FAIL", "cycleSequence not monotonic")
            )

    # Publisher metadata consistency
    for e in entries:
        if e.simulationRunId != run_id:
            report.checks.append(
                CheckResult("publisher", e.entityId, "FAIL", "runId mismatch")
            )

    success_entries = [e for e in entries if e.publishStatus == "success"]
    pending = [e for e in entries if e.publishStatus == "pending"]
    if pending and strict:
        for e in pending:
            report.missing_entities.append(e.entityId)
            report.checks.append(
                CheckResult("publisher", e.entityId, "FAIL", "still pending")
            )

    # Orion
    if not skip_orion and orion is not None:
        try:
            for e in success_entries:
                try:
                    ent = orion.get_entity(e.entityId)
                    st = OrionProbe.prop(ent, "simulationTime")
                    rid = OrionProbe.prop(ent, "simulationRunId")
                    if rid is not None and rid != run_id:
                        report.payload_mismatches.append(f"orion runId {e.entityId}")
                        report.checks.append(
                            CheckResult("orion", e.entityId, "FAIL", f"runId={rid}")
                        )
                        continue
                    if st is not None and float(st) + 1e-9 < float(e.simulationTime):
                        report.payload_mismatches.append(f"orion stale {e.entityId}")
                        report.checks.append(
                            CheckResult(
                                "orion",
                                e.entityId,
                                "FAIL",
                                f"simTime {st} < expected {e.simulationTime}",
                            )
                        )
                        continue
                    report.orion_matched += 1
                    report.checks.append(CheckResult("orion", e.entityId, "PASS"))
                except OrionProbeError as ex:
                    if ex.connectivity:
                        report.connectivity_error = str(ex)
                        report.final_result = "ERROR"
                        return report
                    report.missing_entities.append(e.entityId)
                    report.checks.append(
                        CheckResult("orion", e.entityId, "FAIL", str(ex))
                    )
        except OrionProbeError as ex:
            if ex.connectivity:
                report.connectivity_error = str(ex)
                report.final_result = "ERROR"
                return report

    # Server (sample first node if provided)
    if not skip_server and server is not None and nodes:
        node = nodes[0]
        try:
            body = server.get_realtime_intersection(node)
            data = body.get("data") or body
            meta = data.get("metadata") or {}
            s_run = meta.get("simulationRunId")
            s_t = meta.get("simulationTime")
            if s_run and s_run != run_id:
                report.payload_mismatches.append("server runId")
                report.checks.append(
                    CheckResult("server", node, "FAIL", f"runId={s_run}")
                )
            else:
                report.server_matched += 1
                report.checks.append(
                    CheckResult(
                        "server",
                        node,
                        "PASS" if meta.get("consistent", True) else "WARNING",
                        f"consistent={meta.get('consistent')} simTime={s_t}",
                    )
                )
        except ServerProbeError as ex:
            if ex.connectivity:
                report.connectivity_error = str(ex)
                report.final_result = "ERROR"
                return report
            report.checks.append(CheckResult("server", node, "FAIL", str(ex)))

    # ClickHouse
    if not skip_clickhouse and clickhouse is not None:
        try:
            for e in success_entries:
                rows = clickhouse.find_entity_in_payloads(e.entityId, run_id=run_id)
                if not rows:
                    report.missing_entities.append(f"ch:{e.entityId}")
                    report.checks.append(
                        CheckResult("clickhouse", e.entityId, "FAIL", "not in raw")
                    )
                else:
                    # duplicate beyond 1 row for same notification policy: warn if many
                    if len(rows) > 5:
                        report.duplicate_events.append(e.entityId)
                        report.checks.append(
                            CheckResult(
                                "clickhouse",
                                e.entityId,
                                "WARNING",
                                f"many raw rows={len(rows)}",
                            )
                        )
                    report.clickhouse_matched += 1
                    report.checks.append(CheckResult("clickhouse", e.entityId, "PASS"))
        except ClickHouseProbeError as ex:
            if ex.connectivity:
                report.connectivity_error = str(ex)
                report.final_result = "ERROR"
                return report
            report.checks.append(CheckResult("clickhouse", "*", "FAIL", str(ex)))

    fails = [c for c in report.checks if c.verdict == "FAIL"]
    if report.ordering_violations or report.missing_entities or fails:
        report.final_result = "FAIL"
    elif any(c.verdict == "WARNING" for c in report.checks):
        report.final_result = "WARNING" if not strict else "FAIL"
    else:
        report.final_result = "PASS"
    return report


def exit_code_for(report: VerificationReport) -> int:
    if report.connectivity_error or report.final_result == "ERROR":
        return 2
    if report.final_result == "FAIL":
        return 1
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Cross-layer Orion publish data verification")
    p.add_argument("--run-id", required=True)
    p.add_argument("--from-sim-time", type=float, default=None)
    p.add_argument("--to-sim-time", type=float, default=None)
    p.add_argument("--nodes", default="A")
    p.add_argument("--entity-id", default=None)
    p.add_argument("--cycle-sequence", type=int, default=None)
    p.add_argument("--latest", action="store_true")
    p.add_argument("--orion-url", default="http://localhost:1026")
    p.add_argument("--server-url", default="http://localhost:8080")
    p.add_argument("--clickhouse-url", default="http://localhost:8123")
    p.add_argument("--ledger-path", default="artifacts/publisher_ledger.jsonl")
    p.add_argument("--output-json", default=None)
    p.add_argument("--output-csv", default=None)
    p.add_argument("--strict", action="store_true")
    p.add_argument("--skip-orion", action="store_true")
    p.add_argument("--skip-server", action="store_true")
    p.add_argument("--skip-clickhouse", action="store_true")
    p.add_argument("--server-auth", default=None, help="Authorization header value")
    args = p.parse_args(argv)

    try:
        entries = load_ledger_jsonl(Path(args.ledger_path))
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if args.latest and entries:
        # keep only max cycle
        max_seq = max(e.cycleSequence for e in entries if e.simulationRunId == args.run_id)
        entries = [e for e in entries if e.cycleSequence == max_seq]

    nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
    report = verify(
        run_id=args.run_id,
        ledger_entries=entries,
        from_sim_time=args.from_sim_time,
        to_sim_time=args.to_sim_time,
        entity_id=args.entity_id,
        cycle_sequence=args.cycle_sequence,
        nodes=nodes,
        orion=None if args.skip_orion else OrionProbe(args.orion_url),
        server=None
        if args.skip_server
        else ServerProbe(args.server_url, auth_header=args.server_auth),
        clickhouse=None if args.skip_clickhouse else ClickHouseProbe(args.clickhouse_url),
        skip_orion=args.skip_orion,
        skip_server=args.skip_server,
        skip_clickhouse=args.skip_clickhouse,
        strict=args.strict,
    )
    print(report.format_text())
    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(
            json.dumps(report.to_dict(), indent=2), encoding="utf-8"
        )
    if args.output_csv:
        Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
        lines = ["layer,entity_id,verdict,message"]
        for c in report.checks:
            lines.append(f"{c.layer},{c.entity_id},{c.verdict},{c.message.replace(',', ';')}")
        Path(args.output_csv).write_text("\n".join(lines), encoding="utf-8")
    return exit_code_for(report)


if __name__ == "__main__":
    raise SystemExit(main())
