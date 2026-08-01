"""K-6a operator CLI for v2-only Historical parity evidence.

The CLI never auto-detects or falls back to Raw v1. Legacy notifications are an
explicit read-only input only for the bounded ``dual-parity`` command.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

from de.tools.k6_historical_oracles import (
    compare_kafka_to_raw,
    compare_observation_multisets,
    compare_raw_to_bronze,
    normalize_kafka_entity_events,
    normalize_legacy_notifications,
    observations_as_dicts,
)


def _read_records(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        value = json.loads(text)
        if not isinstance(value, list):
            raise ValueError(f"{path} must contain a JSON array or JSONL")
        return value
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _write_report(path: Path, report: dict[str, Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return 0 if report.get("pass") is True else 1


def _dual(args: argparse.Namespace) -> dict[str, Any]:
    legacy = normalize_legacy_notifications(_read_records(args.legacy_notifications))
    kafka = normalize_kafka_entity_events(_read_records(args.kafka_events))
    report = compare_observation_multisets(legacy, kafka)
    report.update(
        {
            "oracle": "dual_ingest_entity_multiset",
            "legacy_input": str(args.legacy_notifications),
            "kafka_input": str(args.kafka_events),
            "normalized_legacy": observations_as_dicts(legacy),
            "normalized_kafka": observations_as_dicts(kafka),
        }
    )
    return report


def _kafka_raw(args: argparse.Namespace) -> dict[str, Any]:
    expected_rows = _read_records(args.expected_offsets)
    expected = {
        (str(row["topic"]), int(row["partition"]), int(row["offset"])): str(
            row["payload_bytes_hash"]
        )
        for row in expected_rows
    }
    report = compare_kafka_to_raw(
        expected,
        _read_records(args.raw_rows),
        _read_records(args.quarantine_rows),
    )
    report["oracle"] = "kafka_offset_to_raw_classification"
    return report


def _raw_bronze(args: argparse.Namespace) -> dict[str, Any]:
    report = compare_raw_to_bronze(
        _read_records(args.raw_rows),
        _read_records(args.raw_quarantine_rows),
        _read_records(args.bronze_entity_rows),
        _read_records(args.bronze_run_rows),
        _read_records(args.bronze_quarantine_rows),
    )
    report["oracle"] = "raw_to_bronze_destination"
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="K-6a v2-only Historical parity CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    dual = sub.add_parser("dual-parity", help="Explicit Raw-v1 observation vs Kafka-v2 parity")
    dual.add_argument("--legacy-notifications", type=Path, required=True)
    dual.add_argument("--kafka-events", type=Path, required=True)
    dual.add_argument("--output", type=Path, required=True)
    dual.set_defaults(handler=_dual)

    kr = sub.add_parser("kafka-raw-parity", help="Kafka offsets to Raw-v2/quarantine parity")
    kr.add_argument("--expected-offsets", type=Path, required=True)
    kr.add_argument("--raw-rows", type=Path, required=True)
    kr.add_argument("--quarantine-rows", type=Path, required=True)
    kr.add_argument("--output", type=Path, required=True)
    kr.set_defaults(handler=_kafka_raw)

    rb = sub.add_parser("raw-bronze-parity", help="Raw-v2/quarantine to Bronze parity")
    rb.add_argument("--raw-rows", type=Path, required=True)
    rb.add_argument("--raw-quarantine-rows", type=Path, required=True)
    rb.add_argument("--bronze-entity-rows", type=Path, required=True)
    rb.add_argument("--bronze-run-rows", type=Path, required=True)
    rb.add_argument("--bronze-quarantine-rows", type=Path, required=True)
    rb.add_argument("--output", type=Path, required=True)
    rb.set_defaults(handler=_raw_bronze)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handler: Callable[[argparse.Namespace], dict[str, Any]] = args.handler
    return _write_report(args.output, handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
