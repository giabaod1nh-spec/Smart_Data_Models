"""Pure K-6a Historical parity oracles (no runtime writes)."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

from contracts.canonical_json import canonical_hash


@dataclass(frozen=True)
class Observation:
    simulation_run_id: str
    entity_id: str
    simulation_time: float
    payload_hash: str
    source: str
    source_location: str
    cycle_sequence: int | None = None

    @property
    def identity(self) -> tuple[str, str, float]:
        return self.simulation_run_id, self.entity_id, self.simulation_time

    @property
    def multiset_key(self) -> tuple[str, str, float, str]:
        return (*self.identity, self.payload_hash)


def _property_value(entity: Mapping[str, Any], name: str) -> Any:
    value = entity.get(name)
    if isinstance(value, Mapping) and "value" in value:
        return value["value"]
    return value


def _normalize_entity(
    entity: Mapping[str, Any],
    *,
    source: str,
    source_location: str,
    run_id: str | None = None,
    simulation_time: float | None = None,
    cycle_sequence: int | None = None,
) -> Observation:
    rid = run_id or _property_value(entity, "simulationRunId")
    sim_time = simulation_time
    if sim_time is None:
        sim_time = _property_value(entity, "simulationTime")
    entity_id = entity.get("id")
    if not rid or not entity_id or sim_time is None:
        raise ValueError(
            f"missing shared observation identity at {source_location}: "
            "simulationRunId/entityId/simulationTime required"
        )
    payload = dict(entity)
    return Observation(
        simulation_run_id=str(rid),
        entity_id=str(entity_id),
        simulation_time=float(sim_time),
        payload_hash=canonical_hash(payload),
        source=source,
        source_location=source_location,
        cycle_sequence=cycle_sequence,
    )


def normalize_legacy_notifications(
    notifications: Iterable[Mapping[str, Any]],
) -> list[Observation]:
    result: list[Observation] = []
    for notification_index, notification in enumerate(notifications):
        notification_id = notification.get("id", f"notification-{notification_index}")
        data = notification.get("data")
        if not isinstance(data, list):
            raise ValueError(f"notification {notification_id} data must be an array")
        for entity_index, entity in enumerate(data):
            if not isinstance(entity, Mapping):
                raise ValueError(f"notification {notification_id} entity must be an object")
            result.append(
                _normalize_entity(
                    entity,
                    source="raw_v1",
                    source_location=f"{notification_id}:data[{entity_index}]",
                )
            )
    return result


def normalize_kafka_entity_events(
    events: Iterable[Mapping[str, Any]],
) -> list[Observation]:
    result: list[Observation] = []
    for index, event in enumerate(events):
        if event.get("eventType") != "TrafficEntityObserved":
            continue
        entity = event.get("entity")
        if not isinstance(entity, Mapping):
            raise ValueError(f"Kafka event {index} entity must be an object")
        result.append(
            _normalize_entity(
                entity,
                source="raw_v2",
                source_location=str(event.get("eventId", f"event-{index}")),
                run_id=str(event.get("simulationRunId") or "") or None,
                simulation_time=float(event["simulationTime"]),
                cycle_sequence=int(event["cycleSequence"]),
            )
        )
    return result


def compare_observation_multisets(
    legacy: Sequence[Observation], kafka: Sequence[Observation]
) -> dict[str, Any]:
    legacy_counter = Counter(item.multiset_key for item in legacy)
    kafka_counter = Counter(item.multiset_key for item in kafka)
    missing_in_legacy = kafka_counter - legacy_counter
    missing_in_kafka = legacy_counter - kafka_counter

    legacy_hashes: dict[tuple[str, str, float], Counter[str]] = defaultdict(Counter)
    kafka_hashes: dict[tuple[str, str, float], Counter[str]] = defaultdict(Counter)
    for item in legacy:
        legacy_hashes[item.identity][item.payload_hash] += 1
    for item in kafka:
        kafka_hashes[item.identity][item.payload_hash] += 1
    payload_mismatches = []
    for identity in sorted(set(legacy_hashes) & set(kafka_hashes)):
        if legacy_hashes[identity] != kafka_hashes[identity]:
            payload_mismatches.append(
                {
                    "simulation_run_id": identity[0],
                    "entity_id": identity[1],
                    "simulation_time": identity[2],
                    "legacy_hashes": dict(legacy_hashes[identity]),
                    "kafka_hashes": dict(kafka_hashes[identity]),
                }
            )

    def _expand(counter: Counter) -> list[dict[str, Any]]:
        return [
            {
                "simulation_run_id": key[0],
                "entity_id": key[1],
                "simulation_time": key[2],
                "payload_hash": key[3],
                "count": count,
            }
            for key, count in sorted(counter.items())
        ]

    unexpected_duplicate_delta = sum(
        abs(legacy_counter[key] - kafka_counter[key])
        for key in set(legacy_counter) | set(kafka_counter)
        if max(legacy_counter[key], kafka_counter[key]) > 1
    )
    report = {
        "legacy_observations": len(legacy),
        "kafka_observations": len(kafka),
        "missing_in_legacy": _expand(missing_in_legacy),
        "missing_in_kafka": _expand(missing_in_kafka),
        "payload_hash_mismatches": payload_mismatches,
        "unexpected_duplicate_delta": unexpected_duplicate_delta,
    }
    report["pass"] = not any(
        (
            report["missing_in_legacy"],
            report["missing_in_kafka"],
            payload_mismatches,
            unexpected_duplicate_delta,
        )
    )
    return report


def compare_kafka_to_raw(
    expected: Mapping[tuple[str, int, int], str],
    raw_rows: Iterable[Mapping[str, Any]],
    quarantine_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    classified: dict[tuple[str, int, int], list[tuple[str, str]]] = defaultdict(list)
    for destination, rows in (("RAW", raw_rows), ("QUARANTINE", quarantine_rows)):
        for row in rows:
            key = (str(row["topic"]), int(row["partition"]), int(row["offset"]))
            classified[key].append((destination, str(row["payload_bytes_hash"])))
    missing = [key for key in expected if key not in classified]
    unexpected = [key for key in classified if key not in expected]
    overlaps = {key: values for key, values in classified.items() if len(values) != 1}
    hash_mismatches = {
        key: {"expected": expected[key], "actual": values[0][1]}
        for key, values in classified.items()
        if key in expected and len(values) == 1 and values[0][1] != expected[key]
    }
    return {
        "expected_offsets": len(expected),
        "classified_offsets": len(classified),
        "missing_offsets": missing,
        "unexpected_offsets": unexpected,
        "overlap_or_duplicate": overlaps,
        "payload_hash_mismatches": hash_mismatches,
        "pass": not any((missing, unexpected, overlaps, hash_mismatches)),
    }


def compare_raw_to_bronze(
    raw_rows: Iterable[Mapping[str, Any]],
    raw_quarantine_rows: Iterable[Mapping[str, Any]],
    bronze_entity_rows: Iterable[Mapping[str, Any]],
    bronze_run_rows: Iterable[Mapping[str, Any]],
    bronze_quarantine_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    expected: dict[str, str] = {}
    source_duplicates: list[str] = []
    for row in raw_rows:
        rid = str(row["raw_ingestion_id"])
        destination = "RUN" if row.get("event_type") == "TrafficSimulationRunStarted" else "ENTITY"
        if rid in expected:
            source_duplicates.append(rid)
        expected[rid] = destination
    for row in raw_quarantine_rows:
        rid = str(row["raw_ingestion_id"])
        if rid in expected:
            source_duplicates.append(rid)
        expected[rid] = "QUARANTINE"

    actual: dict[str, list[str]] = defaultdict(list)
    for destination, rows in (
        ("ENTITY", bronze_entity_rows),
        ("RUN", bronze_run_rows),
        ("QUARANTINE", bronze_quarantine_rows),
    ):
        for row in rows:
            actual[str(row["raw_ingestion_id"])].append(destination)
    missing = [rid for rid in expected if rid not in actual]
    unexpected = [rid for rid in actual if rid not in expected]
    multi_destination = {rid: dests for rid, dests in actual.items() if len(dests) != 1}
    wrong_destination = {
        rid: {"expected": expected[rid], "actual": dests[0]}
        for rid, dests in actual.items()
        if rid in expected and len(dests) == 1 and dests[0] != expected[rid]
    }
    return {
        "source_rows": len(expected),
        "bronze_rows": len(actual),
        "source_duplicates": source_duplicates,
        "missing_in_bronze": missing,
        "unexpected_in_bronze": unexpected,
        "multi_destination": multi_destination,
        "wrong_destination": wrong_destination,
        "pass": not any(
            (
                source_duplicates,
                missing,
                unexpected,
                multi_destination,
                wrong_destination,
            )
        ),
    }


def observations_as_dicts(items: Sequence[Observation]) -> list[dict[str, Any]]:
    return [asdict(item) for item in items]
