"""Independent Outbox -> Raw -> Bronze source oracle for the Silver workload."""
import argparse
import base64
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from contracts.traffic_observation import observation_event_id, validate_observation, canonical_json
from de.silver.archive_resolver import ArchiveResolver
from de.silver.config import SilverSettings
from de.silver.repositories import SilverClickHouseRepository


def reconcile(run_id: str, cycles: int) -> dict:
    settings = SilverSettings()
    resolver = ArchiveResolver(Path(settings.v1_archive_root))
    if not run_id or run_id in {'.', '..'} or any(c in run_id for c in '/\\\0') or cycles < 1:
        raise ValueError('invalid workload identity')
    manifest, _ = resolver._load(resolver.root / 'runs' / run_id / 'run_manifest.json')
    context = resolver.resolve(run_id, manifest['topologyDigest'])
    expected = {observation_event_id(run_id, node, cycle)
                for node in context.participants for cycle in range(1, cycles + 1)}
    outbox_path = resolver.root / 'kafka_outbox' / 'v1-outbox.sqlite3'
    with sqlite3.connect(outbox_path.resolve(strict=True).as_uri() + '?mode=ro', uri=True) as db:
        published = db.execute('SELECT payload_json,status FROM kafka_outbox WHERE simulation_run_id=?',
                               (run_id,)).fetchall()
    events = {json.loads(payload)['eventId']: json.loads(payload) for payload, _ in published}
    errors = []
    if set(events) != expected or len(published) != len(expected):
        errors.append('Outbox population differs from declared participants/cycles')
    if any(status != 'ACKED' for _, status in published):
        errors.append('Outbox has unacknowledged deliveries')
    repo = SilverClickHouseRepository(settings)
    repo.connect()
    def fetch(table, predicate=''):
        result = repo.client.query(f'SELECT * FROM {repo.database}.{table} '
            'WHERE simulation_run_id={run:String}' + predicate, parameters={'run': run_id})
        return [{key: value.decode('utf-8').rstrip('\x00') if isinstance(value, bytes) else value
                 for key, value in zip(result.column_names, values)} for values in result.result_rows]
    try:
        raw_rows = fetch('v1_raw_events')
        bronze_rows = fetch('bronze_observations_v1', " AND namespace='live'")
        raw = {row['event_id']: row for row in raw_rows}
        bronze = {row['event_id']: row for row in bronze_rows}
        if set(raw) != expected or len(raw_rows) != len(expected):
            errors.append('Raw population incomplete or duplicated')
        if set(bronze) != expected or len(bronze_rows) != len(expected):
            errors.append('Bronze population incomplete or duplicated')
        verified = 0
        for event_id in sorted(expected & set(events) & set(raw) & set(bronze)):
            try:
                event, delivery, canonical = events[event_id], raw[event_id], bronze[event_id]
                validate_observation(event)
                context.validate_event(event)
                wire = base64.b64decode(delivery['value_bytes_base64'], validate=True)
                assert json.loads(wire) == event, 'Raw wire event differs from Outbox'
                assert hashlib.sha256(wire).hexdigest() == delivery['payload_bytes_hash'], 'Raw byte hash'
                assert json.loads(canonical['observation_envelope_json']) == event, 'Bronze envelope'
                assert json.loads(canonical['observation_payload_json']) == event['payload'], 'Bronze payload'
                assert canonical['payload_hash'] == event['payloadHash'], 'Bronze payload hash'
                digest = hashlib.sha256(canonical_json(dict(observation=event,
                    normalization_version=canonical['normalization_version'],
                    bronze_schema_version=canonical['bronze_schema_version'])).encode()).hexdigest()
                assert canonical['bronze_canonical_hash'] == digest, 'Bronze canonical hash'
                for column, source_column in (('primary_raw_ingestion_id', 'raw_ingestion_id'),
                        ('primary_topic', 'topic'), ('primary_partition', 'partition'), ('primary_offset', 'offset')):
                    assert canonical[column] == delivery[source_column], column
                verified += 1
            except (AssertionError, ValueError, KeyError) as exc:
                errors.append(event_id + ': ' + str(exc))
        return dict(run_id=run_id, observed_at=datetime.now(timezone.utc).isoformat(),
            generation=context.generation, manifest_identity=context.manifest_identity,
            manifest_file_hash=context.manifest_file_hash, catalog_file_hash=context.catalog_file_hash,
            topology_digest=context.topology_digest, registry_status=context.registry_status,
            cycles=cycles, participants=list(context.participants), expected_events=len(expected),
            expected_intersection_facts=len(expected),
            expected_traffic_facts=cycles * sum(len(context.directions[node]) for node in context.participants),
            expected_signal_facts=cycles * sum(len(context.directions[node]) for node in context.participants),
            expected_total_facts=len(expected) + 2 * cycles * sum(len(context.directions[node]) for node in context.participants),
            outbox_rows=len(published), outbox_acked=sum(status == 'ACKED' for _, status in published),
            raw_rows=len(raw_rows), bronze_rows=len(bronze_rows), verified_source_events=verified,
            errors=errors, source_complete=not errors and verified == len(expected))
    finally:
        repo.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--cycles', type=int, default=120)
    args = parser.parse_args()
    report = reconcile(args.run_id, args.cycles)
    target = Path(SilverSettings().checkpoint_path).parent / 'acceptance' / args.run_id / 'source-oracle.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))
    raise SystemExit(0 if report['source_complete'] else 1)
