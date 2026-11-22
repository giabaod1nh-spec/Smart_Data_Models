"""Fail-closed whole-run Silver acceptance; does not certify all Phase 3 gates."""
import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from contracts.traffic_observation import observation_event_id
from de.scripts.reconcile_silver_v1 import reconcile
from de.silver.archive_resolver import ArchiveResolver
from de.silver.canonical_reader import CanonicalReadQuery, CanonicalReadSession
from de.silver.config import SilverSettings
from de.silver.source_binding import verify_bronze_source


def certify(run_id, cycles):
    if type(cycles) is not int or cycles < 1:
        raise ValueError('cycles must be positive')
    settings = SilverSettings()
    archive = ArchiveResolver(Path(settings.v1_archive_root))
    manifest, _ = archive._load(archive.root / 'runs' / run_id / 'run_manifest.json')
    context = archive.resolve(run_id, manifest['topologyDigest'])
    expected = {observation_event_id(run_id, node, cycle) for node in context.participants
                for cycle in range(1, cycles + 1)}
    before = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
    with sqlite3.connect(Path(settings.v1_bronze_path).resolve().as_uri() + '?mode=ro', uri=True) as db:
        source = db.execute('SELECT event_id,state,simulation_time_seconds FROM v1_identity '
                            'WHERE namespace=? AND simulation_run_id=?', ('live', run_id)).fetchall()
    with sqlite3.connect(Path(settings.checkpoint_path).resolve().as_uri() + '?mode=ro', uri=True) as db:
        valid = db.execute('SELECT event_id FROM silver_v1_event_validity WHERE namespace=? '
                           'AND simulation_run_id=? AND state=?', ('live', run_id, 'VALID')).fetchall()
    report = dict(run_id=run_id, observed_at=datetime.now(timezone.utc).isoformat(),
        verdict='INCOMPLETE', scope='whole-run Silver runtime gate, not full Phase 3 certification',
        source_boundary=before, expected_events=len(expected), source_events=len(source),
        valid_events=len(valid), reasons=[])
    if {row[0] for row in source} != expected or any(row[1] != 'ACCEPTED' for row in source):
        report['reasons'].append('SOURCE_POPULATION_MISMATCH')
    if {row[0] for row in valid} != expected:
        report['reasons'].append('SILVER_POPULATION_INCOMPLETE')
    if context.registry_status != 'COMPLETE' or context.manifest.get('runStatus') != 'COMPLETE':
        report['reasons'].append('RUN_NOT_COMPLETE')
    if report['reasons']:
        return report
    oracle = reconcile(run_id)
    report['silver_oracle'] = oracle
    if oracle['errors'] or not oracle['run_fully_materialized'] or oracle['verified_events'] != len(expected):
        report['reasons'].append('SOURCE_FACT_ORACLE_FAILED')
        return report
    times = [row[2] for row in source]
    query = CanonicalReadQuery('live', run_id, context.topology_digest, min(times), max(times) + 1.0)
    with CanonicalReadSession(settings) as reader:
        bundles = reader.read_complete_scan(query, page_size=100)
    counts = dict(intersection=len(bundles), traffic=sum(len(bundle.traffic) for bundle in bundles),
                  signal=sum(len(bundle.signal) for bundle in bundles))
    expected_directional = cycles * sum(len(context.directions[node]) for node in context.participants)
    if {bundle.event_id for bundle in bundles} != expected or counts != dict(
            intersection=len(expected), traffic=expected_directional, signal=expected_directional):
        report['reasons'].append('CANONICAL_POPULATION_MISMATCH')
    source_oracle_path = Path(settings.checkpoint_path).parent / 'acceptance' / run_id / 'source-oracle.json'
    source_raw = source_oracle_path.read_bytes()
    source_oracle = json.loads(source_raw)
    if (not source_oracle['source_complete'] or source_oracle['errors'] or
            source_oracle['verified_source_events'] != len(expected) or source_oracle['cycles'] != cycles or
            any(source_oracle[name] != len(expected) for name in
                ('outbox_rows', 'outbox_acked', 'raw_rows', 'bronze_rows', 'expected_events')) or
            source_oracle['generation'] != context.generation or
            source_oracle['topology_digest'] != context.topology_digest or
            source_oracle['manifest_identity'] != context.manifest_identity or
            source_oracle['manifest_file_hash'] != context.manifest_file_hash or
            source_oracle['catalog_file_hash'] != context.catalog_file_hash or
            source_oracle['run_id'] != run_id):
        report['reasons'].append('RECORDED_PRODUCER_SOURCE_ORACLE_MISMATCH')
    after = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
    if before != after:
        report['reasons'].append('SOURCE_BOUNDARY_CHANGED')
    report.update(canonical_counts=counts, canonical_total=sum(counts.values()),
        source_oracle_sha256=hashlib.sha256(source_raw).hexdigest(),
        source_oracle_observed_at=source_oracle['observed_at'],
        recorded_source_evidence_not_rerun=True, ending_source_boundary=after)
    report['verdict'] = 'PASS' if not report['reasons'] else 'INCOMPLETE'
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--cycles', type=int, default=120)
    args = parser.parse_args()
    try:
        report = certify(args.run_id, args.cycles)
    except Exception as exc:
        report = dict(run_id=args.run_id, verdict='INCOMPLETE',
                      reasons=[type(exc).__name__ + ': ' + str(exc)])
    target = Path(SilverSettings().checkpoint_path).parent / 'acceptance' / args.run_id / 'silver-certification.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
    print(json.dumps(report, default=str))
    raise SystemExit(0 if report['verdict'] == 'PASS' else 1)
