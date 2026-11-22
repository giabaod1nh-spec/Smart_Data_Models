"""Read-only source oracle for already materialized Silver V1 events."""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.silver.archive_resolver import ArchiveResolver
from de.silver.config import SilverSettings, validate_namespace_id
from de.silver.repositories import SilverClickHouseRepository
from de.silver.source_binding import verify_bronze_source


def reconcile(run_id: str, replay_id: str | None = None) -> dict:
    settings = SilverSettings()
    state_path = Path(settings.checkpoint_path)
    namespace = 'live'
    if replay_id is not None:
        validate_namespace_id(replay_id)
        state_path = state_path.parent / 'replay' / replay_id / 'checkpoint.sqlite3'
        namespace = 'replay:' + replay_id
    silver = SilverClickHouseRepository(settings)
    bronze = BronzeClickHouseRepository(ObservationSettings(
        checkpoint_path=settings.v1_bronze_path, clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port, clickhouse_database=settings.clickhouse_database,
        clickhouse_user=settings.clickhouse_user, clickhouse_password=settings.clickhouse_password))
    silver.connect()
    bronze.connect()
    errors = []
    checked = 0
    before = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
    db = sqlite3.connect(state_path.resolve(strict=True).as_uri() + '?mode=ro', uri=True)
    try:
        db.execute('PRAGMA query_only=ON')
        candidates = db.execute('SELECT event_id,semantic_revision FROM silver_v1_event_validity '
            'WHERE namespace=? AND simulation_run_id=? AND state=? ORDER BY event_id',
            (namespace, run_id, 'VALID')).fetchall()
        archive = ArchiveResolver(Path(settings.v1_archive_root))
        for event_id, revision in candidates:
            try:
                accepted = bronze.read_valid_observation('live', event_id, revision,
                    state_path=settings.v1_bronze_path)
                assert accepted is not None, 'source no longer accepted'
                event = accepted['event']
                context = archive.resolve(run_id, event['topologyDigest'])
                context.validate_event(event)
                payload = event['payload']
                primary = next(item['row'] for item in json.loads(
                    accepted['primary_delivery']['expected_json'])
                    if item['table'] == 'bronze_delivery_dispositions_v1')
                families = {}
                for family, table in (('traffic', 'silver_traffic_observation_fact_v1'),
                                      ('signal', 'silver_signal_state_fact_v1'),
                                      ('intersection', 'silver_intersection_state_fact_v1')):
                    if replay_id is None:
                        rows = silver.fetch_v1_event_rows(table, run_id, context.topology_digest, event_id)
                    else:
                        result = silver.client.query(
                            f'SELECT * FROM {silver.database}.{table.replace("_v1", "_replay_v1")} '
                            'WHERE replay_run_id={replay:String} AND simulation_run_id={run:String} '
                            'AND topology_digest={digest:String} AND event_id={event:String} LIMIT 1001',
                            parameters=dict(replay=replay_id, run=run_id,
                                            digest=context.topology_digest, event=event_id))
                        rows = [{name: (value.decode('utf-8').rstrip('\x00')
                                         if isinstance(value, bytes) else value)
                                 for name, value in zip(result.column_names, row)}
                                for row in result.result_rows]
                    unique = {}
                    for row in rows:
                        key = row.get('source_direction', 'intersection')
                        if key in unique:
                            assert unique[key] == row, 'conflicting physical duplicate'
                        unique[key] = row
                        for name, expected in dict(event_id=event_id, cycle_sequence=event['cycleSequence'],
                                simulation_time_seconds=event['simulationTimeSeconds'],
                                intersection_id=event['intersectionId'], scenario_id=event['scenarioId'],
                                run_generation=context.generation, manifest_identity=context.manifest_identity,
                                source_payload_hash=event['payloadHash'], bronze_semantic_revision=revision,
                                bronze_canonical_hash=accepted['bronze_canonical_hash'],
                                source_raw_ingestion_id=primary['raw_ingestion_id'],
                                source_topic=primary['topic'], source_partition=primary['partition'],
                                source_offset=primary['offset'],
                                source_physical_record_hash=primary['physical_record_hash'],
                                source_byte_hash=primary['source_byte_hash']).items():
                            assert row[name] == expected, name
                    expected_keys = ({'intersection'} if family == 'intersection' else
                                     set(context.directions[event['intersectionId']]))
                    assert set(unique) == expected_keys, family + ' population'
                    families[family] = unique
                counts = []
                for direction, value in payload['directions'].items():
                    row = families['traffic'][direction]
                    assert json.loads(row['direction_payload_json']) == value, 'direction payload'
                    availability = value.get('metric_availability', {})
                    assert json.loads(row['metric_availability_json']) == availability, 'availability'
                    for metric in ('vehicle_count', 'pcu_equivalent', 'left_count', 'straight_count',
                                   'right_count', 'average_speed_kmh', 'waiting_vehicle_count',
                                   'queue_length_m', 'occupancy_pct'):
                        assert row[metric] == value[metric], metric
                    assert families['signal'][direction]['signal_color'] == payload['colors'][direction]
                    signal = families['signal'][direction]
                    for column, key in (('phase', 'phase'), ('next_phase', 'next_phase'),
                            ('phase_remaining_seconds', 'phase_remaining'),
                            ('phase_duration_seconds', 'phase_duration'),
                            ('green_duration_seconds', 'green_duration'),
                            ('yellow_duration_seconds', 'yellow_duration'),
                            ('red_duration_seconds', 'red_duration'), ('timing_mode', 'timing_mode')):
                        assert signal[column] == payload.get(key), column
                    expected_signal = {key: payload[key] for key in ('colors', 'phase', 'next_phase',
                        'phase_remaining', 'phase_duration', 'green_duration', 'yellow_duration',
                        'red_duration', 'timing_mode') if key in payload}
                    assert json.loads(signal['signal_payload_json']) == expected_signal, 'signal payload'
                    counts.append(value['vehicle_count'] if 'vehicle_count' not in availability else None)
                intersection = families['intersection']['intersection']
                assert json.loads(intersection['intersection_payload_json']) == payload, 'intersection payload'
                total = sum(counts) if all(value is not None for value in counts) else None
                assert intersection['total_vehicle_count'] == total, 'complete-only total'
                assert intersection['total_vehicle_count_quality'] == (
                    'COMPLETE_TOPOLOGY_SUM' if total is not None else 'PARTIAL_OR_UNAVAILABLE')
                for column in ('derived_traffic_state', 'spillback_detected',
                               'intersection_box_blocked', 'downstream_edge_full', 'spillback_pressure'):
                    assert intersection[column] == payload.get(column), column
                checked += 1
            except (AssertionError, KeyError, ValueError) as exc:
                errors.append(dict(event_id=event_id, error=str(exc)))
        after = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
        assert before == after, 'source boundary changed during reconciliation'
        source_db = sqlite3.connect(Path(settings.v1_bronze_path).resolve().as_uri() + '?mode=ro', uri=True)
        try:
            source_count = source_db.execute('SELECT count(*) FROM v1_identity '
                'WHERE namespace=? AND simulation_run_id=? AND state=?',
                ('live', run_id, 'ACCEPTED')).fetchone()[0]
        finally:
            source_db.close()
        return dict(run_id=run_id, replay_id=replay_id, observed_at=datetime.now(timezone.utc).isoformat(),
                    source_boundary=after, candidate_events=len(candidates), verified_events=checked,
                    accepted_source_events=source_count, run_fully_materialized=(checked == source_count),
                    errors=errors, scope='already VALID events; does not certify whole-run completeness')
    finally:
        db.close()
        silver.close()
        bronze.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--replay-id')
    args = parser.parse_args()
    report = reconcile(args.run_id, args.replay_id)
    target = Path(SilverSettings().checkpoint_path).parent / 'acceptance' / ('oracle-' +
        (args.replay_id or args.run_id) + '.json')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))
    raise SystemExit(1 if report['errors'] or not report['verified_events'] else 0)
