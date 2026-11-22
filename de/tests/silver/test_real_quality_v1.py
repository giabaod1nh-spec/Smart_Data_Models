"""Wire-to-ClickHouse quality round trips in isolated replay fixtures."""
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from contracts.traffic_observation import build_observation
from de.silver.archive_resolver import VerifiedRunContext
from de.silver.config import SilverSettings
from de.silver.fact_builders import build_v1_fact_rows
from de.silver.repositories import SilverClickHouseRepository
from de.tests.silver.v1_fixtures import snapshot_v1

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='requires real ClickHouse replay tables')


def test_real_quality_presence_scenario_and_late_cycles():
    base = SilverSettings()
    replay_id = 'p3-quality-' + uuid4().hex[:16]
    repo = SilverClickHouseRepository(base.model_copy(update=dict(
        namespace='replay:' + replay_id, destination_mode='replay', replay_run_id=replay_id)))
    repo.connect()
    observations = []
    try:
        # Arrival order deliberately differs from cycle order. Each scenario is
        # event-local; optional fields from the first event must not leak later.
        for cycle, scenario, mixed in ((9, 'rush_hour', True), (3, 'normal', False)):
            snapshot = snapshot_v1()
            snapshot.update(simulation_run_id=replay_id, scenario=scenario)
            if mixed:
                snapshot.update(timing_mode='ACTUATED', phase_remaining=0.0,
                                spillback_detected=False, downstream_edge_full=False)
                for direction, status in (('East', 'UNAVAILABLE'), ('South', 'NOT_APPLICABLE')):
                    snapshot['directions'][direction]['vehicle_count'] = None
                    snapshot['directions'][direction]['metric_availability'] = {'vehicle_count': status}
                snapshot['directions']['West']['arrival_rate_pcu_per_sec'] = 0.0
            event = build_observation(snapshot, cycle_sequence=cycle, simulation_step=cycle * 10)
            context = VerifiedRunContext(replay_id, 1, event['topologyDigest'], 'a' * 64,
                ('A',), {'A': tuple(snapshot['directions'])}, {}, {}, 'b' * 64, 'c' * 64,
                'RUNNING', 't1')
            provenance = dict(bronze_semantic_revision=1, bronze_canonical_hash='d' * 64,
                source_raw_ingestion_id='raw-' + str(cycle), source_topic='traffic.observations.v1',
                source_partition=0, source_offset=cycle, source_physical_record_hash='e' * 64,
                source_byte_hash='f' * 64)
            families = build_v1_fact_rows(event, context, provenance)
            captured = {}
            for family, table in (('traffic', 'silver_traffic_observation_fact_v1'),
                                  ('signal', 'silver_signal_state_fact_v1'),
                                  ('intersection', 'silver_intersection_state_fact_v1')):
                repo.insert_v1_rows(table, families[family], replay_run_id=replay_id)
                physical = table.replace('_v1', '_replay_v1')
                result = repo.client.query(
                    f'SELECT * FROM {repo.database}.{physical} WHERE replay_run_id={{id:String}} '
                    'AND event_id={event:String}', parameters={'id': replay_id, 'event': event['eventId']})
                rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
                assert len(rows) == (1 if family == 'intersection' else 3)
                assert all(row['scenario_id'] == scenario and row['cycle_sequence'] == cycle for row in rows)
                captured[family] = rows
            traffic = {row['source_direction']: row for row in captured['traffic']}
            for direction, source in event['payload']['directions'].items():
                row = traffic[direction]
                assert json.loads(row['direction_payload_json']) == source
                assert json.loads(row['metric_availability_json']) == source.get('metric_availability', {})
                availability = source.get('metric_availability', {})
                assert row['quality_status'] == ('VALID_WITH_UNAVAILABLE' if availability else 'VALID')
                assert json.loads(row['quality_flags_json']) == [
                    f'METRIC_{status}:{name}' for name, status in sorted(availability.items())]
                for metric in ('vehicle_count', 'pcu_equivalent', 'left_count', 'straight_count',
                               'right_count', 'average_speed_kmh', 'waiting_vehicle_count',
                               'queue_length_m', 'occupancy_pct'):
                    assert row[metric] == source[metric]
            total = captured['intersection'][0]
            assert total['total_vehicle_count'] == (None if mixed else 0)
            assert total['total_vehicle_count_quality'] == ('PARTIAL_OR_UNAVAILABLE' if mixed else 'COMPLETE_TOPOLOGY_SUM')
            assert total['spillback_detected'] == (0 if mixed else None)
            assert total['downstream_edge_full'] == (0 if mixed else None)
            assert total['intersection_box_blocked'] is None
            for row in captured['signal']:
                assert row['timing_mode'] == ('ACTUATED' if mixed else None)
                assert row['phase_remaining_seconds'] == (0.0 if mixed else None)
                signal_payload = json.loads(row['signal_payload_json'])
                assert ('timing_mode' in signal_payload) == mixed
                assert ('phase_remaining' in signal_payload) == mixed
            observations.append(dict(cycle=cycle, scenario=scenario, event_id=event['eventId'], rows=captured))
        target = Path(base.checkpoint_path).parent / 'acceptance' / (replay_id + '.json')
        target.write_text(json.dumps(observations, default=str, indent=2), encoding='utf-8')
    finally:
        repo.close()
