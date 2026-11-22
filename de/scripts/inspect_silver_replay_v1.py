"""Read-only physical replay isolation evidence."""
import argparse
import json
import sqlite3
from pathlib import Path

from de.silver.config import SilverSettings, validate_namespace_id
from de.silver.repositories import SilverClickHouseRepository, V1_TABLES


def inspect(replay_id: str) -> dict:
    validate_namespace_id(replay_id)
    settings = SilverSettings()
    live_path = Path(settings.checkpoint_path)
    replay_path = live_path.parent / 'replay' / replay_id / 'checkpoint.sqlite3'
    assert live_path.resolve() != replay_path.resolve()
    with sqlite3.connect(replay_path.resolve(strict=True).as_uri() + '?mode=ro', uri=True) as db:
        state = db.execute('SELECT status,logical_hash FROM silver_v1_replay_state WHERE replay_run_id=?',
                           (replay_id,)).fetchone()
        namespaces = [r[0] for r in db.execute('SELECT namespace FROM silver_v1_source_binding')]
        cursors = list(db.execute('SELECT change_cursor,invalidation_cursor FROM silver_v1_source_binding'))
    with sqlite3.connect(live_path.resolve(strict=True).as_uri() + '?mode=ro', uri=True) as db:
        live_namespaces = [r[0] for r in db.execute('SELECT namespace FROM silver_v1_source_binding')]
    assert namespaces == ['replay:' + replay_id]
    assert live_namespaces == ['live']
    repo = SilverClickHouseRepository(settings)
    repo.connect()
    try:
        counts, runs = {}, {}
        for table in sorted(V1_TABLES):
            physical = table.replace('_v1', '_replay_v1')
            predicate = ' WHERE replay_run_id={id:String}'
            counts[physical] = repo.client.query(f'SELECT count() FROM {repo.database}.{physical}' +
                predicate, parameters={'id': replay_id}).result_rows[0][0]
            if table != 'silver_topology_dimension_v1':
                runs[physical] = [r[0] for r in repo.client.query(
                    f'SELECT DISTINCT simulation_run_id FROM {repo.database}.{physical}' + predicate,
                    parameters={'id': replay_id}).result_rows]
        return dict(replay_id=replay_id, state=state, cursors=cursors, namespaces=namespaces,
                    live_namespaces=live_namespaces, physical_counts=counts, target_runs=runs)
    finally:
        repo.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--replay-id', required=True)
    parser.add_argument('--compare-with')
    args = parser.parse_args()
    report = inspect(args.replay_id)
    if args.compare_with:
        reference = inspect(args.compare_with)
        assert report['state'][0] == reference['state'][0] == 'COMPLETE'
        assert report['state'][1] == reference['state'][1], 'logical replay hash differs'
        assert report['physical_counts'] == reference['physical_counts'], 'physical cardinality differs'
        assert report['target_runs'] == reference['target_runs'], 'replay run population differs'
        folder = Path(SilverSettings().checkpoint_path).parent / 'acceptance'
        manifests = [json.loads((folder / (name + '.manifest.json')).read_text())
                     for name in (args.replay_id, args.compare_with)]
        for doc in manifests:
            doc.pop('replay_run_id')
        assert manifests[0] == manifests[1], 'frozen source/archive/version differs'
        report['comparison'] = dict(reference=args.compare_with, same_frozen_source=True,
                                    same_logical_hash=True, same_physical_counts=True)
    target = Path(SilverSettings().checkpoint_path).parent / 'acceptance' / (args.replay_id + '.report.json')
    target.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))
