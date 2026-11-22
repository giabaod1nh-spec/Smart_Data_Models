"""Source-advance replay refusal using the recorded 1600 -> 2200 runtime boundary."""
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from de.scripts.inspect_silver_replay_v1 import inspect
from de.silver.checkpoint_store import SilverCheckpointStore
from de.silver.config import SilverSettings
from de.silver.replay import run_replay, build_replay_settings, canonical_manifest_hash
from de.silver.source_binding import SourceBoundaryChangedError, verify_bronze_source

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='requires recorded replay and advanced actual Bronze source')


def test_real_completed_replay_refuses_current_source_without_rewriting_history():
    settings = SilverSettings()
    replay_id = 'p3-real-smoke02-20261121'
    folder = Path(settings.checkpoint_path).parent / 'acceptance'
    manifest = folder / (replay_id + '.manifest.json')
    doc = json.loads(manifest.read_text())
    source = verify_bronze_source(Path(settings.v1_bronze_path), 'live')
    assert source['change_high_water'] > doc['change_high_water']
    before = inspect(replay_id)
    with pytest.raises(SourceBoundaryChangedError) as result:
        run_replay(manifest, replay_id, resume=True, base_settings=settings)
    assert result.value.code == 'SOURCE_BOUNDARY_CHANGED'
    after = inspect(replay_id)
    assert after == before and after['state'][0] == 'COMPLETE'
    (folder / 'completed-replay-source-advance-refusal.json').write_text(json.dumps(dict(
        replay_id=replay_id, frozen_high=doc['change_high_water'], current_high=source['change_high_water'],
        error=result.value.code, historical_state_preserved=True, before=before, after=after), indent=2))


def test_interrupted_replay_is_aborted_before_ch_on_actual_advanced_source():
    settings = SilverSettings()
    folder = Path(settings.checkpoint_path).parent / 'acceptance'
    original = json.loads((folder / 'p3-real-smoke02-20261121.manifest.json').read_text())
    replay_id = 'p3-preflight-abort-' + uuid4().hex[:12]
    doc = dict(original, replay_run_id=replay_id)
    path = folder / (replay_id + '.manifest.json')
    path.write_text(json.dumps(doc))
    replay_settings = build_replay_settings(settings, replay_id)
    store = SilverCheckpointStore(Path(replay_settings.checkpoint_path))
    store.open()
    store.bind_v1_source(replay_settings.namespace, doc['source_binding'])
    store.conn.execute('INSERT INTO silver_v1_replay_state VALUES (?,?,?,?,?)',
        (replay_id, canonical_manifest_hash(doc), 'RUNNING', None, 'interruption-fixture'))
    store.close()
    with pytest.raises(SourceBoundaryChangedError):
        run_replay(path, replay_id, resume=True, base_settings=settings)
    report = inspect(replay_id)
    assert report['state'][0] == 'ABORTED'
    assert all(count == 0 for count in report['physical_counts'].values())
    (folder / (replay_id + '.report.json')).write_text(json.dumps(report, indent=2))
