import pytest

from de.bronze.checkpoint_store import V1SemanticStore, V1SemanticReader
from de.silver.source_binding import SourceVerificationError, verify_bronze_source


def test_append_above_frozen_page_boundary_is_left_for_next_pass(tmp_path):
    from contracts.traffic_observation import build_observation
    from de.tests.silver.v1_fixtures import snapshot_v1
    path = tmp_path / 'bronze.sqlite3'
    writer = V1SemanticStore(path)
    def append(cycle):
        event = build_observation(snapshot_v1(), cycle_sequence=cycle, simulation_step=cycle * 10)
        raw = dict(raw_ingestion_id=str(cycle), topic='traffic.observations.v1',
                   topic_identity='fixture', partition=0, offset=cycle)
        intent = writer.prepare('live', raw, event, 'hash', 'content', lambda *args: [])
        writer.complete('live', raw['raw_ingestion_id'], intent['expected_hash'])
        return event['eventId']
    first = append(1)
    reader = V1SemanticReader(path)
    try:
        cutoff = verify_bronze_source(path, 'live')['change_high_water']
        second = append(2)
        frozen = reader.read_page('live', 'changes', high_water=cutoff)
        assert [row['event_id'] for row in frozen['rows']] == [first]
        assert frozen['next_cursor'] == 1 and not frozen['has_more']
        following = reader.read_page('live', 'changes', after=frozen['next_cursor'])
        assert [row['event_id'] for row in following['rows']] == [second]
        assert following['next_cursor'] == 2
    finally:
        reader.close()
        writer.close()


def _append(store, sequence, kind='changes'):
    store.db.execute('INSERT OR IGNORE INTO v1_sequence(namespace) VALUES (?)', ('live',))
    if kind == 'changes':
        store.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
                         ('live', sequence, 'event-' + str(sequence), 1, 'ACCEPTED',
                          'PENDING', 'ACCEPTED', 'hash', None, 'now'))
        store.db.execute('UPDATE v1_sequence SET next_change=? WHERE namespace=?',
                         (sequence + 1, 'live'))
    else:
        store.db.execute('INSERT INTO v1_invalidations VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                         ('live', 'inv-' + str(sequence), sequence, 'event-1', 2,
                          'first', 'conflict', 'old', 'new', 'PENDING', 'now'))
        next_change = store.db.execute('SELECT next_change FROM v1_sequence WHERE namespace=?', ('live',)).fetchone()[0]
        store.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
                         ('live', next_change, 'event-1', 2, 'CONFLICTED', 'ACCEPTED',
                          'CONFLICTED', 'hash', 'inv-' + str(sequence), 'now'))
        store.db.execute('UPDATE v1_sequence SET next_change=? WHERE namespace=?', (next_change + 1, 'live'))
        store.db.execute('UPDATE v1_sequence SET next_invalidation=? WHERE namespace=?',
                         (sequence + 1, 'live'))


def test_empty_and_contiguous_histories_and_binding(tmp_path):
    path = tmp_path / 'bronze.sqlite'
    store = V1SemanticStore(path)
    empty = verify_bronze_source(path, 'live')
    assert (empty['change_high_water'], empty['invalidation_high_water']) == (0, 0)
    _append(store, 1)
    _append(store, 2)
    _append(store, 1, 'invalidations')
    bound = verify_bronze_source(path, 'live', empty)
    assert (bound['change_high_water'], bound['invalidation_high_water']) == (3, 1)
    with pytest.raises(SourceVerificationError, match='namespace'):
        verify_bronze_source(path, 'other', bound)
    store.close()


def test_gap_and_allocator_mismatch_fail_closed(tmp_path):
    path = tmp_path / 'bronze.sqlite'
    store = V1SemanticStore(path)
    _append(store, 1)
    _append(store, 2)
    _append(store, 3)
    store.db.execute('DELETE FROM v1_changes WHERE change_sequence=2')
    with pytest.raises(SourceVerificationError) as gap:
        verify_bronze_source(path, 'live')
    assert gap.value.code == 'CURSOR_GAP'
    store.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
                     ('live', 2, 'event-2', 1, 'ACCEPTED', 'PENDING', 'ACCEPTED',
                      'hash', None, 'now'))
    store.db.execute('UPDATE v1_sequence SET next_change=9 WHERE namespace=?', ('live',))
    with pytest.raises(SourceVerificationError) as mismatch:
        verify_bronze_source(path, 'live')
    assert mismatch.value.code == 'CURSOR_GAP'
    store.close()


def test_replacement_and_schema_change_fail_closed(tmp_path):
    first = tmp_path / 'first.sqlite'
    second = tmp_path / 'second.sqlite'
    a, b = V1SemanticStore(first), V1SemanticStore(second)
    binding = verify_bronze_source(first, 'live')
    with pytest.raises(SourceVerificationError) as moved:
        verify_bronze_source(second, 'live', binding)
    assert moved.value.code == 'SOURCE_BINDING_CHANGED'
    b.db.execute('DROP TABLE v1_changes')
    with pytest.raises(SourceVerificationError) as schema:
        verify_bronze_source(second, 'live')
    assert schema.value.code == 'SOURCE_SCHEMA_UNSUPPORTED'
    a.close()
    b.close()


@pytest.mark.parametrize('damage', ['identity-sequence', 'invalidation-event', 'conflict-invalidation'])
def test_contiguous_journals_with_broken_links_fail_closed(tmp_path, damage):
    path = tmp_path / 'bronze.sqlite3'
    store = V1SemanticStore(path)
    try:
        _append(store, 1)
        _append(store, 2)
        if damage == 'identity-sequence':
            store.db.execute('INSERT INTO v1_identity VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                ('live', 'event-1', 'ACCEPTED', 1, 'envelope', 'payload', 'hash', 'raw',
                 'run', 'A', 1, 12.0, 'digest', 1))
            verify_bronze_source(path, 'live')
            store.db.execute('UPDATE v1_identity SET change_sequence=2')
        else:
            _append(store, 1, 'invalidations')
            verify_bronze_source(path, 'live')
            if damage == 'invalidation-event':
                store.db.execute("UPDATE v1_invalidations SET event_id='unrelated'")
            else:
                store.db.execute("UPDATE v1_changes SET invalidation_id='missing' WHERE change_type='CONFLICTED'")
        with pytest.raises(SourceVerificationError) as result:
            verify_bronze_source(path, 'live')
        assert result.value.code == 'CURSOR_GAP'
    finally:
        store.close()
