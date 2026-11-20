import sqlite3
import pytest
from de.bronze.checkpoint_store import V1SemanticStore,V1SemanticReader


def test_readonly_journal_pages_keep_history_and_equal_timestamps(tmp_path):
    path=tmp_path/'state.sqlite'
    writer=V1SemanticStore(path)
    for seq,eid,rev,kind in [(1,'A',1,'ACCEPTED'),(2,'B',1,'ACCEPTED'),(3,'A',2,'CONFLICTED')]:
        writer.db.execute('INSERT INTO v1_changes VALUES (?,?,?,?,?,?,?,?,?,?)',
            ('live',seq,eid,rev,kind,None,kind,'hash',None,'same-time'))
    reader=V1SemanticReader(path)
    first=reader.read_page('live','changes',limit=2)
    assert first['high_water']==3 and first['next_cursor']==2
    reader.close()
    reader=V1SemanticReader(path)
    second=reader.read_page('live','changes',after=first['next_cursor'],high_water=first['high_water'])
    assert [r['change_type'] for r in second['rows']]==['CONFLICTED']
    with pytest.raises(sqlite3.OperationalError):
        reader.db.execute('DELETE FROM v1_changes')
    reader.close()
    writer.close()
