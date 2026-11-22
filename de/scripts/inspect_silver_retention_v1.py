"""Read-only retained-journal/code inventory for the operational no-prune contract."""
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from de.silver.config import SilverSettings
from de.silver.source_binding import verify_bronze_source, SourceBoundaryChangedError


def capture(settings):
    path = Path(settings.v1_bronze_path)
    before = verify_bronze_source(path, settings.v1_source_namespace)
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        inventory = {}
        for table, sequence in (('v1_changes', 'change_sequence'), ('v1_invalidations', 'invalidation_sequence')):
            inventory[table] = dict(zip(('count', 'min_sequence', 'max_sequence'), db.execute(
                f'SELECT count(*),min({sequence}),max({sequence}) FROM {table} WHERE namespace=?',
                (settings.v1_source_namespace,)).fetchone()))
        journal_mode = db.execute('PRAGMA journal_mode').fetchone()[0]
    root = Path(__file__).resolve().parents[2]
    code = {}
    for filename in ('de/bronze/checkpoint_store.py', 'de/bronze/processor.py',
                     'de/bronze/replay.py', 'de/silver/source_binding.py'):
        raw = (root / filename).read_bytes()
        # Evidence of explicit journal pruning statements, not a general proof
        # against dynamic SQL or a privileged out-of-process file mutation.
        statements = re.findall(r'(?:DELETE\s+FROM|DROP\s+TABLE|TRUNCATE\s+TABLE)\s+v1_(?:changes|invalidations)\b',
                                raw.decode('utf-8'), flags=re.I)
        code[filename] = dict(sha256=hashlib.sha256(raw).hexdigest(), explicit_pruning_statements=statements)
    after = verify_bronze_source(path, settings.v1_source_namespace)
    if before != after:
        raise SourceBoundaryChangedError()
    return dict(observed_at=datetime.now(timezone.utc).isoformat(), source_binding=after,
        journal_mode=journal_mode, journals=inventory, code_inventory=code,
        scope='retained 1..H verification and explicit source statement inventory',
        operational_contract='Preserve this Bronze authority and both journals; no external prune, replacement or rewind while bound Silver/replays exist.',
        limitation='Does not attest to external schedulers or prevent privileged out-of-band file rewriting.')


if __name__ == '__main__':
    settings = SilverSettings()
    report = capture(settings)
    target = Path(settings.checkpoint_path).parent / 'acceptance/source-retention-binding.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))
