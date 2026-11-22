"""Read-only Bronze V1 journal verification for Silver's source boundary."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from de.bronze import MIGRATION_VERSION
from de.bronze.checkpoint_store import V1_SCHEMA


REQUIRED_TABLES = (
    'v1_identity', 'v1_sequence', 'v1_changes', 'v1_invalidations', 'v1_intents',
)
JOURNALS = (('changes', 'change_sequence', 'next_change'),
            ('invalidations', 'invalidation_sequence', 'next_invalidation'))


class SourceVerificationError(RuntimeError):
    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(detail)


class SourceBoundaryChangedError(SourceVerificationError):
    def __init__(self):
        super().__init__('SOURCE_BOUNDARY_CHANGED', 'Bronze allocator or source binding changed')


def verify_bronze_source(path: Path, namespace: str, expected_binding: dict | None = None,
                         *, full_history: bool = True) -> dict:
    """Verify retained 1..H histories in one SQLite snapshot.

    Full verification is for bootstrap, restart, or binding changes, not each
    polling cycle. The caller must also establish the operational no-prune
    contract; SQLite alone cannot attest to arbitrary out-of-band rewriting.
    """
    try:
        source = Path(path).resolve(strict=True)
    except OSError as exc:
        raise SourceVerificationError('AUTHORITY_UNAVAILABLE', str(exc)) from exc
    if not namespace:
        raise SourceVerificationError('SOURCE_BINDING_CHANGED', 'namespace required')
    try:
        db = sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            rows = db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' "
                              "AND name IN ('v1_identity','v1_sequence','v1_changes',"
                              "'v1_invalidations','v1_intents') ORDER BY name").fetchall()
            schema = {row['name']: row['sql'] for row in rows}
            if set(schema) != set(REQUIRED_TABLES):
                raise SourceVerificationError('SOURCE_SCHEMA_UNSUPPORTED', 'required Bronze V1 tables missing')
            reference = sqlite3.connect(':memory:')
            try:
                reference.executescript(V1_SCHEMA)
                expected_schema = dict(reference.execute(
                    "SELECT name,sql FROM sqlite_master WHERE type='table' "
                    "AND name IN ('v1_identity','v1_sequence','v1_changes',"
                    "'v1_invalidations','v1_intents')"))
            finally:
                reference.close()
            if schema != expected_schema:
                raise SourceVerificationError('SOURCE_SCHEMA_UNSUPPORTED', 'Bronze V1 DDL differs')
            expected_columns = {
                'v1_sequence': {'namespace', 'next_change', 'next_invalidation'},
                'v1_changes': {'namespace', 'change_sequence', 'event_id', 'revision'},
                'v1_invalidations': {'namespace', 'invalidation_sequence', 'event_id', 'revision'},
                'v1_identity': {'namespace', 'event_id', 'state', 'revision', 'content_hash'},
                'v1_intents': {'namespace', 'raw_id', 'expected_json', 'processing_status'},
            }
            for table, required in expected_columns.items():
                actual = {row['name'] for row in db.execute('PRAGMA table_info(' + table + ')')}
                if not required <= actual:
                    raise SourceVerificationError('SOURCE_SCHEMA_UNSUPPORTED', table + ' columns differ')
            fingerprint = hashlib.sha256(json.dumps(schema, sort_keys=True).encode()).hexdigest()
            allocator = db.execute('SELECT next_change,next_invalidation FROM v1_sequence '
                                   'WHERE namespace=?', (namespace,)).fetchone()
            if allocator is None:
                # Another namespace is not an acceptable replacement for the
                # configured live authority.
                other = db.execute('SELECT 1 FROM v1_sequence LIMIT 1').fetchone()
                if other:
                    raise SourceVerificationError('SOURCE_BINDING_CHANGED', 'namespace allocator missing')
            boundaries = {}
            for kind, sequence, column in JOURNALS:
                high = int(allocator[column]) - 1 if allocator else 0
                if high < 0:
                    raise SourceVerificationError('CURSOR_GAP', 'negative allocator boundary')
                if full_history:
                    stats = db.execute('SELECT min(' + sequence + '),max(' + sequence + '),count(*) '
                                       'FROM v1_' + kind + ' WHERE namespace=?', (namespace,)).fetchone()
                    minimum, maximum, count = stats
                    if (high == 0 and count != 0) or (high > 0 and
                            (minimum != 1 or maximum != high or count != high)):
                        raise SourceVerificationError('CURSOR_GAP', kind + ' journal is incomplete')
                boundaries[kind] = high
            if full_history:
                bad_identity = db.execute(
                    "SELECT 1 FROM v1_identity i LEFT JOIN v1_changes c "
                    "ON c.namespace=i.namespace AND c.change_sequence=i.change_sequence "
                    "WHERE i.namespace=? AND i.state!='PENDING' AND (c.change_sequence IS NULL "
                    "OR c.event_id!=i.event_id OR c.revision!=i.revision OR c.new_state!=i.state "
                    "OR c.change_type!=i.state) LIMIT 1", (namespace,)).fetchone()
                bad_invalidation = db.execute(
                    "SELECT 1 FROM v1_invalidations i WHERE i.namespace=? AND "
                    "(SELECT count(*) FROM v1_changes c WHERE c.namespace=i.namespace "
                    "AND c.invalidation_id=i.invalidation_id AND c.change_type='CONFLICTED' "
                    "AND c.event_id=i.event_id AND c.revision=i.revision)!=1 LIMIT 1",
                    (namespace,)).fetchone()
                bad_conflict = db.execute(
                    "SELECT 1 FROM v1_changes c WHERE c.namespace=? AND c.change_type='CONFLICTED' "
                    "AND (SELECT count(*) FROM v1_invalidations i WHERE i.namespace=c.namespace "
                    "AND i.invalidation_id=c.invalidation_id AND i.event_id=c.event_id "
                    "AND i.revision=c.revision)!=1 LIMIT 1", (namespace,)).fetchone()
                if bad_identity or bad_invalidation or bad_conflict:
                    raise SourceVerificationError('CURSOR_GAP', 'Bronze identity/journal/invalidation linkage is incomplete')
            db.execute('COMMIT')
        finally:
            if db.in_transaction:
                db.execute('ROLLBACK')
            db.close()
    except sqlite3.DatabaseError as exc:
        unavailable = any(term in str(exc).lower() for term in (
            'unable to open', 'database is locked', 'disk i/o', 'readonly database'))
        raise SourceVerificationError('AUTHORITY_UNAVAILABLE' if unavailable else
                                      'SOURCE_SCHEMA_UNSUPPORTED', str(exc)) from exc
    stat = source.stat()
    binding = dict(namespace=namespace, source_path=str(source),
                   schema_fingerprint=fingerprint, bronze_version=MIGRATION_VERSION,
                   file_identity=[stat.st_dev, stat.st_ino],
                   change_high_water=boundaries['changes'],
                   invalidation_high_water=boundaries['invalidations'])
    if expected_binding:
        for key in ('namespace', 'source_path', 'schema_fingerprint', 'bronze_version', 'file_identity'):
            if binding[key] != expected_binding.get(key):
                raise SourceVerificationError('SOURCE_BINDING_CHANGED', key + ' changed')
    return binding
