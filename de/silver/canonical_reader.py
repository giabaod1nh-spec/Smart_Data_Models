"""Complete-only, in-process Silver V1 observation bundle reader."""
from __future__ import annotations

import base64
import json
import hashlib
import math
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from clickhouse_connect.driver.exceptions import ClickHouseError

from de.bronze.checkpoint_store import V1SemanticReader
from de.silver.source_binding import SourceVerificationError, verify_bronze_source
from de.silver.archive_resolver import ArchiveUnavailable, ArchiveIntegrityError
from de.silver.repositories import FactBusinessKeyConflictError, RetryableRepositoryError
from de.silver.incremental_contract import FeedLimits
from de.silver.incremental_reader import IncrementalSilverSource


class CanonicalReadError(RuntimeError):
    code = "CANONICAL_READ_ERROR"
    retryable = False


class SourceLagError(CanonicalReadError):
    code, retryable = "SOURCE_LAG", True


class InvalidationLagError(CanonicalReadError):
    code, retryable = "INVALIDATION_LAG", True


class AuthorityUnavailableError(CanonicalReadError):
    code, retryable = "AUTHORITY_UNAVAILABLE", True


class ValidityUnknownError(CanonicalReadError):
    code, retryable = "VALIDITY_UNKNOWN", True


class SnapshotStaleError(CanonicalReadError):
    code, retryable = "SNAPSHOT_STALE", True


class SourceBindingChangedError(CanonicalReadError):
    code, retryable = "SOURCE_BINDING_CHANGED", False


class CanonicalContentConflict(CanonicalReadError):
    code, retryable = "CANONICAL_CONTENT_CONFLICT", False


class SourceChurnError(CanonicalReadError):
    code, retryable = "SOURCE_CHURN", True


@dataclass(frozen=True)
class CanonicalReadQuery:
    namespace: str
    run_id: str
    topology_digest: str
    start_simulation_time: float
    end_simulation_time: float

    def __post_init__(self):
        if (self.namespace != "live" or not self.run_id or not self.topology_digest or
                not math.isfinite(self.start_simulation_time) or
                not math.isfinite(self.end_simulation_time) or
                self.start_simulation_time >= self.end_simulation_time):
            raise ValueError("invalid finite Silver V1 read query")


@dataclass(frozen=True)
class ObservationBundle:
    event_id: str
    intersection: dict[str, Any]
    traffic: tuple[dict[str, Any], ...]
    signal: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class CanonicalPage:
    bundles: tuple[ObservationBundle, ...]
    next_key: tuple[str, str, str] | None
    token: str | None
    complete: bool
    has_more: bool


@dataclass(frozen=True)
class InvalidationFeedPage:
    rows: tuple[dict[str, Any], ...]
    next_sequence: int
    source_high_water: int
    source_binding: dict[str, Any]
    has_more: bool
    mirror_pending: int


class CanonicalSilverReader(IncrementalSilverSource):
    """Caller supplies SELECT-only ClickHouse repositories; SQLite is opened RO."""

    def __init__(self, bronze_path: Path, silver_path: Path, silver_repo: Any,
                 bronze_repo: Any, archive_resolver: Any, *, feed_limits: FeedLimits | None = None):
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.silver_repo = silver_repo
        self.bronze_repo = bronze_repo
        self.archive_resolver = archive_resolver
        self.feed_limits = feed_limits if feed_limits is not None else FeedLimits()
        try:
            self._feed_file_identities = {}
            for path in (self.bronze_path, self.silver_path):
                info = path.stat()
                self._feed_file_identities[path] = (info.st_dev, info.st_ino)
            self.bronze = V1SemanticReader(self.bronze_path)
            self.silver = sqlite3.connect(self.silver_path.resolve(strict=True).as_uri() +
                                          "?mode=ro", uri=True, isolation_level=None)
            self.silver.row_factory = sqlite3.Row
            self.silver.execute("PRAGMA query_only=ON")
        except (OSError, sqlite3.Error) as exc:
            if hasattr(self, 'silver'):
                self.silver.close()
            if hasattr(self, 'bronze'):
                self.bronze.close()
            raise AuthorityUnavailableError(str(exc)) from exc

    def close(self) -> None:
        self.bronze.close()
        self.silver.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @staticmethod
    def _token(claims: dict) -> str:
        raw = json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _decode(token: str) -> dict:
        try:
            value = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
            if not isinstance(value, dict):
                raise ValueError('token must contain an object')
            return value
        except (ValueError, UnicodeError, TypeError) as exc:
            raise SnapshotStaleError("invalid page token") from exc

    def _bound_source(self, namespace: str) -> tuple[dict, dict]:
        try:
            source = verify_bronze_source(self.bronze_path, namespace, full_history=False)
            row = self.silver.execute("SELECT * FROM silver_v1_source_binding WHERE namespace=?",
                                      (namespace,)).fetchone()
        except SourceVerificationError as exc:
            if exc.code in {"SOURCE_BINDING_CHANGED", "SOURCE_SCHEMA_UNSUPPORTED", "SOURCE_BOUNDARY_CHANGED"}:
                raise SourceBindingChangedError(str(exc)) from exc
            raise AuthorityUnavailableError(str(exc)) from exc
        except (OSError, sqlite3.Error) as exc:
            raise AuthorityUnavailableError(str(exc)) from exc
        if row is None:
            raise ValidityUnknownError("Silver source binding absent")
        applied = dict(row)
        expected = json.loads(applied["binding_json"])
        if any(source.get(key) != expected.get(key) for key in expected):
            raise SourceBindingChangedError("Silver source binding changed")
        if (applied['change_cursor'] > source['change_high_water'] or
                applied['invalidation_cursor'] > source['invalidation_high_water']):
            raise SourceBindingChangedError('Bronze allocator regressed behind applied Silver cursor')
        return source, applied

    def _boundary(self, namespace: str) -> tuple[dict, dict]:
        source, applied = self._bound_source(namespace)
        if applied["change_cursor"] < source["change_high_water"]:
            raise SourceLagError("Silver change cursor trails Bronze")
        if applied["invalidation_cursor"] < source["invalidation_high_water"]:
            raise InvalidationLagError("Silver invalidation cursor trails Bronze")
        return source, applied

    def read_invalidation_page(self, after_sequence: int = 0, limit: int = 100,
                               source_binding: dict | None = None) -> InvalidationFeedPage:
        """Incremental durable suppression feed, including pending CH mirrors.

        Persist both next_sequence and source_binding when acknowledging a page.
        This feed does not certify completeness of a fact scan.
        """
        if type(after_sequence) is not int or after_sequence < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('feed requires nonnegative sequence and 1..100 entries')
        if after_sequence and source_binding is None:
            raise SourceBindingChangedError('resuming feed requires its saved source binding')
        try:
            source, applied = self._bound_source('live')
            stable = json.loads(applied['binding_json'])
            if source_binding is not None and source_binding != stable:
                raise SourceBindingChangedError('invalidation consumer source binding changed')
            high = source['invalidation_high_water']
            if after_sequence > high:
                raise SnapshotStaleError('invalidation cursor exceeds source high-water')
            page = self.bronze.read_page('live', 'invalidations', after=after_sequence,
                                         limit=limit, high_water=high)
            expected = list(range(after_sequence + 1, min(after_sequence + limit, high) + 1))
            if [row['invalidation_sequence'] for row in page['rows']] != expected:
                raise ValidityUnknownError('invalidation source page has a sequence gap')
            rows = []
            for entry in page['rows']:
                obligations = self.silver.execute('SELECT expected_json,expected_hash,status '
                    'FROM silver_v1_obligation WHERE namespace=? AND source_kind=? AND source_sequence=? '
                    'AND target_table=?', ('live', 'invalidations', entry['invalidation_sequence'],
                                          'silver_invalidation_v1')).fetchall()
                if not obligations:
                    raise InvalidationLagError('local durable invalidation trails source')
                if len(obligations) != 1:
                    raise ValidityUnknownError('ambiguous local invalidation obligation')
                obligation = obligations[0]
                if hashlib.sha256(obligation['expected_json'].encode()).hexdigest() != obligation['expected_hash']:
                    raise ValidityUnknownError('frozen invalidation content hash differs')
                row = json.loads(obligation['expected_json'])
                identity = self.bronze.current('live', entry['event_id'])
                if identity is None or identity['state'] != 'CONFLICTED' or identity['revision'] != entry['revision']:
                    raise SnapshotStaleError('invalidation source identity changed')
                for key in ('simulation_run_id', 'topology_digest', 'intersection_id',
                            'cycle_sequence', 'simulation_time_seconds'):
                    if row.get(key) != identity[key]:
                        raise ValidityUnknownError('invalidation source coordinates differ')
                for key, value in dict(namespace='live', event_id=entry['event_id'],
                        invalidation_id=entry['invalidation_id'], invalidation_sequence=entry['invalidation_sequence'],
                        semantic_revision=entry['revision'], previous_immutable_envelope_hash=entry['previous_hash'],
                        conflicting_immutable_envelope_hash=entry['conflicting_hash']).items():
                    if row.get(key) != value:
                        raise ValidityUnknownError('local/source invalidation identity differs')
                local = self.silver.execute('SELECT state,semantic_revision,invalidation_sequence '
                    'FROM silver_v1_event_validity WHERE namespace=? AND event_id=?',
                    ('live', entry['event_id'])).fetchone()
                if local is None or tuple(local) != ('INVALIDATED', entry['revision'], entry['invalidation_sequence']):
                    raise ValidityUnknownError('durable local suppression missing')
                rows.append(dict(row, mirror_status=obligation['status']))
            ending, _ = self._bound_source('live')
            if ending != source:
                raise SnapshotStaleError('invalidation authority advanced during page')
            next_sequence = rows[-1]['invalidation_sequence'] if rows else after_sequence
            return InvalidationFeedPage(tuple(rows), next_sequence, high, stable,
                                        next_sequence < high,
                                        sum(row['mirror_status'] != 'RECONCILED' for row in rows))
        except CanonicalReadError:
            raise
        except (OSError, sqlite3.Error) as exc:
            raise AuthorityUnavailableError(str(exc)) from exc
        except (ValueError, KeyError, TypeError) as exc:
            raise ValidityUnknownError('malformed frozen invalidation') from exc

    def _bundle(self, event_id: str, revision: int, change_sequence: int,
                query: CanonicalReadQuery) -> ObservationBundle:
        current = self.bronze.current(query.namespace, event_id)
        if current is None or current["state"] != "ACCEPTED" or current["revision"] != revision:
            raise SnapshotStaleError("Bronze event identity changed")
        try:
            accepted = self.bronze_repo.read_valid_observation(
                query.namespace, event_id, revision, state_path=self.bronze_path)
        except RuntimeError as exc:
            raise ValidityUnknownError('Bronze accepted observation cannot be reconciled') from exc
        if accepted is None:
            raise SnapshotStaleError("Bronze event is no longer current")
        context = self.archive_resolver.resolve(query.run_id, query.topology_digest)
        context.validate_event(accepted["event"])
        directions = set(context.directions[accepted["event"]["intersectionId"]])
        families = (
            ("intersection", "silver_intersection_state_fact_v1", 1),
            ("traffic", "silver_traffic_observation_fact_v1", len(directions)),
            ("signal", "silver_signal_state_fact_v1", len(directions)),
        )
        found = {}
        for family, table, count in families:
            physical = self.silver_repo.fetch_v1_event_rows(table, query.run_id,
                                                              query.topology_digest, event_id)
            expected = [json.loads(row[0]) for row in self.silver.execute(
                "SELECT expected_json FROM silver_v1_obligation WHERE namespace=? "
                "AND source_kind='changes' AND source_sequence=? AND target_table=? "
                "AND status='RECONCILED'", (query.namespace, change_sequence, table))]
            if len(expected) != count:
                raise ValidityUnknownError("frozen event bundle is incomplete")
            by_key = {}
            for row in physical:
                key = row.get("source_direction") if family != "intersection" else "intersection"
                if key in by_key and by_key[key] != row:
                    raise CanonicalContentConflict("same-key physical fact content differs")
                by_key[key] = row
            if len(by_key) != count or (family != "intersection" and set(by_key) != directions):
                raise ValidityUnknownError("ClickHouse event bundle is incomplete")
            for row in expected:
                if not self.silver_repo.reconcile_v1_row(table, row):
                    raise ValidityUnknownError("frozen fact is missing from ClickHouse")
            found[family] = by_key
        return ObservationBundle(event_id, found["intersection"]["intersection"],
            tuple(found["traffic"][key] for key in sorted(directions)),
            tuple(found["signal"][key] for key in sorted(directions)))

    def read_valid_page(self, query: CanonicalReadQuery,
                        after_key: tuple[str, str, str] | None = None,
                        limit: int = 100, token: str | None = None) -> CanonicalPage:
        """Return a complete page or a typed failure, never partial bundles."""
        try:
            return self._read_valid_page(query, after_key, limit, token)
        except CanonicalReadError:
            raise
        except FactBusinessKeyConflictError as exc:
            raise CanonicalContentConflict(str(exc)) from exc
        except (ClickHouseError, RetryableRepositoryError, ArchiveUnavailable,
                OSError, sqlite3.Error) as exc:
            raise AuthorityUnavailableError(str(exc)) from exc
        except ArchiveIntegrityError as exc:
            raise ValidityUnknownError(str(exc)) from exc

    def _read_valid_page(self, query: CanonicalReadQuery,
                         after_key: tuple[str, str, str] | None,
                         limit: int, token: str | None) -> CanonicalPage:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100 event bundles")
        if after_key is not None and (not isinstance(after_key, tuple) or len(after_key) != 3 or
                any(not isinstance(value, str) for value in after_key) or
                after_key[:2] != (query.run_id, query.topology_digest)):
            raise SnapshotStaleError('page key does not match query')
        if token is None and after_key is not None:
            raise SnapshotStaleError("first page cannot skip an event key")
        try:
            source, applied = self._boundary(query.namespace)
        except (SourceLagError, InvalidationLagError) as exc:
            if token is not None:
                raise SnapshotStaleError("authority advanced after prior page") from exc
            raise
        fixed = dict(version=1, reader_version=1, schema_version="008",
            ordering_key_version=1, query=query.__dict__, limit=limit,
            binding=json.loads(applied["binding_json"]),
            change_high=source["change_high_water"],
            invalidation_high=source["invalidation_high_water"],
            change_cursor=applied["change_cursor"],
            invalidation_cursor=applied["invalidation_cursor"])
        if token is not None:
            claims = self._decode(token)
            if {key: claims.get(key) for key in fixed} != fixed or claims.get("next_key") != list(after_key or ()):
                raise SnapshotStaleError("page token boundary or cursor changed")
        cursor = after_key[2] if after_key else ""
        rows = self.silver.execute(
            "SELECT event_id,semantic_revision,change_sequence FROM silver_v1_event_validity "
            "WHERE namespace=? AND simulation_run_id=? AND topology_digest=? "
            "AND simulation_time_seconds>=? AND simulation_time_seconds<? "
            "AND state='VALID' AND event_id>? ORDER BY event_id LIMIT ?",
            (query.namespace, query.run_id, query.topology_digest,
             query.start_simulation_time, query.end_simulation_time, cursor, limit + 1)).fetchall()
        selected = rows[:limit]
        bundles = tuple(self._bundle(row["event_id"], row["semantic_revision"],
                                     row["change_sequence"], query) for row in selected)
        for row in selected:
            latest = self.silver.execute("SELECT state,semantic_revision,change_sequence "
                "FROM silver_v1_event_validity WHERE namespace=? AND event_id=?",
                (query.namespace, row["event_id"])).fetchone()
            if latest is None or tuple(latest) != ("VALID", row["semantic_revision"], row["change_sequence"]):
                raise SnapshotStaleError("local validity changed during page")
        try:
            ending, ending_applied = self._boundary(query.namespace)
        except (SourceLagError, InvalidationLagError) as exc:
            raise SnapshotStaleError("authority advanced during page") from exc
        if (ending["change_high_water"] != source["change_high_water"] or
                ending["invalidation_high_water"] != source["invalidation_high_water"] or
                ending_applied["binding_json"] != applied["binding_json"]):
            raise SnapshotStaleError("authority changed during page")
        next_key = ((query.run_id, query.topology_digest, selected[-1]["event_id"])
                    if selected else after_key)
        has_more = len(rows) > limit
        next_token = self._token(dict(fixed, next_key=list(next_key))) if has_more else None
        return CanonicalPage(bundles, next_key, next_token, True, has_more)

    def read_complete_scan(self, query: CanonicalReadQuery,
                           page_size: int = 100) -> tuple[ObservationBundle, ...]:
        """Stage all pages; discard them and restart on a stale source boundary."""
        backoff = (0.1, 0.25, 0.5)
        for restart in range(4):
            staged = []
            key = None
            token = None
            try:
                while True:
                    for attempt in range(4):
                        try:
                            page = self.read_valid_page(query, key, page_size, token)
                            break
                        except (AuthorityUnavailableError, ValidityUnknownError, OSError) as exc:
                            if attempt == 3:
                                raise
                            time.sleep(backoff[attempt])
                    staged.extend(page.bundles)
                    if not page.has_more:
                        return tuple(staged)
                    key, token = page.next_key, page.token
            except SnapshotStaleError as exc:
                if restart == 3:
                    raise SourceChurnError("source did not stabilize across pages") from exc
                time.sleep(backoff[restart])
        raise SourceChurnError("source did not stabilize")


class CanonicalReadSession(CanonicalSilverReader):
    """Own all read connections; configure CH credentials with SELECT grants."""

    def __init__(self, settings, *, feed_limits: FeedLimits | None = None):
        from de.bronze.clickhouse_repository import BronzeClickHouseRepository
        from de.bronze.config import ObservationSettings
        from de.silver.archive_resolver import ArchiveResolver
        from de.silver.repositories import SilverClickHouseRepository

        bronze_settings = ObservationSettings(
            checkpoint_path=settings.v1_bronze_path,
            clickhouse_host=settings.clickhouse_host,
            clickhouse_port=settings.clickhouse_port,
            clickhouse_user=settings.clickhouse_user,
            clickhouse_password=settings.clickhouse_password,
            clickhouse_database=settings.clickhouse_database,
            clickhouse_secure=settings.clickhouse_secure)
        self._owned_silver_repo = SilverClickHouseRepository(settings)
        self._owned_bronze_repo = BronzeClickHouseRepository(bronze_settings)
        try:
            self._owned_silver_repo.connect()
            self._owned_bronze_repo.connect()
            super().__init__(Path(settings.v1_bronze_path), Path(settings.checkpoint_path),
                self._owned_silver_repo, self._owned_bronze_repo,
                ArchiveResolver(Path(settings.v1_archive_root)), feed_limits=feed_limits)
        except Exception as exc:
            self._owned_silver_repo.close()
            self._owned_bronze_repo.close()
            if isinstance(exc, (ClickHouseError, OSError, sqlite3.Error)):
                raise AuthorityUnavailableError(str(exc)) from exc
            raise

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._owned_silver_repo.close()
            self._owned_bronze_repo.close()
