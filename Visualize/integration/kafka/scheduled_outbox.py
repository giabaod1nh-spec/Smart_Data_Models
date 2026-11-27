"""Opt-in producer horizon for isolated runs on a disposable V1 outbox."""

import json
import os
import sqlite3
import stat
from contextlib import closing, contextmanager
from hashlib import sha256
from pathlib import Path
from re import fullmatch
from tempfile import NamedTemporaryFile, mkdtemp
from threading import Lock

from contracts.observation_measurements import digest as valid_digest, encode_measurements, original_anchor
from contracts.observation_schedule import ObservationSchedule
from contracts.traffic_observation import canonical_json, observation_key, validate_observation
from integration.kafka.outbox_store import KafkaOutboxStore, OutboxIdentityConflictError


_SCHEMA = """
CREATE TABLE IF NOT EXISTS observation_schedule_head (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schedule_id TEXT NOT NULL,
    schedule_bytes BLOB NOT NULL,
    lifecycle TEXT NOT NULL,
    lifecycle_tick INTEGER NOT NULL,
    settled_ordinal INTEGER NOT NULL DEFAULT -1,
    journaled_ordinal INTEGER NOT NULL DEFAULT -1
);
CREATE TABLE IF NOT EXISTS observation_measurement_revision (
    sequence INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    digest TEXT NOT NULL UNIQUE,
    content BLOB NOT NULL,
    index_digest TEXT NOT NULL UNIQUE,
    index_bytes BLOB NOT NULL,
    UNIQUE(event_id,revision)
);
CREATE TABLE IF NOT EXISTS observation_schedule_failure (
    ordinal INTEGER PRIMARY KEY,
    reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observation_schedule_journal (
    sequence INTEGER PRIMARY KEY,
    digest TEXT NOT NULL UNIQUE,
    segment BLOB NOT NULL
);
"""

_TRANSITIONS = {
    "REGISTERED": {"RUNNING", "ABORTED"},
    "RUNNING": {"PAUSED", "ENDED", "ABORTED"},
    "PAUSED": {"RUNNING", "ENDED", "ABORTED"},
}
_TERMINAL = {"ENDED", "ABORTED"}
_MAX_JOURNAL_SLOTS = 1024
_EXPORT_BATCH = 4
_HEAD_LIMIT = 1024
_APPLICATION_ID = 0x53434841
_SCRATCH_PREFIX = "stage-a-schedule-"
_DB_NAME = "scheduled-outbox.sqlite3"


def _plain_path(path, *, directory=False):
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if (not expected(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400
            or (not directory and info.st_nlink != 1)):
        raise OutboxIdentityConflictError(f"unsafe disposable archive path: {path.name}")


def _verified_path(db_path):
    db_path = Path(db_path).absolute()
    if db_path.name != _DB_NAME or not db_path.parent.name.startswith(_SCRATCH_PREFIX):
        raise OutboxIdentityConflictError("scheduled store requires a dedicated disposable database")
    try:
        _plain_path(db_path.parent, directory=True)
        _plain_path(db_path)
        for suffix in ("-wal", "-shm", "-journal"):
            companion = db_path.with_name(db_path.name + suffix)
            if companion.exists() or companion.is_symlink():
                _plain_path(companion)
        with db_path.open("rb") as stream:
            header = stream.read(100)
    except OSError as exc:
        raise OutboxIdentityConflictError("disposable database identity is unavailable") from exc
    # Inspect the checkpointed SQLite application ID before any writable SQLite
    # connection, WAL transition or table creation. Creation stamps it in DELETE mode.
    if header[:16] != b"SQLite format 3\x00" or int.from_bytes(header[68:72], "big") != _APPLICATION_ID:
        raise OutboxIdentityConflictError("database has no isolated schedule application identity")
    return db_path.resolve(strict=True)


def _verify_existing_binding(db_path, schedule):
    wal = db_path.with_name(db_path.name + "-wal")
    # A clean close leaves no WAL contents. In that case avoid SQLite
    # creating temporary WAL side files even for the read-only preflight.
    mode = "?mode=ro" if wal.exists() and wal.stat().st_size else "?mode=ro&immutable=1"
    with closing(sqlite3.connect(db_path.as_uri() + mode, uri=True)) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "observation_schedule_head" in tables:
            bound = db.execute("SELECT schedule_id,schedule_bytes FROM observation_schedule_head WHERE singleton=1").fetchone()
            if bound is not None:
                if bound != (schedule.schedule_id, schedule.canonical_bytes()):
                    raise OutboxIdentityConflictError("outbox is bound to another immutable schedule")
                return
        if "kafka_outbox" in tables and db.execute("SELECT 1 FROM kafka_outbox LIMIT 1").fetchone():
            raise OutboxIdentityConflictError("schedule requires a new disposable outbox")


def _writer_guard(path):
    try:
        with path.open("xb"):
            pass
    except FileExistsError:
        pass
    _plain_path(path)
    handle = path.open("r+b")
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise OutboxIdentityConflictError("disposable schedule already has a writer") from exc
    return handle


def _read_bounded(path, size):
    with path.open("rb") as stream:
        return stream.read(size + 1)


def _sync_directory(path):
    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _install_archive_file(path, content, *, immutable):
    if immutable and (path.exists() or path.is_symlink()):
        _plain_path(path)
        if _read_bounded(path, len(content)) != content:
            raise OutboxIdentityConflictError("immutable schedule archive content differs")
        return
    with NamedTemporaryFile(dir=path.parent, prefix=".schedule-", delete=False) as handle:
        staging = Path(handle.name)
        try:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            handle.close()
            staging.unlink(missing_ok=True)
            raise
    try:
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
            move.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
            move.restype = wintypes.BOOL
            flags = 0x08 | (0 if immutable else 0x01)  # WRITE_THROUGH, REPLACE_EXISTING
            if not move(_windows_path(staging), _windows_path(path), flags):
                raise ctypes.WinError(ctypes.get_last_error())
        else:
            # The disposable directory belongs to a single writer process. The
            # immutable-existence check and rename are both under its guard.
            os.replace(staging, path)
    finally:
        staging.unlink(missing_ok=True)


def _windows_path(path):
    value = str(path.absolute())
    return "\\\\?\\UNC\\" + value[2:] if value.startswith("\\\\") else "\\\\?\\" + value


class IsolatedScheduledOutbox(KafkaOutboxStore):
    @classmethod
    def create(cls, scratch_parent, schedule, **kwargs):
        parent = Path(scratch_parent).resolve(strict=True)
        directory = Path(mkdtemp(prefix=_SCRATCH_PREFIX, dir=parent))
        db_path = directory / _DB_NAME
        connection = sqlite3.connect(db_path)
        try:
            connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
        finally:
            connection.close()
        return cls(db_path, schedule, **kwargs)

    def __init__(self, db_path: Path, schedule: ObservationSchedule, **kwargs):
        if fullmatch(r"[A-Za-z0-9_-]{1,128}", schedule.run_id) is None:
            raise OutboxIdentityConflictError("run ID is unsafe for a disposable archive")
        db_path = _verified_path(db_path)
        self.schedule = schedule
        self.schedule_id = schedule.schedule_id
        self.run_archive = db_path.parent / "runs" / schedule.run_id
        self._export_guard = Lock()
        self._owner_handle = _writer_guard(db_path.parent / ".writer.lock")
        try:
            _verify_existing_binding(db_path, schedule)
            super().__init__(db_path, **kwargs)
            self._conn.executescript(_SCHEMA)
            with self._transaction() as db:
                existing = self._head(db)
                if existing is None:
                    if db.execute("SELECT 1 FROM kafka_outbox LIMIT 1").fetchone():
                        raise OutboxIdentityConflictError("schedule requires a new disposable outbox")
                    db.execute(
                        "INSERT INTO observation_schedule_head "
                        "(singleton,schedule_id,schedule_bytes,lifecycle,lifecycle_tick) "
                        "VALUES (1,?,?,'REGISTERED',0)",
                        (self.schedule_id, schedule.canonical_bytes()),
                    )
                    self._journal(db, self._head(db), through=-1)
                elif (existing["schedule_id"] != self.schedule_id
                      or existing["schedule_bytes"] != schedule.canonical_bytes()):
                    raise OutboxIdentityConflictError("outbox is bound to another immutable schedule")
        except BaseException:
            if hasattr(self, "_conns_lock"):
                self.close()
            else:
                self._owner_handle.close()
            raise

    def close(self):
        with self._export_guard:
            try:
                super().close()
            finally:
                self._owner_handle.close()

    # The normal V1 publisher uses the base store and creates none of these tables.
    def append_v1_cycle(self, rows, **kwargs):
        raise OutboxIdentityConflictError("scheduled observations require an explicit ordinal")

    def append_scheduled_cycle(self, ordinal, rows, *, measurement_evidence=None):
        rows = tuple(rows)
        expected = dict(zip(self.schedule.event_ids(ordinal), self.schedule.participants))
        if len(rows) != len(expected) or {r.event_id for r in rows} != set(expected):
            raise OutboxIdentityConflictError("slot must cover every scheduled participant exactly once")
        evidence = {} if measurement_evidence is None else dict(measurement_evidence)
        if set(evidence) - set(expected):
            raise OutboxIdentityConflictError("measurement evidence was supplied for an unrelated event")
        encoded = {}
        for row in rows:
            event = json.loads(row.payload_json)
            validate_observation(event)
            self.schedule.verify_capture(ordinal, event["simulationTimeSeconds"], event["simulationStep"])
            node, directions = expected[row.event_id]
            if (event["eventId"] != row.event_id or event["intersectionId"] != node
                    or row.simulation_run_id != self.schedule.run_id
                    or row.cycle_sequence != self.schedule.cycle(ordinal)
                    or row.event_key != observation_key(event)
                    or row.payload_hash != event["payloadHash"]
                    or event["topologyDigest"] != self.schedule.topology_digest
                    or tuple(sorted(event["payload"]["directions"])) != directions
                    or row.payload_json != canonical_json(event)):
                raise OutboxIdentityConflictError("frozen row differs from scheduled observation")
            if row.event_id in evidence:
                anchor = original_anchor(event, self.schedule.generation, self.schedule.manifest_digest)
                encoded[row.event_id] = encode_measurements(anchor, directions, evidence[row.event_id])

        def seal(db, inserted):
            head = self._head(db)
            if ordinal <= head["settled_ordinal"]:
                failed = db.execute("SELECT 1 FROM observation_schedule_failure WHERE ordinal=?", (ordinal,)).fetchone()
                if inserted or failed:
                    raise OutboxIdentityConflictError("slot cannot be recreated or replace a failure")
                for event_id, content in encoded.items():
                    prior = db.execute("SELECT content FROM observation_measurement_revision "
                                       "WHERE event_id=? AND revision=1", (event_id,)).fetchone()
                    if prior is None or prior["content"] != content:
                        raise OutboxIdentityConflictError("settled measurement differs from the frozen revision")
                return
            self._advance(db, head, ordinal)
            for event_id, content in encoded.items():
                self._save_measurement(db, event_id, 1, content)

        return super().append_v1_cycle(rows, _seal_slot=seal)

    def correct_measurement(self, event_id, *, previous_digest, evidence):
        """Append one explicit replacement or withdrawal of previously captured evidence."""
        if not valid_digest(previous_digest):
            raise ValueError("measurement correction requires the preceding digest")
        with self._transaction() as db:
            prior = db.execute("SELECT * FROM observation_measurement_revision WHERE event_id=? "
                               "ORDER BY revision DESC LIMIT 1", (event_id,)).fetchone()
            if prior is None:
                raise OutboxIdentityConflictError("measurement correction has no original evidence")
            record = json.loads(prior["content"])
            if sha256(prior["content"]).hexdigest() != prior["digest"]:
                raise OutboxIdentityConflictError("prior measurement identity changed")
            if record["previousDigest"] == previous_digest and record["evidence"] == evidence:
                return prior["digest"]
            if prior["digest"] != previous_digest or record["evidence"] is None:
                raise OutboxIdentityConflictError("measurement correction is stale or withdrawn")
            anchor = record["anchor"]
            directions = dict(self.schedule.participants)[anchor["intersectionId"]]
            revision = prior["revision"] + 1
            content = encode_measurements(anchor, directions, evidence, previous=previous_digest, revision=revision)
            return self._save_measurement(db, event_id, revision, content)

    def _save_measurement(self, db, event_id, revision, content):
        digest = sha256(content).hexdigest()
        prior = db.execute("SELECT sequence,index_digest FROM observation_measurement_revision "
                           "ORDER BY sequence DESC LIMIT 1").fetchone()
        sequence, previous = (prior["sequence"] + 1, prior["index_digest"]) if prior else (1, None)
        index = canonical_json({"sequence": sequence, "scheduleId": self.schedule_id,
                                "previousIndexDigest": previous, "objectDigest": digest}).encode()
        db.execute("INSERT INTO observation_measurement_revision VALUES (?,?,?,?,?,?,?)",
                   (sequence, event_id, revision, digest, content, sha256(index).hexdigest(), index))
        return digest

    def record_failure(self, ordinal, reason):
        self.schedule.due_tick(ordinal)
        if reason not in ("CAPTURE_FAILED", "MISSED_TICK", "CLOCK_MISMATCH"):
            raise ValueError("unsupported producer slot failure")
        with self._transaction() as db:
            head = self._head(db)
            if ordinal <= head["settled_ordinal"]:
                previous = db.execute("SELECT reason FROM observation_schedule_failure WHERE ordinal=?", (ordinal,)).fetchone()
                if previous and previous["reason"] == reason:
                    return
                raise OutboxIdentityConflictError("slot already has a different outcome")
            self._advance(db, head, ordinal)
            db.execute("INSERT INTO observation_schedule_failure VALUES (?,?)", (ordinal, reason))

    def set_lifecycle(self, state, *, tick):
        """Seal a lifecycle change at a proven clock without settling more slots."""
        with self._transaction() as db:
            head = self._head(db)
            before = head["lifecycle"]
            if state == before and tick == head["lifecycle_tick"]:
                return
            if state not in _TRANSITIONS.get(before, ()):
                raise OutboxIdentityConflictError("invalid schedule lifecycle transition")
            if (type(tick) is not int or tick % self.schedule.step_ticks
                    or tick < head["lifecycle_tick"]
                    or (before == "PAUSED" and tick != head["lifecycle_tick"])):
                raise OutboxIdentityConflictError("lifecycle clock is inconsistent")
            settled = head["settled_ordinal"]
            if ((settled >= 0 and tick < self.schedule.due_tick(settled))
                    or (tick >= self.schedule.due_tick(settled + 1)
                        and (state != "RUNNING" or before != "REGISTERED"
                             or tick != self.schedule.first_tick))):
                raise OutboxIdentityConflictError("lifecycle would skip or precede a settled slot")
            if settled - head["journaled_ordinal"] > _MAX_JOURNAL_SLOTS:
                raise OutboxIdentityConflictError("checkpoint the producer prefix before transition")
            db.execute("UPDATE observation_schedule_head SET lifecycle=?,lifecycle_tick=?", (state, tick))
            self._journal(db, self._head(db), through=settled)

    def checkpoint(self):
        """Persist one bounded prefix segment. Retry while the cursor advances."""
        with self._transaction() as db:
            head = self._head(db)
            previous = head["journaled_ordinal"]
            through = min(head["settled_ordinal"], previous + _MAX_JOURNAL_SLOTS)
            if through > previous:
                self._journal(db, head, through=through)
            return through

    def read_journal(self, *, after=0, limit=100):
        if not 1 <= limit <= _MAX_JOURNAL_SLOTS or after < 0:
            raise ValueError("invalid journal page bounds")
        return tuple(tuple(row) for row in self._conn.execute(
            "SELECT sequence,digest,segment FROM observation_schedule_journal "
            "WHERE sequence > ? ORDER BY sequence LIMIT ?", (after, limit),
        ))

    def export_checkpoint(self):
        return self.export_archive_prefixes()[0]

    def sealed_archive_prefixes(self):
        return tuple(self._conn.execute("""
            SELECT coalesce((SELECT max(sequence) FROM observation_schedule_journal),0),
                   coalesce((SELECT max(sequence) FROM observation_measurement_revision),0)
        """).fetchone())

    def source_status(self):
        row = self._conn.execute("""SELECT lifecycle,EXISTS (SELECT 1 FROM observation_schedule_failure)
            FROM observation_schedule_head WHERE singleton=1""").fetchone()
        if row["lifecycle"] == "ENDED":
            return "INCOMPLETE" if row[1] else "COMPLETE"
        if row["lifecycle"] == "ABORTED":
            return "INCOMPLETE"
        return "RUNNING"

    def export_archive_prefixes(self):
        """Export a bounded batch of committed segments, publishing its head last."""
        directory = self.run_archive / "observation_schedule_v1"
        head_path = directory / "head.json"
        # Lifetime writer ownership covers restart and competing processes. The
        # export guard orders local exports without blocking the Outbox write gate.
        with self._export_guard:
            db = self._conn
            for folder in (self.run_archive.parent, self.run_archive, directory):
                folder.mkdir(exist_ok=True)
                _plain_path(folder, directory=True)
            sequence = 0
            measurement_sequence = 0
            segment_digest = measurement_digest = None
            if head_path.exists() or head_path.is_symlink():
                _plain_path(head_path)
                raw = _read_bounded(head_path, _HEAD_LIMIT)
                try:
                    head = json.loads(raw) if len(raw) <= _HEAD_LIMIT else {}
                    sequence = head["sequence"]
                    if type(sequence) is not int or not 1 <= sequence <= 2**53 - 1:
                        raise ValueError("invalid sequence")
                    measurement_sequence = head["measurementSequence"]
                    if type(measurement_sequence) is not int or not 0 <= measurement_sequence <= 2**53 - 1:
                        raise ValueError("invalid measurement sequence")
                    row = db.execute("SELECT digest,segment FROM observation_schedule_journal WHERE sequence=?",
                                     (sequence,)).fetchone()
                    measurement = db.execute("SELECT * FROM observation_measurement_revision WHERE sequence=?",
                                             (measurement_sequence,)).fetchone() if measurement_sequence else None
                    if row is None or (measurement_sequence and measurement is None):
                        raise ValueError("head is unrelated to this producer")
                    segment_digest = row["digest"]
                    measurement_digest = measurement["index_digest"] if measurement else None
                    if raw != self._archive_head(sequence, segment_digest, measurement_sequence, measurement_digest):
                        raise ValueError("head changed")
                    self._validate_archive_file(directory / f"schedule-{sequence}.json", row["segment"])
                    if measurement:
                        self._validate_archive_file(directory / f"measurement-{measurement_sequence}.json",
                                                    measurement["index_bytes"])
                        self._validate_archive_file(directory / (measurement["digest"] + ".json"),
                                                    measurement["content"])
                except (OSError, ValueError, TypeError, KeyError) as exc:
                    raise OutboxIdentityConflictError("schedule archive head failed validation") from exc
            _install_archive_file(directory / (self.schedule_id + ".json"),
                                  self.schedule.canonical_bytes(), immutable=True)
            records = self.read_journal(after=sequence, limit=_EXPORT_BATCH)
            measurements = db.execute("SELECT * FROM observation_measurement_revision WHERE sequence>? "
                                      "ORDER BY sequence LIMIT ?", (measurement_sequence, _EXPORT_BATCH)).fetchall()
            for sequence, segment_digest, segment in records:
                if sha256(segment).hexdigest() != segment_digest:
                    raise OutboxIdentityConflictError("committed producer segment hash differs")
                _install_archive_file(directory / f"schedule-{sequence}.json", segment, immutable=True)
            for measurement in measurements:
                measurement_sequence = measurement["sequence"]
                measurement_digest = measurement["index_digest"]
                if (sha256(measurement["content"]).hexdigest() != measurement["digest"]
                        or sha256(measurement["index_bytes"]).hexdigest() != measurement_digest):
                    raise OutboxIdentityConflictError("committed measurement hash differs")
                _install_archive_file(directory / (measurement["digest"] + ".json"),
                                      measurement["content"], immutable=True)
                _install_archive_file(directory / f"measurement-{measurement_sequence}.json",
                                      measurement["index_bytes"], immutable=True)
            if records or measurements:
                _sync_directory(directory)
                _install_archive_file(head_path, self._archive_head(sequence, segment_digest,
                                      measurement_sequence, measurement_digest), immutable=False)
            for folder in (directory, self.run_archive, self.run_archive.parent, self.db_path.parent):
                _sync_directory(folder)
            return sequence, measurement_sequence

    @staticmethod
    def _validate_archive_file(path, expected):
        _plain_path(path)
        if _read_bounded(path, len(expected)) != expected:
            raise ValueError("published archive object changed")

    def _archive_head(self, sequence, digest, measurement_sequence, measurement_digest):
        return canonical_json({"schemaVersion": 1, "scheduleId": self.schedule_id,
                               "sequence": sequence, "segmentDigest": digest,
                               "measurementSequence": measurement_sequence,
                               "measurementDigest": measurement_digest}).encode("utf-8")

    def _advance(self, db, head, ordinal):
        if (head["lifecycle"] != "RUNNING" or ordinal != head["settled_ordinal"] + 1
                or self.schedule.due_tick(ordinal) < head["lifecycle_tick"]):
            raise OutboxIdentityConflictError("slot cannot advance the running contiguous prefix")
        db.execute("UPDATE observation_schedule_head SET settled_ordinal=?", (ordinal,))

    @contextmanager
    def _transaction(self):
        db = self._conn
        with self._write_gate.priority():
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                self._rollback_quiet(db)
                raise

    @staticmethod
    def _head(db):
        return db.execute("SELECT * FROM observation_schedule_head WHERE singleton=1").fetchone()

    def _journal(self, db, head, *, through):
        prior = db.execute("SELECT sequence,digest FROM observation_schedule_journal ORDER BY sequence DESC LIMIT 1").fetchone()
        sequence, previous_digest = (prior["sequence"] + 1, prior["digest"]) if prior else (1, None)
        after = head["journaled_ordinal"]
        failures = db.execute(
            "SELECT ordinal,reason FROM observation_schedule_failure "
            "WHERE ordinal > ? AND ordinal <= ? ORDER BY ordinal", (after, through),
        )
        record = {
            "schemaVersion": 1, "sequence": sequence, "previousDigest": previous_digest,
            "scheduleId": self.schedule_id, "lifecycle": head["lifecycle"],
            "lifecycleTick": head["lifecycle_tick"], "settledOrdinal": through,
            "defaultSuccessRange": [after + 1, through] if through > after else None,
            "sparseFailures": [dict(row) for row in failures],
            "terminalCutoffTick": head["lifecycle_tick"] if head["lifecycle"] in _TERMINAL else None,
        }
        segment = canonical_json(record).encode("utf-8")
        db.execute("INSERT INTO observation_schedule_journal VALUES (?,?,?)",
                   (sequence, sha256(segment).hexdigest(), segment))
        db.execute("UPDATE observation_schedule_head SET journaled_ordinal=?", (through,))
