"""Explicit disposable Gold Core and Business runtime for a Stage A run."""

import argparse
import json
import os
import shutil
import time
from collections import deque
from dataclasses import asdict
from pathlib import Path

from contracts.observation_measurements import digest
from de.gold_runtime.business_clickhouse import IsolatedClickHouseRows
from de.gold_runtime.business_experiments import compare, read_manifest
from de.gold_runtime.business_materialization import compute_business_once, iter_seal_business_work
from de.gold_runtime.business_metadata import BusinessMetadata
from de.gold_runtime.business_publication import BusinessPublisher
from de.gold_runtime.business_reader import GoldReader, intersection_detail, overview
from de.gold_runtime.business_state import BusinessState, IsolatedBusinessStore, _MEMBER_LIMIT
from de.gold_runtime.checkpoint_store import CoreStateFault
from de.gold_runtime.config import GoldCoreSettings
from de.gold_runtime.instance_lock import InstanceLock
from de.gold_runtime.processing_ledger import canonical_run_identity, core_run_identity
from de.gold_runtime.processor import (
    CORE_PROCESSOR_VERSION, CoreInputStream, CoreWorkChanged, GoldCoreProcessor, discovery_boundary,
)
from de.gold_runtime.replay import CoreReplayBoundaryChanged, CoreReplayManifest
from de.silver.config import SilverSettings
from de.silver.incremental_contract import canonical_hash


class BatchedStageAInput(CoreInputStream):
    """Verify contiguous member runs through Silver, then hash in the original event-ID order."""

    def __init__(self, runtime, work):
        super().__init__(runtime, work)
        members = runtime.ledger.members(self.window_id, limit=_MEMBER_LIMIT + 1)
        if len(members) > _MEMBER_LIMIT:
            raise CoreWorkChanged("Stage A member population exceeded its admitted bound")
        self.pending = deque(sorted(members, key=lambda member: member["change_sequence"]))
        self.page = deque()
        self.proofs = {}

    def __next__(self):
        if self.done:
            raise StopIteration
        self._check_generation()
        if not self.pending:
            self._verify_end()
            for _, proof in sorted(self.proofs.items()):
                self.digest.update(len(proof).to_bytes(8, "big"))
                self.digest.update(proof)
            self.done = True
            raise StopIteration
        if not self.page:
            start = self.pending[0]["change_sequence"]
            limit = 0
            for member in self.pending:
                if limit == 8 or member["change_sequence"] != start + limit:
                    break
                limit += 1
            self.page.extend(self._read_members(start, limit).entries)
        member = self.pending.popleft()
        entry = self.page.popleft()
        self.proofs[member["id"]] = self._verify_member(member, entry)
        return entry.canonical_bundle


class StageABusinessProcessor(GoldCoreProcessor):
    """Drive Silver receipts, Core proofs, Business computation and publication under one lease."""

    def __init__(self, settings, silver, store, lock, *, run, target, deadline, archive_head=None, **kwargs):
        super().__init__(settings, silver, store, lock, **kwargs)
        self.run_coordinates = run
        self.run_identity = canonical_run_identity(*run)
        self.run_key = canonical_hash(self.run_identity)
        self.deadline = deadline
        self.business = BusinessState(store, self.namespace, lock.lease_token)
        self.metadata = BusinessMetadata(self.business)
        self.publisher = BusinessPublisher(self.business, target)
        self._proof = None
        self._metadata_at_tail = False
        self._metadata_empty_until = {}
        self._proof_started = 0.0
        self.archive_head = archive_head
        self._verified_heads = {}

    def _bootstrap(self):
        if self.store.conn.execute("SELECT 1 FROM core_run WHERE id<>? LIMIT 1", (self.run_key,)).fetchone():
            raise CoreStateFault("Stage A checkpoint belongs to another run")
        self.business.bind_runtime(dict(run_key=self.run_key, archive_head=self.archive_head,
            publish_target=getattr(self.publisher.target, "destination_identity", None), namespace=self.namespace))
        self.business.recover_reservations()
        self.publisher.recover()
        return super()._bootstrap()

    def _ack(self, page, started):
        if any(core_run_identity(entry) != self.run_identity for entry in page.entries):
            raise CoreStateFault("Stage A requires a dedicated Bronze/Silver authority for the requested run")
        return super()._ack(page, started)

    def _read_metadata(self, stream):
        now = time.monotonic()
        if now < self._metadata_empty_until.get(stream, 0):
            return "EMPTY"
        run_id, generation, topology, manifest = self.run_coordinates
        previous = self.metadata.cursor(*self.run_coordinates, stream)
        self.add_count("canonical_" + stream.lower() + "_requests", 1)
        receipt = self.source.observation_metadata(self.binding_id, run_id, generation, topology, previous)
        if receipt.manifest_identity != manifest:
            raise CoreStateFault("Stage A metadata manifest differs from the requested run")
        if self.replay and receipt.archive_head_digest != self.archive_head:
            raise CoreReplayBoundaryChanged("Replay observation archive head changed")
        disposition = self.metadata.apply(receipt)
        if disposition == "EMPTY":
            self._metadata_empty_until[stream] = now + 1
            self._verified_heads[stream] = receipt.archive_head_digest
        else:
            self._verified_heads.pop(stream, None)
        return disposition

    def _complete_replay(self):
        if self.drained(self.status()) and self.confirm_archive():
            super()._complete_replay()

    def confirm_archive(self):
        self._metadata_empty_until.clear()
        if tuple(self._read_metadata(stream) for stream in ("SCHEDULE", "MEASUREMENT")) != ("EMPTY", "EMPTY"):
            self._metadata_at_tail = False
            return None
        heads = set(self._verified_heads.values())
        return heads.pop() if len(heads) == 1 else None

    def replay_capture(self, archive_head):
        boundary = discovery_boundary(self.discover())
        schedules = self.store.conn.execute("SELECT start_tick,cutoff FROM business_observation_schedule "
                                            "WHERE run_key=?", (self.run_key,)).fetchone()
        if (boundary[:2] != boundary[2:] or schedules[0] != 0
                or schedules[1] is None or schedules[1] % 300_000_000):
            return None
        run_id, generation, topology, manifest = self.run_coordinates
        core = CoreReplayManifest(self.binding_id, boundary, run_id, generation, topology,
                                  manifest, 0, schedules[1] // 1_000_000)
        return dict(core=asdict(core), archive_head_digest=archive_head)

    def _changes(self):
        if self.pending_change is None and self.store.cursors()["CHANGE"] >= self.latest.verified_change_prefix:
            return False
        return super()._changes()

    def materialize(self, work):
        return BatchedStageAInput(self, work)

    def _continue_proof(self):
        work, proof = self._proof
        try:
            # Keep the feed, lease and guard checks responsive during a long window.
            stop = time.monotonic() + 0.1
            for _ in range(8):
                next(proof)
                if time.monotonic() >= stop:
                    break
        except StopIteration as result:
            self._proof = None
            self.business.release_unsealed(work)
            self.add_count("business_work_" + result.value.lower(), 1)
            if result.value == "SEALED":
                duration = self.store.conn.execute("SELECT duration FROM core_window WHERE id=?",
                                                  (work["window_id"],)).fetchone()[0]
                self.timing(f"materialization_{duration}s_wall", time.monotonic() - self._proof_started)
        except TimeoutError:
            self._proof = None
            self.business.release_unsealed(work)
            return False
        except BaseException:
            self._proof = None
            self.business.release_unsealed(work)
            raise
        return True

    def _work(self):
        if self._proof is not None:
            return self._continue_proof()
        self._metadata_at_tail = False
        dispositions = tuple(self._read_metadata(stream) for stream in ("SCHEDULE", "MEASUREMENT"))
        if dispositions != ("EMPTY", "EMPTY"):
            return any(status in ("APPLIED", "REUSED") for status in dispositions)
        self._metadata_at_tail = True
        if self.replay:
            schedule = self.store.conn.execute("SELECT start_tick,cutoff FROM business_observation_schedule "
                                               "WHERE run_key=?", (self.run_key,)).fetchone()
            if (schedule is None or tuple(schedule) != (round(self.replay.start_simulation_time * 1_000_000),
                                                         round(self.replay.end_simulation_time * 1_000_000))):
                raise CoreReplayBoundaryChanged("Replay range differs from the frozen terminal schedule")
        promoted = self.ledger.promote(self.settings.promotion_limit)
        promoted += self.business.promote_terminal_windows(self.run_key, limit=self.settings.promotion_limit)
        rechecked = self.business.recheck_waiting_schedule()
        computed = compute_business_once(self.publisher)
        publication = self.publisher.publish_once()
        work = self.business.reserve_next()
        if work is None:
            return bool(promoted or rechecked or computed or publication != "IDLE")
        self._proof = work, iter_seal_business_work(self.business, self.ledger, work, self.materialize,
                                                    deadline=self.deadline, measure_occupancy=True)
        self._proof_started = time.monotonic()
        return self._continue_proof()

    def status(self):
        db = self.store.conn
        schedule = db.execute("SELECT lifecycle FROM business_observation_schedule WHERE run_key=?",
                              (self.run_key,)).fetchone()
        holds = dict(db.execute("""SELECT h.reason, count(*) FROM business_hold h
            JOIN core_window w ON w.id=h.window_id AND w.generation=h.generation GROUP BY h.reason"""))
        publication = dict(db.execute("""SELECT p.state,count(*) FROM business_publication p
            JOIN business_result r ON p.result_id=r.id JOIN business_obligation o ON o.id=r.obligation_id
            WHERE o.state='COMPUTED' GROUP BY p.state"""))
        pending, oldest = db.execute("""SELECT count(*),min(c.eligible_at) FROM core_work c
            JOIN core_window w ON w.id=c.window_id AND w.generation=c.generation
            LEFT JOIN business_hold h ON h.window_id=c.window_id AND h.generation=c.generation
            WHERE c.state IN ('PENDING','RUNNING') AND h.window_id IS NULL""").fetchone()
        pending_business = db.execute("SELECT count(*) FROM business_obligation WHERE state='PENDING'").fetchone()[0]
        published = db.execute("""SELECT r.row_manifest
            FROM business_scope s JOIN business_result r ON r.id=s.result_id
            JOIN business_publication p ON p.result_id=r.id AND p.state='PUBLISHED'""")
        published_rows = sum(len(json.loads(manifest)) for (manifest,) in published)
        members = db.execute("""SELECT coalesce(sum(c.input_count),0) FROM core_work c JOIN core_window w
            ON w.id=c.window_id AND w.generation=c.generation WHERE c.state='COMPLETE'""").fetchone()[0]
        timings = {f"{duration}s": list(self._timings.get(f"materialization_{duration}s_wall", ()))
                   for duration in (60, 300)}
        cursors = self.store.cursors()
        source = discovery_boundary(self.latest) if self.latest else None
        at_source_tail = source is not None and source == (cursors.get("CHANGE"), cursors.get("INVALIDATION")) * 2
        return dict(run_id=self.run_coordinates[0], prefixes=self.store.cursors(),
                    lifecycle=schedule[0] if schedule else None, metadata_at_tail=self._metadata_at_tail,
                    at_source_tail=at_source_tail,
                    holds=holds, pending_core=pending, pending_business=pending_business, publication=publication,
                    oldest_pending_seconds=max(0, time.time() - oldest) if oldest is not None else None,
                    published_rows=published_rows, verified_members=members, materialization_seconds=timings,
                    canonical_work_counts={name: count for name, count in self._counts.items()
                        if name.startswith(("canonical_", "materialization_", "change_acknowledged",
                                            "invalidation_acknowledged"))},
                    faults=dict(self._faults))

    @staticmethod
    def drained(status):
        publication = status["publication"]
        return (status["lifecycle"] == "ENDED" and status["metadata_at_tail"] and publication.get("PUBLISHED")
                and status["at_source_tail"]
                and not status["faults"] and not status["pending_core"] and not status["pending_business"]
                and not (publication.get("PENDING") or publication.get("WRITING") or publication.get("BLOCKED"))
                and not set(status["holds"]).difference(("OUTSIDE_RUN", "PARTIAL_BOUNDARY", "TERMINAL_PARTIAL",
                                                         "MISSING_WINDOW")))

    def close(self):
        try:
            if self._proof is not None:
                work, proof = self._proof
                proof.close()
                self.business.release_unsealed(work)
                self._proof = None
        finally:
            super().close()


def check_guards(path):
    import psutil

    if psutil.virtual_memory().available < 2_500_000_000:
        raise CoreStateFault("Stage A host reserve fell below 2.5 GB")
    if shutil.disk_usage(path).free < 512 * 1024**2:
        raise CoreStateFault("Stage A scratch reserve fell below 512 MiB")


def open_clickhouse_target(silver=None):
    import clickhouse_connect

    if os.environ.get("STAGE_A_CH_PROBE") != "1":
        raise ValueError("Set STAGE_A_CH_PROBE=1 with an explicitly allocated disposable ClickHouse database")
    required = ("HOST", "PORT", "USER", "PASSWORD", "DATABASE")
    missing = [key for key in required if f"STAGE_A_CH_PROBE_{key}" not in os.environ]
    if missing:
        raise ValueError("Missing Stage A ClickHouse configuration: " + ", ".join(missing))
    values = {key: os.environ[f"STAGE_A_CH_PROBE_{key}"] for key in required}
    database = values["DATABASE"]
    if silver is not None and database == silver.clickhouse_database:
        raise ValueError("Gold Business requires a ClickHouse database distinct from Bronze/Silver")
    target = IsolatedClickHouseRows(None, database, max_query_seconds=2)
    target.client = clickhouse_connect.get_client(host=values["HOST"], port=int(values["PORT"]),
        username=values["USER"], password=values["PASSWORD"], database=database,
        secure=os.environ.get("STAGE_A_CH_PROBE_SECURE") == "1",
        connect_timeout=2, send_receive_timeout=2, query_retries=0)
    target.destination_identity = canonical_hash([values["HOST"], int(values["PORT"]), database,
                                                  os.environ.get("STAGE_A_CH_PROBE_SECURE") == "1"])
    return target


def drain(args):
    if not args.run_id or args.generation < 1 or not digest(args.manifest):
        raise ValueError("Pass the run ID, positive generation and exact manifest digest from the source run")
    if not 1 <= args.seconds <= 3600:
        raise ValueError("Stage A run budget must be between 1 and 3600 seconds")
    silver = SilverSettings()
    store = IsolatedBusinessStore(args.checkpoint)
    check_guards(store.db_path.parent)
    settings = GoldCoreSettings(checkpoint_path=store.db_path,
        instance_lock_path=store.db_path.with_name("instance.lock"), replay_manifest=None).validate_core()
    _, lock_path = settings.state_paths(silver)
    coordinates = args.run_id, args.generation, args.topology, args.manifest
    replay = archive_head = None
    if args.replay_capture:
        document = read_manifest(args.replay_capture)
        core = dict(document["core"])
        core["boundary"] = tuple(core["boundary"])
        replay = CoreReplayManifest(**core)
        archive_head = document["archive_head_digest"]
        if (not digest(archive_head) or (replay.run_id, replay.run_generation, replay.topology_digest,
                replay.manifest_identity) != coordinates or replay.start_simulation_time != 0
                or replay.end_simulation_time % 300):
            raise ValueError("Stage A replay requires the frozen run, archive head and complete aligned source range")
    deadline = time.monotonic() + args.seconds
    target = open_clickhouse_target(silver)
    lock = InstanceLock(lock_path, replay.namespace if replay else "live", store,
                        processor_version=CORE_PROCESSOR_VERSION, exact_path=True)
    processor = None
    try:
        lock.acquire(open_store=True)
        processor = StageABusinessProcessor(settings, silver, store, lock,
            run=coordinates, target=target, deadline=deadline, replay=replay, archive_head=archive_head)
        processor.business.bind_runtime(dict(run_key=processor.run_key, archive_head=archive_head,
            publish_target=target.destination_identity, namespace=processor.namespace))
        target.initialize()
        next_guard = 0.0
        while time.monotonic() < deadline:
            if time.monotonic() >= next_guard:
                check_guards(store.db_path.parent)
                lock.renew()
                next_guard = time.monotonic() + 1
            if processor.run_cycle():
                continue
            status = processor.status()
            publication = status["publication"]
            if publication.get("BLOCKED") or any(reason in status["holds"] for reason in (
                    "RESOURCE_LIMIT", "MEMBER_LIMIT", "NO_LINKED_MEMBERS", "CONFLICTED_WINDOW")):
                break
            if replay and processor._replay_terminal and not status["faults"]:
                print(json.dumps(dict(status, state="DRAINED"), sort_keys=True))
                return 0
            if not replay and processor.drained(status):
                archive_head = processor._lane("work", processor.confirm_archive)
                if archive_head:
                    print(json.dumps(dict(status, state="DRAINED",
                        replay_capture=processor.replay_capture(archive_head)), sort_keys=True))
                    return 0
            if status["lifecycle"] == "ABORTED" and status["metadata_at_tail"] and not status["pending_core"]:
                break
            time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        print(json.dumps(dict(processor.status(), state="INCOMPLETE"), sort_keys=True))
        return 1
    except CoreReplayBoundaryChanged:
        if processor and store.conn.execute("SELECT 1 FROM core_replay WHERE status='RUNNING'").fetchone():
            processor._set_replay_status("ABORTED")
        raise
    finally:
        try:
            if processor is not None:
                processor.close()
        finally:
            lock.release()
            store.close()
            target.client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    allocate = commands.add_parser("allocate")
    allocate.add_argument("scratch_parent", type=Path)
    run = commands.add_parser("drain")
    run.add_argument("--checkpoint", required=True, type=Path)
    run.add_argument("--run-id", required=True)
    run.add_argument("--generation", type=int, required=True)
    run.add_argument("--topology", required=True)
    run.add_argument("--manifest", required=True)
    run.add_argument("--seconds", type=int, default=300)
    run.add_argument("--replay-capture", type=Path)
    catalog = commands.add_parser("catalog")
    catalog.add_argument("--checkpoint", required=True, type=Path)
    catalog.add_argument("--after", default="")
    read = commands.add_parser("read")
    read.add_argument("--checkpoint", required=True, type=Path)
    read.add_argument("--run-key", required=True)
    read.add_argument("--duration", type=int, choices=(60, 300), required=True)
    read.add_argument("--start", type=int)
    read.add_argument("--end", type=int)
    read.add_argument("--intersection")
    read.add_argument("--replay-id")
    experiment = commands.add_parser("compare")
    for role in ("baseline", "adaptive"):
        experiment.add_argument(f"--{role}-checkpoint", required=True, type=Path)
        experiment.add_argument(f"--{role}-manifest", required=True, type=Path)
        experiment.add_argument(f"--{role}-run-key", required=True)
    experiment.add_argument("--duration", type=int, choices=(60, 300), required=True)
    experiment.add_argument("--start", required=True, type=int)
    experiment.add_argument("--end", required=True, type=int)
    args = parser.parse_args()
    if args.command == "allocate":
        check_guards(args.scratch_parent)
        print(IsolatedBusinessStore.allocate_checkpoint(args.scratch_parent))
        return 0
    if args.command == "catalog":
        print(json.dumps(GoldReader(args.checkpoint, None).runs(after=args.after), sort_keys=True))
        return 0
    if args.command == "read":
        check_guards(args.checkpoint.resolve(strict=True).parent)
        target = open_clickhouse_target()
        try:
            from de.gold_runtime.config import replay_namespace

            namespace = replay_namespace(args.replay_id) if args.replay_id else "live"
            selection = GoldReader(args.checkpoint, target, namespace=namespace).windows(
                args.run_key, args.duration, args.start, args.end)
            view = intersection_detail(selection, args.intersection) if args.intersection else overview(selection)
            print(json.dumps(dict(selection=selection, view=view), sort_keys=True))
        finally:
            target.client.close()
        return 0
    if args.command == "compare":
        target = open_clickhouse_target()
        try:
            readers = [GoldReader(path, target) for path in (args.baseline_checkpoint, args.adaptive_checkpoint)]
            report = compare(*readers, read_manifest(args.baseline_manifest), read_manifest(args.adaptive_manifest),
                baseline_run_key=args.baseline_run_key, adaptive_run_key=args.adaptive_run_key,
                duration=args.duration, start=args.start, end=args.end)
            print(json.dumps(report, sort_keys=True))
            return 0 if report["status"] == "PASS" else 1
        finally:
            target.client.close()
    return drain(args)


if __name__ == "__main__":
    raise SystemExit(main())
