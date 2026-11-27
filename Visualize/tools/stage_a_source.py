"""Capture and deliver one disposable fixed-time Stage A dataset."""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from contracts.observation_measurements import evidence_from_isolated_capture
from contracts.observation_schedule import ObservationSchedule, TICKS_PER_SECOND, seconds_to_ticks
from contracts.traffic_observation import TOPIC, canonical_json, observation_key

_VISUALIZE = Path(__file__).resolve().parents[1]
_STEP = 10_000
_ASSETS = ("intersection.sumocfg", "intersection.net.xml", "intersection.rou.xml",
           "detectors.add.xml", "viewsettings.xml")


class ReadOnce:
    """Share identical read-only TraCI queries within one completed step."""

    def __init__(self, target):
        self.target = target
        self.cache = {}

    def __getattr__(self, name):
        method = getattr(self.target, name)
        if not name.startswith("get"):
            raise AttributeError(name)

        def read(*args):
            key = name, args
            if key not in self.cache:
                self.cache[key] = method(*args)
            return self.cache[key]

        return read


def guard(path, deadline):
    import psutil

    if time.monotonic() >= deadline:
        raise TimeoutError("Stage A source reached its wall-clock guard")
    if psutil.virtual_memory().available < 2_500_000_000:
        raise RuntimeError("Stage A source host reserve fell below 2.5 GB")
    if shutil.disk_usage(path).free < 512 * 1024**2:
        raise RuntimeError("Stage A source scratch reserve fell below 512 MiB")


def export_all(store, deadline):
    sealed = store.sealed_archive_prefixes()
    while True:
        guard(store.db_path.parent, deadline)
        position = store.export_archive_prefixes()
        if position == sealed:
            return position


def bind_run_registry(store):
    """Adopt the immutable package into this Outbox's disposable archive only."""
    from integration.kafka.scheduled_outbox import _plain_path
    from runtime.active_run_registry import ActiveRunRegistry, manifest_identity
    from runtime.run_manifest import read_verified_v1_manifest

    schedule = store.schedule
    registry_path = store.run_archive.parent / "active_run.sqlite3"
    for path in (store.run_archive.parent, store.run_archive):
        _plain_path(path, directory=True)
    for suffix in ("", "-wal", "-shm", "-journal"):
        path = registry_path.with_name(registry_path.name + suffix)
        if path.exists() or path.is_symlink():
            _plain_path(path)
    manifest_path = store.run_archive / "run_manifest.json"
    _plain_path(manifest_path)
    manifest = read_verified_v1_manifest(manifest_path, run_id=schedule.run_id,
        topology_digest=schedule.topology_digest, generation=schedule.generation,
        participants=tuple(node for node, _ in schedule.participants))
    if (manifest_identity(manifest), manifest["config_hash"]) != (schedule.manifest_digest, schedule.config_digest):
        raise ValueError("Disposable run manifest differs from the Outbox schedule")
    status = store.source_status()
    registry = ActiveRunRegistry(registry_path)
    selected = registry.selected()
    if selected is None:
        selected = registry.activate(manifest)
    else:
        registry.assert_selected(manifest)
    if selected["status"] != status:
        if selected["status"] != "RUNNING":
            raise ValueError("Sealed Outbox disagrees with the existing disposable registry status")
        registry.set_status(schedule.run_id, status)
    return status


def capture_slots(traci, store, context, schedule, duration, deadline):
    from types import SimpleNamespace

    from integration.kafka.outbox_schema import EVENT_KIND_OBSERVATION_V1
    from integration.kafka.outbox_store import OutboxRow
    from observation.canonical_cycle import generate_v1_cycle
    from observation.snapshot_provider import SumoSnapshotProvider
    from simulation.scenario_manager import SumoScenarioManager
    from simulation.signal_controller import SumoSignalController

    catalog = context.catalog
    configured = {}
    for node, directions in schedule.participants:
        tls = catalog["node_to_tls"][node]
        canonical = SumoSnapshotProvider(tls, node)
        analytics = SumoSnapshotProvider(tls, node, stage_a_measurements=True)
        if tuple(sorted(canonical.directions)) != directions or tuple(sorted(analytics.directions)) != directions:
            raise ValueError("Capture provider disagrees with the archived approach inventory")
        canonical.simulation_run_id = analytics.simulation_run_id = schedule.run_id
        controller = SumoSignalController(tls, node_id=node)
        configured[node] = canonical, analytics, controller, SumoScenarioManager(tls)

    if seconds_to_ticks(traci.simulation.getTime()) != 0 or seconds_to_ticks(traci.simulation.getDeltaT()) != _STEP:
        raise ValueError("Stage A requires the pinned zero-based 0.01 second SUMO clock")
    store.set_lifecycle("RUNNING", tick=0)
    guard(store.db_path.parent, deadline)
    ordinal = 0
    completed_tick = 0
    counts = Counter()
    end = duration * TICKS_PER_SECOND
    for step in range(1, end // _STEP + 1):
        traci.simulationStep()
        if (expected := step * _STEP) < schedule.due_tick(ordinal) and expected != end:
            continue
        guard(store.db_path.parent, deadline)
        actual = traci.simulation.getTime()
        completed_tick = seconds_to_ticks(actual)
        if completed_tick != expected:
            raise ValueError(f"SUMO clock mismatch at completed step {step}")
        if completed_tick == end:
            break
        cached = SimpleNamespace(**{name: ReadOnce(getattr(traci, name)) for name in (
            "simulation", "lane", "lanearea", "vehicle", "trafficlight")})
        sources = {}
        for node, (canonical, _, controller, scenario) in configured.items():
            snap = canonical.build_snapshot(cached, controller, scenario, capture_quality=True)
            sources[node] = dict(snap, control_mode="FIXED", timing_mode="FIXED_TIME")
        result = generate_v1_cycle(context=context, participating_nodes=tuple(configured),
            sources=sources, simulation_time_seconds=actual, simulation_step=step,
            cycle_sequence=schedule.cycle(ordinal))
        if not result.success:
            store.record_failure(ordinal, "CAPTURE_FAILED")
            counts["capture_failed"] += 1
            logging.error("Cycle %s failed V1 capture: %s", ordinal, result.failure_code)
        else:
            rows, evidence = [], {}
            for index, event in enumerate(result.observations):
                node = event["intersectionId"]
                _, analytics, controller, scenario = configured[node]
                measured = analytics.build_snapshot(cached, controller, scenario, capture_quality=True)
                proof = evidence_from_isolated_capture(event, measured, completed_step=step)
                if proof["directions"] or proof["spillback"] is not None:
                    evidence[event["eventId"]] = proof
                rows.append(OutboxRow(event["eventId"], schedule.run_id, result.cycle_sequence,
                    index, observation_key(event), TOPIC, canonical_json(event), event["payloadHash"],
                    EVENT_KIND_OBSERVATION_V1))
            counts["outboxed"] += store.append_scheduled_cycle(ordinal, rows, measurement_evidence=evidence).inserted
            counts["measurement_objects"] += len(evidence)
        ordinal += 1
        if ordinal % 16 == 0:
            store.checkpoint()
    store.checkpoint()
    store.set_lifecycle("ENDED", tick=completed_tick)
    return dict(counts, slots=ordinal, end_tick=completed_tick)


def capture(args, deadline):
    from dataclasses import replace

    from configuration import config as cfg
    from configuration.parameter_registry import get_registry
    from integration.kafka.scheduled_outbox import IsolatedScheduledOutbox
    from observation.canonical_cycle import freeze_run_catalog
    from runtime.active_run_registry import manifest_identity
    from runtime.run_manifest import build_and_write_manifest, read_verified_v1_manifest
    from topology.catalog import verify_against_net

    if not 60 <= args.duration <= 600 or args.duration % 60:
        raise ValueError("Capture duration must be a multiple of 60 from 60 to 600 simulation seconds")
    sumo_home = Path(os.environ["SUMO_HOME"])
    sys.path.insert(0, str(sumo_home / "tools"))
    import traci

    parent = args.scratch.resolve(strict=True)
    guard(parent, deadline)
    run_id = "stage-a-" + uuid4().hex
    generation = 1
    catalog = cfg._TOPOLOGY_CATALOG
    nodes = tuple(sorted(catalog["node_to_tls"]))
    population = tuple((node, tuple(sorted(cfg.directions_for_node(node)))) for node in nodes)
    with TemporaryDirectory(prefix="stage-a-sumo-", dir=parent) as temp:
        package = Path(temp)
        for filename in _ASSETS:
            shutil.copyfile(cfg.SUMO_ASSET_ROOT / filename, package / filename)
        run_folder = package / "runs" / run_id
        context = freeze_run_catalog(run_id, catalog, run_folder / "network_topology_catalog.json")
        if not context.archive_ready:
            raise ValueError(context.error)
        errors = verify_against_net(package / "intersection.net.xml", context.archive_path)
        if errors:
            raise ValueError("Archived catalog does not match pinned SUMO network: " + "; ".join(errors))
        build_and_write_manifest(registry=get_registry(), artifacts_root=package, simulation_run_id=run_id,
            scenario="normal", sumo_seed=args.seed, demand_seed=args.seed,
            control_mode="FIXED", topology_hash=catalog["topology_hash"], topology_digest=context.topology_digest,
            participating_intersection_ids=list(nodes), run_generation=generation)
        manifest = read_verified_v1_manifest(run_folder / "run_manifest.json", run_id=run_id,
            topology_digest=context.topology_digest, participants=nodes, generation=generation)
        schedule = ObservationSchedule(run_id, generation, manifest_identity(manifest), manifest["config_hash"],
            context.topology_digest, population, _STEP, TICKS_PER_SECOND, _STEP, 1)
        with managed_outbox(IsolatedScheduledOutbox.create(parent, schedule)) as store:
            store.run_archive.parent.mkdir()
            run_folder.replace(store.run_archive)
            context = replace(context, archive_path=store.run_archive / context.archive_path.name)
            bind_run_registry(store)
            print(json.dumps(dict(state="ALLOCATED", outbox=str(store.db_path), archive=str(store.db_path.parent),
                run_id=run_id, generation=generation, topology=schedule.topology_digest,
                manifest=schedule.manifest_digest, schedule=schedule.schedule_id)), flush=True)
            sumo = shutil.which("sumo", path=str(sumo_home / "bin"))
            if sumo is None:
                raise FileNotFoundError("SUMO executable unavailable in SUMO_HOME/bin")
            traci.start([sumo, "-c", str(package / "intersection.sumocfg"), "--seed", str(args.seed),
                "--no-step-log", "true", "--duration-log.disable", "true", "--no-warnings", "true"], numRetries=2)
            try:
                status = capture_slots(traci, store, context, schedule, args.duration, deadline)
            finally:
                traci.close()
            prefix = export_all(store, deadline)
            print(json.dumps(dict(status, state="CAPTURED", archive_prefixes=prefix, registry=bind_run_registry(store)),
                             sort_keys=True))
            return 1 if status.get("capture_failed") else 0


def deliver(store, args, deadline):
    from confluent_kafka import Producer
    from integration.kafka.outbox_worker import OutboxDeliveryWorker

    guard(store.db_path.parent, deadline)
    if bind_run_registry(store) != "COMPLETE":
        raise ValueError("A complete source capture is required for Stage A delivery")
    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "stage-a-source",
        "allow.auto.create.topics": False, "acks": "all", "enable.idempotence": True,
        "delivery.timeout.ms": 10_000})
    metadata = producer.list_topics(timeout=3)
    topic = metadata.topics.get(TOPIC)
    if not args.cluster_id or metadata.cluster_id != args.cluster_id or topic is None or topic.error:
        raise ValueError("Expected the explicitly identified disposable Kafka cluster and its existing V1 topic")
    worker = OutboxDeliveryWorker(store, producer=producer)
    worker.start()
    try:
        while True:
            guard(store.db_path.parent, deadline)
            counts = store.count_by_status()
            if counts.get("FAILED_PERMANENT") or not any(
                    counts.get(state) for state in ("PENDING", "QUEUED", "FAILED_RETRYABLE")):
                print(json.dumps(counts, sort_keys=True))
                return 0 if counts.get("ACKED") and not counts.get("FAILED_PERMANENT") else 1
            time.sleep(0.2)
    finally:
        worker.stop(timeout=3)


def managed_outbox(store):
    from contextlib import closing

    return closing(store)


def open_outbox(path):
    import sqlite3
    from contextlib import closing
    from hashlib import sha256

    from integration.kafka.scheduled_outbox import IsolatedScheduledOutbox, _verified_path

    path = _verified_path(path)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        identity, document = db.execute(
            "SELECT schedule_id,schedule_bytes FROM observation_schedule_head WHERE singleton=1").fetchone()
    if sha256(document).hexdigest() != identity:
        raise ValueError("Durable Outbox schedule does not match its identity")
    return managed_outbox(IsolatedScheduledOutbox(path, ObservationSchedule.decode(document, identity)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("capture")
    source.add_argument("scratch", type=Path)
    source.add_argument("--duration", type=int, default=300)
    source.add_argument("--seed", type=int, default=42)
    export = commands.add_parser("export")
    export.add_argument("outbox", type=Path)
    kafka = commands.add_parser("deliver")
    kafka.add_argument("outbox", type=Path)
    kafka.add_argument("--bootstrap", required=True)
    kafka.add_argument("--cluster-id", required=True)
    for command in (source, export, kafka):
        command.add_argument("--seconds", type=int, default=300)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error("wall-clock budget must be between 1 and 3600 seconds")
    deadline = time.monotonic() + args.seconds
    sys.path.insert(0, str(_VISUALIZE))
    if args.command == "capture":
        return capture(args, deadline)
    guard(args.outbox.resolve(strict=True).parent, deadline)
    with open_outbox(args.outbox) as store:
        if args.command == "deliver":
            return deliver(store, args, deadline)
        while True:
            guard(store.db_path.parent, deadline)
            before = store.sealed_archive_prefixes()
            store.checkpoint()
            if before == store.sealed_archive_prefixes():
                break
        prefix = export_all(store, deadline)
        print(json.dumps(dict(archive_prefixes=prefix, registry=bind_run_registry(store))))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
