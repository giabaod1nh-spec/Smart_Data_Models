"""Capture, deliver and correct a disposable fixed-time Stage A dataset."""

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

from contracts.observation_measurements import MAX_MEASUREMENT_BYTES, evidence_from_isolated_capture
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


def guard(path, deadline, *, startup=False):
    import psutil

    if time.monotonic() >= deadline:
        raise TimeoutError("Stage A source reached its wall-clock guard")
    floor = 2_500_000_000 if startup else 1024**3
    if psutil.virtual_memory().available < floor:
        raise RuntimeError("Stage A source host memory is below the startup reserve" if startup
                           else "Stage A source host memory is below the 1 GiB runtime floor")
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


def capture_slots(traci, store, context, schedule, duration, deadline, *, analytics_profile, reference=None,
                  adaptive=None, calibration_incident=False):
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
        analytics = SumoSnapshotProvider(tls, node, stage_a_measurements=True, analytics_profile=analytics_profile)
        if tuple(sorted(canonical.directions)) != directions or tuple(sorted(analytics.directions)) != directions:
            raise ValueError("Capture provider disagrees with the archived approach inventory")
        canonical.simulation_run_id = analytics.simulation_run_id = schedule.run_id
        controller = SumoSignalController(tls, node_id=node)
        configured[node] = canonical, analytics, controller, SumoScenarioManager(tls)

    if seconds_to_ticks(traci.simulation.getTime()) != 0 or seconds_to_ticks(traci.simulation.getDeltaT()) != _STEP:
        raise ValueError("Stage A requires the pinned zero-based 0.01 second SUMO clock")
    control_mode, timing_mode = "FIXED", "FIXED_TIME"
    treatment = None
    if adaptive is not None:
        from Visualize.tools.stage_a_treatment import AdaptiveTreatment

        treatment = AdaptiveTreatment(*adaptive, traci, configured)
        control_mode = timing_mode = "ADAPTIVE"
    store.set_lifecycle("RUNNING", tick=0)
    guard(store.db_path.parent, deadline)
    ordinal = 0
    completed_tick = 0
    counts = Counter()
    end = duration * TICKS_PER_SECOND
    incident_controller = configured["A"][2] if calibration_incident else None
    incident_previous_phase = None
    incident_vehicle_ids = set()
    for step in range(1, end // _STEP + 1):
        traci.simulationStep()
        if (incident_controller is not None and step * _STEP >= 30 * TICKS_PER_SECOND
                and step * _STEP == schedule.due_tick(ordinal)):
            # Reproduce a temporary lane obstruction with actual stopped vehicles.
            # The reference classifier observes their residual positions independently.
            phase = incident_controller.current_phase_name(traci)
            if phase == "NS_GREEN" and phase != incident_previous_phase:
                from configuration import config as cfg

                lanes = cfg.approach_lane_ids(incident_controller.tls_id, "South")
                for lane in lanes:
                    for vehicle in traci.lane.getLastStepVehicleIDs(lane):
                        if traci.vehicle.getSpeed(vehicle) < cfg.HALTING_SPEED_MS:
                            traci.vehicle.setSpeed(vehicle, 0)
                            incident_vehicle_ids.add(vehicle)
            if phase != "NS_GREEN" and incident_previous_phase == "NS_GREEN":
                for vehicle in sorted(incident_vehicle_ids & set(traci.vehicle.getIDList())):
                    traci.vehicle.setSpeed(vehicle, -1)
                incident_vehicle_ids.clear()
            incident_previous_phase = phase
        if treatment is not None and step * _STEP < end:
            treatment.step(step * _STEP)
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
            sources[node] = dict(snap, control_mode=control_mode, timing_mode=timing_mode)
        if reference is not None:
            reference.observe(cached, completed_tick, {node: obj["colors"] for node, obj in sources.items()})
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
    return dict(counts, slots=ordinal, end_tick=completed_tick), treatment.finish() if treatment else []


def capture(args, deadline):
    from dataclasses import replace
    from hashlib import sha256

    from configuration import config as cfg
    from configuration.parameter_registry import get_registry
    from integration.kafka.scheduled_outbox import IsolatedScheduledOutbox
    from observation.canonical_cycle import freeze_run_catalog
    from observation.analytics_profile import AnalyticsProfile
    from runtime.active_run_registry import manifest_identity
    from runtime.run_manifest import atomic_write_json, build_and_write_manifest, read_verified_v1_manifest
    from topology.catalog import verify_against_net
    from Visualize.tools.stage_a_treatment import adaptive_spec, validate_offered_demand

    if not 60 <= args.duration <= 600 or args.duration % 60:
        raise ValueError("Capture duration must be a multiple of 60 from 60 to 600 simulation seconds")
    if not 0 < args.demand_scale <= 3 or (args.demand_scale != 1 and not args.calibration_reference):
        raise ValueError("Scaled demand is only supported for a flagged G-CAL dataset")
    if args.calibration_incident and not args.calibration_reference:
        raise ValueError("The lane-obstruction reproduction is only supported for a flagged G-CAL dataset")
    if args.adaptive_models is not None and args.offered_demand is None:
        raise ValueError("Adaptive inference requires the same explicit departure schedule as Fixed-Time")
    if args.offered_demand is not None and (args.calibration_reference or not 0 <= args.warmup < args.duration):
        raise ValueError("Use a distinct experiment run with a valid warm-up and an explicit departure schedule")
    sumo_home = Path(os.environ["SUMO_HOME"])
    sys.path.insert(0, str(sumo_home / "tools"))
    import traci

    parent = args.scratch.resolve(strict=True)
    guard(parent, deadline, startup=True)
    run_id = "stage-a-" + uuid4().hex
    generation = 1
    catalog = cfg._TOPOLOGY_CATALOG
    nodes = tuple(sorted(catalog["node_to_tls"]))
    population = tuple((node, tuple(sorted(cfg.directions_for_node(node)))) for node in nodes)
    adaptive = adaptive_spec(args.adaptive_models, nodes) if args.adaptive_models else None
    demand = validate_offered_demand(args.offered_demand) if args.offered_demand else None
    mode = "ADAPTIVE" if adaptive else "FIXED"
    with TemporaryDirectory(prefix="stage-a-sumo-", dir=parent) as temp:
        package = Path(temp)
        for filename in _ASSETS:
            shutil.copyfile(cfg.SUMO_ASSET_ROOT / filename, package / filename)
        if demand:
            (package / "intersection.rou.xml").write_bytes(demand[0])
        run_folder = package / "runs" / run_id
        context = freeze_run_catalog(run_id, catalog, run_folder / "network_topology_catalog.json")
        if not context.archive_ready:
            raise ValueError(context.error)
        errors = verify_against_net(package / "intersection.net.xml", context.archive_path)
        if errors:
            raise ValueError("Archived catalog does not match pinned SUMO network: " + "; ".join(errors))
        profile = AnalyticsProfile(package / "intersection.net.xml", package / "detectors.add.xml")
        original_manifest = build_and_write_manifest(registry=get_registry(), artifacts_root=package, simulation_run_id=run_id,
            scenario="normal", sumo_seed=args.seed, demand_seed=args.seed,
            control_mode=mode, topology_hash=catalog["topology_hash"], topology_digest=context.topology_digest,
            participating_intersection_ids=list(nodes), run_generation=generation)
        experiment = {}
        if demand:
            assets = dict(original_manifest["assets"])
            assets["rou_file"] = dict(assets["rou_file"], sha256=demand[1])
            experiment.update(route_hash=demand[1], assets=assets, offered_demand_digest=demand[1],
                initial_conditions_digest=sha256(b"empty-network-at-zero-default-signal-program").hexdigest(),
                scenario_schedule_digest=sha256(b"normal-no-interventions-v1").hexdigest(),
                measurement_scope_digest=profile.digest, warmup_seconds=args.warmup,
                evaluation_end_seconds=args.duration, step_ticks=_STEP, period_ticks=TICKS_PER_SECOND)
        if adaptive:
            config, build, model_config = adaptive
            experiment.update(controller_build=build, controller_config_digest=model_config,
                              decision_interval_ticks=round(config.decision_interval_sec * TICKS_PER_SECOND))
        atomic_write_json(run_folder / "run_manifest.json", dict(original_manifest, **experiment,
            stage_a_detector_profile=dict(profile.identity, digest=profile.digest),
            **({"stage_a_calibration_reference": True, "demand_scale": args.demand_scale,
                "calibration_incident": "A-South-obstruction-through-green-after-30s" if args.calibration_incident else None}
               if args.calibration_reference else {})))
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
            reference = None
            if args.calibration_reference:
                from observation.pressure_reference import PressureReference

                reference = PressureReference({(node, direction): cfg.approach_lane_ids(cfg.NODE_TO_TLS[node], direction)
                    for node, directions in population for direction in directions})
            traci.start([sumo, "-c", str(package / "intersection.sumocfg"), "--seed", str(args.seed),
                "--scale", str(args.demand_scale), "--no-step-log", "true", "--duration-log.disable", "true",
                "--no-warnings", "true"], numRetries=2)
            try:
                guard(parent, deadline, startup=True)
                status, decisions = capture_slots(traci, store, context, schedule, args.duration, deadline,
                    analytics_profile=profile, reference=reference,
                    adaptive=(adaptive[0], args.adaptive_models) if adaptive else None,
                    calibration_incident=args.calibration_incident)
            finally:
                traci.close()
            prefix = export_all(store, deadline)
            if reference is not None and not status.get("capture_failed"):
                atomic_write_json(store.run_archive / "pressure_reference.json", dict(run_id=run_id,
                    manifest=schedule.manifest_digest, seed=args.seed, demand_scale=args.demand_scale,
                    measurements=reference.document()))
            if demand is not None and not status.get("capture_failed"):
                atomic_write_json(store.run_archive / "experiment_treatment.json", dict(run_id=run_id,
                    manifest=schedule.manifest_digest, mode=mode, end_tick=status["end_tick"],
                    controller_config_digest=experiment.get("controller_config_digest"), decisions=decisions))
            print(json.dumps(dict(status, state="CAPTURED", archive_prefixes=prefix, registry=bind_run_registry(store)),
                             sort_keys=True))
            return 1 if status.get("capture_failed") else 0


def deliver(store, args, deadline):
    from confluent_kafka import Producer
    from integration.kafka.outbox_schema import PENDING_STATUSES, STATUS_ACKED, STATUS_FAILED_PERMANENT
    from integration.kafka.outbox_worker import OutboxDeliveryWorker

    guard(store.db_path.parent, deadline)
    if store.source_status() != "COMPLETE":
        raise ValueError("A complete source capture is required for Stage A delivery")
    export_all(store, deadline)
    bind_run_registry(store)
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
            if counts.get(STATUS_FAILED_PERMANENT) or not any(counts.get(state) for state in PENDING_STATUSES):
                print(json.dumps(counts, sort_keys=True))
                return 0 if counts.get(STATUS_ACKED) and not counts.get(STATUS_FAILED_PERMANENT) else 1
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


def correct(store, args, deadline):
    guard(store.db_path.parent, deadline)
    if store.source_status() != "COMPLETE":
        raise ValueError("Correction command requires a complete disposable source capture")
    evidence = None
    if args.evidence is not None:
        with args.evidence.open("rb") as stream:
            raw = stream.read(MAX_MEASUREMENT_BYTES + 1)
        if len(raw) > MAX_MEASUREMENT_BYTES:
            raise ValueError("Replacement evidence exceeds the measurement byte cap")
        evidence = json.loads(raw)
        if not isinstance(evidence, dict):
            raise ValueError("Use an evidence object for replacement or --withdraw to withdraw")
    revision = store.correct_measurement(args.event_id, previous_digest=args.previous_digest, evidence=evidence)
    archives = export_all(store, deadline)
    print(json.dumps(dict(event_id=args.event_id, measurement_digest=revision, archive_prefixes=archives,
                          disposition="WITHDRAWN" if args.withdraw else "REPLACED")))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("capture")
    source.add_argument("scratch", type=Path)
    source.add_argument("--duration", type=int, default=300)
    source.add_argument("--seed", type=int, default=42)
    source.add_argument("--calibration-reference", action="store_true")
    source.add_argument("--demand-scale", type=float, default=1)
    source.add_argument("--calibration-incident", action="store_true")
    source.add_argument("--offered-demand", type=Path)
    source.add_argument("--adaptive-models", type=Path)
    source.add_argument("--warmup", type=int, default=0)
    export = commands.add_parser("export")
    export.add_argument("outbox", type=Path)
    kafka = commands.add_parser("deliver")
    kafka.add_argument("outbox", type=Path)
    kafka.add_argument("--bootstrap", required=True)
    kafka.add_argument("--cluster-id", required=True)
    correction = commands.add_parser("correct")
    correction.add_argument("outbox", type=Path)
    correction.add_argument("--event-id", required=True)
    correction.add_argument("--previous-digest", required=True)
    action = correction.add_mutually_exclusive_group(required=True)
    action.add_argument("--evidence", type=Path)
    action.add_argument("--withdraw", action="store_true")
    for command in (source, export, kafka, correction):
        command.add_argument("--seconds", type=int, default=300)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error("wall-clock budget must be between 1 and 3600 seconds")
    deadline = time.monotonic() + args.seconds
    sys.path.insert(0, str(_VISUALIZE))
    if args.command == "capture":
        return capture(args, deadline)
    guard(args.outbox.resolve(strict=True).parent, deadline, startup=True)
    with open_outbox(args.outbox) as store:
        if args.command == "deliver":
            return deliver(store, args, deadline)
        if args.command == "correct":
            return correct(store, args, deadline)
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
