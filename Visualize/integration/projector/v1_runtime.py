"""V1 projector orchestration: run fencing, reconciliation, and durable apply."""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from contracts.topology.v1_digest import resolve_v1_catalog, ResolvedRunCatalog, validate_digest_syntax
from contracts.traffic_observation import canonical_json, observation_key, validate_observation
from integration.projector.v1_orion import PermanentOrionError, RetryableOrionError, V1Orion, entity_is_exclusively_owned, local_attribute_name
from integration.projector.v1_projection import DYNAMIC, STATIC, expected_ids, project, static_entities
from integration.projector.v1_store import V1Store
from runtime.active_run_registry import manifest_identity


class ProjectionFault(RuntimeError):
    """Unsafe state requires repair before offset advancement."""


class RetryLater(RuntimeError):
    """Infrastructure or archive is temporarily unavailable."""


class InvalidObservation(ValueError):
    """Permanent pre-write input error; may be quarantined."""


class ArchiveResolver:
    def __init__(self, runs_dir: Path) -> None:
        self.runs_dir = Path(runs_dir)

    def _run_file(self, run_id: str, name: str) -> Path:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id) is None:
            raise InvalidObservation("invalid run ID path component")
        return self.runs_dir / run_id / name

    def manifest(self, run_id: str) -> dict:
        try:
            manifest = json.loads(self._run_file(run_id, "run_manifest.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError) as exc:
            raise RetryLater(f"run manifest unavailable: {exc}") from exc
        except (ValueError, TypeError) as exc:
            raise InvalidObservation(f"invalid run manifest: {exc}") from exc
        if not isinstance(manifest, dict):
            raise InvalidObservation("run manifest must be an object")
        if manifest.get("simulationRunId") != run_id:
            raise InvalidObservation("run manifest identity mismatch")
        if manifest.get("runStatus") not in ("READY", "RUNNING", "COMPLETE", "INCOMPLETE", "STOPPED"):
            raise InvalidObservation("unknown run manifest status")
        participants = manifest.get("participatingIntersectionIds")
        if (not isinstance(participants, list) or not participants
                or any(not isinstance(node, str) or not node for node in participants)
                or len(participants) != len(set(participants))):
            raise InvalidObservation("invalid manifest participant set")
        if type(manifest.get("runGeneration")) is not int or manifest["runGeneration"] < 1:
            raise InvalidObservation("invalid run generation")
        try:
            validate_digest_syntax(manifest["topologyDigest"])
        except (KeyError, ValueError, TypeError) as exc:
            raise InvalidObservation(f"invalid manifest topology digest: {exc}") from exc
        return manifest

    def resolve(self, simulation_run_id: str) -> ResolvedRunCatalog:
        try:
            data = json.loads(self._run_file(simulation_run_id, "network_topology_catalog.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError) as exc:
            raise RetryLater(f"run catalog unavailable: {exc}") from exc
        except (ValueError, TypeError) as exc:
            raise InvalidObservation(f"invalid run catalog: {exc}") from exc
        return ResolvedRunCatalog(simulation_run_id, data)


class ReadOnlyRunSelector:
    """Read the launcher-owned SQLite registry without writing to its volume."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _query(self, sql: str, args: tuple = ()) -> dict | None:
        if not self.path.is_file():
            raise RetryLater(f"active-run registry unavailable: {self.path}")
        try:
            with sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=5) as db:
                db.row_factory = sqlite3.Row
                row = db.execute(sql, args).fetchone()
                return dict(row) if row else None
        except sqlite3.Error as exc:
            raise RetryLater(f"active-run registry read failed: {exc}") from exc

    def selected(self) -> dict | None:
        return self._query("SELECT r.generation, r.run_id, r.manifest_identity, r.status FROM live_selection s JOIN runs r ON r.generation=s.generation WHERE s.singleton=1")

    def select_replay(self, run_id: str) -> dict | None:
        return self._query("SELECT generation, run_id, manifest_identity, status FROM runs WHERE run_id=?", (run_id,))


class V1Projector:
    def __init__(self, *, store: V1Store, orion: V1Orion, runs_dir: Path, replay_run: str | None = None) -> None:
        self.store, self.orion = store, orion
        self.archives = ArchiveResolver(runs_dir)
        self.registry = ReadOnlyRunSelector(Path(runs_dir) / "active_run.sqlite3")
        self.replay_run = replay_run
        self.fault: str | None = None

    def selected(self) -> dict | None:
        return self.registry.select_replay(self.replay_run) if self.replay_run else self.registry.selected()

    def health(self) -> dict:
        selected = self.selected()
        ready = self.store.latest_ready()
        current = self.store.reconciliation(selected["generation"]) if selected else None
        return {
            "selectedRunId": selected["run_id"] if selected else None,
            "selectedGeneration": selected["generation"] if selected else None,
            "selectedStatus": selected["status"] if selected else None,
            "reconciledGeneration": ready["generation"] if ready else None,
            "reconciliationStatus": current["status"] if current else "PENDING",
            "projectionReady": bool(selected and current and current["status"] == "READY" and not self.fault),
            "fault": self.fault,
        }

    def _verified_package(self, run_id: str, selected: dict) -> tuple[dict, dict]:
        try:
            manifest = self.archives.manifest(run_id)
        except InvalidObservation as exc:
            raise ProjectionFault(f"selected run manifest invalid: {exc}") from exc
        if (selected["run_id"] != run_id or selected["generation"] != manifest["runGeneration"]
                or selected["manifest_identity"] != manifest_identity(manifest)):
            raise ProjectionFault("run manifest differs from selected registry identity")
        if selected["status"] == "READY":
            raise RetryLater("selected run has not started")
        if selected["status"] not in ("RUNNING", "COMPLETE", "INCOMPLETE", "STOPPED"):
            raise ProjectionFault(f"selected run has invalid status {selected['status']}")
        try:
            catalog = resolve_v1_catalog(run_id, manifest["topologyDigest"], self.archives)
        except RetryLater:
            raise
        except Exception as exc:
            raise ProjectionFault(f"catalog integrity failure: {exc}") from exc
        try:
            for node in manifest["participatingIntersectionIds"]:
                if node not in catalog["node_to_tls"]:
                    raise ProjectionFault(f"participant {node} absent from archive")
            for node in catalog["node_to_tls"]:
                expected_ids(catalog, node)
                static_entities(catalog, node)
        except (KeyError, TypeError, ValueError) as exc:
            raise ProjectionFault(f"archived topology cannot project all nodes: {exc}") from exc
        return manifest, catalog

    def ensure_reconciled(self) -> None:
        selected = self.selected()
        if selected is None:
            if self.replay_run:
                raise InvalidObservation(f"unknown replay run {self.replay_run}")
            return
        generation = int(selected["generation"])
        existing = self.store.reconciliation(generation)
        if existing and existing["status"] == "READY":
            return
        if existing and existing["status"] == "FAULTED" and self.fault:
            raise ProjectionFault(self.fault)
        manifest, catalog = self._verified_package(selected["run_id"], selected)
        previous = self.store.latest_ready()
        prior_catalog = None
        if previous and previous["generation"] < generation:
            try:
                prior_manifest = self.archives.manifest(previous["run_id"])
                prior_catalog = resolve_v1_catalog(previous["run_id"], prior_manifest["topologyDigest"], self.archives)
            except RetryLater:
                raise
            except InvalidObservation as exc:
                raise ProjectionFault(f"prior run archive invalid: {exc}") from exc
            except Exception as exc:
                raise ProjectionFault(f"prior run catalog invalid: {exc}") from exc
        inventory = self.orion.inventory()
        if existing and json.loads(existing["plan"]):
            plan = json.loads(existing["plan"])
        else:
            try:
                plan = self._reconciliation_plan(catalog, prior_catalog, inventory)
            except ProjectionFault as exc:
                self.store.start_reconciliation(generation, selected["run_id"], inventory, [])
                self.store.finish_reconciliation(generation, error=str(exc))
                self.fault = str(exc)
                raise
        journal = self.store.start_reconciliation(generation, selected["run_id"], inventory, plan)
        plan = json.loads(journal["plan"])
        done = set(json.loads(journal["completed_ops"]))
        try:
            for item in plan:
                if item["kind"] != "delete_entity" or item["op"] in done:
                    continue
                current = self.orion.get(item["id"])
                if current is not None and not self._owned_obsolete(current, item["prior"]):
                    raise ProjectionFault(f"ownership changed for obsolete entity {item['id']}")
                self.orion.delete_entity(item["id"])
                self.store.mark_reconciliation_op(generation, item["op"])
            pending_static = [item for item in plan if item["kind"] == "upsert" and item["op"] not in done]
            if pending_static:
                outcome = self.orion.upsert([item["entity"] for item in pending_static])
                for eid in sorted(outcome.succeeded):
                    self.store.mark_reconciliation_op(generation, f"upsert:{eid}")
                if outcome.permanent:
                    raise ProjectionFault(f"static reconciliation rejected: {sorted(outcome.permanent)}")
                if outcome.retryable:
                    raise RetryLater(f"static reconciliation uncertain: {sorted(outcome.retryable)}")
            for item in plan:
                if item["kind"] != "delete_attribute" or item["op"] in done:
                    continue
                self.orion.delete_attribute(item["id"], item["attribute"])
                self.store.mark_reconciliation_op(generation, item["op"])
            # A concurrent activation cannot be made atomic with Orion. Keep
            # this generation unready and let the next pass reconcile the new one.
            if not self.replay_run and self.registry.selected()["generation"] != generation:
                raise RetryLater("active run changed during reconciliation")
            for item in plan:
                if item["kind"] == "upsert":
                    self.store.set_entity_attributes(item["entity"]["id"], set())
                elif item["kind"] == "delete_entity":
                    self.store.drop_entity_attributes(item["id"])
            self.store.finish_reconciliation(generation)
            self.fault = None
        except (RetryLater, RetryableOrionError):
            raise
        except Exception as exc:
            self.store.finish_reconciliation(generation, error=str(exc))
            self.fault = str(exc)
            raise

    @staticmethod
    def _owned_obsolete(actual: dict, prior: dict) -> bool:
        kind = prior["type"]
        if actual.get("id") != prior["id"] or actual.get("type") != kind:
            return False
        if not entity_is_exclusively_owned(actual, STATIC[kind], DYNAMIC[kind]):
            return False
        normalized = {local_attribute_name(key): value for key, value in actual.items()}
        identity = ("name", "refIntersection", "trafficDirection") if kind in ("TrafficLight", "VehicleSensor") else ("name", "refIntersection") if kind == "Camera" else ("name",)
        return all(normalized.get(key) == prior.get(key) for key in identity)

    def _reconciliation_plan(self, catalog: dict, prior_catalog: dict | None, inventory: dict[str, dict]) -> list[dict]:
        current_nodes = set(catalog["node_to_tls"])
        prior_nodes = set(prior_catalog["node_to_tls"]) if prior_catalog else set()
        expected = {eid for node in current_nodes for eid in expected_ids(catalog, node)}
        prior_static = {entity["id"]: entity for node in prior_nodes for entity in static_entities(prior_catalog, node)} if prior_catalog else {}
        plan: list[dict] = []
        for eid, actual in sorted(inventory.items()):
            node = eid.split(":", 3)[-1].split("-")[0].split(":")[0]
            if node not in current_nodes | prior_nodes or eid in expected:
                continue
            prior = prior_static.get(eid)
            if prior is None or not self._owned_obsolete(actual, prior):
                raise ProjectionFault(f"ownership ambiguous for obsolete entity {eid}")
            plan.append({"op": f"delete-entity:{eid}", "kind": "delete_entity", "id": eid, "prior": prior})
        for node in sorted(current_nodes):
            for entity in static_entities(catalog, node):
                entity["@context"] = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
                eid = entity["id"]
                plan.append({"op": f"upsert:{eid}", "kind": "upsert", "entity": entity})
                original = inventory.get(eid, {})
                stale = {local_attribute_name(attr) for attr in original if local_attribute_name(attr) in DYNAMIC[entity["type"]]}
                for attr in sorted(stale):
                    plan.append({"op": f"delete-attr:{eid}:{attr}", "kind": "delete_attribute", "id": eid, "attribute": attr})
        return plan

    def process(self, *, topic: str, partition: int, offset: int, key: bytes | None, value: bytes | None) -> str:
        terminal = self.store.terminal_outcome(topic, partition, offset)
        if terminal:
            return terminal
        if value is None:
            raise InvalidObservation("null Kafka value")
        try:
            event = json.loads(value)
            validate_observation(event)
            if key is None or key.decode("utf-8") != observation_key(event):
                raise InvalidObservation("Kafka key differs from observation identity")
        except (UnicodeError, ValueError, TypeError, KeyError) as exc:
            raise InvalidObservation(str(exc)) from exc
        selected = self.selected()
        if selected is None:
            raise RetryLater("no active run selected")
        if event["simulationRunId"] != selected["run_id"]:
            try:
                manifest = self.archives.manifest(event["simulationRunId"])
            except InvalidObservation as exc:
                raise RetryLater(f"cannot fence run with invalid manifest: {exc}") from exc
            if not self.replay_run and manifest["runGeneration"] >= selected["generation"]:
                raise RetryLater("unselected run is not older than current selection")
            existing = self.store.event(event["eventId"])
            if existing and existing["frozen_event"] != canonical_json(event):
                raise InvalidObservation("conflicting frozen event identity")
            if existing and existing["status"] == "APPLIED":
                self.store.record_outcome(topic=topic, partition=partition, offset=offset, event_id=event["eventId"], status="FENCED")
            else:
                if existing is None:
                    self.store.begin(event, manifest["runGeneration"], [])
                self.store.finish(event, "FENCED", topic=topic, partition=partition, offset=offset)
            return "FENCED"
        self.ensure_reconciled()
        manifest, catalog = self._verified_package(event["simulationRunId"], selected)
        if event["topologyDigest"] != manifest["topologyDigest"] or event["intersectionId"] not in manifest["participatingIntersectionIds"]:
            raise InvalidObservation("observation is outside selected manifest topology or participants")
        try:
            projection = project(event, catalog)
        except (ValueError, KeyError, TypeError) as exc:
            raise InvalidObservation(f"V1 projection invalid: {exc}") from exc
        removals = [(eid, attr) for eid, attr in projection.removals if attr in self.store.entity_attributes(eid)]
        operations = [f"upsert:{entity['id']}" for entity in projection.entities]
        operations += [f"delete:{eid}:{attribute}" for eid, attribute in removals]
        existing = self.store.event(event["eventId"])
        if existing and (existing["frozen_event"] != canonical_json(event) or existing["payload_hash"] != event["payloadHash"]):
            raise InvalidObservation("conflicting event identity")
        if existing and existing["status"] == "APPLIED":
            self.store.finish(event, "APPLIED", topic=topic, partition=partition, offset=offset)
            return "APPLIED"
        if event["cycleSequence"] <= self.store.watermark(event["simulationRunId"], event["intersectionId"]):
            if existing is None:
                self.store.begin(event, manifest["runGeneration"], [])
            self.store.finish(event, "STALE", topic=topic, partition=partition, offset=offset)
            return "STALE"
        try:
            self.store.begin(event, manifest["runGeneration"], operations)
        except ValueError as exc:
            raise ProjectionFault(f"V1 ledger conflict: {exc}") from exc
        done = self.store.completed_ops(event["eventId"])
        pending = [entity for entity in projection.entities if f"upsert:{entity['id']}" not in done]
        if pending:
            if not self.replay_run and self.registry.selected()["generation"] != selected["generation"]:
                raise RetryLater("run selection changed before Orion write")
            try:
                outcome = self.orion.upsert(pending)
            except PermanentOrionError as exc:
                raise ProjectionFault(f"Orion permanently rejected observation batch: {exc}") from exc
            for eid in sorted(outcome.succeeded):
                self.store.mark_op(event["eventId"], f"upsert:{eid}")
            if outcome.permanent:
                raise ProjectionFault(f"Orion permanently rejected entities {sorted(outcome.permanent)}")
            if outcome.retryable:
                raise RetryLater(f"Orion upsert uncertain for {sorted(outcome.retryable)}")
        for eid, attr in removals:
            operation = f"delete:{eid}:{attr}"
            if operation in done:
                continue
            try:
                self.orion.delete_attribute(eid, attr)
            except PermanentOrionError as exc:
                raise ProjectionFault(f"permanent delete failure after partial projection: {exc}") from exc
            self.store.mark_op(event["eventId"], operation)
        current_attrs = {entity["id"]: set(entity) & DYNAMIC[entity["type"]] for entity in projection.entities}
        self.store.finish(event, "APPLIED", topic=topic, partition=partition, offset=offset, entity_attrs=current_attrs)
        self.fault = None
        return "APPLIED"
