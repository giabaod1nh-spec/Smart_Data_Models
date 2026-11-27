"""Isolated use of the existing Adaptive inference controller."""

import json
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from xml.etree import ElementTree


def validate_offered_demand(path):
    """Require a materialized departure schedule shared by both treatment arms."""
    if path.stat().st_size > 16 * 1024**2:
        raise ValueError("Experiment demand file exceeds its size cap")
    raw = path.read_bytes()
    root = ElementTree.fromstring(raw)
    departures = {}
    if root.tag != "routes" or any(element.tag not in ("vehicle", "vType", "route") for element in root):
        raise ValueError("Experiment demand requires explicit vehicles and routes")
    for vehicle in root.findall("vehicle"):
        name = vehicle.attrib["id"]
        depart = float(vehicle.attrib["depart"])
        if name in departures or not 0 <= depart < 3600 or ("route" not in vehicle.attrib and vehicle.find("route") is None):
            raise ValueError("Experiment vehicle lacks a unique timed departure or route")
        for field in ("departLane", "departPos", "departSpeed"):
            if vehicle.attrib.get(field, "") in ("random", "random_free", "free"):
                raise ValueError("Experiment vehicle uses a nondeterministic departure rule")
        departures[name] = depart
    if not departures:
        raise ValueError("Experiment demand has no explicit departures")
    return raw, sha256(raw).hexdigest()


def adaptive_spec(models, participants):
    from AI_Control_Traffic_Light.config.settings import load_rl_config

    config = load_rl_config()
    interval = round(config.decision_interval_sec * 1_000_000)
    if set(config.agent_nodes) != set(participants) or interval < 10_000 or interval % 10_000:
        raise ValueError("Existing Adaptive configuration differs from the experiment participants")
    files = {node: models / f"agent_{node}.keras" for node in participants}
    missing = [node for node, path in files.items() if not path.is_file()]
    if missing:
        raise ValueError("Missing qualified Adaptive model checkpoints for " + ", ".join(missing))
    identities = {node: sha256(path.read_bytes()).hexdigest() for node, path in files.items()}
    content = json.dumps(dict(config=asdict(config), models=identities), sort_keys=True).encode()
    controller = Path(__file__).resolve().parents[2] / "AI_Control_Traffic_Light"
    files = sorted(controller.rglob("*.py"))
    build = sha256(b"".join(path.relative_to(controller).as_posix().encode() + path.read_bytes()
                            for path in files if "tests" not in path.relative_to(controller).parts)).hexdigest()
    return config, build, sha256(content).hexdigest()


class AdaptiveTreatment:
    """Drive protected inference through its existing agent manager and signal actions."""

    def __init__(self, config, models, traci, configured):
        from AI_Control_Traffic_Light.runtime.multi_agent_controller import MultiAgentController

        controller = MultiAgentController(config)
        controller.manager.load_models(models)
        self.manager = controller.manager
        self.action_executor = controller.action_exec
        self.signals = {node: item[2] for node, item in configured.items()}
        self._traci = traci
        self.configured = configured
        self.interval = round(config.decision_interval_sec * 1_000_000)
        self.next_due = self.interval
        self.decisions = []
        self.action_executor.enter_adaptive_mode(self.signals, traci)

    def get_snapshot_fresh(self, node):
        provider, _, signal, scenario = self.configured[node]
        return provider.build_snapshot(self._traci, signal, scenario)

    def step(self, tick):
        for controller in self.signals.values():
            controller.tick_pending(self._traci)
        if tick < self.next_due:
            return
        self.manager.decision_step(self, train=False, greedy=True)
        self.decisions.append([tick, dict(self.manager._prev_actions)])
        self.next_due += self.interval

    def finish(self):
        if not self.decisions:
            raise ValueError("Adaptive capture completed without a controller decision")
        return self.decisions
