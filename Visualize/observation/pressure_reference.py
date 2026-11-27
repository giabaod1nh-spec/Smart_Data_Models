"""Independent vehicle trajectory observations for G-CAL in isolated SUMO."""

from collections import defaultdict


class PressureReference:
    """Track sampled phase transitions and residual vehicles through a complete green."""

    def __init__(self, scopes):
        self.scopes = scopes
        self.states = {}
        self.records = defaultdict(list)

    def observe(self, traci, tick, colors):
        for (node, direction), lanes in self.scopes.items():
            key = node, direction
            green = colors[node].get(direction) == "green"
            present = {vehicle for lane in lanes for vehicle in traci.lane.getLastStepVehicleIDs(lane)}
            stopped = {vehicle for vehicle in present if traci.vehicle.getSpeed(vehicle) < 0.1}
            previous = self.states.get(key)
            if previous is None:
                self.states[key] = (green, stopped if not green else set(), None)
                continue
            was_green, red_queue, entering = previous
            if green and not was_green:
                entering = (tick, red_queue & present)
            if not green and was_green:
                if entering and entering[1]:
                    started, vehicles = entering
                    residual = vehicles & stopped
                    self.records[key].append(dict(started_tick=started, ended_tick=tick, entering=len(vehicles),
                        residual=len(residual), exited_scope=len(vehicles - present), label=(
                            "BOTTLENECK" if residual else ("NORMAL_RED" if not vehicles & present else "UNKNOWN"))))
                entering = None
                red_queue = set()
            if not green:
                red_queue |= stopped
            self.states[key] = (green, red_queue, entering)

    def document(self):
        return [{"intersection": node, "direction": direction, "observations": values}
                for (node, direction), values in sorted(self.records.items())]
