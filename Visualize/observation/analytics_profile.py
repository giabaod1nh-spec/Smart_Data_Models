"""Qualified detector and approach geometry for isolated Analytics capture."""

import json
import math
from collections import defaultdict
from hashlib import sha256
from xml.etree import ElementTree

from configuration import config as cfg
from observation.detector_manager import build_detectors_xml

_DIR = {"l": "left", "r": "right", "s": "straight", "t": "uturn"}
PROFILE_VERSION = "stage_a_detector_geometry_v2"
ROUTE_METHOD = "route_stopped_length_v2"


class AnalyticsProfile:
    def __init__(self, network, detectors):
        net = ElementTree.parse(network).getroot()
        deployed = tuple(ElementTree.parse(detectors).getroot())
        self.lanes = {lane.attrib["id"]: (edge.attrib["id"], int(lane.attrib["index"]), float(lane.attrib["length"]))
                      for edge in net.findall("edge") for lane in edge.findall("lane")}
        expected = ElementTree.fromstring(build_detectors_xml(
            {lane: item[2] for lane, item in self.lanes.items()}))

        def inventory(rows):
            values = {}
            for row in rows:
                attributes = row.attrib
                name, lane = attributes["id"], attributes["lane"]
                start = float(attributes["pos"])
                end = float(attributes.get("endPos", start))
                if (name in values or lane not in self.lanes or not 0 <= start <= end <= self.lanes[lane][2]
                        or (row.tag == "laneAreaDetector" and start == end)):
                    raise ValueError(f"Invalid Analytics detector placement for {name}")
                values[name] = (row.tag, lane, start, end, attributes["freq"])
            return values

        self.detectors = inventory(deployed)
        if self.detectors != inventory(expected):
            raise ValueError("Current detector geometry differs from the configured approach and outgoing inventory")
        self.connections = defaultdict(list)
        for row in net.findall("connection"):
            a = row.attrib
            self.connections[a["from"], a["to"], int(a["fromLane"])].append(
                (int(a["toLane"]), a.get("tl"), _DIR.get(a.get("dir"))))
        self.identity = dict(version=PROFILE_VERSION, network_sha256=sha256(network.read_bytes()).hexdigest(),
            detector_sha256=sha256(detectors.read_bytes()).hexdigest(), detector_count=len(self.detectors),
            topology_hash=cfg._TOPOLOGY_CATALOG["topology_hash"],
            scope="exact_detector_lanes_or_verified_inbound_sensing_route")
        self.digest = sha256(json.dumps(self.identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self._e2_ok = {}
        for tls, approaches in cfg.APPROACH_EDGES.items():
            for direction, terminal in approaches.items():
                terminal_lanes = cfg.edge_lane_ids(terminal)
                by_lane = defaultdict(set)
                for (edge, _, lane), rows in self.connections.items():
                    if edge == terminal:
                        by_lane[lane].update(movement for _, control, movement in rows if control == tls)
                self._e2_ok[tls, direction] = all(
                    len(by_lane[i]) == 1 and cfg.movement_for_lane(terminal, i) in by_lane[i]
                    and self.detectors[cfg.detector_id_e2(tls, direction, i)][1] == lane
                    for i, lane in enumerate(terminal_lanes))

    def e2_queue_matches_approach(self, tls, direction):
        return self._e2_ok[tls, direction]

    def stopped_movement(self, traci, vehicle, tls, direction):
        """Return a proven turn, None for an ambiguous vehicle, or outside for stale lane membership."""
        lane = traci.vehicle.getLaneID(vehicle)
        if lane not in cfg.approach_lane_ids(tls, direction):
            return "outside"
        edge, lane_index, length = self.lanes[lane]
        position = float(traci.vehicle.getLanePosition(vehicle))
        if not math.isfinite(position) or not 0 <= position <= length:
            return None
        route = traci.vehicle.getRoute(vehicle)
        index = traci.vehicle.getRouteIndex(vehicle)
        sensing = cfg.SENSING_EDGES[tls][direction]
        if traci.vehicle.getRoadID(vehicle) != edge or not 0 <= index < len(route) or route[index] != edge:
            return None
        suffix = tuple(sensing[sensing.index(edge):])
        if tuple(route[index:index + len(suffix)]) != suffix or index + len(suffix) >= len(route):
            return None
        lane_set = {lane_index}
        for a, b in zip(suffix, suffix[1:]):
            lane_set = {to for item in lane_set for to, _, movement in self.connections.get((a, b, item), ())
                        if movement == "straight"}
        outgoing = route[index + len(suffix)]
        movements = {movement for item in lane_set
                     for _, control, movement in self.connections.get((suffix[-1], outgoing, item), ()) if control == tls}
        if len(movements) != 1:
            return None
        return movements.pop()
