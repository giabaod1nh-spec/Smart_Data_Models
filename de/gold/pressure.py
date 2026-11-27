"""Versioned pressure calculation over already reduced Gold metrics."""

from dataclasses import asdict, dataclass
from decimal import Decimal
from re import fullmatch

from de.silver.incremental_contract import canonical_hash


@dataclass(frozen=True)
class PressureDefinition:
    version: str
    occupancy_weight_pct: int
    moderate_threshold: float
    high_threshold: float
    empty_policy: str
    approval_reference: str
    holdout_sha256: str

    def __post_init__(self):
        if (not isinstance(self.version, str) or not fullmatch(r"[a-z0-9_]{1,64}", self.version)
                or type(self.occupancy_weight_pct) is not int or not 0 <= self.occupancy_weight_pct <= 100
                or type(self.moderate_threshold) not in (int, float) or type(self.high_threshold) not in (int, float)
                or not 0 < self.moderate_threshold < self.high_threshold <= 100
                or self.empty_policy not in ("MEASURED_ZERO", "NOT_APPLICABLE")
                or not isinstance(self.approval_reference, str) or not self.approval_reference.strip()
                or len(self.approval_reference) > 256 or not isinstance(self.holdout_sha256, str)
                or not fullmatch(r"[0-9a-f]{64}", self.holdout_sha256)):
            raise ValueError("Invalid pressure definition, approval or holdout identity")

    @property
    def digest(self):
        return canonical_hash(asdict(self))

    def severity(self, score):
        if score >= Decimal(str(self.high_threshold)):
            return "HIGH"
        if score >= Decimal(str(self.moderate_threshold)):
            return "MODERATE"
        return "LOW"


def _direction(row, definition):
    occupancy, ratio = row["occupancy_mean"], row["stopped_ratio"]
    expected = occupancy["expected"]
    # Both sets must cover the identical applicable slot population.
    joint = row["pressure_joint_count"]
    full = joint == occupancy["valid"] == ratio["valid"] == ratio["expected"] == expected
    value = None
    status = "UNAVAILABLE"
    if full and occupancy["status"] == ratio["status"] == "OK":
        weight = Decimal(definition.occupancy_weight_pct) / 100
        # Use sufficient statistics. Presentation rounding cannot drive classification.
        value = (weight * Decimal(str(row["occupancy_sum"])) / expected
                 + (1 - weight) * 100 * row["stopped_count"] / row["ratio_vehicle_count"])
    elif full and occupancy["status"] == "OK" and ratio["status"] == "NOT_APPLICABLE":
        if row["ratio_vehicle_count"] == row["load_vehicle_count"] == row["occupancy_sum"] == 0:
            if definition.empty_policy == "MEASURED_ZERO":
                value = Decimal(0)
            else:
                status = "NOT_APPLICABLE"
    return value, expected, joint, status


def _score(value, expected, valid, definition, status="UNAVAILABLE"):
    return dict(value=float(value) if value is not None else None, expected=expected, valid=valid,
                status="OK" if value is not None else status,
                severity=definition.severity(value) if value is not None else None)


def add_pressure(rows, definition):
    """Compute direction scores, strict intersection maxima and a full-population high count."""
    approaches, scores = {}, {}
    for row in rows:
        if row["grain"] != "direction":
            continue
        value, expected, joint, status = _direction(row, definition)
        row["pressure_score"] = _score(value, expected, joint, definition, status)
        approaches.setdefault(row["intersection"], []).append((row["direction"], value))
    identity = definition.digest
    for row in rows:
        del row["pressure_joint_count"]
        if row["grain"] != "intersection":
            continue
        node = row["intersection"]
        members = approaches[node]
        valid = [(direction, score) for direction, score in members if score is not None]
        direction, score = max(sorted(valid), key=lambda item: item[1]) if len(valid) == len(members) else (None, None)
        row["pressure_score"] = _score(score, len(members), len(valid), definition)
        row["worst_pressure_direction"] = [node, direction] if direction is not None else None
        scores[node] = score
    high = sorted(node for node, score in scores.items() if score is not None and definition.severity(score) == "HIGH")
    valid = sum(score is not None for score in scores.values())
    network, = (row for row in rows if row["grain"] == "network")
    network["high_pressure_count"] = dict(value=len(high) if valid == len(scores) else None,
        expected=len(scores), valid=valid, high_intersections=high if valid == len(scores) else None,
        status="OK" if valid == len(scores) else "UNAVAILABLE")
    for row in rows:
        row["pressure_definition_version"] = definition.version
        row["pressure_definition_digest"] = identity
