"""Plan 1/2 regression marker test."""
from __future__ import annotations

from de.silver.engine import TransformationEngine
from de.tests.silver.conftest import make_entity


def test_plan2_engine_still_pure():
    engine = TransformationEngine()
    r = engine.transform(
        make_entity(entity_type="VehicleSensor", payload_name="VehicleSensor.example.jsonld")
    )
    assert r.proposed_disposition in {"PROCESSED", "QUARANTINED"}
    if r.facts:
        assert r.facts[0].processed_at is None
