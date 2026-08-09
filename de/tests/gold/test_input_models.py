from dataclasses import FrozenInstanceError

import pytest

from de.gold.input_models import SilverTrafficObservationInput


def test_inputs_are_typed_and_frozen(traffic_factory):
    row = traffic_factory()
    assert isinstance(row, SilverTrafficObservationInput)
    with pytest.raises(FrozenInstanceError):
        row.vehicle_count = 99

