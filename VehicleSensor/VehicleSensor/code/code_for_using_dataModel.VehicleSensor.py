"""
Minimal helper: build a key-values VehicleSensor dict matching vehiclesensor_model.yaml.
"""


def build_sample_vehicle_sensor() -> dict:
    return {
        "id": "urn:ngsi-ld:VehicleSensor:Intersection001:NORTHBOUND",
        "type": "VehicleSensor",
        "name": "Nguyen Hue - Le Loi North approach",
        "sensorType": "VIRTUAL",
        "sensorStatus": "ok",
        "trafficDirection": "NORTHBOUND",
        "refIntersection": "urn:ngsi-ld:Intersection:Intersection001",
        "dateObserved": "2026-07-18T05:00:00.000Z",
        "vehicleCount": 42,
        "pcuEquivalent": 18.56,
        "occupancyRate": 35.2,
        "trafficStatus": "MODERATE",
        "queueLength": 28.4,
        "averageSpeed": 22.5,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(build_sample_vehicle_sensor(), indent=2))
