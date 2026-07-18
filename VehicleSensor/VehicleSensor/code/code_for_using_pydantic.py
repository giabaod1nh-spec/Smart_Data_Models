"""
Pydantic model for VehicleSensor — matching vehiclesensor_model.yaml v1.0.0.
"""
from __future__ import annotations

from enum import Enum
from typing import Dict, Optional

from pydantic import BaseModel, Field


class EntityType(str, Enum):
    VehicleSensor = "VehicleSensor"


class SensorType(str, Enum):
    VIRTUAL = "VIRTUAL"
    INDUCTIVE_LOOP = "INDUCTIVE_LOOP"
    CAMERA_DERIVED = "CAMERA_DERIVED"
    RADAR = "RADAR"
    MAGNETOMETER = "MAGNETOMETER"


class SensorStatus(str, Enum):
    ok = "ok"
    error = "error"
    maintenance = "maintenance"
    offline = "offline"


class TrafficDirection(str, Enum):
    NORTHBOUND = "NORTHBOUND"
    SOUTHBOUND = "SOUTHBOUND"
    EASTBOUND = "EASTBOUND"
    WESTBOUND = "WESTBOUND"


class TrafficStatus(str, Enum):
    FREE_FLOW = "FREE_FLOW"
    LIGHT = "LIGHT"
    MODERATE = "MODERATE"
    HEAVY = "HEAVY"
    CONGESTED = "CONGESTED"


class DominantWaitingReason(str, Enum):
    RED_PHASE = "RED_PHASE"
    CONGESTION = "CONGESTION"
    INCIDENT = "INCIDENT"


class GeoPoint(BaseModel):
    type: str = Field("Point", pattern="^Point$")
    coordinates: list[float] = Field(..., min_length=2)


class VehicleClassComposition(BaseModel):
    motorcycle: float = Field(0.0, ge=0.0, le=1.0)
    car: float = Field(0.0, ge=0.0, le=1.0)
    bus: float = Field(0.0, ge=0.0, le=1.0)
    truck: float = Field(0.0, ge=0.0, le=1.0)


class VehicleSensor(BaseModel):
    id: str
    type: EntityType = EntityType.VehicleSensor
    name: Optional[str] = None
    description: Optional[str] = None
    dateCreated: Optional[str] = None
    dateModified: Optional[str] = None
    location: Optional[GeoPoint] = None
    sensorType: Optional[SensorType] = None
    sensorStatus: SensorStatus
    trafficDirection: TrafficDirection
    refIntersection: str
    refCamera: Optional[str] = None
    refTrafficLight: Optional[str] = None
    dateObserved: str
    vehicleCount: int = Field(..., ge=0)
    pcuEquivalent: float = Field(..., ge=0)
    vehicleClassComposition: Optional[VehicleClassComposition] = None
    leftTurnCount: Optional[int] = Field(None, ge=0)
    straightCount: Optional[int] = Field(None, ge=0)
    rightTurnCount: Optional[int] = Field(None, ge=0)
    averageSpeed: Optional[float] = Field(None, ge=0)
    waitingVehicleCount: Optional[int] = Field(None, ge=0)
    queueLength: Optional[float] = Field(None, ge=0)
    queueStraight: Optional[float] = Field(None, ge=0)
    queueLeft: Optional[float] = Field(None, ge=0)
    queueRight: Optional[float] = Field(None, ge=0)
    occupancyRate: Optional[float] = Field(None, ge=0, le=100)
    trafficStatus: Optional[TrafficStatus] = None
    arrivalRatePcuPerSec: Optional[float] = Field(None, ge=0)
    waitingReasonCounts: Optional[Dict[str, int]] = None
    dominantWaitingReason: Optional[DominantWaitingReason] = None
    theoreticalSpeed: Optional[float] = Field(None, ge=0)


if __name__ == "__main__":
    sample = VehicleSensor(
        id="urn:ngsi-ld:VehicleSensor:Intersection001:NORTHBOUND",
        sensorStatus=SensorStatus.ok,
        trafficDirection=TrafficDirection.NORTHBOUND,
        refIntersection="urn:ngsi-ld:Intersection:Intersection001",
        dateObserved="2026-07-18T05:00:00.000Z",
        vehicleCount=42,
        pcuEquivalent=18.56,
        occupancyRate=35.2,
        trafficStatus=TrafficStatus.MODERATE,
    )
    print(sample.model_dump_json(indent=2))
