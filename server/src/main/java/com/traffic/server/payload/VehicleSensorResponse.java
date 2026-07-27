package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.Map;

/** DTO theo vehiclesensor_model.yaml v1.0.0 + RT-DE Contract v1 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VehicleSensorResponse {

    private String id;
    private String type;
    private String name;
    private String description;
    private GeoPoint location;

    private String sensorType;
    private String sensorStatus;
    private String trafficDirection;

    private String refIntersection;
    private String refCamera;
    private String refTrafficLight;

    private String dateObserved;
    private Integer vehicleCount;
    private Double pcuEquivalent;
    private Map<String, Double> vehicleClassComposition;
    private Integer leftTurnCount;
    private Integer straightCount;
    private Integer rightTurnCount;
    private Double averageSpeed;
    private Integer waitingVehicleCount;
    private Double queueLength;
    private Double queueStraight;
    private Double queueLeft;
    private Double queueRight;
    private Double occupancyRate;
    private String trafficStatus;
    private Double arrivalRatePcuPerSec;
    private Map<String, Integer> waitingReasonCounts;
    private String dominantWaitingReason;
    private Double theoreticalSpeed;

    private Double simulationTime;
    private String simulationRunId;
    private String scenarioId;
    private String derivedTrafficState;
    private Boolean spillbackRisk;
    private Map<String, Object> operationalState;
}
