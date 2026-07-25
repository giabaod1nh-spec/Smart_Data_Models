package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

/** DTO theo camera_model.yaml v2.0.0 + RT-DE Contract v1 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CameraResponse {

    private String id;
    private String type;
    private String name;
    private String description;
    private GeoPoint location;

    private Integer cameraNum;
    private String cameraType;
    private String cameraUsage;
    private Double orientationAngle;
    private String streamURL;
    private String cameraStatus;

    private String refIntersection;
    private String refTrafficLight;
    private List<String> refVehicleSensor;

    private String dateObserved;
    private String trafficDirection;
    private List<String> monitoredLane;
    private Integer vehicleCount;
    private Double averageSpeed;
    private Double occupancyRate;
    private String trafficStatus;
    private Double confidence;
    private Boolean incidentDetected;

    private Double simulationTime;
    private String simulationRunId;
    private String scenarioId;
    private String recommendedSignalAction;
    private String incidentType;
    private String incidentSeverity;
}
