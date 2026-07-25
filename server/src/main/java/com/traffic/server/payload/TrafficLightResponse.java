package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

/** DTO theo trafficlight_model.yaml v1.0.0 + RT-DE Contract v1 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TrafficLightResponse {

    private String id;
    private String type;
    private String name;
    private String description;
    private GeoPoint location;

    private String currentStatus;
    private String phaseStartedAt;
    private String timingMode;
    private String workingState;
    private String trafficDirection;

    private Integer greenDurationCurrent;
    private Integer redDurationCurrent;
    private Integer yellowDuration;

    private String refIntersection;
    private String refCamera;

    private Double simulationTime;
    private String simulationRunId;
    private String scenarioId;
    private String currentPhase;
}
