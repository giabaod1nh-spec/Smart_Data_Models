package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

/** DTO theo intersection_model.yaml v1.1.0 + RT-DE Contract v1 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IntersectionResponse {

    private String id;
    private String type;
    private String name;
    private String description;
    private GeoPoint location;
    private Integer numberOfApproaches;
    private String intersectionStatus;
    private Boolean frequentCongestion;

    private List<String> refTrafficLights;
    private List<String> refCameras;
    private List<String> refVehicleSensors;

    private String dateObserved;
    private String overallTrafficStatus;
    private Integer totalVehicleCount;
    private Boolean hasActiveIncident;

    private Double simulationTime;
    private String simulationRunId;
    private String scenarioId;
    private String currentPhase;
    private String derivedTrafficState;
    private Boolean hasSpillback;
    private Boolean isBoxBlocked;
    private String probableCauseType;
    private String affectedBy;
    private Double causeDetectedAt;
}
