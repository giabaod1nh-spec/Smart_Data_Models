package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RealtimeIntersectionResponse {

    private IntersectionResponse intersection;
    private List<TrafficLightResponse> trafficLights;
    private List<VehicleSensorResponse> vehicleSensors;
    private List<CameraResponse> cameras;
    private RealtimeMetadata metadata;
}
