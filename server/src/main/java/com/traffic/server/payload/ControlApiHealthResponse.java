package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ControlApiHealthResponse(
        @JsonProperty("simulation_run_id") String simulationRunId,
        String status
) {
}
