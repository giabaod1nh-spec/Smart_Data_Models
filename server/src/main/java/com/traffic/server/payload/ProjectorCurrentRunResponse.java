package com.traffic.server.payload;



import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.fasterxml.jackson.annotation.JsonInclude;



@JsonInclude(JsonInclude.Include.NON_NULL)

@JsonIgnoreProperties(ignoreUnknown = true)

public record ProjectorCurrentRunResponse(

        String simulationRunId,

        String scenarioId,

        Double simulationTime,

        String status,

        Integer lastAppliedCycle,

        Double freshnessSeconds,

        String updatedAt

) {}


