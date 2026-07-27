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
public class RealtimeMetadata {

    private String simulationRunId;
    private Double simulationTime;
    private String scenarioId;
    private Boolean consistent;
    private List<String> consistencyIssues;
}
