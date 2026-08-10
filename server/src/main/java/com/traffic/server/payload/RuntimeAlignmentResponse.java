package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RuntimeAlignmentResponse {

    private String projectorRunId;
    private String producerRunId;
    private String orionSampleRunId;
    private Boolean aligned;
    private String reason;
}
