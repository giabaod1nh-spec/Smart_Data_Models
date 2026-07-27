package com.traffic.server.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SystemHealthDetailsResponse {

    private SystemHealthResponse status;
    private String orionHealthUrl;
    private String orionApiBaseUrl;
    private String contextProviderHealthUrl;
    private String controlApiBaseUrl;
}
