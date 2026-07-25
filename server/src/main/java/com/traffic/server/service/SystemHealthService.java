package com.traffic.server.service;

import com.traffic.server.config.AppProperties;
import com.traffic.server.config.OrionProperties;
import com.traffic.server.payload.SystemHealthDetailsResponse;
import com.traffic.server.payload.SystemHealthResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class SystemHealthService {

    private final WebClient healthCheckWebClient;
    private final OrionProperties orionProperties;
    private final AppProperties appProperties;

    public SystemHealthService(WebClient healthCheckWebClient,
                               OrionProperties orionProperties,
                               AppProperties appProperties) {
        this.healthCheckWebClient = healthCheckWebClient;
        this.orionProperties = orionProperties;
        this.appProperties = appProperties;
    }

    public SystemHealthResponse publicHealth() {
        return SystemHealthResponse.builder()
                .server("UP")
                .orion(checkUrl(orionProperties.healthUrl()))
                .contextProvider(checkUrl(appProperties.contextProvider().healthUrl()))
                .controlApi(checkUrl(appProperties.controlApi().baseUrl() + "/health"))
                .build();
    }

    public SystemHealthDetailsResponse adminDetails() {
        SystemHealthResponse status = publicHealth();
        return SystemHealthDetailsResponse.builder()
                .status(status)
                .orionHealthUrl(orionProperties.healthUrl())
                .orionApiBaseUrl(orionProperties.apiBaseUrl())
                .contextProviderHealthUrl(appProperties.contextProvider().healthUrl())
                .controlApiBaseUrl(appProperties.controlApi().baseUrl())
                .build();
    }

    private String checkUrl(String url) {
        try {
            Integer code = healthCheckWebClient.get()
                    .uri(url)
                    .exchangeToMono(response -> reactor.core.publisher.Mono.just(response.statusCode().value()))
                    .block();
            if (code != null && code >= 200 && code < 400) {
                return "UP";
            }
            return "DOWN";
        } catch (Exception e) {
            return "DOWN";
        }
    }
}
