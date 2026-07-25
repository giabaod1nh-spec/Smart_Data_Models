package com.traffic.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "orion")
public record OrionProperties(
        String apiBaseUrl,
        String healthUrl,
        String contextUrl,
        int timeoutMs
) {}
