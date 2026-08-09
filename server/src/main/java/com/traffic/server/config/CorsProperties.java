package com.traffic.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Cross-origin settings for browser Dashboard clients calling the Spring API with session cookies.
 * Disabled by default; enable explicitly per deployment profile (e.g. {@code local}).
 */
@ConfigurationProperties(prefix = "app.cors")
public record CorsProperties(
        boolean enabled,
        List<String> allowedOrigins,
        List<String> allowedMethods,
        List<String> allowedHeaders,
        List<String> exposedHeaders,
        boolean allowCredentials,
        long maxAgeSec
) {
    public CorsProperties {
        if (allowedMethods == null || allowedMethods.isEmpty()) {
            allowedMethods = List.of("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS");
        }
        if (allowedHeaders == null || allowedHeaders.isEmpty()) {
            allowedHeaders = List.of("*");
        }
        if (exposedHeaders == null) {
            exposedHeaders = List.of();
        }
        if (allowedOrigins == null) {
            allowedOrigins = List.of();
        }
    }
}
