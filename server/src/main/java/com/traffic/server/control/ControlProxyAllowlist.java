package com.traffic.server.control;

import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;

import java.util.List;

@Component
public class ControlProxyAllowlist {

    private static final String PREFIX = "/api/control";

    private static final List<String> PATTERNS = List.of(
            "/health",
            "/scenario",
            "/demand-profile",
            "/overlays",
            "/overlays/**",
            "/network-state",
            "/intersections/**/state",
            "/links/**/state",
            "/control-mode",
            "/phase",
            "/green-duration",
            "/snapshot/**",
            "/trip-records",
            "/stats"
    );

    private final AntPathMatcher matcher = new AntPathMatcher();

    public boolean isAllowed(String upstreamPath) {
        if (upstreamPath == null || upstreamPath.isBlank()) {
            return false;
        }
        String normalized = upstreamPath.startsWith("/") ? upstreamPath : "/" + upstreamPath;
        if (normalized.contains("..") || normalized.contains("%2e") || normalized.contains("%2E")) {
            return false;
        }
        return PATTERNS.stream().anyMatch(p -> matcher.match(p, normalized));
    }

    public static String extractUpstreamPath(String requestUri) {
        if (!requestUri.startsWith(PREFIX)) {
            return null;
        }
        String remainder = requestUri.substring(PREFIX.length());
        if (remainder.isEmpty()) {
            return "/";
        }
        return remainder.startsWith("/") ? remainder : "/" + remainder;
    }
}
