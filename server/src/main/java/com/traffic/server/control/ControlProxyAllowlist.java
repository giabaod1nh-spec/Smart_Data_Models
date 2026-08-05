package com.traffic.server.control;

import com.traffic.server.config.AppProperties;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;

import java.util.List;
import java.util.Set;

@Component
public class ControlProxyAllowlist {

    private static final String PREFIX = "/api/control";

    private static final List<String> READ_PATTERNS = List.of(
            "/health",
            "/network-state",
            "/intersections/**/state",
            "/links/**/state",
            "/snapshot/**",
            "/trip-records",
            "/stats",
            "/overlays"
    );

    private static final List<String> MUTATION_PATTERNS = List.of(
            "/scenario",
            "/demand-profile",
            "/overlays",
            "/overlays/**",
            "/control-mode",
            "/phase",
            "/green-duration"
    );

    private static final Set<String> MUTATION_EXACT = Set.of(
            "/scenario",
            "/demand-profile",
            "/control-mode",
            "/phase",
            "/green-duration");

    private final AntPathMatcher matcher = new AntPathMatcher();
    private final boolean adaptersRouteMutations;

    public ControlProxyAllowlist(AppProperties appProperties) {
        this.adaptersRouteMutations = appProperties.control().commandDomainEnabled()
                && appProperties.control().compatibilityAdaptersEnabled();
    }

    public boolean isAllowed(String upstreamPath) {
        if (upstreamPath == null || upstreamPath.isBlank()) {
            return false;
        }
        String normalized = upstreamPath.startsWith("/") ? upstreamPath : "/" + upstreamPath;
        if (normalized.contains("..") || normalized.contains("%2e") || normalized.contains("%2E")) {
            return false;
        }
        if (adaptersRouteMutations && isMutationPath(normalized)) {
            return false;
        }
        return matchesAny(READ_PATTERNS, normalized) || matchesAny(MUTATION_PATTERNS, normalized);
    }

    private boolean isMutationPath(String normalized) {
        if (MUTATION_EXACT.contains(normalized)) {
            return true;
        }
        return normalized.startsWith("/overlays/") && normalized.length() > "/overlays/".length();
    }

    /** Block POST/DELETE mutation proxy when legacy adapters route through command domain. */
    public boolean isMutationBlockedForProxy(HttpMethod method, String upstreamPath) {
        if (!adaptersRouteMutations || method == HttpMethod.GET) {
            return false;
        }
        String normalized = upstreamPath.startsWith("/") ? upstreamPath : "/" + upstreamPath;
        if (method == HttpMethod.POST && "/overlays".equals(normalized)) {
            return true;
        }
        return isMutationPath(normalized);
    }

    private boolean matchesAny(List<String> patterns, String normalized) {
        return patterns.stream().anyMatch(p -> matcher.match(p, normalized));
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
