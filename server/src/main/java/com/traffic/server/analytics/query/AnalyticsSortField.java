package com.traffic.server.analytics.query;

/** Allowlisted sort tokens per endpoint (Section 38). */
public enum AnalyticsSortField {
    DEFAULT,
    INTERSECTION_ASC,
    WINDOW_ASC,
    REVISION_DESC;

    public static AnalyticsSortField parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT;
        }
        return switch (raw.trim().toUpperCase()) {
            case "DEFAULT" -> DEFAULT;
            case "INTERSECTION_ASC" -> INTERSECTION_ASC;
            case "WINDOW_ASC" -> WINDOW_ASC;
            case "REVISION_DESC" -> REVISION_DESC;
            default -> throw new IllegalArgumentException(raw);
        };
    }
}
