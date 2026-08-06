package com.traffic.server.analytics.dto.common;

import java.time.Instant;

public record AnalyticsPageMetadata(
        String source,
        String namespace,
        String simulationRunId,
        String scenarioId,
        int windowSizeSec,
        Instant generatedAt
) {
    public static AnalyticsPageMetadata of(String runId, String scenarioId, int windowSizeSec) {
        return new AnalyticsPageMetadata(
                "GOLD_MART",
                "live",
                runId,
                scenarioId,
                windowSizeSec,
                Instant.now());
    }
}
