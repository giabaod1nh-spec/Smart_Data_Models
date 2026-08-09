package com.traffic.server.analytics.query;

import java.util.Optional;

/** Canonical analytics query parameters (Section 24/38). */
public record AnalyticsQueryCriteria(
        String simulationRunId,
        String scenarioId,
        int windowSizeSec,
        Optional<Double> fromSimulationSec,
        Optional<Double> toSimulationSec,
        Optional<String> intersectionId,
        Optional<String> direction,
        Optional<String> metricCode,
        int page,
        int pageSize,
        Optional<String> sort
) {
    public static final int DEFAULT_PAGE_SIZE = 50;
    public static final int MAX_PAGE_SIZE = 200;

    public int offset() {
        return page * pageSize;
    }

    public int limitPlusOne() {
        return pageSize + 1;
    }
}
