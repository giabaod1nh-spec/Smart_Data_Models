package com.traffic.server.analytics.dto.congestion;

import java.time.Instant;

public record CongestionWindowDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String intersectionId,
        String direction,
        String windowId,
        int windowSizeSec,
        double windowStartSimSec,
        double windowEndSimSec,
        String metricCode,
        String metricVersion,
        Double numericValue,
        String unitCode,
        String status,
        String explanationJson,
        String qualityStatus,
        String qualityFlags,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
