package com.traffic.server.analytics.dto.comparison;

import java.time.Instant;

public record TrafficComparisonDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String intersectionId,
        String direction,
        String metricCode,
        String currentWindowId,
        int currentWindowSizeSec,
        double currentWindowStartSimSec,
        double currentWindowEndSimSec,
        String previousWindowId,
        double previousWindowStartSimSec,
        double previousWindowEndSimSec,
        Double currentValue,
        Double previousValue,
        Double absoluteChange,
        Double percentChange,
        String changeDirection,
        String comparisonStatus,
        String qualityStatus,
        String qualityFlags,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
