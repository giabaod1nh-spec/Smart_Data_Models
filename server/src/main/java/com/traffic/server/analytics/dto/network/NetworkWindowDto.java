package com.traffic.server.analytics.dto.network;

import java.time.Instant;

public record NetworkWindowDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String windowId,
        int windowSizeSec,
        double windowStartSimSec,
        double windowEndSimSec,
        Double avgTotalVehicleCount,
        String latestOverallTrafficStatus,
        String qualityStatus,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
