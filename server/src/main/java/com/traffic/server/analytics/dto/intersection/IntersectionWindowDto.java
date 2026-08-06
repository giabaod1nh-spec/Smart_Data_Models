package com.traffic.server.analytics.dto.intersection;

import java.time.Instant;

public record IntersectionWindowDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String intersectionId,
        String windowId,
        int windowSizeSec,
        double windowStartSimSec,
        double windowEndSimSec,
        double avgTotalVehicleCount,
        Integer maxTotalVehicleCount,
        Integer latestTotalVehicleCount,
        String latestOverallTrafficStatus,
        String latestDerivedTrafficState,
        String latestPhase,
        int incidentOccurrence,
        int spillbackOccurrence,
        int boxBlockedOccurrence,
        String qualityStatus,
        String qualityFlags,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
