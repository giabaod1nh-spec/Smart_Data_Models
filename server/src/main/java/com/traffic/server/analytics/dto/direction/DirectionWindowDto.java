package com.traffic.server.analytics.dto.direction;

import java.time.Instant;

public record DirectionWindowDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String intersectionId,
        String direction,
        String sourceDirection,
        String windowId,
        int windowSizeSec,
        double windowStartSimSec,
        double windowEndSimSec,
        Double avgVehicleCount,
        Integer maxVehicleCount,
        Integer latestVehicleCount,
        Double avgPcuEquivalent,
        Double avgSpeedKmh,
        Double avgQueueLengthM,
        Double maxQueueLengthM,
        Double latestQueueLengthM,
        Double avgOccupancyPct,
        Double spillbackRatioPct,
        String latestTrafficStatus,
        String qualityStatus,
        String qualityFlags,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
