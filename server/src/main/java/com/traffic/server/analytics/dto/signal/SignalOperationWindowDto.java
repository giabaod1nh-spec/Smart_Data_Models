package com.traffic.server.analytics.dto.signal;

import java.time.Instant;

public record SignalOperationWindowDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String intersectionId,
        String direction,
        String windowId,
        int windowSizeSec,
        double windowStartSimSec,
        double windowEndSimSec,
        long observationCount,
        long greenObservationCount,
        long redObservationCount,
        long yellowObservationCount,
        long otherStatusCount,
        Double greenSharePct,
        Double redSharePct,
        Double yellowSharePct,
        String dominantSignalStatus,
        String dominantPhase,
        String latestTimingMode,
        Double avgConfiguredGreenDurationSec,
        String qualityStatus,
        String qualityFlags,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
