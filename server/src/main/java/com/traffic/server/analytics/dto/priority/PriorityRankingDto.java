package com.traffic.server.analytics.dto.priority;

import java.time.Instant;

public record PriorityRankingDto(
        String simulationRunId,
        String scenarioId,
        String namespace,
        String intersectionId,
        String direction,
        String windowId,
        int windowSizeSec,
        double windowStartSimSec,
        double windowEndSimSec,
        Double priorityScore,
        Double priorityRank,
        String scoreStatus,
        String rankStatus,
        String explanationJson,
        String qualityStatus,
        String qualityFlags,
        String analyticalFreshnessStatus,
        long revisionSeq,
        Instant computedAt
) {}
