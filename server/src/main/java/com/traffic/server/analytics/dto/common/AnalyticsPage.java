package com.traffic.server.analytics.dto.common;

import java.time.Instant;
import java.util.List;

public record AnalyticsPage<T>(
        List<T> items,
        int page,
        int pageSize,
        boolean hasNext,
        AnalyticsPageMetadata metadata
) {}
