package com.traffic.server.analytics.service;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.common.AnalyticsPageMetadata;
import com.traffic.server.analytics.exception.InvalidAnalyticsQueryException;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.query.AnalyticsSortField;
import com.traffic.server.analytics.query.GoldQueryAllowlist;

import java.util.List;
import java.util.Optional;

/** Shared validation and pagination helpers (BS-5). */
public abstract class AbstractAnalyticsService {

    protected final AnalyticsProperties properties;

    protected AbstractAnalyticsService(AnalyticsProperties properties) {
        this.properties = properties;
    }

    protected AnalyticsQueryCriteria validateAndBuild(
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            Integer page,
            Integer pageSize,
            String sort,
            Optional<String> intersectionId,
            Optional<String> direction,
            Optional<String> metricCode) {
        if (simulationRunId == null || simulationRunId.isBlank()) {
            throw new InvalidAnalyticsQueryException("INVALID_QUERY", "simulationRunId is required");
        }
        if (simulationRunId.length() > 128) {
            throw new InvalidAnalyticsQueryException("INVALID_QUERY", "simulationRunId exceeds max length");
        }
        if (scenarioId == null || scenarioId.isBlank()) {
            throw new InvalidAnalyticsQueryException("INVALID_QUERY", "scenarioId is required");
        }
        if (scenarioId.length() > 128) {
            throw new InvalidAnalyticsQueryException("INVALID_QUERY", "scenarioId exceeds max length");
        }
        if (windowSizeSec == null || !GoldQueryAllowlist.WINDOW_SIZES.contains(windowSizeSec)) {
            throw new InvalidAnalyticsQueryException("INVALID_WINDOW_SIZE", "windowSizeSec must be 60 or 300");
        }
        int resolvedPage = page == null ? 0 : page;
        if (resolvedPage < 0) {
            throw new InvalidAnalyticsQueryException("INVALID_QUERY", "page must be >= 0");
        }
        int resolvedPageSize = pageSize == null ? AnalyticsQueryCriteria.DEFAULT_PAGE_SIZE : pageSize;
        if (resolvedPageSize < 1 || resolvedPageSize > AnalyticsQueryCriteria.MAX_PAGE_SIZE) {
            throw new InvalidAnalyticsQueryException("INVALID_QUERY", "pageSize must be between 1 and 200");
        }
        if (sort != null && !sort.isBlank()) {
            try {
                AnalyticsSortField.parse(sort);
            } catch (IllegalArgumentException e) {
                throw new InvalidAnalyticsQueryException("INVALID_QUERY", "unknown sort: " + sort);
            }
        }
        direction.ifPresent(d -> {
            if (!GoldQueryAllowlist.DIRECTIONS.contains(d)) {
                throw new InvalidAnalyticsQueryException("INVALID_QUERY", "invalid direction: " + d);
            }
        });
        metricCode.ifPresent(m -> {
            if (!GoldQueryAllowlist.COMPARISON_METRICS.contains(m)
                    && !GoldQueryAllowlist.METRIC_CONGESTION.equals(m)) {
                throw new InvalidAnalyticsQueryException("INVALID_QUERY", "invalid metricCode: " + m);
            }
        });

        Optional<Double> from = validateBound(fromSimulationSec, "fromSimulationSec");
        Optional<Double> to = validateBound(toSimulationSec, "toSimulationSec");
        if (from.isPresent() && from.get() < 0) {
            throw new InvalidAnalyticsQueryException("INVALID_TIME_RANGE", "fromSimulationSec must be >= 0");
        }
        if (to.isPresent() && to.get() <= 0) {
            throw new InvalidAnalyticsQueryException("INVALID_TIME_RANGE", "toSimulationSec must be > 0");
        }
        if (from.isPresent() && to.isPresent() && to.get() <= from.get()) {
            throw new InvalidAnalyticsQueryException("INVALID_TIME_RANGE", "toSimulationSec must be > fromSimulationSec");
        }
        if (from.isPresent() && to.isPresent()) {
            double range = to.get() - from.get();
            if (range > properties.maxSimulationRangeSec()) {
                throw new InvalidAnalyticsQueryException(
                        "INVALID_TIME_RANGE",
                        "simulation time range exceeds maxSimulationRangeSec="
                                + properties.maxSimulationRangeSec());
            }
        }

        return new AnalyticsQueryCriteria(
                simulationRunId.trim(),
                scenarioId.trim(),
                windowSizeSec,
                from,
                to,
                intersectionId,
                direction,
                metricCode,
                resolvedPage,
                resolvedPageSize,
                Optional.ofNullable(sort == null || sort.isBlank() ? null : sort));
    }

    protected <T> AnalyticsPage<T> toPage(List<T> rows, AnalyticsQueryCriteria criteria) {
        boolean hasNext = rows.size() > criteria.pageSize();
        List<T> items = hasNext ? rows.subList(0, criteria.pageSize()) : rows;
        return new AnalyticsPage<>(
                items,
                criteria.page(),
                criteria.pageSize(),
                hasNext,
                AnalyticsPageMetadata.of(
                        criteria.simulationRunId(),
                        criteria.scenarioId(),
                        criteria.windowSizeSec()));
    }

    private static Optional<Double> validateBound(Double value, String name) {
        if (value == null) {
            return Optional.empty();
        }
        if (value.isNaN() || value.isInfinite()) {
            throw new InvalidAnalyticsQueryException("INVALID_TIME_RANGE", name + " must be finite");
        }
        return Optional.of(value);
    }
}
