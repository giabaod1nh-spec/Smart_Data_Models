package com.traffic.server.analytics.service;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.comparison.TrafficComparisonDto;
import com.traffic.server.analytics.dto.direction.DirectionWindowDto;
import com.traffic.server.analytics.dto.intersection.IntersectionWindowDto;
import com.traffic.server.analytics.exception.AnalyticsNotReadyException;
import com.traffic.server.analytics.exception.AnalyticsUnavailableException;
import com.traffic.server.analytics.metrics.AnalyticsMetrics;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.query.GoldQueryAllowlist;
import com.traffic.server.analytics.repository.GoldWindowRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class IntersectionAnalyticsService extends AbstractAnalyticsService {

    private final GoldWindowRepository windowRepository;
    private final AnalyticsReadinessService readinessService;
    private final ClickHouseQueryExecutor queryExecutor;
    private final AnalyticsMetrics metrics;

    public IntersectionAnalyticsService(
            AnalyticsProperties properties,
            GoldWindowRepository windowRepository,
            AnalyticsReadinessService readinessService,
            ClickHouseQueryExecutor queryExecutor,
            AnalyticsMetrics metrics) {
        super(properties);
        this.windowRepository = windowRepository;
        this.readinessService = readinessService;
        this.queryExecutor = queryExecutor;
        this.metrics = metrics;
    }

    public AnalyticsPage<IntersectionWindowDto> intersectionWindows(
            String intersectionId,
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("intersections.windows", () -> {
            ensureAvailable();
            readinessService.requireMartReady("intersection_window");
            AnalyticsQueryCriteria criteria = validateAndBuild(
                    simulationRunId, scenarioId, windowSizeSec,
                    fromSimulationSec, toSimulationSec, page, pageSize, sort,
                    Optional.of(intersectionId), Optional.empty(), Optional.empty());
            var rows = windowRepository.findIntersectionWindows(criteria, properties.definition());
            return toPage(rows, criteria);
        });
    }

    public AnalyticsPage<DirectionWindowDto> directionWindows(
            String intersectionId,
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            String direction,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("intersections.directions.windows", () -> {
            ensureAvailable();
            readinessService.requireMartReady("direction_window");
            AnalyticsQueryCriteria criteria = validateAndBuild(
                    simulationRunId, scenarioId, windowSizeSec,
                    fromSimulationSec, toSimulationSec, page, pageSize, sort,
                    Optional.of(intersectionId),
                    direction == null || direction.isBlank() ? Optional.empty() : Optional.of(direction),
                    Optional.empty());
            var rows = windowRepository.findDirectionWindows(criteria, properties.definition());
            return toPage(rows, criteria);
        });
    }

    public AnalyticsPage<TrafficComparisonDto> comparisons(
            String intersectionId,
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            String metricCode,
            Double fromSimulationSec,
            Double toSimulationSec,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("intersections.comparisons", () -> {
            ensureAvailable();
            readinessService.requireMartReady("traffic_comparison");
            if (metricCode == null || metricCode.isBlank()) {
                throw new com.traffic.server.analytics.exception.InvalidAnalyticsQueryException(
                        "INVALID_QUERY", "metricCode is required");
            }
            if (!GoldQueryAllowlist.COMPARISON_METRICS.contains(metricCode)) {
                throw new com.traffic.server.analytics.exception.InvalidAnalyticsQueryException(
                        "INVALID_QUERY", "invalid metricCode: " + metricCode);
            }
            AnalyticsQueryCriteria criteria = validateAndBuild(
                    simulationRunId, scenarioId, windowSizeSec,
                    fromSimulationSec, toSimulationSec, page, pageSize, sort,
                    Optional.of(intersectionId), Optional.empty(), Optional.of(metricCode));
            var rows = windowRepository.findComparisons(criteria, properties.definition());
            return toPage(rows, criteria);
        });
    }

    public AnalyticsPage<TrafficComparisonDto> trends(
            String intersectionId,
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            String metricCode,
            Double fromSimulationSec,
            Double toSimulationSec,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("intersections.trends", () ->
                comparisons(intersectionId, simulationRunId, scenarioId, windowSizeSec, metricCode,
                        fromSimulationSec, toSimulationSec, page, pageSize, sort));
    }

    private void ensureAvailable() {
        if (!properties.enabled()) {
            throw new AnalyticsNotReadyException("Analytics feature is disabled");
        }
        if (!queryExecutor.ping()) {
            throw new AnalyticsUnavailableException("ClickHouse is unavailable");
        }
    }
}
