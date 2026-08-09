package com.traffic.server.analytics.service;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.congestion.CongestionWindowDto;
import com.traffic.server.analytics.dto.network.NetworkWindowDto;
import com.traffic.server.analytics.dto.priority.PriorityRankingDto;
import com.traffic.server.analytics.exception.AnalyticsNotReadyException;
import com.traffic.server.analytics.exception.AnalyticsUnavailableException;
import com.traffic.server.analytics.metrics.AnalyticsMetrics;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.repository.GoldKpiRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class KpiAnalyticsService extends AbstractAnalyticsService {

    private final GoldKpiRepository kpiRepository;
    private final AnalyticsReadinessService readinessService;
    private final ClickHouseQueryExecutor queryExecutor;
    private final AnalyticsMetrics metrics;

    public KpiAnalyticsService(
            AnalyticsProperties properties,
            GoldKpiRepository kpiRepository,
            AnalyticsReadinessService readinessService,
            ClickHouseQueryExecutor queryExecutor,
            AnalyticsMetrics metrics) {
        super(properties);
        this.kpiRepository = kpiRepository;
        this.readinessService = readinessService;
        this.queryExecutor = queryExecutor;
        this.metrics = metrics;
    }

    public AnalyticsPage<CongestionWindowDto> congestion(
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            String intersectionId,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("congestion", () -> {
            ensureAvailable();
            readinessService.requireMartReady("congestion");
            AnalyticsQueryCriteria criteria = validateAndBuild(
                    simulationRunId, scenarioId, windowSizeSec,
                    fromSimulationSec, toSimulationSec, page, pageSize, sort,
                    optionalNonBlank(intersectionId), Optional.empty(), Optional.empty());
            var rows = kpiRepository.findCongestion(criteria, properties.definition());
            return toPage(rows, criteria);
        });
    }

    public AnalyticsPage<PriorityRankingDto> priority(
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            String intersectionId,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("priority", () -> {
            ensureAvailable();
            readinessService.requireMartReady("priority");
            AnalyticsQueryCriteria criteria = validateAndBuild(
                    simulationRunId, scenarioId, windowSizeSec,
                    fromSimulationSec, toSimulationSec, page, pageSize, sort,
                    optionalNonBlank(intersectionId), Optional.empty(), Optional.empty());
            var rows = kpiRepository.findPriority(criteria, properties.definition());
            return toPage(rows, criteria);
        });
    }

    public AnalyticsPage<NetworkWindowDto> networkWindows(
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("network.windows", () -> {
            ensureAvailable();
            throw new AnalyticsNotReadyException(
                    "Network overview mart is not populated (Gold WHERE 0 scaffold)");
        });
    }

    private void ensureAvailable() {
        if (!properties.enabled()) {
            throw new AnalyticsNotReadyException("Analytics feature is disabled");
        }
        if (!queryExecutor.ping()) {
            throw new AnalyticsUnavailableException("ClickHouse is unavailable");
        }
    }

    private static Optional<String> optionalNonBlank(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
    }
}
