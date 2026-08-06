package com.traffic.server.analytics.service;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.dto.common.AnalyticsPage;
import com.traffic.server.analytics.dto.signal.SignalOperationWindowDto;
import com.traffic.server.analytics.exception.AnalyticsNotReadyException;
import com.traffic.server.analytics.exception.AnalyticsUnavailableException;
import com.traffic.server.analytics.metrics.AnalyticsMetrics;
import com.traffic.server.analytics.query.AnalyticsQueryCriteria;
import com.traffic.server.analytics.repository.GoldSignalRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class SignalAnalyticsService extends AbstractAnalyticsService {

    private final GoldSignalRepository signalRepository;
    private final AnalyticsReadinessService readinessService;
    private final ClickHouseQueryExecutor queryExecutor;
    private final AnalyticsMetrics metrics;

    public SignalAnalyticsService(
            AnalyticsProperties properties,
            GoldSignalRepository signalRepository,
            AnalyticsReadinessService readinessService,
            ClickHouseQueryExecutor queryExecutor,
            AnalyticsMetrics metrics) {
        super(properties);
        this.signalRepository = signalRepository;
        this.readinessService = readinessService;
        this.queryExecutor = queryExecutor;
        this.metrics = metrics;
    }

    public AnalyticsPage<SignalOperationWindowDto> operationWindows(
            String simulationRunId,
            String scenarioId,
            Integer windowSizeSec,
            Double fromSimulationSec,
            Double toSimulationSec,
            String intersectionId,
            String direction,
            Integer page,
            Integer pageSize,
            String sort) {
        return metrics.record("signals.operation-windows", () -> {
            ensureAvailable();
            readinessService.requireMartReady("signal_operation");
            AnalyticsQueryCriteria criteria = validateAndBuild(
                    simulationRunId, scenarioId, windowSizeSec,
                    fromSimulationSec, toSimulationSec, page, pageSize, sort,
                    optionalNonBlank(intersectionId),
                    direction == null || direction.isBlank() ? Optional.empty() : Optional.of(direction),
                    Optional.empty());
            var rows = signalRepository.findSignalWindows(criteria, properties.definition());
            return toPage(rows, criteria);
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
