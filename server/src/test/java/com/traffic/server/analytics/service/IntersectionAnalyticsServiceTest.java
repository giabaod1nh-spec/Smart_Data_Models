package com.traffic.server.analytics.service;

import com.traffic.server.analytics.config.AnalyticsProperties;
import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.exception.InvalidAnalyticsQueryException;
import com.traffic.server.analytics.metrics.AnalyticsMetrics;
import com.traffic.server.analytics.repository.GoldWindowRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IntersectionAnalyticsServiceTest {

    @Mock
    private GoldWindowRepository windowRepository;
    @Mock
    private AnalyticsReadinessService readinessService;
    @Mock
    private ClickHouseQueryExecutor queryExecutor;
    @Mock
    private AnalyticsMetrics metrics;

    private IntersectionAnalyticsService service;

    @BeforeEach
    void setUp() {
        AnalyticsProperties properties = new AnalyticsProperties(
                true,
                false,
                86400L,
                new AnalyticsProperties.ClickHouse("localhost", 8123, "smart_traffic", "default", "", 3000, 10000, true),
                new AnalyticsProperties.Definition("v1.0", 1, 0));
        service = new IntersectionAnalyticsService(
                properties, windowRepository, readinessService, queryExecutor, metrics);
        when(metrics.record(any(), any())).thenAnswer(inv -> inv.getArgument(1, java.util.function.Supplier.class).get());
        when(queryExecutor.ping()).thenReturn(true);
    }

    @Test
    void rejectsInvalidWindowSize() {
        assertThrows(InvalidAnalyticsQueryException.class, () ->
                service.intersectionWindows("int-1", "run-1", "normal", 120, null, null, 0, 50, null));
    }

    @Test
    void rejectsMissingRunId() {
        InvalidAnalyticsQueryException ex = assertThrows(InvalidAnalyticsQueryException.class, () ->
                service.intersectionWindows("int-1", " ", "normal", 60, null, null, 0, 50, null));
        assertEquals("INVALID_QUERY", ex.errorCode());
    }
}
