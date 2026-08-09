package com.traffic.server.analytics.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Component
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class AnalyticsMetrics {

    private final MeterRegistry registry;

    public AnalyticsMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public <T> T record(String endpoint, Supplier<T> action) {
        long start = System.nanoTime();
        try {
            T result = action.get();
            success(endpoint, System.nanoTime() - start, rowCount(result));
            return result;
        } catch (RuntimeException e) {
            error(endpoint, errorCode(e), System.nanoTime() - start);
            throw e;
        }
    }

    public void setClickHouseAvailable(boolean up) {
        registry.gauge("analytics_clickhouse_available", up ? 1.0 : 0.0);
    }

    private void success(String endpoint, long nanos, int rows) {
        registry.counter("analytics_queries_total", "endpoint", endpoint, "result", "success").increment();
        registry.timer("analytics_query_latency_ms", "endpoint", endpoint)
                .record(nanos, TimeUnit.NANOSECONDS);
        registry.counter("analytics_rows_returned_total", "endpoint", endpoint).increment(rows);
    }

    private void error(String endpoint, String code, long nanos) {
        registry.counter("analytics_queries_total", "endpoint", endpoint, "result", "error").increment();
        registry.counter("analytics_query_errors_total", "endpoint", endpoint, "error_code", code).increment();
        registry.timer("analytics_query_latency_ms", "endpoint", endpoint)
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    private static String errorCode(RuntimeException e) {
        return switch (e) {
            case com.traffic.server.analytics.exception.InvalidAnalyticsQueryException iq -> iq.errorCode();
            case com.traffic.server.analytics.exception.AnalyticsNotReadyException ignored -> "ANALYTICS_NOT_READY";
            case com.traffic.server.analytics.exception.AnalyticsUnavailableException ignored -> "ANALYTICS_UNAVAILABLE";
            case com.traffic.server.analytics.exception.AnalyticsQueryTimeoutException ignored -> "QUERY_TIMEOUT";
            default -> "INTERNAL";
        };
    }

    private static int rowCount(Object page) {
        if (page instanceof com.traffic.server.analytics.dto.common.AnalyticsPage<?> p) {
            return p.items().size();
        }
        return 0;
    }
}
