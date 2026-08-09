package com.traffic.server.analytics.health;

import com.traffic.server.analytics.config.ClickHouseQueryExecutor;
import com.traffic.server.analytics.metrics.AnalyticsMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.analytics.enabled", havingValue = "true")
public class AnalyticsHealthIndicator {

    private final ClickHouseQueryExecutor executor;
    private final AnalyticsMetrics metrics;

    public AnalyticsHealthIndicator(ClickHouseQueryExecutor executor, AnalyticsMetrics metrics) {
        this.executor = executor;
        this.metrics = metrics;
    }

    public Map<String, Object> status() {
        boolean up = executor.ping();
        metrics.setClickHouseAvailable(up);
        return Map.of(
                "analytics", up ? "UP" : "DOWN",
                "clickHouse", up ? "UP" : "DOWN");
    }
}
