package com.traffic.server.analytics.config;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("live")
class ClickHouseConnectivityProbeTest {

    @Test
    void selectOneAgainstLocalClickHouse() {
        AnalyticsProperties props = new AnalyticsProperties(
                true,
                false,
                86400L,
                new AnalyticsProperties.ClickHouse("localhost", 8123, "smart_traffic", "default", "", 5000, 15000, true),
                new AnalyticsProperties.Definition("v1.0", 1, 0));
        ClickHouseQueryExecutor executor = new ClickHouseQueryExecutor(props);
        assertTrue(executor.ping(), "ClickHouse SELECT 1 should succeed on localhost:8123");
    }
}
